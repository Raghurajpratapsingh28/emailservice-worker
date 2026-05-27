package ses

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	infraSes "engageiq-workers/internal/infra/ses"
	"engageiq-workers/pkg/types"
)

// RetryDelays is the production retry schedule consumed by the JetStream
// consumer's BackOff config. The server applies the i-th delay before the
// (i+1)-th delivery attempt. After MaxAttempts deliveries the message is
// terminated by the handler and the domain is marked failed.
//
// Cumulative budget: 5m + 15m + 30m + 1h + 2h + 6h + 12h + 24h ≈ 45.8h
// after 8 redelivery attempts. Ninth slot reuses 24h to push budget to ~70h
// before terminal failure, satisfying the 72h SLA.
var RetryDelays = []time.Duration{
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
	2 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
	24 * time.Hour,
}

// MaxAttempts is the total number of deliveries (initial + retries) before
// the handler gives up and marks the domain failed.
const MaxAttempts = 9

var (
	verifySuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "domain_verification_success_total",
		Help: "Total number of domains successfully verified.",
	})
	verifyFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "domain_verification_failure_total",
		Help: "Total number of domains marked as verification failed.",
	})
	verifyRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "domain_verification_retries_total",
		Help: "Total number of retry-scheduled (pending) verification checks.",
	})
	sesAPIFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ses_api_failures_total",
		Help: "Total number of SES API call failures.",
	})
)

// SESChecker abstracts SES verification checks for testability.
type SESChecker interface {
	CheckDomainVerification(ctx context.Context, domain string) (infraSes.VerificationStatus, error)
}

// DBUpdater abstracts the database mutations the handler performs.
type DBUpdater interface {
	UpdateDomainVerified(ctx context.Context, domainID, workspaceID string) error
	UpdateDomainPending(ctx context.Context, domainID, workspaceID string, attempts int) error
	UpdateDomainFailed(ctx context.Context, domainID, workspaceID string, attempts int) error
}

type Handler struct {
	ses    SESChecker
	db     DBUpdater
	logger *zap.Logger
}

func NewHandler(ses SESChecker, db DBUpdater, logger *zap.Logger) *Handler {
	return &Handler{ses: ses, db: db, logger: logger}
}

// Handle processes a domain.verify.poll message. It owns the message lifecycle
// (Ack / Nak / Term) and is safe under duplicate delivery thanks to idempotent
// SQL updates.
func (h *Handler) Handle(ctx context.Context, msg jetstream.Msg) error {
	var payload types.DomainVerifyPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed payload, terminating",
			zap.Error(err),
			zap.ByteString("data", msg.Data()),
		)
		_ = msg.Term()
		return nil
	}

	if payload.DomainID == "" || payload.WorkspaceID == "" || payload.Domain == "" {
		h.logger.Error("payload missing required fields, terminating",
			zap.Any("payload", payload),
		)
		_ = msg.Term()
		return nil
	}

	// NumDelivered starts at 1 on first delivery.
	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("domain", payload.Domain),
		zap.String("domain_id", payload.DomainID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.Int("attempt", attempt),
	)

	status, err := h.ses.CheckDomainVerification(ctx, payload.Domain)
	if err != nil {
		sesAPIFailures.Inc()
		log.Error("ses api failure", zap.Error(err))
		// Nak: server applies BackOff schedule for the next delivery.
		// On final delivery the message is dropped by the server (MaxDeliver).
		_ = msg.Nak()
		return nil
	}

	log.Info("ses verification check", zap.String("status", string(status)))

	switch status {
	case infraSes.StatusVerified:
		if err := h.db.UpdateDomainVerified(ctx, payload.DomainID, payload.WorkspaceID); err != nil {
			log.Error("db update verified failed", zap.Error(err))
			_ = msg.Nak()
			return nil
		}
		verifySuccess.Inc()
		log.Info("domain verified")
		_ = msg.Ack()

	case infraSes.StatusPending:
		if attempt >= MaxAttempts {
			if err := h.db.UpdateDomainFailed(ctx, payload.DomainID, payload.WorkspaceID, attempt); err != nil {
				log.Error("db update failed status", zap.Error(err))
				_ = msg.Nak()
				return nil
			}
			verifyFailure.Inc()
			log.Warn("max attempts reached, marking failed")
			_ = msg.Term()
			return nil
		}

		if err := h.db.UpdateDomainPending(ctx, payload.DomainID, payload.WorkspaceID, attempt); err != nil {
			log.Error("db update pending failed", zap.Error(err))
			_ = msg.Nak()
			return nil
		}
		verifyRetries.Inc()
		log.Info("scheduling retry via backoff", zap.Duration("next_delay", h.nextDelay(attempt)))
		// Nak triggers JetStream's configured BackOff to delay redelivery.
		_ = msg.Nak()

	case infraSes.StatusFailed:
		if err := h.db.UpdateDomainFailed(ctx, payload.DomainID, payload.WorkspaceID, attempt); err != nil {
			log.Error("db update failed", zap.Error(err))
			_ = msg.Nak()
			return nil
		}
		verifyFailure.Inc()
		log.Warn("domain verification failed at SES")
		_ = msg.Ack()
	}

	return nil
}

// nextDelay returns the BackOff delay that JetStream will apply before the
// next delivery, given the just-completed attempt number (1-based). Used for
// observability only; the actual delay is enforced server-side.
func (h *Handler) nextDelay(attempt int) time.Duration {
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(RetryDelays) {
		return RetryDelays[len(RetryDelays)-1]
	}
	return RetryDelays[idx]
}
