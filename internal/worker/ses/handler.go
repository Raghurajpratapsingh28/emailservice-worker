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

// RetryDelays and MaxAttempts kept for consumer registration compatibility.
var RetryDelays = []time.Duration{}

// MaxAttempts set high so JetStream never terminates the message — handler
// controls termination (only on StatusFailed or unrecoverable errors).
const MaxAttempts = -1 // unlimited

// PollInterval is the fixed delay between verification checks.
const PollInterval = 30 * time.Second

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

type SESChecker interface {
	CheckDomainVerification(ctx context.Context, domain string) (infraSes.VerificationStatus, error)
}

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

func (h *Handler) Handle(ctx context.Context, msg jetstream.Msg) error {
	var payload types.DomainVerifyPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed payload, terminating", zap.Error(err))
		_ = msg.Term()
		return nil
	}

	if payload.DomainID == "" || payload.WorkspaceID == "" || payload.Domain == "" {
		h.logger.Error("payload missing required fields, terminating", zap.Any("payload", payload))
		_ = msg.Term()
		return nil
	}

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
		_ = msg.NakWithDelay(PollInterval)
		return nil
	}

	log.Info("ses verification check", zap.String("status", string(status)))

	switch status {
	case infraSes.StatusVerified:
		if err := h.db.UpdateDomainVerified(ctx, payload.DomainID, payload.WorkspaceID); err != nil {
			log.Error("db update verified failed", zap.Error(err))
			_ = msg.NakWithDelay(PollInterval)
			return nil
		}
		verifySuccess.Inc()
		log.Info("domain verified")
		_ = msg.Ack()

	case infraSes.StatusPending:
		if err := h.db.UpdateDomainPending(ctx, payload.DomainID, payload.WorkspaceID, attempt); err != nil {
			log.Error("db update pending failed", zap.Error(err))
			_ = msg.NakWithDelay(PollInterval)
			return nil
		}
		verifyRetries.Inc()
		log.Info("domain still pending, retrying in 30s")
		_ = msg.NakWithDelay(PollInterval)

	case infraSes.StatusFailed:
		if err := h.db.UpdateDomainFailed(ctx, payload.DomainID, payload.WorkspaceID, attempt); err != nil {
			log.Error("db update failed", zap.Error(err))
			_ = msg.NakWithDelay(PollInterval)
			return nil
		}
		verifyFailure.Inc()
		log.Warn("domain verification failed at SES")
		_ = msg.Ack()
	}

	return nil
}
