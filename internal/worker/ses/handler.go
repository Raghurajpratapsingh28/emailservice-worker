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

// RetryDelays defines the backoff schedule between SES verification polls.
// After the last entry (24h), that value repeats indefinitely — the poller
// never auto-terminates. Domains that stay unverified for too long are expired
// by DomainCleanupScheduler (default: 30 days), not by this worker.
var RetryDelays = []time.Duration{
	5 * time.Minute,
	10 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
	2 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
	24 * time.Hour,
}

// PollInterval is the fallback delay for transient errors (DB/SES unavailable).
const PollInterval = 30 * time.Second

// ReminderAttempts are the specific attempt numbers at which a "DNS still not
// found" reminder event is published to nudge the user.
// ReminderAfterAttempt is the attempt number after which a "DNS still not
// found" reminder event is published so the user gets an email nudge.
const ReminderAfterAttempt = 3

// SubjectDomainVerified and SubjectDomainReminder are the NATS subjects
// published by this worker on state changes.
const (
	SubjectDomainVerified  = "domain.verified.v1"
	SubjectDomainReminder  = "domain.verification.reminder.v1"
)

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
	GetDomainStatus(ctx context.Context, domainID, workspaceID string) (string, error)
	UpdateDomainVerified(ctx context.Context, domainID, workspaceID string) error
	UpdateDomainPending(ctx context.Context, domainID, workspaceID string, attempts int) error
	UpdateDomainFailed(ctx context.Context, domainID, workspaceID string, attempts int) error
}

type EventPublisher interface {
	Publish(ctx context.Context, subject string, payload any, msgID string) error
}

type Handler struct {
	ses       SESChecker
	db        DBUpdater
	publisher EventPublisher
	logger    *zap.Logger
}

func NewHandler(ses SESChecker, db DBUpdater, publisher EventPublisher, logger *zap.Logger) *Handler {
	return &Handler{ses: ses, db: db, publisher: publisher, logger: logger}
}

func (h *Handler) nextDelay(attempt int) time.Duration {
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(RetryDelays) {
		idx = len(RetryDelays) - 1
	}
	return RetryDelays[idx]
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

	// Guard: stop polling if the domain was deleted while this message was queued.
	dbStatus, err := h.db.GetDomainStatus(ctx, payload.DomainID, payload.WorkspaceID)
	if err != nil {
		log.Error("db status check failed", zap.Error(err))
		_ = msg.NakWithDelay(PollInterval)
		return nil
	}
	if dbStatus == "" || dbStatus == "deleting" || dbStatus == "deleted" {
		log.Info("domain deleted or not found, terminating poll", zap.String("db_status", dbStatus))
		_ = msg.Term()
		return nil
	}

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
		h.publishEvent(ctx, SubjectDomainVerified, map[string]any{
			"domainId":    payload.DomainID,
			"workspaceId": payload.WorkspaceID,
			"domain":      payload.Domain,
		})
		_ = msg.Ack()

	case infraSes.StatusPending:
		if err := h.db.UpdateDomainPending(ctx, payload.DomainID, payload.WorkspaceID, attempt); err != nil {
			log.Error("db update pending failed", zap.Error(err))
			_ = msg.NakWithDelay(PollInterval)
			return nil
		}
		verifyRetries.Inc()
		// After ReminderAfterAttempt checks with no DNS found, nudge the user once.
		if attempt == ReminderAfterAttempt {
			h.publishEvent(ctx, SubjectDomainReminder, map[string]any{
				"domainId":    payload.DomainID,
				"workspaceId": payload.WorkspaceID,
				"domain":      payload.Domain,
				"attempt":     attempt,
			})
		}
		delay := h.nextDelay(attempt)
		log.Info("domain still pending, retrying", zap.Duration("delay", delay))
		_ = msg.NakWithDelay(delay)

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

func (h *Handler) publishEvent(ctx context.Context, subject string, payload map[string]any) {
	if err := h.publisher.Publish(ctx, subject, payload, ""); err != nil {
		h.logger.Warn("event publish failed", zap.String("subject", subject), zap.Error(err))
	}
}
