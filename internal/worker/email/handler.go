package email

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	infraSes "engageiq-workers/internal/infra/ses"
	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

const (
	SubjectSend   = "email.send.transactional"
	SubjectEvents = "email.delivery.events"
	SubjectDLQ    = "email.send.dlq"
)

// RetryDelays is the JetStream BackOff schedule for transient failures.
// Cumulative budget: 1m + 5m + 15m + 30m + 2h ≈ 2h51m.
var RetryDelays = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
}

// MaxAttempts is the total number of deliveries (initial + retries).
const MaxAttempts = 6

// Status values written to email_sends.status and to delivery events.
const (
	StatusSent    = "sent"
	StatusFailed  = "failed"
	StatusBounced = "bounced"
)

// rateLimitWaitBudget is the maximum time the handler will block waiting for
// a rate limit token before naking the message back to JetStream for retry.
const rateLimitWaitBudget = 5 * time.Second

// --- metrics ---

var (
	emailsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_processed_total",
		Help: "Total number of email send messages consumed.",
	})
	emailsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_sent_total",
		Help: "Total number of emails successfully sent via SES.",
	})
	emailsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_failed_total",
		Help: "Total number of emails marked as permanently failed.",
	})
	emailRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_retries_total",
		Help: "Total number of transient retries scheduled.",
	})
	emailSESFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_ses_failures_total",
		Help: "Total number of SES API call failures.",
	})
	rateLimitWaits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_rate_limit_waits_total",
		Help: "Total number of rate limit waits encountered.",
	})
	dlqEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "email_dlq_total",
		Help: "Total number of messages routed to DLQ after exhausting retries.",
	})
)

// --- collaborator interfaces ---

// SESSender is the SES send surface required by the handler.
type SESSender interface {
	SendEmail(ctx context.Context, in infraSes.SendEmailInput) (infraSes.SendEmailOutput, error)
}

// EmailDB is the persistence surface required by the handler.
type EmailDB interface {
	GetEmailSendStatus(ctx context.Context, sendID, workspaceID string) (string, error)
	UpdateEmailSendSending(ctx context.Context, sendID, workspaceID string) error
	UpdateEmailSendSent(ctx context.Context, sendID, workspaceID, providerMessageID string) error
	UpdateEmailSendFailed(ctx context.Context, sendID, workspaceID, reason string) error
}

// Limiter is the per-workspace rate limiter.
type Limiter interface {
	Acquire(ctx context.Context, key string, maxWait time.Duration) (time.Duration, error)
}

// EventPublisher publishes delivery events / DLQ messages.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, payload any, msgID string) error
}

// ErrorClassifier reports whether a SES error is permanent (non-retryable).
type ErrorClassifier func(err error) bool

// --- handler ---

type Handler struct {
	ses        SESSender
	db         EmailDB
	limiter    Limiter
	pub        EventPublisher
	renderer   *Renderer
	classifier ErrorClassifier
	logger     *zap.Logger
}

// NewHandler wires the handler with default error classification.
func NewHandler(ses SESSender, db EmailDB, limiter Limiter, pub EventPublisher, logger *zap.Logger) *Handler {
	return &Handler{
		ses:        ses,
		db:         db,
		limiter:    limiter,
		pub:        pub,
		renderer:   NewRenderer(),
		classifier: infraSes.IsPermanentError,
		logger:     logger,
	}
}

// SetClassifier overrides the SES error classifier (used in tests).
func (h *Handler) SetClassifier(c ErrorClassifier) { h.classifier = c }

// Handle processes a single email.send.transactional message. The handler
// owns the message lifecycle (Ack / Nak / Term) on every code path.
func (h *Handler) Handle(ctx context.Context, msg jetstream.Msg) error {
	emailsProcessed.Inc()

	var payload types.TransactionalEmailPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed payload, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if err := validatePayload(&payload); err != nil {
		h.logger.Error("invalid payload, terminating",
			zap.Error(err), zap.String("send_id", payload.SendID))
		_ = msg.Term()
		return nil
	}

	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("send_id", payload.SendID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.String("job_id", payload.JobID),
		zap.Int("attempt", attempt),
	)
	log.Info("consume")

	// --- idempotency: don't re-send terminal sends ---
	status, err := h.db.GetEmailSendStatus(ctx, payload.SendID, payload.WorkspaceID)
	if err != nil {
		log.Error("idempotency check failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}
	if status == StatusSent || status == StatusBounced {
		log.Info("duplicate delivery, skipping", zap.String("status", status))
		_ = msg.Ack()
		return nil
	}

	// --- rate limit (per-workspace token bucket) ---
	rlKey := "ratelimit:ses:" + payload.WorkspaceID
	waited, rlErr := h.limiter.Acquire(ctx, rlKey, rateLimitWaitBudget)
	if rlErr != nil {
		rateLimitWaits.Inc()
		log.Warn("rate limit denied, requeueing", zap.Error(rlErr))
		_ = msg.Nak()
		return nil
	}
	if waited > 0 {
		rateLimitWaits.Inc()
		log.Debug("rate limit wait", zap.Duration("waited", waited))
	}

	// --- transition to sending ---
	if err := h.db.UpdateEmailSendSending(ctx, payload.SendID, payload.WorkspaceID); err != nil {
		log.Error("update sending failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}
	log.Info("send start")

	// --- render ---
	rendered, err := h.renderer.Render(&payload)
	if err != nil {
		log.Error("render failed, terminating", zap.Error(err))
		h.markFailedAndPublishEvent(ctx, &payload, "render: "+err.Error(), log)
		emailsFailed.Inc()
		_ = msg.Term()
		return nil
	}

	// --- send via SES ---
	sendIn := infraSes.SendEmailInput{
		From:     payload.From.Email,
		FromName: payload.From.Name,
		To:       extractEmails(payload.To),
		Subject:  rendered.Subject,
		HTMLBody: rendered.HTML,
		TextBody: rendered.Text,
		Tags:     payload.Tags,
	}
	if payload.ReplyTo != "" {
		sendIn.ReplyTo = []string{payload.ReplyTo}
	}

	out, sendErr := h.ses.SendEmail(ctx, sendIn)
	if sendErr != nil {
		emailSESFailures.Inc()

		if h.classifier(sendErr) {
			log.Error("send failed (permanent)", zap.Error(sendErr))
			h.markFailedAndPublishEvent(ctx, &payload, sendErr.Error(), log)
			emailsFailed.Inc()
			_ = msg.Ack() // do not retry
			return nil
		}

		// transient
		log.Warn("send failed (transient)", zap.Error(sendErr))
		if attempt >= MaxAttempts {
			log.Error("max attempts reached, routing to DLQ")
			h.publishDLQ(ctx, &payload, sendErr.Error(), attempt, log)
			h.markFailedAndPublishEvent(ctx, &payload, "max retries: "+sendErr.Error(), log)
			emailsFailed.Inc()
			dlqEvents.Inc()
			_ = msg.Term()
			return nil
		}
		emailRetries.Inc()
		log.Info("retry scheduled", zap.Duration("next_delay", nextDelay(attempt)))
		_ = msg.Nak()
		return nil
	}

	// --- success ---
	if err := h.db.UpdateEmailSendSent(ctx, payload.SendID, payload.WorkspaceID, out.MessageID); err != nil {
		// SES already accepted the email. Naking would cause a duplicate send.
		// Log loudly and ack to avoid double-send; idempotency check will
		// short-circuit any subsequent redelivery once DB catches up.
		log.Error("update sent failed but SES accepted; acking to prevent duplicate send",
			zap.Error(err), zap.String("provider_message_id", out.MessageID))
	}

	if err := h.publishDeliveryEvent(ctx, &payload, out.MessageID, StatusSent, "", log); err != nil {
		log.Warn("publish delivery event failed", zap.Error(err))
	}

	emailsSent.Inc()
	log.Info("send success", zap.String("provider_message_id", out.MessageID))
	_ = msg.Ack()
	return nil
}

// --- helpers ---

func (h *Handler) markFailedAndPublishEvent(
	ctx context.Context, p *types.TransactionalEmailPayload, reason string, log *zap.Logger,
) {
	if err := h.db.UpdateEmailSendFailed(ctx, p.SendID, p.WorkspaceID, reason); err != nil {
		log.Error("update failed db", zap.Error(err))
	}
	if err := h.publishDeliveryEvent(ctx, p, "", StatusFailed, reason, log); err != nil {
		log.Warn("publish failure event failed", zap.Error(err))
	}
}

func (h *Handler) publishDeliveryEvent(
	ctx context.Context, p *types.TransactionalEmailPayload,
	providerMessageID, status, reason string, log *zap.Logger,
) error {
	evt := types.EmailDeliveryEvent{
		WorkspaceID:       p.WorkspaceID,
		SendID:            p.SendID,
		ProviderMessageID: providerMessageID,
		Status:            status,
		Reason:            reason,
		Timestamp:         time.Now().UTC(),
	}
	msgID := fmt.Sprintf("delivery-%s-%s", p.SendID, status)
	return h.pub.Publish(ctx, SubjectEvents, evt, msgID)
}

func (h *Handler) publishDLQ(
	ctx context.Context, p *types.TransactionalEmailPayload, reason string, attempt int, log *zap.Logger,
) {
	type dlqMessage struct {
		Payload   *types.TransactionalEmailPayload `json:"payload"`
		Reason    string                           `json:"reason"`
		Attempts  int                              `json:"attempts"`
		Timestamp time.Time                        `json:"timestamp"`
	}
	dlq := dlqMessage{
		Payload:   p,
		Reason:    reason,
		Attempts:  attempt,
		Timestamp: time.Now().UTC(),
	}
	msgID := "dlq-" + p.SendID
	if err := h.pub.Publish(ctx, SubjectDLQ, dlq, msgID); err != nil {
		log.Error("DLQ publish failed", zap.Error(err))
		return
	}
	log.Warn("DLQ event published", zap.String("subject", SubjectDLQ))
}

// validatePayload enforces the contract.
func validatePayload(p *types.TransactionalEmailPayload) error {
	if p == nil {
		return errors.New("nil payload")
	}
	if p.SendID == "" || p.WorkspaceID == "" || p.JobID == "" {
		return errors.New("missing required ids (sendId/workspaceId/jobId)")
	}
	if len(p.To) == 0 {
		return errors.New("missing recipients")
	}
	for i, t := range p.To {
		if t.Email == "" {
			return fmt.Errorf("recipient %d has empty email", i)
		}
	}
	if p.From.Email == "" {
		return errors.New("from.email is empty")
	}
	if p.Subject == "" {
		return errors.New("subject is empty")
	}
	if p.HTML == "" && p.Text == "" {
		return errors.New("html and text are both empty")
	}
	if p.Provider != "" && p.Provider != "ses" {
		return fmt.Errorf("unsupported provider %q", p.Provider)
	}
	return nil
}

func extractEmails(addrs []types.EmailAddress) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.Email)
	}
	return out
}

// nextDelay returns the JetStream BackOff delay applied before the next
// delivery. Used for observability only.
func nextDelay(attempt int) time.Duration {
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(RetryDelays) {
		return RetryDelays[len(RetryDelays)-1]
	}
	return RetryDelays[idx]
}


// ============================================================================
// Campaign chunk handler
// ============================================================================

// Campaign-specific metrics. Reuses RetryDelays / MaxAttempts above.
var (
	campaignEmailsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_emails_sent_total",
		Help: "Total per-recipient sends that succeeded.",
	})
	campaignEmailsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_emails_failed_total",
		Help: "Total per-recipient sends that failed permanently.",
	})
	campaignChunkRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_chunk_retries_total",
		Help: "Total chunks naked for retry due to transient errors.",
	})
	campaignChunkDLQ = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_chunk_dlq_total",
		Help: "Total chunks routed to DLQ.",
	})
)

// CampaignDB is the persistence surface required by the chunk handler.
type CampaignDB interface {
	GetCampaignRecipientStatus(ctx context.Context, recipientID, workspaceID string) (string, error)
	MarkCampaignRecipientSending(ctx context.Context, recipientID, workspaceID string) error
	MarkCampaignRecipientSent(ctx context.Context, recipientID, workspaceID, providerMessageID string) error
	MarkCampaignRecipientFailed(ctx context.Context, recipientID, workspaceID, reason string) error
	IncrementCampaignCounts(ctx context.Context, campaignID, workspaceID string, sent, failed int) (postgres.CampaignProgress, error)
	MarkCampaignComplete(ctx context.Context, campaignID, workspaceID string) (bool, error)
}

// CampaignChunkHandler consumes campaign.send.chunk and sends the recipients
// in the chunk via SES, applying per-workspace rate limiting.
type CampaignChunkHandler struct {
	ses        SESSender
	db         CampaignDB
	limiter    Limiter
	pub        EventPublisher
	renderer   *Renderer
	classifier ErrorClassifier
	logger     *zap.Logger
}

func NewCampaignChunkHandler(
	ses SESSender, db CampaignDB, limiter Limiter, pub EventPublisher, logger *zap.Logger,
) *CampaignChunkHandler {
	return &CampaignChunkHandler{
		ses:        ses,
		db:         db,
		limiter:    limiter,
		pub:        pub,
		renderer:   NewRenderer(),
		classifier: infraSes.IsPermanentError,
		logger:     logger,
	}
}

// SetClassifier overrides the SES error classifier (used in tests).
func (h *CampaignChunkHandler) SetClassifier(c ErrorClassifier) { h.classifier = c }

// chunkResult tracks per-recipient outcomes within a single chunk.
type chunkResult struct {
	sent             int
	permanentFailed  int
	transientFailed  int
	transientReason  string
}

// Handle owns the message lifecycle. The chunk is processed recipient-by-recipient.
//
//   - Per-recipient permanent SES errors mark the recipient failed and continue.
//   - Per-recipient transient SES errors are accumulated. If any are present,
//     the chunk is naked (server applies BackOff). On the final delivery, all
//     remaining transient-failed recipients are marked failed and the chunk is
//     routed to the DLQ.
//   - Per-recipient duplicates (already sent) are silently skipped.
func (h *CampaignChunkHandler) Handle(ctx context.Context, msg jetstream.Msg) error {
	var payload types.CampaignChunkPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed chunk payload, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if err := validateCampaignChunk(&payload); err != nil {
		h.logger.Error("invalid chunk payload, terminating",
			zap.Error(err), zap.String("chunk_id", payload.ChunkID))
		_ = msg.Term()
		return nil
	}

	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("chunk_id", payload.ChunkID),
		zap.String("campaign_id", payload.CampaignID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.Int("recipients", len(payload.Recipients)),
		zap.Int("attempt", attempt),
	)
	log.Info("chunk consume")

	// Render once for the whole chunk.
	rendered, err := h.renderer.Render(&types.TransactionalEmailPayload{
		Subject: payload.Subject,
		HTML:    payload.HTML,
		Text:    payload.Text,
	})
	if err != nil {
		log.Error("render failed, terminating chunk", zap.Error(err))
		// All recipients fail.
		h.failAllRecipients(ctx, &payload, "render: "+err.Error(), log)
		_ = msg.Term()
		return nil
	}

	res := chunkResult{}
	transientRecipients := make([]types.CampaignChunkRecipient, 0)

	for _, r := range payload.Recipients {
		if ctx.Err() != nil {
			// Context cancelled mid-chunk. Treat unprocessed as transient
			// for retry on next delivery.
			transientRecipients = append(transientRecipients, r)
			continue
		}

		outcome := h.processRecipient(ctx, &payload, r, rendered, log)
		switch outcome.kind {
		case "sent":
			res.sent++
		case "duplicate":
			// already terminal; counted as sent for completion math? No —
			// the count was already added when it was first marked sent.
			// We do not double-count here.
		case "permanent":
			res.permanentFailed++
		case "transient":
			res.transientFailed++
			res.transientReason = outcome.reason
			transientRecipients = append(transientRecipients, r)
		}
	}

	// Apply progress to the campaign row (only fresh outcomes).
	if res.sent > 0 || res.permanentFailed > 0 {
		progress, err := h.db.IncrementCampaignCounts(ctx, payload.CampaignID, payload.WorkspaceID,
			res.sent, res.permanentFailed)
		if err != nil {
			log.Error("increment campaign counts failed", zap.Error(err))
			// Don't lose the message — nak so we retry.
			_ = msg.Nak()
			return nil
		}
		// Try to mark complete (idempotent and conditional).
		if progress.SentCount+progress.FailedCount >= progress.TotalRecipients && progress.TotalRecipients > 0 {
			done, err := h.db.MarkCampaignComplete(ctx, payload.CampaignID, payload.WorkspaceID)
			if err != nil {
				log.Warn("mark campaign complete failed", zap.Error(err))
			} else if done {
				campaignsCompleted.Inc()
				log.Info("campaign completed",
					zap.Int("sent_count", progress.SentCount),
					zap.Int("failed_count", progress.FailedCount),
				)
			}
		}
	}

	// Decide chunk-level disposition based on transient failures.
	if len(transientRecipients) > 0 {
		if attempt >= MaxAttempts {
			// Final attempt — mark all transient as failed and DLQ.
			log.Error("chunk max attempts reached, routing to DLQ",
				zap.Int("transient_remaining", len(transientRecipients)))
			finalFailed := 0
			for _, r := range transientRecipients {
				if err := h.db.MarkCampaignRecipientFailed(ctx, r.RecipientID, payload.WorkspaceID,
					"max retries: "+res.transientReason); err != nil {
					log.Warn("mark recipient failed (DLQ)", zap.Error(err))
					continue
				}
				h.publishDeliveryEvent(ctx, &payload, r.Email, "", StatusFailed, res.transientReason, log)
				finalFailed++
			}
			if finalFailed > 0 {
				if _, err := h.db.IncrementCampaignCounts(ctx, payload.CampaignID, payload.WorkspaceID, 0, finalFailed); err != nil {
					log.Warn("final increment failed", zap.Error(err))
				}
				_, _ = h.db.MarkCampaignComplete(ctx, payload.CampaignID, payload.WorkspaceID)
			}
			h.publishCampaignDLQ(ctx, &payload, res.transientReason, attempt, log)
			campaignChunkDLQ.Inc()
			_ = msg.Term()
			return nil
		}
		log.Warn("chunk has transient failures, naking for retry",
			zap.Int("transient", len(transientRecipients)))
		campaignChunkRetries.Inc()
		_ = msg.Nak()
		return nil
	}

	log.Info("chunk processed",
		zap.Int("sent", res.sent),
		zap.Int("permanent_failed", res.permanentFailed),
	)
	_ = msg.Ack()
	return nil
}

// recipientOutcome is the per-recipient classification within a chunk.
type recipientOutcome struct {
	kind   string // sent | duplicate | permanent | transient
	reason string
}

// processRecipient sends a single recipient. Returns the outcome.
//
// On "sent": recipient row updated, delivery event published.
// On "permanent": recipient row updated, delivery event published.
// On "transient": no DB change made for this recipient (retries on chunk).
// On "duplicate": no-op.
func (h *CampaignChunkHandler) processRecipient(
	ctx context.Context,
	chunk *types.CampaignChunkPayload,
	r types.CampaignChunkRecipient,
	rendered Rendered,
	log *zap.Logger,
) recipientOutcome {
	rlog := log.With(zap.String("recipient_id", r.RecipientID), zap.String("recipient_email", r.Email))

	// Per-recipient idempotency
	status, err := h.db.GetCampaignRecipientStatus(ctx, r.RecipientID, chunk.WorkspaceID)
	if err != nil {
		rlog.Warn("recipient status check failed; treating as transient", zap.Error(err))
		return recipientOutcome{kind: "transient", reason: err.Error()}
	}
	if status == RecipientStatusSent || status == RecipientStatusBounced {
		rlog.Debug("recipient already terminal, skipping", zap.String("status", status))
		return recipientOutcome{kind: "duplicate"}
	}

	// Rate limit per-workspace
	if _, err := h.limiter.Acquire(ctx, "ratelimit:ses:"+chunk.WorkspaceID, rateLimitWaitBudget); err != nil {
		rateLimitWaits.Inc()
		rlog.Warn("rate limit denied; treating as transient", zap.Error(err))
		return recipientOutcome{kind: "transient", reason: "rate limit"}
	}

	if err := h.db.MarkCampaignRecipientSending(ctx, r.RecipientID, chunk.WorkspaceID); err != nil {
		rlog.Warn("mark sending failed; treating as transient", zap.Error(err))
		return recipientOutcome{kind: "transient", reason: err.Error()}
	}

	out, sendErr := h.ses.SendEmail(ctx, infraSes.SendEmailInput{
		From:     chunk.Sender.Email,
		FromName: chunk.Sender.Name,
		To:       []string{r.Email},
		ReplyTo:  replyToList(chunk.ReplyTo),
		Subject:  rendered.Subject,
		HTMLBody: rendered.HTML,
		TextBody: rendered.Text,
		Tags:     map[string]string{"campaign_id": chunk.CampaignID},
	})
	if sendErr != nil {
		emailSESFailures.Inc()
		if h.classifier(sendErr) {
			rlog.Error("recipient send failed (permanent)", zap.Error(sendErr))
			if err := h.db.MarkCampaignRecipientFailed(ctx, r.RecipientID, chunk.WorkspaceID, sendErr.Error()); err != nil {
				rlog.Warn("mark recipient failed", zap.Error(err))
			}
			h.publishDeliveryEvent(ctx, chunk, r.Email, "", StatusFailed, sendErr.Error(), rlog)
			campaignEmailsFailed.Inc()
			return recipientOutcome{kind: "permanent"}
		}
		rlog.Warn("recipient send failed (transient)", zap.Error(sendErr))
		return recipientOutcome{kind: "transient", reason: sendErr.Error()}
	}

	if err := h.db.MarkCampaignRecipientSent(ctx, r.RecipientID, chunk.WorkspaceID, out.MessageID); err != nil {
		// SES already accepted. Log loudly but count as sent — idempotency on
		// next delivery will resolve any DB lag.
		rlog.Error("mark recipient sent failed but SES accepted",
			zap.Error(err), zap.String("provider_message_id", out.MessageID))
	}
	h.publishDeliveryEvent(ctx, chunk, r.Email, out.MessageID, StatusSent, "", rlog)
	campaignEmailsSent.Inc()
	return recipientOutcome{kind: "sent"}
}

// failAllRecipients marks every recipient in the chunk as failed and publishes
// failed events. Used when the chunk cannot be processed (e.g. render error).
func (h *CampaignChunkHandler) failAllRecipients(
	ctx context.Context,
	chunk *types.CampaignChunkPayload,
	reason string,
	log *zap.Logger,
) {
	failed := 0
	for _, r := range chunk.Recipients {
		if err := h.db.MarkCampaignRecipientFailed(ctx, r.RecipientID, chunk.WorkspaceID, reason); err != nil {
			log.Warn("mark recipient failed", zap.Error(err))
			continue
		}
		h.publishDeliveryEvent(ctx, chunk, r.Email, "", StatusFailed, reason, log)
		failed++
	}
	if failed > 0 {
		if _, err := h.db.IncrementCampaignCounts(ctx, chunk.CampaignID, chunk.WorkspaceID, 0, failed); err != nil {
			log.Warn("increment failed counts", zap.Error(err))
		}
		_, _ = h.db.MarkCampaignComplete(ctx, chunk.CampaignID, chunk.WorkspaceID)
	}
}

func (h *CampaignChunkHandler) publishDeliveryEvent(
	ctx context.Context, chunk *types.CampaignChunkPayload,
	recipientEmail, providerMessageID, status, reason string,
	log *zap.Logger,
) {
	evt := types.EmailDeliveryEvent{
		WorkspaceID:       chunk.WorkspaceID,
		CampaignID:        chunk.CampaignID,
		RecipientEmail:    recipientEmail,
		ProviderMessageID: providerMessageID,
		Status:            status,
		Reason:            reason,
		Timestamp:         time.Now().UTC(),
	}
	msgID := fmt.Sprintf("delivery-%s-%s-%s", chunk.CampaignID, recipientEmail, status)
	if err := h.pub.Publish(ctx, SubjectEvents, evt, msgID); err != nil {
		log.Warn("publish delivery event failed", zap.Error(err))
	}
}

func (h *CampaignChunkHandler) publishCampaignDLQ(
	ctx context.Context, chunk *types.CampaignChunkPayload,
	reason string, attempt int, log *zap.Logger,
) {
	dlq := types.CampaignDLQMessage{
		Payload:   chunk,
		Reason:    reason,
		Attempts:  attempt,
		Timestamp: time.Now().UTC(),
	}
	msgID := "dlq-" + chunk.ChunkID
	if err := h.pub.Publish(ctx, SubjectCampaignDLQ, dlq, msgID); err != nil {
		log.Error("campaign DLQ publish failed", zap.Error(err))
		return
	}
	log.Warn("campaign DLQ event published", zap.String("subject", SubjectCampaignDLQ))
}

func validateCampaignChunk(p *types.CampaignChunkPayload) error {
	if p == nil {
		return errors.New("nil payload")
	}
	if p.CampaignID == "" || p.WorkspaceID == "" || p.ChunkID == "" {
		return errors.New("missing required ids (campaignId/workspaceId/chunkId)")
	}
	if len(p.Recipients) == 0 {
		return errors.New("chunk has no recipients")
	}
	if p.Sender.Email == "" {
		return errors.New("sender.email is empty")
	}
	if p.Subject == "" {
		return errors.New("subject is empty")
	}
	if p.HTML == "" && p.Text == "" {
		return errors.New("html and text are both empty")
	}
	for i, r := range p.Recipients {
		if r.RecipientID == "" || r.Email == "" {
			return fmt.Errorf("recipient %d missing id or email", i)
		}
	}
	return nil
}

func replyToList(replyTo string) []string {
	if replyTo == "" {
		return nil
	}
	return []string{replyTo}
}
