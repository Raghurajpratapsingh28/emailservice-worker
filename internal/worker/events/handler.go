package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

const (
	SubjectEventsRaw      = "events.raw.*"
	SubjectWorkflowTrigger = "workflow.trigger"
)

// RetryDelays is the JetStream BackOff schedule for transient failures.
var RetryDelays = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
}

// MaxAttempts is the total deliveries before a message is terminated.
// Overridden at registration time via config.EventMaxRetries + 1.
const MaxAttempts = 5

// --- metrics ---

var (
	eventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_processed_total",
		Help: "Total raw events consumed.",
	})
	enrichFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_enrich_failures_total",
		Help: "Total enrichment failures.",
	})
	workflowsTriggered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_workflows_triggered_total",
		Help: "Total workflow trigger events published.",
	})
	eventRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_retries_total",
		Help: "Total transient retries.",
	})
	duplicateDrops = promauto.NewCounter(prometheus.CounterOpts{
		Name: "events_duplicate_drops_total",
		Help: "Total duplicate events dropped.",
	})
)

// --- collaborator interfaces ---

// EventDB is the persistence surface required by the handler.
type EventDB interface {
	ContactDB
	GetRawEventStatus(ctx context.Context, eventID, workspaceID string) (string, error)
	MarkRawEventProcessing(ctx context.Context, eventID, workspaceID string) error
	MarkRawEventProcessed(ctx context.Context, eventID, workspaceID string) error
	MarkRawEventFailed(ctx context.Context, eventID, workspaceID, reason string) error
	IncrementRawEventAttempts(ctx context.Context, eventID, workspaceID string) (int, error)
	InsertEnrichedEvent(ctx context.Context, row postgres.EnrichedEventRow) error
	FindMatchingWorkflowTriggers(ctx context.Context, workspaceID, eventType, eventName string) ([]postgres.WorkflowTriggerRule, error)
}

// EventPublisher publishes workflow trigger events.
type EventPublisher interface {
	Publish(ctx context.Context, subject string, payload any, msgID string) error
}

// --- handler ---

type Handler struct {
	db          EventDB
	enricher    *Enricher
	pub         EventPublisher
	maxRetries  int
	logger      *zap.Logger
}

func NewHandler(db EventDB, pub EventPublisher, maxRetries int, logger *zap.Logger) *Handler {
	return &Handler{
		db:         db,
		enricher:   NewEnricher(db, logger),
		pub:        pub,
		maxRetries: maxRetries,
		logger:     logger,
	}
}

// Handle processes a single events.raw.{workspaceId} message.
// Owns the full message lifecycle (Ack / Nak / Term).
func (h *Handler) Handle(ctx context.Context, msg jetstream.Msg) error {
	eventsProcessed.Inc()

	var payload types.RawEventPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed event payload, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if err := validatePayload(&payload); err != nil {
		h.logger.Error("invalid event payload, terminating",
			zap.Error(err), zap.String("event_id", payload.EventID))
		_ = msg.Term()
		return nil
	}

	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("event_id", payload.EventID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.String("event_type", payload.EventType),
		zap.String("event_name", payload.EventName),
		zap.Int("attempt", attempt),
	)
	log.Info("event received")

	// Idempotency: skip already-processed events.
	status, err := h.db.GetRawEventStatus(ctx, payload.EventID, payload.WorkspaceID)
	if err != nil {
		log.Error("idempotency check failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}
	if status == "processed" {
		duplicateDrops.Inc()
		log.Info("duplicate event, skipping")
		_ = msg.Ack()
		return nil
	}

	// Mark processing (idempotent).
	if err := h.db.MarkRawEventProcessing(ctx, payload.EventID, payload.WorkspaceID); err != nil {
		log.Error("mark processing failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}

	// Enrich.
	result, err := h.enricher.Enrich(ctx, &payload)
	if err != nil {
		enrichFailures.Inc()
		log.Error("enrichment failed", zap.Error(err))
		return h.handleTransient(ctx, msg, &payload, attempt, err.Error(), log)
	}

	if result.ContactID != "" {
		log.Info("contact linked", zap.String("contact_id", result.ContactID))
	}

	// Persist enriched event.
	if err := h.persistEnriched(ctx, &payload, result); err != nil {
		log.Error("persist enriched event failed", zap.Error(err))
		return h.handleTransient(ctx, msg, &payload, attempt, err.Error(), log)
	}

	// Evaluate workflow triggers.
	h.evaluateWorkflowTriggers(ctx, &payload, result.ContactID, log)

	// Mark processed.
	if err := h.db.MarkRawEventProcessed(ctx, payload.EventID, payload.WorkspaceID); err != nil {
		// Enriched event is already persisted. Log but ack to avoid re-processing.
		log.Error("mark processed failed (enriched event already persisted, acking)",
			zap.Error(err))
	}

	log.Info("enrichment success")
	_ = msg.Ack()
	return nil
}

func (h *Handler) handleTransient(
	ctx context.Context, msg jetstream.Msg,
	payload *types.RawEventPayload, attempt int, reason string,
	log *zap.Logger,
) error {
	if attempt >= h.maxRetries {
		log.Error("max retries reached, marking failed")
		if err := h.db.MarkRawEventFailed(ctx, payload.EventID, payload.WorkspaceID, reason); err != nil {
			log.Warn("mark failed db error", zap.Error(err))
		}
		enrichFailures.Inc()
		_ = msg.Term()
		return nil
	}
	eventRetries.Inc()
	log.Warn("retry scheduled", zap.String("reason", reason))
	_ = msg.Nak()
	return nil
}

func (h *Handler) persistEnriched(
	ctx context.Context,
	payload *types.RawEventPayload,
	result EnrichmentResult,
) error {
	propsJSON, _ := json.Marshal(payload.Properties)
	traitsJSON, _ := json.Marshal(result.MergedTraits)
	ctxJSON, _ := json.Marshal(payload.Context)
	enrichedJSON, _ := json.Marshal(result.EnrichedData)

	return h.db.InsertEnrichedEvent(ctx, postgres.EnrichedEventRow{
		WorkspaceID:  payload.WorkspaceID,
		RawEventID:   payload.EventID,
		ContactID:    result.ContactID,
		UserID:       payload.UserID,
		AnonymousID:  payload.AnonymousID,
		EventType:    payload.EventType,
		EventName:    payload.EventName,
		Properties:   propsJSON,
		Traits:       traitsJSON,
		Context:      ctxJSON,
		EnrichedData: enrichedJSON,
	})
}

func (h *Handler) evaluateWorkflowTriggers(
	ctx context.Context,
	payload *types.RawEventPayload,
	contactID string,
	log *zap.Logger,
) {
	triggers, err := h.db.FindMatchingWorkflowTriggers(ctx, payload.WorkspaceID, payload.EventType, payload.EventName)
	if err != nil {
		log.Warn("workflow trigger lookup failed", zap.Error(err))
		return
	}
	for _, t := range triggers {
		trigger := types.WorkflowTriggerPayload{
			WorkspaceID: payload.WorkspaceID,
			ContactID:   contactID,
			EventName:   payload.EventName,
			EventID:     payload.EventID,
		}
		msgID := fmt.Sprintf("wf-%s-%s", t.WorkflowID, payload.EventID)
		if err := h.pub.Publish(ctx, SubjectWorkflowTrigger, trigger, msgID); err != nil {
			log.Warn("workflow trigger publish failed",
				zap.Error(err), zap.String("workflow_id", t.WorkflowID))
			continue
		}
		workflowsTriggered.Inc()
		log.Info("workflow trigger published",
			zap.String("workflow_id", t.WorkflowID),
			zap.String("contact_id", contactID),
		)
	}
}

func validatePayload(p *types.RawEventPayload) error {
	if p.EventID == "" || p.WorkspaceID == "" {
		return fmt.Errorf("missing required fields: eventId=%q workspaceId=%q", p.EventID, p.WorkspaceID)
	}
	if p.EventType == "" {
		return fmt.Errorf("eventType is empty")
	}
	return nil
}
