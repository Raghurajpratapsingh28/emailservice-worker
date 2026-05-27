package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	infraRedis "engageiq-workers/internal/infra/redis"
	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

const (
	SubjectWorkflowTrigger  = "workflow.trigger"
	SubjectWorkflowRegister = "workflow.register"
)

// RetryDelays is the JetStream BackOff schedule.
var RetryDelays = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
}

const MaxAttempts = 5

// DistributedLocker is the Redis lock interface.
type DistributedLocker interface {
	AcquireLock(ctx context.Context, key, value string, ttl time.Duration) error
	ReleaseLock(ctx context.Context, key, value string) error
}

// WorkflowDB extends WorkflowDB with execution creation.
type WorkflowDB interface {
	GetWorkflow(ctx context.Context, workflowID, workspaceID string) (*postgres.WorkflowRow, error)
	GetExecution(ctx context.Context, executionID string) (*postgres.ExecutionRow, error)
	CreateExecution(ctx context.Context, workflowID, workspaceID, contactID, triggerEventID string) (string, bool, error)
	AdvanceExecution(ctx context.Context, executionID, nodeID, status string) error
	ScheduleDelay(ctx context.Context, executionID, nodeID string, nextRunAt time.Time) error
	CompleteExecution(ctx context.Context, executionID string) error
	FailExecution(ctx context.Context, executionID, reason string) error
	IncrementExecutionRetry(ctx context.Context, executionID string) (int, error)
	FetchDueExecutions(ctx context.Context, limit int) ([]postgres.ExecutionRow, error)
	GetContactForWorkflow(ctx context.Context, contactID, workspaceID string) (*postgres.ContactRow, error)
}

// --- metrics ---

var (
	workflowTriggers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "workflow_triggers_total",
		Help: "Total workflow.trigger messages consumed.",
	})
	workflowExecutionsStarted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "workflow_executions_started_total",
		Help: "Total workflow executions created.",
	})
	workflowEmailsTriggered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "workflow_emails_triggered_total",
		Help: "Total transactional emails published by workflow engine.",
	})
	workflowDelaysScheduled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "workflow_delays_scheduled_total",
		Help: "Total delay nodes scheduled.",
	})
	workflowCompletions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "workflow_completions_total",
		Help: "Total workflow executions completed.",
	})
	workflowFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "workflow_failures_total",
		Help: "Total workflow executions failed.",
	})
)

// --- trigger handler ---

// TriggerHandler consumes workflow.trigger messages.
type TriggerHandler struct {
	db         WorkflowDB
	executor   *Executor
	locker     DistributedLocker
	maxRetries int
	logger     *zap.Logger
}

func NewTriggerHandler(db WorkflowDB, pub EmailPublisher, locker DistributedLocker, maxRetries int, logger *zap.Logger) *TriggerHandler {
	return &TriggerHandler{
		db:         db,
		executor:   NewExecutor(db, pub, logger),
		locker:     locker,
		maxRetries: maxRetries,
		logger:     logger,
	}
}

// Handle processes a workflow.trigger message.
func (h *TriggerHandler) Handle(ctx context.Context, msg jetstream.Msg) error {
	workflowTriggers.Inc()

	var payload types.WorkflowTriggerPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed workflow trigger, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if payload.WorkspaceID == "" || payload.WorkflowID == "" || payload.ContactID == "" {
		h.logger.Error("invalid workflow trigger payload, terminating",
			zap.Any("payload", payload))
		_ = msg.Term()
		return nil
	}

	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("workflow_id", payload.WorkflowID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.String("contact_id", payload.ContactID),
		zap.String("event_id", payload.EventID),
		zap.Int("attempt", attempt),
	)
	log.Info("trigger received")

	// Load and validate workflow.
	wf, err := h.db.GetWorkflow(ctx, payload.WorkflowID, payload.WorkspaceID)
	if err != nil {
		log.Error("load workflow failed", zap.Error(err))
		return h.nak(msg, attempt, log)
	}
	if wf == nil {
		log.Warn("workflow not found, terminating")
		_ = msg.Term()
		return nil
	}
	if wf.Status != "published" {
		log.Info("workflow not published, skipping", zap.String("status", wf.Status))
		_ = msg.Ack()
		return nil
	}

	// Create execution — idempotent via ON CONFLICT.
	execID, created, err := h.db.CreateExecution(ctx, payload.WorkflowID, payload.WorkspaceID, payload.ContactID, payload.EventID)
	if err != nil {
		log.Error("create execution failed", zap.Error(err))
		return h.nak(msg, attempt, log)
	}
	if !created {
		log.Info("execution already exists for this trigger, skipping")
		_ = msg.Ack()
		return nil
	}
	workflowExecutionsStarted.Inc()
	log.Info("execution started", zap.String("execution_id", execID))

	// Acquire per-execution lock to prevent concurrent execution.
	lockKey := "workflow:execution:" + execID
	lockVal := "trigger-" + payload.EventID
	if err := h.locker.AcquireLock(ctx, lockKey, lockVal, 5*time.Minute); err != nil {
		if errors.Is(err, infraRedis.ErrLockNotAcquired) {
			log.Info("execution already running")
			_ = msg.Ack()
			return nil
		}
		log.Error("acquire execution lock failed", zap.Error(err))
		return h.nak(msg, attempt, log)
	}
	defer func() { _ = h.locker.ReleaseLock(ctx, lockKey, lockVal) }()

	nodes, err := ParseWorkflowNodes(wf.Nodes)
	if err != nil {
		log.Error("parse workflow nodes failed, terminating", zap.Error(err))
		_ = h.db.FailExecution(ctx, execID, "invalid nodes: "+err.Error())
		workflowFailures.Inc()
		_ = msg.Term()
		return nil
	}

	exec := &postgres.ExecutionRow{
		ID:          execID,
		WorkspaceID: payload.WorkspaceID,
		WorkflowID:  payload.WorkflowID,
		ContactID:   payload.ContactID,
		Status:      StatusRunning,
	}

	status, err := h.executor.Run(ctx, exec, nodes)
	if err != nil {
		log.Error("execution failed", zap.Error(err))
		_ = h.db.FailExecution(ctx, execID, err.Error())
		workflowFailures.Inc()
		return h.nak(msg, attempt, log)
	}

	h.recordOutcome(status, log)
	_ = msg.Ack()
	return nil
}

func (h *TriggerHandler) recordOutcome(status string, log *zap.Logger) {
	switch status {
	case StatusCompleted:
		workflowCompletions.Inc()
		log.Info("workflow completed")
	case StatusWaiting:
		workflowDelaysScheduled.Inc()
		log.Info("workflow paused at delay node")
	}
}

func (h *TriggerHandler) nak(msg jetstream.Msg, attempt int, log *zap.Logger) error {
	if attempt >= h.maxRetries {
		log.Error("max retries reached")
		workflowFailures.Inc()
		_ = msg.Term()
		return nil
	}
	log.Warn("retry scheduled")
	_ = msg.Nak()
	return nil
}

// --- register handler ---

// RegisterHandler consumes workflow.register messages (no-op for MVP — workflow
// is loaded on demand from the DB; register just validates it exists).
type RegisterHandler struct {
	db     WorkflowDB
	logger *zap.Logger
}

func NewRegisterHandler(db WorkflowDB, logger *zap.Logger) *RegisterHandler {
	return &RegisterHandler{db: db, logger: logger}
}

func (h *RegisterHandler) Handle(ctx context.Context, msg jetstream.Msg) error {
	var payload types.WorkflowRegisterPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed workflow register, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if payload.WorkspaceID == "" || payload.WorkflowID == "" {
		h.logger.Error("invalid workflow register payload, terminating")
		_ = msg.Term()
		return nil
	}

	wf, err := h.db.GetWorkflow(ctx, payload.WorkflowID, payload.WorkspaceID)
	if err != nil {
		h.logger.Error("load workflow on register failed",
			zap.Error(err),
			zap.String("workflow_id", payload.WorkflowID),
		)
		_ = msg.Nak()
		return nil
	}
	if wf == nil {
		h.logger.Warn("workflow not found on register",
			zap.String("workflow_id", payload.WorkflowID))
		_ = msg.Term()
		return nil
	}

	h.logger.Info("workflow registered",
		zap.String("workflow_id", payload.WorkflowID),
		zap.String("status", wf.Status),
	)
	_ = msg.Ack()
	return nil
}
