package workflows

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	infraRedis "engageiq-workers/internal/infra/redis"
	"engageiq-workers/internal/infra/postgres"
)

const (
	schedulerLockKey = "workflow:scheduler:lock"
	schedulerLockTTL = 45 * time.Second
	schedulerBatch   = 50
)

// Scheduler polls for due delay executions and resumes them.
type Scheduler struct {
	db       WorkflowDB
	executor *Executor
	locker   DistributedLocker
	interval time.Duration
	logger   *zap.Logger
}

func NewScheduler(
	db WorkflowDB,
	executor *Executor,
	locker DistributedLocker,
	interval time.Duration,
	logger *zap.Logger,
) *Scheduler {
	return &Scheduler{db: db, executor: executor, locker: locker, interval: interval, logger: logger}
}

// Run starts the scheduler loop. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.logger.Info("workflow scheduler started", zap.Duration("interval", s.interval))
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("workflow scheduler stopped")
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

func (s *Scheduler) poll(ctx context.Context) {
	lockVal := "scheduler"
	if err := s.locker.AcquireLock(ctx, schedulerLockKey, lockVal, schedulerLockTTL); err != nil {
		if errors.Is(err, infraRedis.ErrLockNotAcquired) {
			return
		}
		s.logger.Warn("scheduler lock error", zap.Error(err))
		return
	}
	defer func() { _ = s.locker.ReleaseLock(ctx, schedulerLockKey, lockVal) }()

	executions, err := s.db.FetchDueExecutions(ctx, schedulerBatch)
	if err != nil {
		s.logger.Error("fetch due executions failed", zap.Error(err))
		return
	}
	if len(executions) == 0 {
		return
	}
	s.logger.Info("resuming due executions", zap.Int("count", len(executions)))

	for i := range executions {
		if ctx.Err() != nil {
			return
		}
		s.resume(ctx, &executions[i])
	}
}

func (s *Scheduler) resume(ctx context.Context, exec *postgres.ExecutionRow) {
	// Per-execution lock prevents double-resume across concurrent scheduler instances.
	lockKey := "workflow:execution:" + exec.ID
	lockVal := "scheduler-resume"
	if err := s.locker.AcquireLock(ctx, lockKey, lockVal, 2*time.Minute); err != nil {
		if errors.Is(err, infraRedis.ErrLockNotAcquired) {
			return
		}
		s.logger.Warn("execution lock error", zap.String("execution_id", exec.ID), zap.Error(err))
		return
	}
	defer func() { _ = s.locker.ReleaseLock(ctx, lockKey, lockVal) }()

	log := s.logger.With(
		zap.String("execution_id", exec.ID),
		zap.String("workflow_id", exec.WorkflowID),
	)
	log.Info("resuming execution after delay")

	wf, err := s.db.GetWorkflow(ctx, exec.WorkflowID, exec.WorkspaceID)
	if err != nil || wf == nil {
		log.Error("load workflow for resume failed", zap.Error(err))
		_ = s.db.FailExecution(ctx, exec.ID, "workflow not found during resume")
		return
	}

	nodes, err := ParseWorkflowNodes(wf.Nodes)
	if err != nil {
		log.Error("parse nodes for resume failed", zap.Error(err))
		_ = s.db.FailExecution(ctx, exec.ID, "invalid workflow nodes: "+err.Error())
		return
	}

	// Re-fetch to get latest state (guards against concurrent updates).
	latest, err := s.db.GetExecution(ctx, exec.ID)
	if err != nil || latest == nil || latest.Status != StatusWaiting {
		return
	}

	status, err := s.executor.ResumeFromDelay(ctx, latest, nodes)
	if err != nil {
		log.Error("resume failed", zap.Error(err))
		_ = s.db.FailExecution(ctx, exec.ID, err.Error())
		return
	}
	log.Info("execution resumed", zap.String("status", status))
}
