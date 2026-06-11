package cleanup

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

const sweepBatch = 1000

var (
	auditLogsDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cleanup_audit_logs_deleted_total",
		Help: "Total audit_logs rows deleted by the cleanup scheduler.",
	})
	eventsRawDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cleanup_events_raw_deleted_total",
		Help: "Total events_raw rows deleted by the cleanup scheduler (cascades to events_enriched).",
	})
)

// DB is the subset of the postgres client used by the cleanup scheduler.
type DB interface {
	DeleteOldAuditLogs(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
	DeleteOldEvents(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}

// Scheduler runs periodic TTL cleanup sweeps against audit_logs and events_raw.
// It deletes in bounded batches to avoid long-running transactions that would
// lock tables and spike replication lag.
type Scheduler struct {
	db       DB
	interval time.Duration
	retainFor time.Duration
	logger   *zap.Logger
}

func NewScheduler(db DB, interval time.Duration, retainFor time.Duration, logger *zap.Logger) *Scheduler {
	return &Scheduler{db: db, interval: interval, retainFor: retainFor, logger: logger}
}

// Run starts the cleanup loop. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.logger.Info("cleanup scheduler started",
		zap.Duration("interval", s.interval),
		zap.Duration("retain_for", s.retainFor),
	)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("cleanup scheduler stopped")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Scheduler) sweep(ctx context.Context) {
	s.sweepAuditLogs(ctx)
	s.sweepEvents(ctx)
}

func (s *Scheduler) sweepAuditLogs(ctx context.Context) {
	var total int64
	for {
		n, err := s.db.DeleteOldAuditLogs(ctx, s.retainFor, sweepBatch)
		if err != nil {
			s.logger.Error("cleanup: delete audit_logs failed", zap.Error(err))
			return
		}
		total += n
		if n < int64(sweepBatch) {
			break
		}
		// Yield briefly between batches to reduce lock pressure.
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if total > 0 {
		auditLogsDeleted.Add(float64(total))
		s.logger.Info("cleanup: audit_logs swept", zap.Int64("deleted", total))
	}
}

func (s *Scheduler) sweepEvents(ctx context.Context) {
	var total int64
	for {
		n, err := s.db.DeleteOldEvents(ctx, s.retainFor, sweepBatch)
		if err != nil {
			s.logger.Error("cleanup: delete events_raw failed", zap.Error(err))
			return
		}
		total += n
		if n < int64(sweepBatch) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if total > 0 {
		eventsRawDeleted.Add(float64(total))
		s.logger.Info("cleanup: events_raw swept (cascades to events_enriched)", zap.Int64("deleted", total))
	}
}
