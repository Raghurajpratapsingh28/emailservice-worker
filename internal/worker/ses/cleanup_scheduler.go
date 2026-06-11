package ses

import (
	"context"
	"time"

	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
)

const cleanupBatch = 50

// CleanupDB is the subset of postgres.Client used by DomainCleanupScheduler.
type CleanupDB interface {
	GetStaleVerifyingDomains(ctx context.Context, staleDuration time.Duration, limit int) ([]postgres.StaleVerifyingDomain, error)
	MarkDomainExpired(ctx context.Context, domainID, workspaceID string) error
}

// DomainCleanupScheduler periodically expires domains that have been stuck in
// pending/verifying for longer than StaleAfter. It publishes a reminder event
// for each expired domain so the notification layer can email the workspace owner.
//
// This is intentionally separate from the verification poller — the poller runs
// indefinitely; this scheduler is the only thing that ever marks a domain failed
// due to timeout.
type DomainCleanupScheduler struct {
	db         CleanupDB
	publisher  EventPublisher
	interval   time.Duration
	staleAfter time.Duration
	logger     *zap.Logger
}

func NewDomainCleanupScheduler(
	db CleanupDB,
	publisher EventPublisher,
	interval time.Duration,
	staleAfter time.Duration,
	logger *zap.Logger,
) *DomainCleanupScheduler {
	return &DomainCleanupScheduler{
		db:         db,
		publisher:  publisher,
		interval:   interval,
		staleAfter: staleAfter,
		logger:     logger,
	}
}

// Run starts the cleanup loop. Blocks until ctx is cancelled.
func (s *DomainCleanupScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.logger.Info("domain cleanup scheduler started",
		zap.Duration("interval", s.interval),
		zap.Duration("stale_after", s.staleAfter),
	)
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("domain cleanup scheduler stopped")
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *DomainCleanupScheduler) sweep(ctx context.Context) {
	domains, err := s.db.GetStaleVerifyingDomains(ctx, s.staleAfter, cleanupBatch)
	if err != nil {
		s.logger.Error("domain cleanup: fetch stale domains failed", zap.Error(err))
		return
	}
	if len(domains) == 0 {
		return
	}

	s.logger.Info("domain cleanup: expiring stale domains", zap.Int("count", len(domains)))
	for _, d := range domains {
		if err := s.db.MarkDomainExpired(ctx, d.ID, d.WorkspaceID); err != nil {
			s.logger.Error("domain cleanup: mark expired failed",
				zap.String("domain_id", d.ID),
				zap.Error(err),
			)
			continue
		}
		s.publisher.Publish(ctx, "domain.verification.expired.v1", map[string]any{ //nolint:errcheck
			"domainId":    d.ID,
			"workspaceId": d.WorkspaceID,
			"domain":      d.Domain,
			"ownerEmail":  d.OwnerEmail,
			"ownerName":   d.OwnerName,
			"expiredAt":   time.Now().UTC().Format(time.RFC3339),
		}, "")
		s.logger.Info("domain cleanup: expired",
			zap.String("domain", d.Domain),
			zap.String("workspace_id", d.WorkspaceID),
		)
	}
}
