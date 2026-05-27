package segments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	infraRedis "engageiq-workers/internal/infra/redis"
	"engageiq-workers/pkg/types"
)

const (
	SubjectSegmentRefresh = "segment.refresh"
	lockTTL               = 10 * time.Minute
	evalPageSize          = 500
)

// RetryDelays is the JetStream BackOff schedule.
var RetryDelays = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
}

const MaxAttempts = 5

// --- metrics ---

var (
	refreshJobs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "segment_refresh_jobs_total",
		Help: "Total segment refresh jobs consumed.",
	})
	contactsMatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "segment_contacts_matched_total",
		Help: "Total contacts matched across all segment refreshes.",
	})
	membershipInserts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "segment_membership_inserts_total",
		Help: "Total new segment memberships inserted.",
	})
	membershipRemovals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "segment_membership_removals_total",
		Help: "Total stale segment memberships removed.",
	})
	refreshFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "segment_refresh_failures_total",
		Help: "Total segment refresh failures.",
	})
	refreshRetries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "segment_refresh_retries_total",
		Help: "Total segment refresh retries.",
	})
)

// --- collaborator interfaces ---

// SegmentDB is the persistence surface required by the handler.
type SegmentDB interface {
	EventChecker
	GetSegment(ctx context.Context, segmentID, workspaceID string) (*postgres.SegmentRow, error)
	UpdateSegmentStatus(ctx context.Context, segmentID, workspaceID, status string) error
	UpdateSegmentReady(ctx context.Context, segmentID, workspaceID string, count int) error
	GetCurrentMemberIDs(ctx context.Context, segmentID, workspaceID string) (map[string]struct{}, error)
	InsertSegmentMembers(ctx context.Context, segmentID, workspaceID string, contactIDs []string) error
	DeleteSegmentMembers(ctx context.Context, segmentID, workspaceID string, contactIDs []string) error
	StreamContactsForEval(ctx context.Context, workspaceID string, pageSize int, fn func([]postgres.ContactForEval) error) error
}

// DistributedLocker acquires and releases distributed locks.
type DistributedLocker interface {
	AcquireLock(ctx context.Context, key, value string, ttl time.Duration) error
	ReleaseLock(ctx context.Context, key, value string) error
}

// --- handler ---

type Handler struct {
	db         SegmentDB
	locker     DistributedLocker
	evaluator  *Evaluator
	maxRetries int
	logger     *zap.Logger
}

func NewHandler(db SegmentDB, locker DistributedLocker, maxRetries int, logger *zap.Logger) *Handler {
	return &Handler{
		db:         db,
		locker:     locker,
		evaluator:  NewEvaluator(db),
		maxRetries: maxRetries,
		logger:     logger,
	}
}

// Handle processes a single segment.refresh message.
func (h *Handler) Handle(ctx context.Context, msg jetstream.Msg) error {
	refreshJobs.Inc()

	var payload types.SegmentRefreshPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed segment payload, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if payload.WorkspaceID == "" || payload.SegmentID == "" {
		h.logger.Error("invalid segment payload, terminating",
			zap.Any("payload", payload))
		_ = msg.Term()
		return nil
	}

	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("segment_id", payload.SegmentID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.Int("attempt", attempt),
	)
	log.Info("refresh received")

	// Acquire distributed lock to prevent concurrent recomputes.
	lockKey := fmt.Sprintf("segment:%s:refresh", payload.SegmentID)
	lockVal := uuid.New().String()
	if err := h.locker.AcquireLock(ctx, lockKey, lockVal, lockTTL); err != nil {
		if errors.Is(err, infraRedis.ErrLockNotAcquired) {
			log.Info("segment refresh already in progress, skipping")
			_ = msg.Ack() // another worker is handling it
			return nil
		}
		log.Error("acquire lock failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}
	defer func() {
		if err := h.locker.ReleaseLock(context.Background(), lockKey, lockVal); err != nil {
			log.Warn("release lock failed", zap.Error(err))
		}
	}()

	// Load segment definition.
	seg, err := h.db.GetSegment(ctx, payload.SegmentID, payload.WorkspaceID)
	if err != nil {
		log.Error("load segment failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}
	if seg == nil {
		log.Warn("segment not found, terminating")
		_ = msg.Term()
		return nil
	}
	log.Info("segment loaded", zap.String("name", seg.Name))

	// Parse filter tree.
	tree, err := ParseFilterTree(seg.FilterTree)
	if err != nil {
		log.Error("parse filter tree failed, terminating", zap.Error(err))
		_ = h.db.UpdateSegmentStatus(ctx, payload.SegmentID, payload.WorkspaceID, "failed")
		_ = msg.Term()
		return nil
	}

	// Mark processing.
	if err := h.db.UpdateSegmentStatus(ctx, payload.SegmentID, payload.WorkspaceID, "processing"); err != nil {
		log.Error("update status processing failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}

	// Evaluate filter tree against all contacts, collecting matching IDs.
	matched := make(map[string]struct{})
	evalErr := h.db.StreamContactsForEval(ctx, payload.WorkspaceID, evalPageSize, func(page []postgres.ContactForEval) error {
		for i := range page {
			ok, err := h.evaluator.Matches(ctx, payload.WorkspaceID, &page[i], tree)
			if err != nil {
				return fmt.Errorf("evaluate contact %s: %w", page[i].ID, err)
			}
			if ok {
				matched[page[i].ID] = struct{}{}
			}
		}
		return nil
	})
	if evalErr != nil {
		log.Error("filter evaluation failed", zap.Error(evalErr))
		_ = h.db.UpdateSegmentStatus(ctx, payload.SegmentID, payload.WorkspaceID, "failed")
		return h.nak(msg, attempt, evalErr.Error(), log)
	}

	log.Info("contacts matched", zap.Int("count", len(matched)))
	contactsMatched.Add(float64(len(matched)))

	// Diff against current memberships.
	current, err := h.db.GetCurrentMemberIDs(ctx, payload.SegmentID, payload.WorkspaceID)
	if err != nil {
		log.Error("get current members failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}

	toAdd := diff(matched, current)
	toRemove := diff(current, matched)

	// Apply diff.
	if err := h.db.InsertSegmentMembers(ctx, payload.SegmentID, payload.WorkspaceID, toAdd); err != nil {
		log.Error("insert members failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}
	if err := h.db.DeleteSegmentMembers(ctx, payload.SegmentID, payload.WorkspaceID, toRemove); err != nil {
		log.Error("delete members failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}

	membershipInserts.Add(float64(len(toAdd)))
	membershipRemovals.Add(float64(len(toRemove)))

	// Update segment to ready with final count.
	finalCount := len(current) + len(toAdd) - len(toRemove)
	if err := h.db.UpdateSegmentReady(ctx, payload.SegmentID, payload.WorkspaceID, finalCount); err != nil {
		log.Error("update segment ready failed", zap.Error(err))
		return h.nak(msg, attempt, err.Error(), log)
	}

	log.Info("memberships updated",
		zap.Int("added", len(toAdd)),
		zap.Int("removed", len(toRemove)),
		zap.Int("total", finalCount),
	)
	_ = msg.Ack()
	return nil
}

func (h *Handler) nak(msg jetstream.Msg, attempt int, reason string, log *zap.Logger) error {
	if attempt >= h.maxRetries {
		log.Error("max retries reached", zap.String("reason", reason))
		refreshFailures.Inc()
		_ = msg.Term()
		return nil
	}
	refreshRetries.Inc()
	log.Warn("retry scheduled", zap.String("reason", reason))
	_ = msg.Nak()
	return nil
}

// diff returns keys in a that are not in b.
func diff(a, b map[string]struct{}) []string {
	var out []string
	for k := range a {
		if _, ok := b[k]; !ok {
			out = append(out, k)
		}
	}
	return out
}
