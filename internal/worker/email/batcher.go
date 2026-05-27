package email

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

const (
	SubjectCampaignStart = "campaign.send.start"
	SubjectCampaignChunk = "campaign.send.chunk"
	SubjectCampaignDLQ   = "campaign.send.dlq"
)

// Campaign status values.
const (
	CampaignStatusQueued    = "queued"
	CampaignStatusSending   = "sending"
	CampaignStatusCompleted = "completed"
	CampaignStatusFailed    = "failed"
)

// Recipient status values.
const (
	RecipientStatusQueued  = "queued"
	RecipientStatusSending = "sending"
	RecipientStatusSent    = "sent"
	RecipientStatusFailed  = "failed"
	RecipientStatusBounced = "bounced"
)

// --- metrics ---

var (
	campaignsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_processed_total",
		Help: "Total campaign-start jobs consumed.",
	})
	campaignsCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_completed_total",
		Help: "Total campaigns marked completed.",
	})
	campaignRecipientsResolved = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_recipients_resolved_total",
		Help: "Total contacts resolved from segments.",
	})
	campaignChunksCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "campaign_chunks_created_total",
		Help: "Total chunk jobs published.",
	})
)

// --- collaborator interfaces (start handler / batcher) ---

// SegmentDB is the surface used by the batcher to resolve and persist recipients.
type SegmentDB interface {
	GetCampaignStatus(ctx context.Context, campaignID, workspaceID string) (string, error)
	FetchSegmentContactsBatch(ctx context.Context, workspaceID, segmentID, afterID string, limit int) ([]postgres.Contact, error)
	BulkInsertCampaignRecipients(ctx context.Context, campaignID, workspaceID string, contacts []postgres.Contact) ([]postgres.CampaignRecipientRow, error)
	UpdateCampaignStarted(ctx context.Context, campaignID, workspaceID string, totalRecipients int) error
	MarkCampaignCompleteEmpty(ctx context.Context, campaignID, workspaceID string) error
	UpdateCampaignFailed(ctx context.Context, campaignID, workspaceID, reason string) error
}

// --- batcher ---

// Batcher streams segment members in keyset-paginated batches, persists
// campaign_recipients rows, and publishes one chunk job per batch.
type Batcher struct {
	db        SegmentDB
	pub       EventPublisher
	chunkSize int
	logger    *zap.Logger
}

func NewBatcher(db SegmentDB, pub EventPublisher, chunkSize int, logger *zap.Logger) *Batcher {
	if chunkSize < 1 {
		chunkSize = 500
	}
	return &Batcher{db: db, pub: pub, chunkSize: chunkSize, logger: logger}
}

// StreamAndChunk resolves the campaign's segment, writes campaign_recipients
// rows in batches of chunkSize, and publishes a chunk job per batch.
//
// Returns (totalRecipients, chunksCreated). Chunk publishing is idempotent:
// each chunk's NATS msgID is deterministic ({campaignId}-chunk-{idx}), so
// re-running this method with overlapping data is safe within the stream's
// dedup window.
func (b *Batcher) StreamAndChunk(
	ctx context.Context,
	p *types.CampaignStartPayload,
	log *zap.Logger,
) (int, int, error) {
	totalRecipients := 0
	chunkIdx := 0
	afterID := ""

	for {
		select {
		case <-ctx.Done():
			return totalRecipients, chunkIdx, ctx.Err()
		default:
		}

		contacts, err := b.db.FetchSegmentContactsBatch(ctx, p.WorkspaceID, p.SegmentID, afterID, b.chunkSize)
		if err != nil {
			return totalRecipients, chunkIdx, fmt.Errorf("fetch contacts: %w", err)
		}
		if len(contacts) == 0 {
			break
		}

		recipients, err := b.db.BulkInsertCampaignRecipients(ctx, p.CampaignID, p.WorkspaceID, contacts)
		if err != nil {
			return totalRecipients, chunkIdx, fmt.Errorf("insert recipients (chunk %d): %w", chunkIdx, err)
		}
		campaignRecipientsResolved.Add(float64(len(recipients)))

		chunk := types.CampaignChunkPayload{
			CampaignID:  p.CampaignID,
			WorkspaceID: p.WorkspaceID,
			ChunkID:     fmt.Sprintf("%s-chunk-%d", p.CampaignID, chunkIdx),
			Sender:      p.Sender,
			ReplyTo:     p.ReplyTo,
			Subject:     p.Subject,
			HTML:        p.HTML,
			Text:        p.Text,
			Recipients:  make([]types.CampaignChunkRecipient, 0, len(recipients)),
		}
		for _, r := range recipients {
			chunk.Recipients = append(chunk.Recipients, types.CampaignChunkRecipient{
				RecipientID: r.ID,
				Email:       r.Email,
				Name:        r.Name,
			})
		}

		if err := b.pub.Publish(ctx, SubjectCampaignChunk, chunk, chunk.ChunkID); err != nil {
			return totalRecipients, chunkIdx, fmt.Errorf("publish chunk %d: %w", chunkIdx, err)
		}
		campaignChunksCreated.Inc()

		log.Info("chunk created",
			zap.Int("chunk_idx", chunkIdx),
			zap.String("chunk_id", chunk.ChunkID),
			zap.Int("recipients", len(recipients)),
		)

		afterID = contacts[len(contacts)-1].ID
		totalRecipients += len(recipients)
		chunkIdx++

		// If the DB returned fewer rows than requested, we're at the end.
		if len(contacts) < b.chunkSize {
			break
		}
	}

	return totalRecipients, chunkIdx, nil
}

// --- campaign start handler ---

// CampaignStartHandler consumes campaign.send.start, streams the segment,
// publishes chunk jobs, and updates the campaign row.
type CampaignStartHandler struct {
	batcher *Batcher
	db      SegmentDB
	logger  *zap.Logger
}

func NewCampaignStartHandler(db SegmentDB, pub EventPublisher, chunkSize int, logger *zap.Logger) *CampaignStartHandler {
	return &CampaignStartHandler{
		batcher: NewBatcher(db, pub, chunkSize, logger),
		db:      db,
		logger:  logger,
	}
}

// Handle owns the message lifecycle (Ack / Nak / Term).
func (h *CampaignStartHandler) Handle(ctx context.Context, msg jetstream.Msg) error {
	campaignsProcessed.Inc()

	var payload types.CampaignStartPayload
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		h.logger.Error("malformed campaign payload, terminating",
			zap.Error(err), zap.ByteString("data", msg.Data()))
		_ = msg.Term()
		return nil
	}
	if err := validateCampaignStart(&payload); err != nil {
		h.logger.Error("invalid campaign payload, terminating",
			zap.Error(err), zap.String("campaign_id", payload.CampaignID))
		_ = msg.Term()
		return nil
	}

	attempt := 1
	if md, err := msg.Metadata(); err == nil && md != nil {
		attempt = int(md.NumDelivered)
	}

	log := h.logger.With(
		zap.String("campaign_id", payload.CampaignID),
		zap.String("workspace_id", payload.WorkspaceID),
		zap.String("segment_id", payload.SegmentID),
		zap.String("job_id", payload.JobID),
		zap.Int("attempt", attempt),
	)
	log.Info("campaign job received")

	// Idempotency: skip campaigns already past the queued state.
	status, err := h.db.GetCampaignStatus(ctx, payload.CampaignID, payload.WorkspaceID)
	if err != nil {
		log.Error("get campaign status failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}
	switch status {
	case CampaignStatusCompleted, CampaignStatusFailed:
		log.Info("campaign already terminal, skipping", zap.String("status", status))
		_ = msg.Ack()
		return nil
	case CampaignStatusSending:
		// A previous attempt may have crashed mid-way. Re-running is safe
		// because chunk publish dedup + recipient ON CONFLICT make the
		// streaming idempotent. Continue.
		log.Warn("campaign already in sending state, resuming")
	}

	total, chunks, err := h.batcher.StreamAndChunk(ctx, &payload, log)
	if err != nil {
		log.Error("stream-and-chunk failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}

	if total == 0 {
		// Empty segment — mark complete and ack.
		if err := h.db.MarkCampaignCompleteEmpty(ctx, payload.CampaignID, payload.WorkspaceID); err != nil {
			log.Error("mark complete (empty segment) failed", zap.Error(err))
			_ = msg.Nak()
			return nil
		}
		campaignsCompleted.Inc()
		log.Info("contacts resolved (empty segment), campaign completed")
		_ = msg.Ack()
		return nil
	}

	if err := h.db.UpdateCampaignStarted(ctx, payload.CampaignID, payload.WorkspaceID, total); err != nil {
		log.Error("update campaign started failed", zap.Error(err))
		_ = msg.Nak()
		return nil
	}

	log.Info("contacts resolved, chunks dispatched",
		zap.Int("total_recipients", total),
		zap.Int("chunks_created", chunks),
	)
	_ = msg.Ack()
	return nil
}

func validateCampaignStart(p *types.CampaignStartPayload) error {
	if p == nil {
		return errors.New("nil payload")
	}
	if p.JobID == "" || p.WorkspaceID == "" || p.CampaignID == "" || p.SegmentID == "" {
		return errors.New("missing required ids")
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
	return nil
}
