package email

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"engageiq-workers/internal/infra/postgres"
	"engageiq-workers/pkg/types"
)

const (
	campaignSchedulerLockKey = "campaign:scheduler:lock"
	campaignSchedulerBatch   = 20
)

// CampaignSchedulerDB is the subset of postgres.Client used by CampaignScheduler.
type CampaignSchedulerDB interface {
	GetDueCampaigns(ctx context.Context, limit int) ([]postgres.DueCampaign, error)
}

// CampaignPublisher can publish a CampaignStartPayload to NATS.
type CampaignPublisher interface {
	Publish(ctx context.Context, subject string, payload any, msgID string) error
}

// CampaignScheduler polls for campaigns whose scheduled_at has passed and
// fires them by publishing a campaign.send.start NATS message.
type CampaignScheduler struct {
	db        CampaignSchedulerDB
	publisher CampaignPublisher
	interval  time.Duration
	logger    *zap.Logger
}

func NewCampaignScheduler(
	db CampaignSchedulerDB,
	publisher CampaignPublisher,
	interval time.Duration,
	logger *zap.Logger,
) *CampaignScheduler {
	return &CampaignScheduler{db: db, publisher: publisher, interval: interval, logger: logger}
}

// Run starts the polling loop. Blocks until ctx is cancelled.
func (s *CampaignScheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.logger.Info("campaign scheduler started", zap.Duration("interval", s.interval))
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("campaign scheduler stopped")
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

func (s *CampaignScheduler) poll(ctx context.Context) {
	due, err := s.db.GetDueCampaigns(ctx, campaignSchedulerBatch)
	if err != nil {
		s.logger.Error("campaign scheduler: fetch due campaigns failed", zap.Error(err))
		return
	}
	if len(due) == 0 {
		return
	}

	s.logger.Info("campaign scheduler: firing due campaigns", zap.Int("count", len(due)))
	for _, c := range due {
		s.fire(ctx, c)
	}
}

func (s *CampaignScheduler) fire(ctx context.Context, c postgres.DueCampaign) {
	jobID := uuid.NewString()
	payload := types.CampaignStartPayload{
		JobID:       jobID,
		WorkspaceID: c.WorkspaceID,
		CampaignID:  c.ID,
		SegmentID:   c.SegmentID,
		Sender: types.EmailAddress{
			Email: c.SenderEmail,
			Name:  c.SenderName,
		},
		ReplyTo: c.ReplyTo,
		Subject: c.Subject,
		HTML:    c.HTMLBody,
		Text:    c.TextBody,
	}

	if err := s.publisher.Publish(ctx, SubjectCampaignStart, payload, jobID); err != nil {
		s.logger.Error("campaign scheduler: publish failed",
			zap.String("campaign_id", c.ID),
			zap.Error(err),
		)
		return
	}

	s.logger.Info("campaign scheduler: campaign fired",
		zap.String("campaign_id", c.ID),
		zap.String("workspace_id", c.WorkspaceID),
	)
}
