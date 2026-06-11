package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type Client struct {
	Conn   *nats.Conn
	JS     jetstream.JetStream
	logger *zap.Logger
}

func NewClient(ctx context.Context, url string, logger *zap.Logger) (*Client, error) {
	nc, err := nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("nats disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			logger.Info("nats reconnected")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	c := &Client{Conn: nc, JS: js, logger: logger}
	if err := c.ensureStreams(ctx); err != nil {
		nc.Close()
		return nil, err
	}
	return c, nil
}

// ensureStreams creates or updates all streams used by workers in this binary.
//
//   - DOMAIN: domain verification polling (work queue, 96h retention)
//   - EMAIL_SEND: transactional email send queue + DLQ (work queue, 24h retention)
//   - EMAIL_EVENTS: delivery events fan-out (limits-based, 7d retention,
//     enables dedup via Nats-Msg-Id)
func (c *Client) ensureStreams(ctx context.Context) error {
	streams := []jetstream.StreamConfig{
		{
			Name:      "DOMAIN",
			Subjects:  []string{"domain.>"},
			Retention: jetstream.WorkQueuePolicy,
			// No MaxAge — the verification poller runs indefinitely.
			// Domain expiry is handled by DomainCleanupScheduler (default: 30 days).
		},
		{
			Name:      "EMAIL_SEND",
			Subjects:  []string{"email.send.>"},
			Retention: jetstream.WorkQueuePolicy,
			MaxAge:    24 * time.Hour,
		},
		{
			Name:       "CAMPAIGN",
			Subjects:   []string{"campaign.>"},
			Retention:  jetstream.WorkQueuePolicy,
			MaxAge:     48 * time.Hour,
			Duplicates: 30 * time.Minute,
		},
		{
			Name:       "EMAIL_EVENTS",
			Subjects:   []string{"email.delivery.>"},
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     7 * 24 * time.Hour,
			Duplicates: 10 * time.Minute,
		},
		{
			// EVENTS_RAW: wildcard per-workspace raw event ingestion.
			// WorkQueue ensures each event is processed by exactly one consumer.
			Name:       "EVENTS_RAW",
			Subjects:   []string{"events.raw.>"},
			Retention:  jetstream.WorkQueuePolicy,
			MaxAge:     48 * time.Hour,
			Duplicates: 24 * time.Hour, // dedup window covers re-ingestion within a day
		},
		{
			// WORKFLOW: workflow trigger fan-out. Limits policy so multiple
			// workflow engine consumers can subscribe independently.
			Name:       "WORKFLOW",
			Subjects:   []string{"workflow.>"},
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     7 * 24 * time.Hour,
			Duplicates: 10 * time.Minute,
		},
		{
			// SEGMENTS: segment refresh work queue. Dedup window prevents
			// redundant recomputes when multiple refresh jobs arrive for the
			// same segment within a short window.
			Name:       "SEGMENTS",
			Subjects:   []string{"segment.>"},
			Retention:  jetstream.WorkQueuePolicy,
			MaxAge:     24 * time.Hour,
			Duplicates: 5 * time.Minute,
		},
	}
	for _, s := range streams {
		if _, err := c.JS.CreateOrUpdateStream(ctx, s); err != nil {
			return fmt.Errorf("ensure stream %s: %w", s.Name, err)
		}
	}
	return nil
}

func (c *Client) Close() {
	c.Conn.Close()
}
