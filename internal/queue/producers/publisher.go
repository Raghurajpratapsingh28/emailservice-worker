package producers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	infraNats "engageiq-workers/internal/infra/nats"
)

// Publisher publishes JSON payloads to JetStream subjects with optional
// per-message deduplication via Nats-Msg-Id.
type Publisher struct {
	js     jetstream.JetStream
	logger *zap.Logger
}

func NewPublisher(natsClient *infraNats.Client, logger *zap.Logger) *Publisher {
	return &Publisher{js: natsClient.JS, logger: logger}
}

// Publish marshals payload as JSON and publishes to subject. If msgID is
// non-empty, it is used as the deduplication key (within the stream's
// Duplicates window).
func (p *Publisher) Publish(ctx context.Context, subject string, payload any, msgID string) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("publisher marshal: %w", err)
	}
	opts := []jetstream.PublishOpt{}
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}
	if _, err := p.js.Publish(ctx, subject, data, opts...); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}
