package consumers

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	infraNats "engageiq-workers/internal/infra/nats"
)

// MessageHandler processes a single message. It MUST call exactly one of
// Ack, Nak, NakWithDelay, or Term on the message before returning. Returning
// an error without acking is treated as a panic-equivalent and the message
// is Nak'd by the registry as a safety net.
type MessageHandler func(ctx context.Context, msg jetstream.Msg) error

type ConsumerConfig struct {
	Stream      string
	Subject     string
	DurableName string
	MaxDeliver  int
	AckWait     time.Duration
	BackOff     []time.Duration // server-side redelivery backoff per attempt
	Handler     MessageHandler
	HandlerTimeout time.Duration
}

type Registry struct {
	nats      *infraNats.Client
	logger    *zap.Logger
	consumers []jetstream.ConsumeContext
}

func NewRegistry(nats *infraNats.Client, logger *zap.Logger) *Registry {
	return &Registry{nats: nats, logger: logger}
}

func (r *Registry) Register(ctx context.Context, cfg ConsumerConfig) error {
	if cfg.HandlerTimeout == 0 {
		cfg.HandlerTimeout = 30 * time.Second
	}

	consumer, err := r.nats.JS.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       cfg.DurableName,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
		BackOff:       cfg.BackOff,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("create consumer %s: %w", cfg.DurableName, err)
	}

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		r.dispatch(ctx, cfg, msg)
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", cfg.DurableName, err)
	}

	r.consumers = append(r.consumers, cc)
	r.logger.Info("consumer registered",
		zap.String("durable", cfg.DurableName),
		zap.String("subject", cfg.Subject),
		zap.Int("max_deliver", cfg.MaxDeliver),
	)
	return nil
}

func (r *Registry) dispatch(parentCtx context.Context, cfg ConsumerConfig, msg jetstream.Msg) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("panic in handler",
				zap.Any("recover", rec),
				zap.String("subject", cfg.Subject),
			)
			// Nak so the message is redelivered (with backoff) for transient panics.
			_ = msg.Nak()
		}
	}()

	ctx, cancel := context.WithTimeout(parentCtx, cfg.HandlerTimeout)
	defer cancel()

	if err := cfg.Handler(ctx, msg); err != nil {
		r.logger.Error("handler returned error",
			zap.Error(err),
			zap.String("subject", cfg.Subject),
		)
		// Safety net: if handler returned an error without acking, nak it.
		_ = msg.Nak()
	}
}

func (r *Registry) Stop() {
	for _, cc := range r.consumers {
		cc.Stop()
	}
}
