package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	NatsURL            string `envconfig:"NATS_URL" required:"true"`
	DatabaseURL        string `envconfig:"DATABASE_URL" required:"true"`
	RedisURL           string `envconfig:"REDIS_URL" required:"true"`
	AWSRegion          string `envconfig:"AWS_REGION" required:"true"`
	MetricsPort        int    `envconfig:"METRICS_PORT" default:"9090"`
	SESRateLimitPerSec int    `envconfig:"SES_RATE_LIMIT_PER_SEC" default:"14"`
	CampaignChunkSize  int    `envconfig:"CAMPAIGN_CHUNK_SIZE" default:"500"`
	EventMaxRetries    int    `envconfig:"EVENT_MAX_RETRIES" default:"5"`
	EventBatchSize     int    `envconfig:"EVENT_BATCH_SIZE" default:"100"`
	SegmentMaxRetries  int    `envconfig:"SEGMENT_MAX_RETRIES" default:"5"`
	WorkflowMaxRetries int    `envconfig:"WORKFLOW_MAX_RETRIES" default:"5"`
	WorkflowSchedulerPollInterval  time.Duration `envconfig:"WORKFLOW_SCHEDULER_POLL_INTERVAL" default:"30s"`
	CampaignSchedulerPollInterval  time.Duration `envconfig:"CAMPAIGN_SCHEDULER_POLL_INTERVAL" default:"60s"`
	DomainCleanupInterval          time.Duration `envconfig:"DOMAIN_CLEANUP_INTERVAL" default:"6h"`
	DomainVerificationStaleAfter   time.Duration `envconfig:"DOMAIN_VERIFICATION_STALE_AFTER" default:"720h"` // 30 days
	DataCleanupInterval            time.Duration `envconfig:"DATA_CLEANUP_INTERVAL" default:"6h"`
	DataRetainFor                  time.Duration `envconfig:"DATA_RETAIN_FOR" default:"1080h"` // 45 days
}

func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
