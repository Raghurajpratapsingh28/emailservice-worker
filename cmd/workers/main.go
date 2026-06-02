package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"engageiq-workers/internal/config"
	infraNats "engageiq-workers/internal/infra/nats"
	"engageiq-workers/internal/infra/postgres"
	infraRedis "engageiq-workers/internal/infra/redis"
	infraSes "engageiq-workers/internal/infra/ses"
	"engageiq-workers/internal/queue/consumers"
	"engageiq-workers/internal/queue/producers"
	"engageiq-workers/internal/ratelimit"
	emailWorker "engageiq-workers/internal/worker/email"
	eventsWorker "engageiq-workers/internal/worker/events"
	segmentsWorker "engageiq-workers/internal/worker/segments"
	sesWorker "engageiq-workers/internal/worker/ses"
	workflowsWorker "engageiq-workers/internal/worker/workflows"
)

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("config load failed", zap.Error(err))
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Infrastructure ---
	natsClient, err := infraNats.NewClient(rootCtx, cfg.NatsURL, logger)
	if err != nil {
		logger.Fatal("nats init failed", zap.Error(err))
	}
	defer natsClient.Close()

	db, err := postgres.NewClient(rootCtx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("postgres init failed", zap.Error(err))
	}
	defer db.Close()

	rdb, err := infraRedis.NewClient(rootCtx, cfg.RedisURL, logger)
	if err != nil {
		logger.Fatal("redis init failed", zap.Error(err))
	}
	defer func() { _ = rdb.Close() }()

	sesClient, err := infraSes.NewClient(rootCtx, cfg.AWSRegion, logger)
	if err != nil {
		logger.Fatal("ses init failed", zap.Error(err))
	}

	// --- Shared collaborators ---
	publisher := producers.NewPublisher(natsClient, logger)
	limiter := ratelimit.NewTokenBucket(rdb.R, cfg.SESRateLimitPerSec)
	registry := consumers.NewRegistry(natsClient, logger)

	// --- Domain verification consumer ---
	domainHandler := sesWorker.NewHandler(sesClient, db, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "DOMAIN",
		Subject:        "domain.verify.poll",
		DurableName:    "domain-verify-worker",
		MaxDeliver:     -1,
		AckWait:        60 * time.Second,
		Handler:        domainHandler.Handle,
		HandlerTimeout: 30 * time.Second,
	}); err != nil {
		logger.Fatal("domain consumer register failed", zap.Error(err))
	}

	// --- Transactional email consumer ---
	emailHandler := emailWorker.NewHandler(sesClient, db, limiter, publisher, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "EMAIL_SEND",
		Subject:        emailWorker.SubjectSend,
		DurableName:    "email-send-worker",
		MaxDeliver:     emailWorker.MaxAttempts,
		AckWait:        90 * time.Second,
		BackOff:        emailWorker.RetryDelays,
		Handler:        emailHandler.Handle,
		HandlerTimeout: 60 * time.Second,
	}); err != nil {
		logger.Fatal("email consumer register failed", zap.Error(err))
	}

	// --- Campaign start consumer ---
	campaignStart := emailWorker.NewCampaignStartHandler(db, publisher, cfg.CampaignChunkSize, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:      "CAMPAIGN",
		Subject:     emailWorker.SubjectCampaignStart,
		DurableName: "campaign-start-worker",
		MaxDeliver:  emailWorker.MaxAttempts,
		AckWait:     5 * time.Minute, // segment streaming may take a while for large audiences
		BackOff:     emailWorker.RetryDelays,
		Handler:     campaignStart.Handle,
		// Larger handler timeout: streaming + bulk insert + chunk publish for huge segments
		HandlerTimeout: 10 * time.Minute,
	}); err != nil {
		logger.Fatal("campaign start consumer register failed", zap.Error(err))
	}

	// --- Campaign chunk consumer ---
	campaignChunk := emailWorker.NewCampaignChunkHandler(sesClient, db, limiter, publisher, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "CAMPAIGN",
		Subject:        emailWorker.SubjectCampaignChunk,
		DurableName:    "campaign-chunk-worker",
		MaxDeliver:     emailWorker.MaxAttempts,
		AckWait:        5 * time.Minute,
		BackOff:        emailWorker.RetryDelays,
		Handler:        campaignChunk.Handle,
		HandlerTimeout: 4 * time.Minute,
	}); err != nil {
		logger.Fatal("campaign chunk consumer register failed", zap.Error(err))
	}

	// --- Event enrichment consumer (wildcard: events.raw.*) ---
	eventsHandler := eventsWorker.NewHandler(db, publisher, cfg.EventMaxRetries+1, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "EVENTS_RAW",
		Subject:        eventsWorker.SubjectEventsRaw,
		DurableName:    "events-enrichment-worker",
		MaxDeliver:     cfg.EventMaxRetries + 1,
		AckWait:        60 * time.Second,
		BackOff:        eventsWorker.RetryDelays,
		Handler:        eventsHandler.Handle,
		HandlerTimeout: 30 * time.Second,
	}); err != nil {
		logger.Fatal("events consumer register failed", zap.Error(err))
	}

	// --- Segment computation consumer ---
	segmentHandler := segmentsWorker.NewHandler(db, rdb, cfg.SegmentMaxRetries+1, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "SEGMENTS",
		Subject:        segmentsWorker.SubjectSegmentRefresh,
		DurableName:    "segment-refresh-worker",
		MaxDeliver:     cfg.SegmentMaxRetries + 1,
		AckWait:        15 * time.Minute,
		BackOff:        segmentsWorker.RetryDelays,
		Handler:        segmentHandler.Handle,
		HandlerTimeout: 12 * time.Minute,
	}); err != nil {
		logger.Fatal("segment consumer register failed", zap.Error(err))
	}

	// --- Workflow trigger consumer ---
	wfTrigger := workflowsWorker.NewTriggerHandler(db, publisher, rdb, cfg.WorkflowMaxRetries+1, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "WORKFLOW",
		Subject:        workflowsWorker.SubjectWorkflowTrigger,
		DurableName:    "workflow-trigger-worker",
		MaxDeliver:     cfg.WorkflowMaxRetries + 1,
		AckWait:        2 * time.Minute,
		BackOff:        workflowsWorker.RetryDelays,
		Handler:        wfTrigger.Handle,
		HandlerTimeout: 90 * time.Second,
	}); err != nil {
		logger.Fatal("workflow trigger consumer register failed", zap.Error(err))
	}

	// --- Workflow register consumer ---
	wfRegister := workflowsWorker.NewRegisterHandler(db, logger)
	if err := registry.Register(rootCtx, consumers.ConsumerConfig{
		Stream:         "WORKFLOW",
		Subject:        workflowsWorker.SubjectWorkflowRegister,
		DurableName:    "workflow-register-worker",
		MaxDeliver:     3,
		AckWait:        30 * time.Second,
		Handler:        wfRegister.Handle,
		HandlerTimeout: 15 * time.Second,
	}); err != nil {
		logger.Fatal("workflow register consumer register failed", zap.Error(err))
	}

	// --- Workflow delay scheduler (background goroutine) ---
	executor := workflowsWorker.NewExecutor(db, publisher, logger)
	scheduler := workflowsWorker.NewScheduler(db, executor, rdb, cfg.WorkflowSchedulerPollInterval, logger)
	go scheduler.Run(rootCtx)

	// --- Metrics + health HTTP server ---
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	logger.Info("worker started",
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.String("aws_region", cfg.AWSRegion),
		zap.Int("ses_rate_limit_per_sec", cfg.SESRateLimitPerSec),
		zap.Int("campaign_chunk_size", cfg.CampaignChunkSize),
	)

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown signal received", zap.String("signal", sig.String()))

	cancel()
	registry.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", zap.Error(err))
	}
	logger.Info("shutdown complete")
}
