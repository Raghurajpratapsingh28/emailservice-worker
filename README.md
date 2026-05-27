<div align="center">

# EngageIQ Workers

**Production-grade distributed worker infrastructure for the EngageIQ platform**

[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat-square&logo=go)](https://go.dev)
[![NATS JetStream](https://img.shields.io/badge/NATS-JetStream-27AAE1?style=flat-square&logo=natsdotio)](https://nats.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql)](https://postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat-square&logo=redis)](https://redis.io)
[![AWS SES](https://img.shields.io/badge/AWS-SES-FF9900?style=flat-square&logo=amazonaws)](https://aws.amazon.com/ses)
[![Tests](https://img.shields.io/badge/tests-126%20passing-brightgreen?style=flat-square)](#testing)

</div>

---

## Overview

`engageiq-workers` is a single Go binary that hosts **8 JetStream consumers** across **7 streams**. It handles all asynchronous workloads for the EngageIQ platform — from transactional email delivery and campaign execution to real-time event enrichment, audience segmentation, and workflow automation.

Every worker is built to the same production standard: explicit message acknowledgement, server-side backoff retries, distributed locking, idempotent database operations, structured logging, and Prometheus metrics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Fastify API                                │
│  /domains/verify  /emails/send  /campaigns/send                     │
│  /events/track    /segments/refresh  /workflows/trigger             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  NATS JetStream
        ┌──────────┬───────────┼──────────┬──────────┬───────────┐
        ▼          ▼           ▼          ▼          ▼           ▼
     DOMAIN   EMAIL_SEND   CAMPAIGN  EVENTS_RAW  SEGMENTS   WORKFLOW
                           ┌──┴──┐
                         start  chunk
```

| Stream | Policy | Retention | Workers |
|--------|--------|-----------|---------|
| `DOMAIN` | WorkQueue | 96h | `domain-verify-worker` |
| `EMAIL_SEND` | WorkQueue | 24h | `email-send-worker` |
| `CAMPAIGN` | WorkQueue | 48h | `campaign-start-worker`, `campaign-chunk-worker` |
| `EVENTS_RAW` | WorkQueue | 48h | `events-enrichment-worker` |
| `SEGMENTS` | WorkQueue | 24h | `segment-refresh-worker` |
| `WORKFLOW` | Limits | 7d | `workflow-trigger-worker`, `workflow-register-worker` |
| `EMAIL_EVENTS` | Limits | 7d | *(downstream consumers)* |

---

## Workers

### Domain Verification
Polls AWS SES for DNS verification status of customer sending domains. Retries over a 72-hour window using an exponential backoff schedule (5m → 15m → 30m → 1h → 2h → 6h → 12h → 24h).

### Transactional Email
Sends individual emails via AWS SES. Applies per-workspace Redis token bucket rate limiting (default 14/sec), idempotency checks, and publishes delivery events to `email.delivery.events`.

### Campaign Email *(two-stage pipeline)*
**Stage 1 — Start handler:** Streams segment contacts via keyset pagination, bulk-inserts `campaign_recipients` rows, and publishes one chunk job per batch of 500 recipients.  
**Stage 2 — Chunk handler:** Sends each recipient via SES with per-recipient error classification. Permanent failures are handled inline; transient failures retry the whole chunk via JetStream BackOff.

### Event Enrichment
Consumes raw analytics events (`track`, `identify`, `page`, `alias`, `group`), resolves and stitches contact identities, persists enriched events to `events_enriched`, and evaluates workflow trigger rules.

### Segment Computation
Recomputes segment membership by evaluating a filter tree against all active contacts. Uses a Redis distributed lock to prevent concurrent recomputes. Applies a membership diff (insert new, delete stale) rather than a full rebuild.

### Workflow Engine *(MVP: trigger → email → delay → end)*
Executes multi-step workflows triggered by analytics events. Supports immediate node execution (trigger, email, end) and time-delayed pauses. A DB-backed scheduler polls every 30 seconds to resume delayed executions.

---

## Quick Start

```bash
# 1. Clone
git clone <repo> && cd engageiq-workers

# 2. Configure
cp .env.example .env
# Edit .env — set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

# 3. Start everything
docker compose -f infra/docker/docker-compose.yml up --build
```

The compose file starts PostgreSQL 16, Redis 7, NATS 2.10 (with JetStream), and the workers binary. All services use health checks — workers will not start until all dependencies are ready.

**Local development (infra only):**
```bash
docker compose -f infra/docker/docker-compose.yml up postgres redis nats
go run ./cmd/workers
```

---

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | — | NATS server URL *(required)* |
| `DATABASE_URL` | — | PostgreSQL DSN *(required)* |
| `REDIS_URL` | — | Redis URL *(required)* |
| `AWS_REGION` | — | AWS region for SES *(required)* |
| `AWS_ACCESS_KEY_ID` | — | Omit when using IAM roles |
| `AWS_SECRET_ACCESS_KEY` | — | Omit when using IAM roles |
| `SES_RATE_LIMIT_PER_SEC` | `14` | Per-workspace SES send rate |
| `CAMPAIGN_CHUNK_SIZE` | `500` | Recipients per campaign chunk |
| `EVENT_MAX_RETRIES` | `5` | Max event enrichment retries |
| `SEGMENT_MAX_RETRIES` | `5` | Max segment refresh retries |
| `WORKFLOW_MAX_RETRIES` | `5` | Max workflow execution retries |
| `WORKFLOW_SCHEDULER_POLL_INTERVAL` | `30s` | Delay scheduler poll interval |
| `METRICS_PORT` | `9090` | Prometheus metrics port |

---

## Observability

### Metrics

Prometheus metrics are exposed at `http://localhost:9090/metrics`. **36 counters** across all workers:

| Prefix | Metrics |
|--------|---------|
| `domain_verification_*` | success, failure, retries |
| `email_*` | processed, sent, failed, retries, ses_failures, rate_limit_waits, dlq |
| `campaign_*` | processed, completed, recipients_resolved, chunks_created, emails_sent, emails_failed, chunk_retries, chunk_dlq |
| `events_*` | processed, enrich_failures, workflows_triggered, retries, duplicate_drops |
| `segment_*` | refresh_jobs, contacts_matched, membership_inserts, membership_removals, failures, retries |
| `workflow_*` | triggers, executions_started, emails_triggered, delays_scheduled, completions, failures |

### Health

```
GET http://localhost:9090/health  →  200 OK
```

### Structured Logs

All logs are JSON via `go.uber.org/zap`. Every log line includes worker-specific context fields (`send_id`, `campaign_id`, `event_id`, `segment_id`, `execution_id`, etc.) for easy correlation in log aggregation systems.

---

## Testing

```bash
go test ./...          # 126 tests, all passing
go test -race ./...    # with race detector
```

| Package | Tests | Coverage |
|---------|-------|---------|
| `internal/worker/email` | 34 | Transactional + campaign (success, retry, DLQ, duplicate, rate limit, render) |
| `internal/worker/events` | 26 | Track, identify, alias, merge, workflow trigger, duplicate, malformed |
| `internal/worker/segments` | 30 | All 10 operators, AND/OR/nested, event filters, lock, diff, stale cleanup |
| `internal/worker/ses` | 12 | Verified, pending, failed, SES error, max retries, duplicate |
| `internal/worker/workflows` | 19 | Trigger→email→end, delay, resume, duplicate lock, malformed |
| `internal/ratelimit` | 7 | Burst, refill, isolation, blocking, context cancel, concurrent |

Tests require no running infrastructure. Rate limiter tests use `miniredis`. All other tests use interface mocks.

---

## Project Structure

```
engageiq-workers/
├── cmd/workers/              # Binary entrypoint
├── internal/
│   ├── config/               # Environment config
│   ├── infra/
│   │   ├── nats/             # JetStream client (7 streams)
│   │   ├── postgres/         # pgxpool + all SQL
│   │   ├── redis/            # Client + distributed lock
│   │   └── ses/              # AWS SES v2
│   ├── queue/
│   │   ├── consumers/        # Registry (panic recovery, timeouts, explicit ack)
│   │   └── producers/        # Publisher with Nats-Msg-Id dedup
│   ├── ratelimit/            # Redis token bucket (atomic Lua)
│   └── worker/
│       ├── ses/              # Domain verification
│       ├── email/            # Transactional + campaign
│       ├── events/           # Event enrichment + identity stitching
│       ├── segments/         # Segment computation + filter evaluator
│       └── workflows/        # Workflow engine + delay scheduler
├── pkg/types/                # All NATS payload contracts
├── docs/                     # Full documentation
└── infra/
    ├── docker/               # Dockerfile + docker-compose.yml
    └── k8s/                  # Kubernetes manifests (Kustomize)
```

---

## Documentation

Full documentation is in [`docs/`](docs/):

| Document | Description |
|----------|-------------|
| [architecture.md](docs/architecture.md) | System design, streams, consumers, message lifecycle |
| [queue-contracts.md](docs/queue-contracts.md) | All NATS payload schemas (locked) |
| [configuration.md](docs/configuration.md) | All environment variables |
| [observability.md](docs/observability.md) | All metrics, alerts, log events |
| [runbook.md](docs/runbook.md) | On-call procedures, DLQ recovery, scaling |
| [development.md](docs/development.md) | Local setup, conventions, adding new workers |
| [workers/domain-verification.md](docs/workers/domain-verification.md) | Domain verification worker |
| [workers/transactional-email.md](docs/workers/transactional-email.md) | Transactional email worker |
| [workers/campaign-email.md](docs/workers/campaign-email.md) | Campaign email worker |
| [workers/event-enrichment.md](docs/workers/event-enrichment.md) | Event enrichment worker |
| [workers/segment-computation.md](docs/workers/segment-computation.md) | Segment computation worker |

---

## Production Deployment

**AWS IAM (recommended):** Assign an IAM role to the ECS task or EKS pod with `ses:SendEmail` and `ses:GetIdentityVerificationAttributes`. Do not set `AWS_ACCESS_KEY_ID` in production.

**Scaling:** Workers are stateless. Scale horizontally — NATS WorkQueue policy ensures each message is delivered to exactly one replica. Redis rate limits and distributed locks are shared across all replicas.

```bash
kubectl scale deployment engageiq-workers --replicas=5
```

Kubernetes manifests are in [`infra/k8s/`](infra/k8s/) (Kustomize, staging + production overlays).

---

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Go | 1.25 | Runtime |
| NATS JetStream | 2.10 | Message queue |
| PostgreSQL | 16 | Persistent state |
| Redis | 7 | Rate limiting + distributed locks |
| AWS SES | v2 SDK | Email delivery |
| Prometheus | — | Metrics |
| Zap | — | Structured logging |
