# Architecture

## Overview

`engageiq-workers` is a single Go binary hosting six JetStream consumers across seven streams. All consumers share infrastructure connections (NATS, Postgres, Redis, SES).

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Fastify API                                     │
│  POST /domains/verify    →  domain.verify.poll                               │
│  POST /emails/send       →  email.send.transactional                         │
│  POST /campaigns/send    →  campaign.send.start                              │
│  POST /events/track      →  events.raw.{workspaceId}                         │
│  POST /segments/refresh  →  segment.refresh                                  │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │ NATS JetStream
   ┌───────┬───────────┬───────┴──────┬──────────────┬──────────┐
   ▼       ▼           ▼              ▼              ▼          ▼
DOMAIN  EMAIL_SEND  CAMPAIGN     EVENTS_RAW      SEGMENTS   WORKFLOW
                    ┌──┴──┐                                 (fan-out)
                 start   chunk
                    │       │
                    ▼       ▼
              Batcher   ChunkHandler
```

## Component map

| Package | Responsibility |
|---------|---------------|
| `cmd/workers` | Entrypoint. Wires infra, registers 6 consumers, starts metrics server, graceful shutdown. |
| `internal/config` | Env config via `envconfig`. |
| `internal/infra/nats` | NATS + JetStream. Ensures all 7 streams on startup. |
| `internal/infra/postgres` | pgxpool. SQL for `domains`, `email_sends`, `campaigns`, `campaign_recipients`, `contacts`, `events_raw`, `events_enriched`, `segments`, `segment_members`, `workflow_triggers`. |
| `internal/infra/redis` | Redis client. Rate limiter + distributed lock (`SET NX EX` + Lua release). |
| `internal/infra/ses` | AWS SES v2. `CheckDomainVerification`, `SendEmail`, `IsPermanentError`. |
| `internal/queue/consumers` | Consumer registry. Durable consumers, explicit ack, panic recovery, per-handler timeout. |
| `internal/queue/producers` | JetStream publisher with `Nats-Msg-Id` dedup. |
| `internal/ratelimit` | Redis-backed token bucket (atomic Lua). Per-workspace key isolation. |
| `internal/worker/ses` | Domain verification handler. |
| `internal/worker/email` | Transactional email handler, campaign start (batcher), campaign chunk handler, HTML renderer. |
| `internal/worker/events` | Event enrichment handler + identity stitcher (enricher). |
| `internal/worker/segments` | Segment computation handler + filter tree evaluator. |
| `pkg/types` | All NATS payload types. Contracts defined here. |

## JetStream streams

| Stream | Subjects | Policy | Retention | Dedup window |
|--------|----------|--------|-----------|--------------|
| `DOMAIN` | `domain.>` | WorkQueue | 96h | — |
| `EMAIL_SEND` | `email.send.>` | WorkQueue | 24h | — |
| `CAMPAIGN` | `campaign.>` | WorkQueue | 48h | 30 min |
| `EMAIL_EVENTS` | `email.delivery.>` | Limits | 7d | 10 min |
| `EVENTS_RAW` | `events.raw.>` | WorkQueue | 48h | 24h |
| `WORKFLOW` | `workflow.>` | Limits | 7d | 10 min |
| `SEGMENTS` | `segment.>` | WorkQueue | 24h | 5 min |

## Registered consumers

| Durable name | Stream | Subject | AckWait | MaxDeliver | Handler timeout |
|---|---|---|---|---|---|
| `domain-verify-worker` | `DOMAIN` | `domain.verify.poll` | 2m | 9 | 30s |
| `email-send-worker` | `EMAIL_SEND` | `email.send.transactional` | 90s | 6 | 60s |
| `campaign-start-worker` | `CAMPAIGN` | `campaign.send.start` | 5m | 6 | 10m |
| `campaign-chunk-worker` | `CAMPAIGN` | `campaign.send.chunk` | 5m | 6 | 4m |
| `events-enrichment-worker` | `EVENTS_RAW` | `events.raw.*` | 60s | configurable | 30s |
| `segment-refresh-worker` | `SEGMENTS` | `segment.refresh` | 15m | configurable | 12m |

## Message lifecycle

Every handler owns the full message lifecycle. The registry never auto-acks.

```
Message received
      │
      ├─ JSON parse fails              → Term()   (poison)
      ├─ Validation fails              → Term()   (poison)
      ├─ Idempotency: already terminal → Ack()    (safe no-op)
      ├─ Lock already held             → Ack()    (another worker processing)
      ├─ Rate limit denied             → Nak()    (BackOff)
      ├─ DB / enrichment transient     → Nak()    (BackOff)
      ├─ SES permanent error           → Ack()    (mark failed, no retry)
      ├─ Transient at max retries      → Term()   (DLQ + mark failed)
      └─ Success                       → Ack()
```

## Graceful shutdown

On `SIGINT` / `SIGTERM`:
1. Root context cancelled — all in-flight handlers receive cancellation.
2. `registry.Stop()` drains all 6 consumer subscriptions.
3. HTTP server shuts down with 10-second timeout.
4. Deferred `Close()` on NATS, Postgres, Redis.

In-flight messages redelivered by NATS after `AckWait` expires.
