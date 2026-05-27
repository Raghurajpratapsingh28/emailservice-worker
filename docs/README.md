# EngageIQ Workers — Documentation

Production worker infrastructure for the EngageIQ platform. Consumes NATS JetStream queues, sends email via AWS SES, and writes state to PostgreSQL.

## Contents

| Document | Description |
|----------|-------------|
| [architecture.md](./architecture.md) | System design, component map, data flow |
| [workers/domain-verification.md](./workers/domain-verification.md) | Domain DNS verification worker |
| [workers/transactional-email.md](./workers/transactional-email.md) | Transactional email send worker |
| [workers/campaign-email.md](./workers/campaign-email.md) | Campaign bulk email worker |
| [workers/event-enrichment.md](./workers/event-enrichment.md) | Event enrichment + identity stitching worker |
| [workers/segment-computation.md](./workers/segment-computation.md) | Segment membership computation worker |
| [queue-contracts.md](./queue-contracts.md) | All NATS subject contracts (locked) |
| [configuration.md](./configuration.md) | Environment variables and defaults |
| [observability.md](./observability.md) | Metrics, structured logs, health endpoint |
| [rate-limiting.md](./rate-limiting.md) | Redis token bucket design |
| [runbook.md](./runbook.md) | On-call runbook: alerts, DLQ, recovery |
| [development.md](./development.md) | Local setup, testing, contributing |

## Quick start

```bash
cp .env.example .env
docker compose -f infra/docker/docker-compose.yml up -d
go run ./cmd/workers
```

## Stack

- **Go 1.25** — runtime
- **NATS JetStream** — message queue (7 streams, 6 consumers)
- **PostgreSQL** — persistent state
- **Redis** — distributed rate limiting + distributed locks
- **AWS SES v2** — email delivery
- **Prometheus** — metrics (30 counters)
- **Zap** — structured logging
