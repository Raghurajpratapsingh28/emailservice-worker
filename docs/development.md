# Development

## Prerequisites

- Go 1.25+
- Docker + Docker Compose
- `nats` CLI (optional): `brew install nats-io/nats-tools/nats`

## Local setup

```bash
git clone <repo> && cd engageiq-workers
docker compose -f infra/docker/docker-compose.yml up -d
cp .env.example .env   # fill in AWS credentials
go run ./cmd/workers
```

## Running tests

```bash
go test ./...                              # all tests
go test ./internal/worker/email/ -v        # transactional + campaign (34 tests)
go test ./internal/worker/events/ -v       # event enrichment (26 tests)
go test ./internal/worker/segments/ -v     # segment computation (30 tests)
go test ./internal/worker/ses/ -v          # domain verification (12 tests)
go test ./internal/ratelimit/ -v           # rate limiter (7 tests)
go test -race ./...                        # with race detector
```

Tests require no running infrastructure. Rate limiter tests use `miniredis`. All other tests use interface mocks.

## Project structure

```
engageiq-workers/
├── cmd/workers/          # Binary entrypoint (6 consumers registered)
├── internal/
│   ├── config/           # Env config (12 variables)
│   ├── infra/
│   │   ├── nats/         # NATS + JetStream (7 streams)
│   │   ├── postgres/     # pgxpool + all SQL queries
│   │   ├── redis/        # Redis client + distributed lock
│   │   └── ses/          # AWS SES v2 client
│   ├── queue/
│   │   ├── consumers/    # Consumer registry (panic recovery, timeouts)
│   │   └── producers/    # JetStream publisher with dedup
│   ├── ratelimit/        # Redis token bucket (Lua script)
│   └── worker/
│       ├── ses/          # Domain verification handler
│       ├── email/        # Transactional + campaign handlers + renderer
│       ├── events/       # Event enrichment handler + identity enricher
│       └── segments/     # Segment computation handler + filter evaluator
├── pkg/
│   └── types/            # All NATS payload types (locked contracts)
├── docs/                 # Documentation
└── infra/
    ├── docker/
    └── k8s/
```

## Adding a new worker

1. Add payload type to `pkg/types/nats_payloads.go`.
2. Add stream to `internal/infra/nats/client.go` (`ensureStreams`).
3. Add DB methods to `internal/infra/postgres/client.go`.
4. Create `internal/worker/{name}/handler.go` — implement `Handle(ctx, msg) error`.
5. Register consumer in `cmd/workers/main.go`.
6. Write tests in `internal/worker/{name}/handler_test.go`.

## Code conventions

- **Handler owns ack/nak/term.** Every code path must call exactly one of `Ack()`, `Nak()`, or `Term()`.
- **Interfaces for testability.** Handlers depend on interfaces, not concrete types.
- **Idempotent SQL.** All `UPDATE` statements are conditional on current status.
- **Structured logging.** Use `logger.With(...)` to attach context fields at the start of each handler.
- **No panics.** The registry recovers panics and naks, but handlers should handle errors explicitly.

## Build

```bash
go build -o bin/workers ./cmd/workers
go vet ./...
```
