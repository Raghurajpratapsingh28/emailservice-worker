# Event Enrichment Worker

Consumes raw analytics events, resolves and stitches contact identities, persists enriched events, and evaluates workflow trigger rules.

## Subject

`events.raw.*` (wildcard — one subject per workspace: `events.raw.{workspaceId}`)  
**Stream:** `EVENTS_RAW` | **Consumer:** `events-enrichment-worker`

## Consumer config

| Setting | Value |
|---------|-------|
| Durable name | `events-enrichment-worker` |
| Max deliveries | `EVENT_MAX_RETRIES + 1` (default 6) |
| Ack wait | 60 seconds |
| Handler timeout | 30 seconds |

## Retry schedule (JetStream BackOff)

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1 → 2 | 1 min | 1 min |
| 2 → 3 | 5 min | 6 min |
| 3 → 4 | 15 min | 21 min |
| 4 → 5 | 30 min | 51 min |

## Processing flow

```
Receive events.raw.{workspaceId}
      │
      ├─ Parse / validate → Term() on poison
      ├─ GetRawEventStatus
      │     └─ status='processed' → Ack() (duplicate)
      ├─ MarkRawEventProcessing
      ├─ Enrich (contact resolution + identity stitching)
      │     └─ error → Nak() or Term() at max retries
      ├─ InsertEnrichedEvent (ON CONFLICT raw_event_id DO NOTHING)
      │     └─ error → Nak() or Term() at max retries
      ├─ FindMatchingWorkflowTriggers → publish workflow.trigger per match
      ├─ MarkRawEventProcessed
      └─ Ack()
```

## Identity stitching

| Event type | Behavior |
|------------|---------|
| `track` / `page` / `screen` / `group` | Resolve by `userId` → `anonymousId` → create anonymous profile |
| `identify` | Upsert contact with merged traits; link existing anonymous profile |
| `alias` | Upsert canonical contact; link previous `anonymousId` to it |
| No identity | Event persisted unlinked (`contact_id = NULL`) |

Traits merge: incoming non-null values override existing. Null incoming values do not overwrite.

## Idempotency

- `events_raw.status = 'processed'` check before any work.
- `InsertEnrichedEvent` uses `ON CONFLICT (raw_event_id) DO NOTHING`.
- If enrichment succeeds but `MarkRawEventProcessed` fails, the handler acks anyway — the idempotency check will short-circuit on the next delivery.

## Workflow trigger evaluation

Queries `workflow_triggers` for rows matching `(workspace_id, event_type, event_name)`. Each match publishes to `workflow.trigger` with dedup key `wf-{workflowId}-{eventId}`. Trigger lookup failures are non-fatal (logged as warnings).

## Database tables

### events_raw

| Column | Updated by | When |
|--------|-----------|------|
| `status` | Handler | `queued → processing → processed / failed` |
| `failure_reason` | Handler | On permanent failure |
| `processed_at` | Handler | On success |
| `attempts` | Handler | Incremented each delivery |

### events_enriched

| Column | Description |
|--------|-------------|
| `raw_event_id` | Unique FK to `events_raw.id` — prevents duplicate inserts |
| `contact_id` | Resolved contact. NULL if unlinked. |
| `event_type` | `track` \| `identify` \| `page` \| `screen` \| `alias` \| `group` |
| `properties` | JSONB event properties |
| `traits` | JSONB merged contact traits at event time |
| `context` | JSONB event context (IP, user agent, etc.) |
| `enriched_data` | JSONB enrichment metadata (`identified`, `aliased` flags) |

## Metrics

| Metric | Description |
|--------|-------------|
| `events_processed_total` | Raw events consumed |
| `events_enrich_failures_total` | Enrichment failures |
| `events_workflows_triggered_total` | Workflow triggers published |
| `events_retries_total` | Transient retries |
| `events_duplicate_drops_total` | Duplicates skipped |

## Structured log fields

`event_id`, `workspace_id`, `event_type`, `event_name`, `attempt`, `contact_id` (on link), `workflow_id` (on trigger)
