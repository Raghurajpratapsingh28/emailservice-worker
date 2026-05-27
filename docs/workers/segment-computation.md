# Segment Computation Worker

Recomputes segment membership by evaluating a filter tree against all active contacts, diffing against current memberships, and applying inserts/deletes.

## Subject

`segment.refresh`  
**Stream:** `SEGMENTS` | **Consumer:** `segment-refresh-worker`

## Consumer config

| Setting | Value |
|---------|-------|
| Durable name | `segment-refresh-worker` |
| Max deliveries | `SEGMENT_MAX_RETRIES + 1` (default 6) |
| Ack wait | 15 minutes |
| Handler timeout | 12 minutes |

The 15-minute AckWait accommodates large workspaces with millions of contacts.

## Retry schedule (JetStream BackOff)

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1 → 2 | 1 min | 1 min |
| 2 → 3 | 5 min | 6 min |
| 3 → 4 | 15 min | 21 min |
| 4 → 5 | 30 min | 51 min |

## Processing flow

```
Receive segment.refresh
      │
      ├─ Parse / validate → Term() on poison
      ├─ AcquireLock(segment:{id}:refresh, 10min TTL)
      │     ├─ ErrLockNotAcquired → Ack() (another worker is processing)
      │     └─ Redis error → Nak()
      ├─ GetSegment → Term() if not found
      ├─ ParseFilterTree → Term() + mark failed if invalid JSON
      ├─ UpdateSegmentStatus('processing')
      ├─ StreamContactsForEval (keyset-paginated, 500 contacts/page)
      │     └─ For each contact: Evaluator.Matches(filterTree)
      ├─ GetCurrentMemberIDs
      ├─ diff(matched, current) → toAdd
      ├─ diff(current, matched) → toRemove
      ├─ InsertSegmentMembers(toAdd)   — ON CONFLICT DO NOTHING
      ├─ DeleteSegmentMembers(toRemove)
      ├─ UpdateSegmentReady(finalCount)
      └─ Ack()
```

## Distributed lock

Lock key: `segment:{segmentId}:refresh`  
TTL: 10 minutes  
Implementation: Redis `SET NX EX` (atomic). Release uses a Lua script that only deletes the key if the value matches the caller's UUID — prevents a slow worker from releasing another worker's lock.

If a worker crashes while holding the lock, the lock expires after 10 minutes and the next delivery will acquire it.

## Filter tree evaluator

### Supported operators

| Operator | Description |
|----------|-------------|
| `equals` | Exact string match (case-sensitive) |
| `not_equals` | Negated exact match |
| `contains` | Case-insensitive substring |
| `starts_with` | Case-insensitive prefix |
| `ends_with` | Case-insensitive suffix |
| `greater_than` | Numeric comparison |
| `less_than` | Numeric comparison |
| `exists` | Field is non-null and non-empty |
| `not_exists` | Field is null or empty |
| `in` | Value is in a list |
| `not_in` | Value is not in a list |

### Logical operators

`AND`, `OR`, arbitrarily nested.

### Field resolution

| Field path | Resolves to |
|------------|-------------|
| `email` | `contacts.email` |
| `first_name` / `firstName` | `contacts.first_name` |
| `last_name` / `lastName` | `contacts.last_name` |
| `phone` | `contacts.phone` |
| `user_id` / `userId` | `contacts.user_id` |
| `status` | `contacts.status` |
| `created_at` / `createdAt` | `contacts.created_at` (RFC3339) |
| `traits.*` | JSONB field from `contacts.traits` |
| `properties.*` | JSONB field from `contacts.properties` |
| `traits.a.b` | Nested JSONB (one level of nesting) |

### Event filter

A leaf node with `eventName` (instead of `field`/`operator`) matches contacts that have performed that event:

```json
{ "eventName": "Trial Started" }
```

Queries `events_enriched` via `ContactHasPerformedEvent`.

### Filter tree examples

Simple condition:
```json
{ "field": "traits.plan", "operator": "equals", "value": "pro" }
```

AND group:
```json
{
  "logic": "AND",
  "children": [
    { "field": "email", "operator": "contains", "value": "@acme.com" },
    { "field": "traits.plan", "operator": "in", "value": ["pro", "enterprise"] }
  ]
}
```

Nested OR with event filter:
```json
{
  "logic": "OR",
  "children": [
    { "field": "traits.plan", "operator": "equals", "value": "enterprise" },
    { "eventName": "Trial Started" }
  ]
}
```

Nil / empty filter tree matches all contacts.

## Membership diff

The worker reads current `segment_members` rows, computes the new matched set, and applies only the delta:

- `toAdd = matched - current` → `INSERT ... ON CONFLICT DO NOTHING`
- `toRemove = current - matched` → `DELETE`

This avoids a full delete + rebuild and is safe for concurrent reads.

## Database tables

### segments

| Column | Updated by | When |
|--------|-----------|------|
| `status` | Handler | `queued → processing → ready / failed` |
| `contact_count` | Handler | Set to final count on `ready` |
| `last_computed` | Handler | On `ready` |

### segment_members

| Column | Description |
|--------|-------------|
| `segment_id` | FK to `segments.id` |
| `contact_id` | FK to `contacts.id` |
| Unique constraint | `(segment_id, contact_id)` — enables idempotent inserts |

## Metrics

| Metric | Description |
|--------|-------------|
| `segment_refresh_jobs_total` | Refresh jobs consumed |
| `segment_contacts_matched_total` | Contacts matched across all refreshes |
| `segment_membership_inserts_total` | New memberships inserted |
| `segment_membership_removals_total` | Stale memberships removed |
| `segment_refresh_failures_total` | Jobs permanently failed |
| `segment_refresh_retries_total` | Transient retries |

## Structured log fields

`segment_id`, `workspace_id`, `attempt`, `name` (on load), `count` (on match), `added`, `removed`, `total` (on complete)
