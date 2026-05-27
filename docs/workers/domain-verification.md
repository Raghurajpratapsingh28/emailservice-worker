# Domain Verification Worker

Polls AWS SES for DNS verification status of customer-owned sending domains and writes the result to the `domains` table.

## Subject

`domain.verify.poll` (stream: `DOMAIN`)

## Consumer config

| Setting | Value |
|---------|-------|
| Durable name | `domain-verify-worker` |
| Max deliveries | 9 |
| Ack wait | 2 minutes |
| Handler timeout | 30 seconds |

## Retry schedule (JetStream BackOff)

The server applies these delays between redelivery attempts. The handler calls `Nak()` to trigger the next slot.

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1 → 2 | 5 min | 5 min |
| 2 → 3 | 15 min | 20 min |
| 3 → 4 | 30 min | 50 min |
| 4 → 5 | 1 hr | 1h 50m |
| 5 → 6 | 2 hr | 3h 50m |
| 6 → 7 | 6 hr | 9h 50m |
| 7 → 8 | 12 hr | 21h 50m |
| 8 → 9 | 24 hr | 45h 50m |

After attempt 9 with no `Success` status, the domain is marked `failed`.

## Processing flow

```
Receive message
      │
      ├─ Parse / validate payload
      │     └─ invalid → Term() (poison)
      │
      ├─ Call SES GetIdentityVerificationAttributes
      │     └─ API error → Nak() (transient, retry with backoff)
      │
      ├─ Status = Success
      │     ├─ UPDATE domains SET status='verified', verified_at=COALESCE(verified_at, NOW())
      │     └─ Ack()
      │
      ├─ Status = Pending
      │     ├─ attempt < MaxAttempts
      │     │     ├─ UPDATE domains SET status='pending', verification_attempts=N
      │     │     └─ Nak() (server applies next BackOff delay)
      │     └─ attempt >= MaxAttempts
      │           ├─ UPDATE domains SET status='failed'
      │           └─ Term()
      │
      └─ Status = Failed / unknown
            ├─ UPDATE domains SET status='failed'
            └─ Ack()
```

## Database updates

All updates are conditional on `workspace_id` for tenant isolation.

```sql
-- Verified
UPDATE domains
SET status = 'verified',
    verified_at = COALESCE(verified_at, NOW()),
    updated_at = NOW()
WHERE id = $1 AND workspace_id = $2;

-- Pending
UPDATE domains
SET status = 'pending',
    verification_attempts = $3,
    last_verification_check_at = NOW(),
    updated_at = NOW()
WHERE id = $1 AND workspace_id = $2;

-- Failed
UPDATE domains
SET status = 'failed',
    verification_attempts = $3,
    last_verification_check_at = NOW(),
    updated_at = NOW()
WHERE id = $1 AND workspace_id = $2;
```

`verified_at` uses `COALESCE` so it is set only on the first successful verification and is never overwritten by duplicate deliveries.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `domain_verification_success_total` | Counter | Domains successfully verified |
| `domain_verification_failure_total` | Counter | Domains marked failed (max retries or SES failed) |
| `domain_verification_retries_total` | Counter | Pending checks scheduled for retry |
| `ses_api_failures_total` | Counter | SES API call errors |

## Structured log fields

Every log line includes:

| Field | Description |
|-------|-------------|
| `domain` | The domain being verified |
| `domain_id` | UUID of the domains row |
| `workspace_id` | UUID of the owning workspace |
| `attempt` | Current delivery attempt number (1-based) |
| `status` | SES verification status returned |
| `error` | Error message (on error paths only) |
