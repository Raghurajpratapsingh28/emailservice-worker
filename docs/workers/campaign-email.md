# Campaign Email Worker

Sends bulk email campaigns to audience segments via a two-stage pipeline: a start handler that resolves the segment and publishes chunk jobs, and a chunk handler that sends each batch via SES.

## Subjects

| Subject | Consumer | Role |
|---------|----------|------|
| `campaign.send.start` | `campaign-start-worker` | Receives campaign job from API |
| `campaign.send.chunk` | `campaign-chunk-worker` | Processes one batch of recipients |

Both subjects live on the `CAMPAIGN` stream (WorkQueue, 48h retention, 30-min dedup window).

## Consumer config

| Setting | Start worker | Chunk worker |
|---------|-------------|--------------|
| Durable name | `campaign-start-worker` | `campaign-chunk-worker` |
| Max deliveries | 6 | 6 |
| Ack wait | 5 minutes | 5 minutes |
| Handler timeout | 10 minutes | 4 minutes |

The start handler's 10-minute timeout accommodates large segments (millions of contacts) that require many paginated DB fetches and chunk publishes.

## Retry schedule (JetStream BackOff — both consumers)

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1 → 2 | 1 min | 1 min |
| 2 → 3 | 5 min | 6 min |
| 3 → 4 | 15 min | 21 min |
| 4 → 5 | 30 min | 51 min |
| 5 → 6 | 2 hr | 2h 51m |

## Stage 1: Campaign start handler

```
Receive campaign.send.start
      │
      ├─ Parse / validate payload
      │     └─ invalid → Term() (poison)
      │
      ├─ GetCampaignStatus
      │     ├─ completed / failed → Ack() (idempotent skip)
      │     └─ sending → resume (idempotent re-run is safe)
      │
      ├─ Stream segment contacts (keyset pagination, chunkSize rows/page)
      │     ├─ FetchSegmentContactsBatch (active, non-suppressed, valid email)
      │     ├─ BulkInsertCampaignRecipients (ON CONFLICT DO NOTHING)
      │     ├─ Publish campaign.send.chunk (ChunkID = {campaignId}-chunk-{idx})
      │     └─ Repeat until page < chunkSize
      │
      ├─ 0 contacts → MarkCampaignCompleteEmpty → Ack()
      │
      └─ UpdateCampaignStarted(totalRecipients) → Ack()
```

### Idempotency

- Campaign already `completed` or `failed`: acked immediately, no work done.
- Campaign already `sending` (crashed mid-run): re-runs safely. `BulkInsertCampaignRecipients` uses `ON CONFLICT DO NOTHING`. Chunk publish dedup (30-min window) prevents duplicate chunk jobs.

### Keyset pagination

Contacts are fetched with `WHERE id > $afterID ORDER BY id LIMIT $chunkSize`. This avoids OFFSET performance degradation on large tables and is safe for concurrent inserts (new contacts with higher IDs are included; lower IDs are not missed).

## Stage 2: Campaign chunk handler

```
Receive campaign.send.chunk
      │
      ├─ Parse / validate payload
      │     └─ invalid → Term() (poison)
      │
      ├─ Render HTML + text once for the whole chunk
      │     └─ render error → failAllRecipients → Term()
      │
      ├─ For each recipient:
      │     ├─ GetCampaignRecipientStatus
      │     │     └─ sent / bounced → skip (duplicate)
      │     ├─ Rate limit (Redis token bucket, max wait 5s)
      │     │     └─ denied → mark as transient
      │     ├─ MarkCampaignRecipientSending
      │     ├─ SES SendEmail
      │     │     ├─ permanent error → MarkRecipientFailed + publish event
      │     │     └─ transient error → accumulate in transientRecipients
      │     └─ MarkCampaignRecipientSent + publish event
      │
      ├─ IncrementCampaignCounts(sent, permanentFailed)
      ├─ MarkCampaignComplete (conditional, idempotent)
      │
      ├─ transientRecipients empty → Ack()
      ├─ transientRecipients present, attempt < MaxAttempts → Nak()
      └─ transientRecipients present, attempt = MaxAttempts
            ├─ MarkRecipientFailed for all transient
            ├─ IncrementCampaignCounts(0, transientCount)
            ├─ MarkCampaignComplete
            ├─ Publish campaign.send.dlq
            └─ Term()
```

### Per-recipient error classification

| SES error | Classification | Outcome |
|-----------|---------------|---------|
| `MessageRejected` | Permanent | Recipient marked failed, chunk continues |
| `MailFromDomainNotVerifiedException` | Permanent | Recipient marked failed, chunk continues |
| `AccessDeniedException` | Permanent | Recipient marked failed, chunk continues |
| `ValidationException` | Permanent | Recipient marked failed, chunk continues |
| Network timeout, 5xx, throttling | Transient | Accumulated; chunk naks for retry |

### Completion detection

After each chunk, `IncrementCampaignCounts` returns the post-update `sent_count`, `failed_count`, and `total_recipients`. When `sent + failed >= total`, `MarkCampaignComplete` runs a conditional UPDATE that sets `status='completed'` only if `completed_at IS NULL`. This is safe under concurrent chunk completion — exactly one chunk will perform the transition.

## Database tables

### campaigns

| Column | Updated by | When |
|--------|-----------|------|
| `status` | Start handler | `queued → sending`; Chunk handler: `sending → completed/failed` |
| `total_recipients` | Start handler | After segment resolution |
| `sent_count` | Chunk handler | Atomically incremented per chunk |
| `failed_count` | Chunk handler | Atomically incremented per chunk |
| `started_at` | Start handler | `COALESCE(started_at, NOW())` — set once |
| `completed_at` | Chunk handler | Set when `sent + failed >= total` |

### campaign_recipients

| Column | Updated by | When |
|--------|-----------|------|
| `status` | Chunk handler | `queued → sending → sent/failed` |
| `provider_message_id` | Chunk handler | On successful SES send |
| `failure_reason` | Chunk handler | On permanent failure or max retries |
| `sent_at` | Chunk handler | `COALESCE(sent_at, NOW())` — set once |

## Metrics

| Metric | Description |
|--------|-------------|
| `campaign_processed_total` | Campaign-start jobs consumed |
| `campaign_completed_total` | Campaigns marked completed |
| `campaign_recipients_resolved_total` | Contacts resolved from segments |
| `campaign_chunks_created_total` | Chunk jobs published |
| `campaign_emails_sent_total` | Per-recipient sends that succeeded |
| `campaign_emails_failed_total` | Per-recipient sends that failed permanently |
| `campaign_chunk_retries_total` | Chunks naked for retry |
| `campaign_chunk_dlq_total` | Chunks routed to DLQ |

## Structured log fields

### Start handler

| Field | Description |
|-------|-------------|
| `campaign_id` | UUID of the campaign |
| `workspace_id` | UUID of the owning workspace |
| `segment_id` | UUID of the segment |
| `job_id` | UUID of the originating job |
| `attempt` | Delivery attempt number |
| `total_recipients` | Total contacts resolved |
| `chunks_created` | Number of chunk jobs published |

### Chunk handler

| Field | Description |
|-------|-------------|
| `chunk_id` | Deterministic chunk identifier |
| `campaign_id` | UUID of the campaign |
| `workspace_id` | UUID of the owning workspace |
| `recipients` | Number of recipients in this chunk |
| `attempt` | Delivery attempt number |
| `sent` | Recipients sent in this processing |
| `permanent_failed` | Recipients permanently failed |
| `transient` | Recipients pending retry |
