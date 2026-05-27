# Observability

## Metrics

Exposed at `http://localhost:{METRICS_PORT}/metrics` in Prometheus text format.

### Domain verification

| Metric | Description |
|--------|-------------|
| `domain_verification_success_total` | Domains successfully verified |
| `domain_verification_failure_total` | Domains marked failed |
| `domain_verification_retries_total` | Pending checks rescheduled |
| `ses_api_failures_total` | SES API errors (shared with email workers) |

### Transactional email

| Metric | Description |
|--------|-------------|
| `email_processed_total` | Messages consumed |
| `email_sent_total` | Emails accepted by SES |
| `email_failed_total` | Emails permanently failed |
| `email_retries_total` | Transient retries |
| `email_ses_failures_total` | SES API errors |
| `email_rate_limit_waits_total` | Rate limit waits or denials |
| `email_dlq_total` | Messages routed to DLQ |

### Campaign email

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

### Event enrichment

| Metric | Description |
|--------|-------------|
| `events_processed_total` | Raw events consumed |
| `events_enrich_failures_total` | Enrichment failures (transient + permanent) |
| `events_workflows_triggered_total` | Workflow trigger events published |
| `events_retries_total` | Transient retries |
| `events_duplicate_drops_total` | Duplicate events skipped |

### Segment computation

| Metric | Description |
|--------|-------------|
| `segment_refresh_jobs_total` | Refresh jobs consumed |
| `segment_contacts_matched_total` | Contacts matched across all refreshes |
| `segment_membership_inserts_total` | New memberships inserted |
| `segment_membership_removals_total` | Stale memberships removed |
| `segment_refresh_failures_total` | Refresh jobs permanently failed |
| `segment_refresh_retries_total` | Transient retries |

### Recommended alerts

```yaml
- alert: EmailHighFailureRate
  expr: rate(email_failed_total[5m]) / rate(email_processed_total[5m]) > 0.05
  for: 5m

- alert: EmailDLQNonZero
  expr: increase(email_dlq_total[5m]) > 0
  for: 1m

- alert: CampaignChunkDLQNonZero
  expr: increase(campaign_chunk_dlq_total[5m]) > 0
  for: 1m

- alert: SESAPIFailures
  expr: rate(email_ses_failures_total[5m]) > 1
  for: 5m

- alert: DomainVerificationStalled
  expr: increase(domain_verification_success_total[1h]) == 0
  for: 1h

- alert: EventEnrichmentHighFailureRate
  expr: rate(events_enrich_failures_total[5m]) > 1
  for: 5m

- alert: SegmentRefreshFailures
  expr: increase(segment_refresh_failures_total[15m]) > 0
  for: 5m
```

---

## Structured logging

All logs are JSON via `go.uber.org/zap` in production mode.

### Domain verification

| Event | Level | Key fields |
|-------|-------|------------|
| SES check result | `info` | `domain`, `domain_id`, `workspace_id`, `attempt`, `status` |
| Domain verified | `info` | `domain`, `workspace_id` |
| Retry scheduled | `info` | `domain`, `attempt`, `next_delay` |
| Max attempts | `warn` | `domain`, `attempt` |
| SES API failure | `error` | `domain`, `error` |

### Transactional email

| Event | Level | Key fields |
|-------|-------|------------|
| Message consumed | `info` | `send_id`, `workspace_id`, `job_id`, `attempt` |
| Duplicate skipped | `info` | `send_id`, `status` |
| Rate limit denied | `warn` | `send_id`, `error` |
| Send success | `info` | `send_id`, `provider_message_id` |
| Permanent failure | `error` | `send_id`, `error` |
| Max retries / DLQ | `error` | `send_id`, `attempts` |

### Campaign email

| Event | Level | Key fields |
|-------|-------|------------|
| Campaign job received | `info` | `campaign_id`, `workspace_id`, `segment_id`, `attempt` |
| Chunk created | `info` | `campaign_id`, `chunk_id`, `chunk_idx`, `recipients` |
| Contacts resolved | `info` | `campaign_id`, `total_recipients`, `chunks_created` |
| Chunk processed | `info` | `chunk_id`, `sent`, `permanent_failed` |
| Campaign completed | `info` | `campaign_id`, `sent_count`, `failed_count` |
| Max attempts / DLQ | `error` | `chunk_id`, `transient_remaining` |

### Event enrichment

| Event | Level | Key fields |
|-------|-------|------------|
| Event received | `info` | `event_id`, `workspace_id`, `event_type`, `event_name`, `attempt` |
| Duplicate skipped | `info` | `event_id` |
| Contact linked | `info` | `event_id`, `contact_id` |
| Workflow trigger published | `info` | `event_id`, `workflow_id`, `contact_id` |
| Enrichment success | `info` | `event_id` |
| Enrichment failed | `error` | `event_id`, `error` |
| Max retries | `error` | `event_id` |

### Segment computation

| Event | Level | Key fields |
|-------|-------|------------|
| Refresh received | `info` | `segment_id`, `workspace_id`, `attempt` |
| Lock already held | `info` | `segment_id` |
| Segment loaded | `info` | `segment_id`, `name` |
| Contacts matched | `info` | `segment_id`, `count` |
| Memberships updated | `info` | `segment_id`, `added`, `removed`, `total` |
| Refresh failed | `error` | `segment_id`, `error` |
| Max retries | `error` | `segment_id` |

---

## Health endpoint

```
GET http://localhost:{METRICS_PORT}/health  →  200 OK
```

Returns `200` as long as the process is running. Use for Kubernetes liveness probes. All infra connections are verified at startup before the HTTP server starts.
