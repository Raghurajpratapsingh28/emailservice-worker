# Transactional Email Worker

Consumes send jobs from the queue, applies per-workspace rate limiting, sends via AWS SES, updates the `email_sends` table, and publishes a delivery event.

## Subject

`email.send.transactional` (stream: `EMAIL_SEND`)

## Consumer config

| Setting | Value |
|---------|-------|
| Durable name | `email-send-worker` |
| Max deliveries | 6 |
| Ack wait | 90 seconds |
| Handler timeout | 60 seconds |

## Retry schedule (JetStream BackOff)

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1 → 2 | 1 min | 1 min |
| 2 → 3 | 5 min | 6 min |
| 3 → 4 | 15 min | 21 min |
| 4 → 5 | 30 min | 51 min |
| 5 → 6 | 2 hr | 2h 51m |

After 6 attempts with transient failures, the message is routed to the DLQ and the send is marked `failed`.

## Processing flow

```
Receive message
      │
      ├─ Parse JSON
      │     └─ invalid JSON → Term() (poison)
      │
      ├─ Validate payload (required fields, provider)
      │     └─ invalid → Term() (poison)
      │
      ├─ Idempotency check: SELECT status FROM email_sends
      │     ├─ status = 'sent' or 'bounced' → Ack() (duplicate, skip)
      │     └─ DB error → Nak()
      │
      ├─ Rate limit: acquire token from Redis bucket (max wait 5s)
      │     └─ denied → Nak() (requeue)
      │
      ├─ UPDATE email_sends SET status='sending'
      │     └─ DB error → Nak()
      │
      ├─ Render: trim subject, generate text fallback from HTML if needed
      │     └─ no body → Term() + mark failed
      │
      ├─ SES SendEmail
      │     ├─ permanent error (MessageRejected, etc.)
      │     │     ├─ UPDATE email_sends SET status='failed'
      │     │     ├─ Publish email.delivery.events {status: "failed"}
      │     │     └─ Ack() (no retry)
      │     │
      │     ├─ transient error, attempt < MaxAttempts
      │     │     └─ Nak() (server applies BackOff)
      │     │
      │     └─ transient error, attempt = MaxAttempts
      │           ├─ Publish email.send.dlq
      │           ├─ UPDATE email_sends SET status='failed'
      │           ├─ Publish email.delivery.events {status: "failed"}
      │           └─ Term()
      │
      └─ Success
            ├─ UPDATE email_sends SET status='sent', provider_message_id=...
            ├─ Publish email.delivery.events {status: "sent"}
            └─ Ack()
```

## Idempotency

Before any SES call, the handler reads the current `status` from `email_sends`. If the status is already `sent` or `bounced`, the message is acked immediately without re-sending. This protects against duplicate delivery from NATS at-least-once semantics.

If SES accepts the email but the subsequent DB update fails, the handler **acks** (not naks) to prevent a duplicate send. The idempotency check on the next delivery will short-circuit once the DB is consistent.

## Permanent vs transient SES errors

The following SES error codes are treated as **permanent** (no retry, ack immediately):

| Error code | Meaning |
|------------|---------|
| `MessageRejected` | Content policy violation |
| `MailFromDomainNotVerifiedException` | Sender domain not verified in SES |
| `ConfigurationSetDoesNotExistException` | Bad SES config set |
| `ConfigurationSetSendingPausedException` | Config set paused |
| `AccountSendingPausedException` | Account-level sending paused |
| `InvalidParameterValue` | Bad request parameter |
| `InvalidRenderingParameter` | Template rendering error |
| `AccessDeniedException` | IAM permission missing |
| `ValidationException` | Request validation failure |

All other errors (network timeouts, 5xx, throttling) are treated as **transient** and retried.

## Database status transitions

```
queued → sending → sent
                 → failed
       → failed  (permanent SES error or max retries)
```

All `UPDATE` statements are conditional on `workspace_id` (tenant isolation) and on the current status to prevent invalid transitions:

- `UpdateEmailSendSent` will not overwrite a `bounced` row.
- `UpdateEmailSendFailed` will not overwrite a `sent` or `bounced` row.

## Rate limiting

Each workspace has an independent token bucket in Redis keyed by `ratelimit:ses:{workspaceId}`. The default rate is 14 tokens/second (matching the AWS SES default sending quota). The handler waits up to 5 seconds for a token before naking the message back to JetStream.

See [rate-limiting.md](../rate-limiting.md) for the full design.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `email_processed_total` | Counter | Messages consumed from the queue |
| `email_sent_total` | Counter | Emails successfully sent via SES |
| `email_failed_total` | Counter | Emails permanently failed |
| `email_retries_total` | Counter | Transient retries scheduled |
| `email_ses_failures_total` | Counter | SES API call errors |
| `email_rate_limit_waits_total` | Counter | Rate limit waits or denials |
| `email_dlq_total` | Counter | Messages routed to DLQ |

## Structured log fields

| Field | Description |
|-------|-------------|
| `send_id` | UUID of the email_sends row |
| `workspace_id` | UUID of the owning workspace |
| `job_id` | UUID of the originating job |
| `attempt` | Current delivery attempt (1-based) |
| `provider_message_id` | SES MessageId (on success) |
| `error` | Error message (on error paths) |
