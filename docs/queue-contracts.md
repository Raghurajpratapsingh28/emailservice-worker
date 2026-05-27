# Queue Contracts

All NATS subjects and payload schemas are locked. The Fastify API publishes to these subjects. **Do not change field names or types without a coordinated API + worker release.**

---

## domain.verify.poll

**Stream:** `DOMAIN` | **Consumer:** `domain-verify-worker`

```json
{ "domainId": "uuid", "workspaceId": "uuid", "domain": "acme.com" }
```

| Field | Required | Description |
|-------|----------|-------------|
| `domainId` | ✓ | PK of `domains` row |
| `workspaceId` | ✓ | Owning workspace |
| `domain` | ✓ | Bare domain name |

---

## email.send.transactional

**Stream:** `EMAIL_SEND` | **Consumer:** `email-send-worker`

```json
{
  "jobId": "uuid", "workspaceId": "uuid", "sendId": "uuid",
  "to": [{ "email": "alice@example.com", "name": "Alice" }],
  "from": { "email": "hello@acme.com", "name": "Acme" },
  "replyTo": "support@acme.com",
  "subject": "Welcome", "html": "<h1>Hello</h1>", "text": "Hello",
  "tags": { "source": "signup" }, "provider": "ses"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `jobId` | ✓ | Idempotency key |
| `workspaceId` | ✓ | Owning workspace |
| `sendId` | ✓ | PK of `email_sends` row |
| `to` | ✓ | At least one recipient with `email` |
| `from.email` | ✓ | SES-verified sender address |
| `subject` | ✓ | Email subject |
| `html` / `text` | ✓* | At least one required. `text` auto-generated from `html` if omitted. |
| `provider` | — | Must be `"ses"` or omitted |

---

## campaign.send.start

**Stream:** `CAMPAIGN` | **Consumer:** `campaign-start-worker`

```json
{
  "jobId": "uuid", "workspaceId": "uuid",
  "campaignId": "uuid", "segmentId": "uuid",
  "sender": { "email": "hello@acme.com", "name": "Acme" },
  "replyTo": "support@acme.com",
  "subject": "Welcome", "html": "<h1>Hello</h1>", "text": "Hello"
}
```

---

## campaign.send.chunk  *(internal — worker-to-worker)*

**Stream:** `CAMPAIGN` | **Consumer:** `campaign-chunk-worker`  
Published by `campaign-start-worker`. Not published by the API.

```json
{
  "campaignId": "uuid", "workspaceId": "uuid",
  "chunkId": "{campaignId}-chunk-{idx}",
  "sender": { "email": "hello@acme.com", "name": "Acme" },
  "replyTo": "support@acme.com",
  "subject": "Welcome", "html": "<h1>Hello</h1>", "text": "Hello",
  "recipients": [{ "recipientId": "uuid", "email": "alice@example.com", "name": "Alice" }]
}
```

`chunkId` is deterministic and used as the NATS dedup key (30-min window).

---

## events.raw.{workspaceId}

**Stream:** `EVENTS_RAW` | **Consumer:** `events-enrichment-worker` (wildcard `events.raw.*`)

```json
{
  "eventId": "uuid",
  "workspaceId": "uuid",
  "apiKeyId": "uuid",
  "eventType": "track",
  "eventName": "Trial Upgraded",
  "userId": "user_123",
  "anonymousId": "anon_abc",
  "groupId": null,
  "traits": {},
  "properties": {},
  "context": {},
  "receivedAt": "2026-05-26T01:00:00Z"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `eventId` | ✓ | Unique event ID. Used for idempotency. |
| `workspaceId` | ✓ | Owning workspace |
| `eventType` | ✓ | `track` \| `identify` \| `page` \| `screen` \| `alias` \| `group` |
| `eventName` | — | Required for `track`. Empty for `identify`, `alias`, etc. |
| `userId` | — | Known user identity |
| `anonymousId` | — | Anonymous session identity |

---

## segment.refresh

**Stream:** `SEGMENTS` | **Consumer:** `segment-refresh-worker`

```json
{ "workspaceId": "uuid", "segmentId": "uuid" }
```

| Field | Required | Description |
|-------|----------|-------------|
| `workspaceId` | ✓ | Owning workspace |
| `segmentId` | ✓ | PK of `segments` row |

---

## workflow.trigger  *(locked output contract)*

**Stream:** `WORKFLOW` | **Producer:** `events-enrichment-worker`  
**Consumers:** Workflow engine (downstream)

```json
{
  "workspaceId": "uuid",
  "contactId": "uuid",
  "eventName": "Trial Upgraded",
  "eventId": "uuid"
}
```

Dedup key: `wf-{workflowId}-{eventId}` (10-min window).

---

## email.delivery.events  *(locked output contract)*

**Stream:** `EMAIL_EVENTS` | **Producers:** `email-send-worker`, `campaign-chunk-worker`

```json
{
  "workspaceId": "uuid",
  "sendId": "uuid",
  "campaignId": "uuid",
  "recipientEmail": "alice@example.com",
  "providerMessageId": "ses-message-id",
  "status": "sent",
  "reason": "",
  "timestamp": "2026-05-26T01:00:00Z"
}
```

`sendId` is set for transactional sends; `campaignId` + `recipientEmail` for campaign sends.

---

## Dead letter queues

| Subject | Stream | Producer | Trigger |
|---------|--------|----------|---------|
| `email.send.dlq` | `EMAIL_SEND` | `email-send-worker` | Transient failure at MaxAttempts (6) |
| `campaign.send.dlq` | `CAMPAIGN` | `campaign-chunk-worker` | Transient failure at MaxAttempts (6) |
