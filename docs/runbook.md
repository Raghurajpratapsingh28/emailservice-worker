# Runbook

Operational procedures for on-call engineers.

---

## Alert: EmailDLQNonZero

**Meaning:** Transactional emails exhausted all 6 delivery attempts.

1. Check logs: `kubectl logs -l app=engageiq-workers | jq 'select(.msg == "DLQ event published")'`
2. Inspect DLQ: `nats sub 'email.send.dlq' --count 10`
3. Common causes:

   | Reason | Fix |
   |--------|-----|
   | SES throttling | Reduce `SES_RATE_LIMIT_PER_SEC` or request quota increase |
   | Network timeout | Check VPC connectivity to `email.{region}.amazonaws.com` |
   | SES sandbox | Move account out of SES sandbox |

4. Reprocess: reset DB row then republish.
   ```sql
   UPDATE email_sends SET status='queued', failure_reason=NULL, updated_at=NOW()
   WHERE id='<send_id>' AND workspace_id='<workspace_id>';
   ```
   ```bash
   nats pub email.send.transactional '{...original payload...}'
   ```

---

## Alert: CampaignChunkDLQNonZero

**Meaning:** Campaign chunk exhausted all 6 delivery attempts.

1. Inspect DLQ: `nats sub 'campaign.send.dlq' --count 10`
2. Reset affected recipients and republish chunk:
   ```sql
   UPDATE campaign_recipients SET status='queued', failure_reason=NULL, updated_at=NOW()
   WHERE campaign_id='<id>' AND status='failed' AND workspace_id='<ws>';
   ```
   ```bash
   nats pub campaign.send.chunk '{...chunk payload from DLQ...}'
   ```

---

## Alert: EmailHighFailureRate / CampaignHighFailureRate

1. Check which SES error is causing failures:
   ```bash
   kubectl logs -l app=engageiq-workers | jq 'select(.msg | contains("permanent"))'
   ```
2. `MailFromDomainNotVerifiedException` → verify sender domain in SES console.
3. `MessageRejected` → review email content for policy violations.
4. `AccountSendingPausedException` → check SES console and AWS support.

---

## Alert: SESAPIFailures

1. Verify IAM permissions: `aws ses get-identity-verification-attributes --identities example.com`
2. Check VPC connectivity: `curl -v https://email.us-east-1.amazonaws.com`
3. Check AWS service health: https://health.aws.amazon.com/

---

## Alert: DomainVerificationStalled

1. Check consumer lag: `nats consumer info DOMAIN domain-verify-worker`
2. Check worker logs: `kubectl logs -l app=engageiq-workers | jq 'select(.msg | contains("domain"))'`

---

## Alert: EventEnrichmentHighFailureRate

1. Check enrichment errors:
   ```bash
   kubectl logs -l app=engageiq-workers | jq 'select(.msg == "enrichment failed")'
   ```
2. Common causes: DB connectivity, missing `contacts` table columns, malformed JSONB traits.
3. Reprocess a failed event:
   ```sql
   UPDATE events_raw SET status='queued', failure_reason=NULL, updated_at=NOW()
   WHERE id='<event_id>' AND workspace_id='<ws>';
   ```
   ```bash
   nats pub events.raw.<workspaceId> '{...original payload...}'
   ```

---

## Alert: SegmentRefreshFailures

1. Check logs: `kubectl logs -l app=engageiq-workers | jq 'select(.msg == "max retries reached" and .segment_id != null)'`
2. Common causes: invalid filter tree JSON, DB connectivity, missing `workflow_triggers` table.
3. Reprocess: republish the refresh job.
   ```bash
   nats pub segment.refresh '{"workspaceId":"<ws>","segmentId":"<seg>"}'
   ```
4. Manually complete a stuck segment:
   ```sql
   UPDATE segments SET status='ready', last_computed=NOW(), updated_at=NOW()
   WHERE id='<seg>' AND workspace_id='<ws>';
   ```

---

## Checking consumer health

```bash
# All consumers
nats consumer ls DOMAIN
nats consumer ls EMAIL_SEND
nats consumer ls CAMPAIGN
nats consumer ls EVENTS_RAW
nats consumer ls SEGMENTS

# Detailed (pending count, redelivered count, last delivery time)
nats consumer info EVENTS_RAW events-enrichment-worker
nats consumer info SEGMENTS segment-refresh-worker
```

A healthy consumer shows `Num Pending: 0` (or low and decreasing) and `Num Redelivered: 0`.

---

## Scaling workers

Workers are stateless. Scale horizontally:
```bash
kubectl scale deployment engageiq-workers --replicas=5
```

- NATS WorkQueue policy ensures each message is delivered to exactly one replica.
- Redis rate limiter and distributed lock are shared across all replicas — limits are global.
- Segment refresh: if a replica holds the lock and crashes, the lock expires after 10 minutes and the next delivery will acquire it.
