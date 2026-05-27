# Configuration

All configuration is loaded from environment variables at startup. Missing required variables cause an immediate fatal exit.

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NATS_URL` | ✓ | — | NATS server URL. e.g. `nats://localhost:4222` |
| `DATABASE_URL` | ✓ | — | PostgreSQL DSN. e.g. `postgres://user:pass@localhost:5432/engageiq` |
| `REDIS_URL` | ✓ | — | Redis URL. e.g. `redis://localhost:6379/0` or `rediss://...` for TLS |
| `AWS_REGION` | ✓ | — | AWS region for SES. e.g. `us-east-1` |
| `AWS_ACCESS_KEY_ID` | — | — | AWS access key. Omit when using IAM roles. |
| `AWS_SECRET_ACCESS_KEY` | — | — | AWS secret key. Omit when using IAM roles. |
| `SES_RATE_LIMIT_PER_SEC` | — | `14` | SES send rate per workspace per second. Matches AWS SES default quota. |
| `CAMPAIGN_CHUNK_SIZE` | — | `500` | Recipients per campaign chunk job. |
| `EVENT_MAX_RETRIES` | — | `5` | Max delivery attempts for event enrichment before marking failed. |
| `EVENT_BATCH_SIZE` | — | `100` | Reserved for future batch processing. |
| `SEGMENT_MAX_RETRIES` | — | `5` | Max delivery attempts for segment refresh before marking failed. |
| `METRICS_PORT` | — | `9090` | Port for Prometheus `/metrics` and `/health` endpoints. |

## AWS credentials

Standard AWS SDK credential chain (in order):
1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars
2. Shared credentials file (`~/.aws/credentials`)
3. IAM instance profile / ECS task role / EKS IRSA

**Production:** Use IAM roles. Do not set static credentials.

## Required IAM permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "ses:SendEmail",
      "ses:GetIdentityVerificationAttributes"
    ],
    "Resource": "*"
  }]
}
```

## Connection pool settings (postgres)

| Setting | Value |
|---------|-------|
| `MaxConns` | 10 |
| `MinConns` | 2 |
| `MaxConnLifetime` | 30m |

## Example `.env`

```bash
NATS_URL=nats://localhost:4222
DATABASE_URL=postgres://engageiq:secret@localhost:5432/engageiq?sslmode=disable
REDIS_URL=redis://localhost:6379/0
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
SES_RATE_LIMIT_PER_SEC=14
CAMPAIGN_CHUNK_SIZE=500
EVENT_MAX_RETRIES=5
EVENT_BATCH_SIZE=100
SEGMENT_MAX_RETRIES=5
METRICS_PORT=9090
```
