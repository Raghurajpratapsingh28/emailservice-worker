# Rate Limiting

## Design

The worker uses a **distributed token bucket** backed by Redis to enforce per-workspace SES send rates. The bucket state is stored in a Redis hash and updated atomically via a Lua script, making it safe across multiple worker replicas.

## Token bucket algorithm

```
On each request:
  1. Compute elapsed time since last update
  2. Refill tokens: min(capacity, current_tokens + elapsed * rate)
  3. If tokens >= cost: deduct cost, allow
  4. If tokens < cost: deny, return wait_ms = ceil((cost - tokens) / rate * 1000)
```

The Lua script runs atomically on the Redis server — no race conditions between worker replicas.

## Redis key structure

```
ratelimit:ses:{workspaceId}
```

Each workspace has an independent bucket. Buckets expire after 1 hour of inactivity.

## Default configuration

| Parameter | Value | Source |
|-----------|-------|--------|
| Rate | 14 tokens/sec | `SES_RATE_LIMIT_PER_SEC` env var |
| Capacity (burst) | 14 tokens | Equal to rate (1 second burst) |
| Max wait | 5 seconds | Hardcoded in handler |

The default of 14/sec matches the [AWS SES default sending quota](https://docs.aws.amazon.com/ses/latest/dg/manage-sending-quotas.html). Increase `SES_RATE_LIMIT_PER_SEC` after requesting a quota increase from AWS.

## Behavior under load

| Scenario | Outcome |
|----------|---------|
| Token available immediately | Acquire returns in < 1ms |
| Token available within 5s | Handler blocks, then proceeds |
| Token not available within 5s | `Nak()` — message requeued with JetStream BackOff |
| Redis unavailable | Error propagated → `Nak()` — message requeued |

## Scaling

With N worker replicas, the effective rate is still `SES_RATE_LIMIT_PER_SEC` per workspace because all replicas share the same Redis bucket. There is no per-replica rate — the limit is global.

## Adjusting the rate limit

To increase the rate for a specific workspace (e.g. after an SES quota increase), update `SES_RATE_LIMIT_PER_SEC` and redeploy. The bucket will refill at the new rate immediately on the next request.

Per-workspace overrides are not currently supported. All workspaces share the same configured rate.
