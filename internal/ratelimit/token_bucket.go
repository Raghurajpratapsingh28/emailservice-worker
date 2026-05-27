package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// luaTokenBucket implements a token bucket atomically in Redis.
//
//	KEYS[1] = bucket key
//	ARGV[1] = refill rate (tokens per second, float)
//	ARGV[2] = bucket capacity (max tokens, float)
//	ARGV[3] = current time (epoch seconds, float)
//	ARGV[4] = cost (tokens to consume, float)
//
// Returns {allowed (1|0), wait_ms (int)}.
//
// The bucket state is stored in a hash {tokens, ts}. On each call the bucket
// is refilled based on elapsed time, capped at capacity. If sufficient tokens
// exist, cost is deducted and 1 is returned. Otherwise the request is denied
// and the suggested wait time (in ms) until enough tokens accumulate is returned.
const luaTokenBucket = `
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(capacity, tokens + elapsed * rate)

if tokens < cost then
  redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
  redis.call('EXPIRE', KEYS[1], 3600)
  local wait_ms = math.ceil((cost - tokens) / rate * 1000)
  return {0, wait_ms}
end

tokens = tokens - cost
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], 3600)
return {1, 0}
`

// ErrRateLimitExceeded indicates Acquire could not get a token within the wait budget.
var ErrRateLimitExceeded = errors.New("rate limit exceeded")

// TokenBucket is a Redis-backed distributed token bucket.
type TokenBucket struct {
	rdb      *redis.Client
	rate     float64
	capacity float64
	script   *redis.Script
}

// NewTokenBucket constructs a limiter with refill rate and burst capacity equal
// to ratePerSec (i.e. burst of 1 second worth of traffic).
func NewTokenBucket(rdb *redis.Client, ratePerSec int) *TokenBucket {
	if ratePerSec < 1 {
		ratePerSec = 1
	}
	return &TokenBucket{
		rdb:      rdb,
		rate:     float64(ratePerSec),
		capacity: float64(ratePerSec),
		script:   redis.NewScript(luaTokenBucket),
	}
}

// TryAcquire attempts to consume one token without waiting. Returns whether the
// request is allowed and, if not, the suggested wait until the next token.
func (l *TokenBucket) TryAcquire(ctx context.Context, key string) (bool, time.Duration, error) {
	now := float64(time.Now().UnixMilli()) / 1000.0
	res, err := l.script.Run(ctx, l.rdb, []string{key}, l.rate, l.capacity, now, 1.0).Result()
	if err != nil {
		return false, 0, fmt.Errorf("token bucket: %w", err)
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 2 {
		return false, 0, fmt.Errorf("unexpected lua result: %v", res)
	}
	allowed, _ := arr[0].(int64)
	waitMs, _ := arr[1].(int64)
	return allowed == 1, time.Duration(waitMs) * time.Millisecond, nil
}

// Acquire blocks (up to maxWait) until a token is available. Returns the total
// time spent waiting. Returns ErrRateLimitExceeded if maxWait is exhausted.
func (l *TokenBucket) Acquire(ctx context.Context, key string, maxWait time.Duration) (time.Duration, error) {
	deadline := time.Now().Add(maxWait)
	var totalWait time.Duration
	for {
		ok, wait, err := l.TryAcquire(ctx, key)
		if err != nil {
			return totalWait, err
		}
		if ok {
			return totalWait, nil
		}
		if wait <= 0 {
			wait = 10 * time.Millisecond
		}
		if time.Now().Add(wait).After(deadline) {
			return totalWait, fmt.Errorf("%w: would wait %v", ErrRateLimitExceeded, wait)
		}
		select {
		case <-ctx.Done():
			return totalWait, ctx.Err()
		case <-time.After(wait):
			totalWait += wait
		}
	}
}
