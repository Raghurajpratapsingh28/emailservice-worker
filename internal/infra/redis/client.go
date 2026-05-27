package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Client struct {
	R      *redis.Client
	logger *zap.Logger
}

// NewClient parses a redis:// (or rediss://) URL and verifies connectivity.
func NewClient(ctx context.Context, url string, logger *zap.Logger) (*Client, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	r := redis.NewClient(opt)
	if err := r.Ping(ctx).Err(); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	logger.Info("redis connected")
	return &Client{R: r, logger: logger}, nil
}

func (c *Client) Close() error {
	return c.R.Close()
}

// ErrLockNotAcquired is returned when the distributed lock is already held.
var ErrLockNotAcquired = errors.New("lock not acquired")

// AcquireLock attempts to acquire a distributed lock using SET NX EX.
// Returns ErrLockNotAcquired if the lock is already held.
// The caller MUST call ReleaseLock when done.
func (c *Client) AcquireLock(ctx context.Context, key, value string, ttl time.Duration) error {
	ok, err := c.R.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return fmt.Errorf("acquire lock %s: %w", key, err)
	}
	if !ok {
		return ErrLockNotAcquired
	}
	return nil
}

// ReleaseLock releases the lock only if the value matches (prevents releasing
// another holder's lock). Uses a Lua script for atomicity.
func (c *Client) ReleaseLock(ctx context.Context, key, value string) error {
	const script = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0`
	return c.R.Eval(ctx, script, []string{key}, value).Err()
}
