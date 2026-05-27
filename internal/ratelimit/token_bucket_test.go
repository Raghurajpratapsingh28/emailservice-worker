package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLimiter(t *testing.T, ratePerSec int) (*TokenBucket, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewTokenBucket(rdb, ratePerSec), mr
}

func TestTokenBucket_TryAcquire_AllowsBurstUpToCapacity(t *testing.T) {
	lim, _ := newTestLimiter(t, 5)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		ok, _, err := lim.TryAcquire(ctx, "ws:1")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("call %d: expected allow within burst capacity", i)
		}
	}

	// 6th call within the same instant should be denied.
	ok, wait, err := lim.TryAcquire(ctx, "ws:1")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected deny after burst exhausted")
	}
	if wait <= 0 {
		t.Errorf("expected positive wait time, got %v", wait)
	}
}

func TestTokenBucket_TryAcquire_PerKeyIsolation(t *testing.T) {
	lim, _ := newTestLimiter(t, 1)
	ctx := context.Background()

	// ws:A consumes its only token
	ok, _, _ := lim.TryAcquire(ctx, "ws:A")
	if !ok {
		t.Fatal("ws:A first acquire should succeed")
	}
	// ws:A is now empty
	ok, _, _ = lim.TryAcquire(ctx, "ws:A")
	if ok {
		t.Fatal("ws:A second acquire should fail")
	}
	// ws:B has its own bucket
	ok, _, _ = lim.TryAcquire(ctx, "ws:B")
	if !ok {
		t.Fatal("ws:B acquire should succeed (different bucket)")
	}
}

func TestTokenBucket_Refills(t *testing.T) {
	lim, _ := newTestLimiter(t, 10) // 10 per sec
	ctx := context.Background()

	// Drain the bucket
	for i := 0; i < 10; i++ {
		if ok, _, _ := lim.TryAcquire(ctx, "ws:1"); !ok {
			t.Fatalf("burst call %d should succeed", i)
		}
	}
	if ok, _, _ := lim.TryAcquire(ctx, "ws:1"); ok {
		t.Fatal("should be exhausted after burst")
	}

	// Wait > 100ms so at least 1 token should refill (10/sec means 1 token every 100ms)
	time.Sleep(150 * time.Millisecond)
	if ok, _, _ := lim.TryAcquire(ctx, "ws:1"); !ok {
		t.Fatal("expected at least one token to refill after 150ms")
	}
}

func TestTokenBucket_Acquire_BlocksUntilAvailable(t *testing.T) {
	lim, _ := newTestLimiter(t, 5) // 5 per sec, capacity 5
	ctx := context.Background()

	// Drain
	for i := 0; i < 5; i++ {
		if _, err := lim.Acquire(ctx, "ws:1", 100*time.Millisecond); err != nil {
			t.Fatalf("burst %d: %v", i, err)
		}
	}

	// Next acquire should block ~200ms (5/sec → 1 token / 200ms)
	start := time.Now()
	waited, err := lim.Acquire(ctx, "ws:1", 1*time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("acquire after drain: %v", err)
	}
	if waited <= 0 {
		t.Errorf("expected non-zero wait, got %v", waited)
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("expected at least 100ms wait, elapsed %v", elapsed)
	}
}

func TestTokenBucket_Acquire_ReturnsErrIfBudgetExceeded(t *testing.T) {
	lim, _ := newTestLimiter(t, 1) // very tight budget: 1/sec
	ctx := context.Background()

	// Use the only token
	if _, err := lim.Acquire(ctx, "ws:1", 50*time.Millisecond); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Next acquire would need ~1s; budget is 50ms.
	_, err := lim.Acquire(ctx, "ws:1", 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected error when wait budget would be exceeded")
	}
	if !errors.Is(err, ErrRateLimitExceeded) {
		t.Errorf("expected ErrRateLimitExceeded, got %v", err)
	}
}

func TestTokenBucket_Acquire_RespectsContextCancellation(t *testing.T) {
	lim, _ := newTestLimiter(t, 1)
	ctx, cancel := context.WithCancel(context.Background())

	// Drain
	if _, err := lim.Acquire(ctx, "ws:1", 50*time.Millisecond); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Cancel during a long wait
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := lim.Acquire(ctx, "ws:1", 5*time.Second)
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestTokenBucket_ConcurrentAcquire_AllAccountedFor(t *testing.T) {
	// Run many goroutines against a single bucket — confirm we never exceed
	// capacity + small refill slack (since the test takes wall-clock time).
	const capacity = 20
	lim, _ := newTestLimiter(t, capacity)

	var wg sync.WaitGroup
	allowed := make(chan bool, 100)
	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := lim.TryAcquire(context.Background(), "ws:concurrent")
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				return
			}
			allowed <- ok
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(allowed)

	count := 0
	for ok := range allowed {
		if ok {
			count++
		}
	}
	// During elapsed time ~ N ms, up to ceil(N*rate/1000) extra tokens may refill.
	maxAllowed := capacity + int(elapsed.Seconds()*float64(capacity)) + 1
	if count < capacity {
		t.Errorf("expected at least %d allowed (capacity), got %d", capacity, count)
	}
	if count > maxAllowed {
		t.Errorf("expected at most %d allowed (capacity + refill in %v), got %d",
			maxAllowed, elapsed, count)
	}
}
