//go:build integration

package lock_test

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/lock"
)

// redisAddr returns the server address from REDIS_ADDR and skips the test when
// it is unset, so a local run without a server still passes. CI also sets
// REDIS_TEST_REQUIRED, which turns a missing address into a failure: broken
// service wiring must not pass as a run of skipped tests.
func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		if os.Getenv("REDIS_TEST_REQUIRED") != "" {
			t.Fatal("REDIS_TEST_REQUIRED is set but REDIS_ADDR is empty")
		}
		t.Skip("set REDIS_ADDR to run the integration test")
	}
	return addr
}

// TestIntegrationLock exercises real expiry against a server: auto-extend
// keeps a short lease alive well past its TTL, and without it the lease runs
// out and a stale release is refused. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./lock/
func TestIntegrationLock(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()
	c := connect(t, redis.WithAddr(addr))
	key := fmt.Sprintf("it:lock:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = c.Redis().Del(context.Background(), key).Err() })

	// Auto-extend holds a 300ms lease for a full second.
	l, err := lock.Acquire(ctx, c, key, 300*time.Millisecond, lock.WithAutoExtend(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	time.Sleep(time.Second)
	if _, err := lock.Acquire(ctx, c, key, time.Second); !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("auto-extended lock was free after 1s: %v", err)
	}
	select {
	case <-l.Lost():
		t.Fatal("Lost fired while auto-extend was keeping the lock")
	default:
	}
	if err := l.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Without auto-extend the lease really expires, and the stale holder
	// cannot release the next holder's lock.
	stale, err := lock.Acquire(ctx, c, key, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	next, err := lock.Acquire(ctx, c, key, 5*time.Second, lock.WithWait(2*time.Second))
	if err != nil {
		t.Fatalf("waiting Acquire after expiry: %v", err)
	}
	if err := stale.Release(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("stale Release: %v, want ErrNotHeld", err)
	}
	if got, _ := c.Redis().Get(ctx, key).Result(); got != next.Token() {
		t.Fatal("the stale Release touched the new holder's lock")
	}
	if err := next.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
}
