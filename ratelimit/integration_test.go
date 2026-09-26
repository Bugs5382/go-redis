//go:build integration

package ratelimit_test

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
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/ratelimit"
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

// TestIntegrationRateLimit runs both strategies against a real server: the
// scripts' use of TIME, exact admission under concurrency, and refill on the
// server clock. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./ratelimit/
func TestIntegrationRateLimit(t *testing.T) {
	addr := redisAddr(t)
	c := connect(t, redis.WithAddr(addr))
	prefix := fmt.Sprintf("it:rl:%d:", time.Now().UnixNano())

	for name, strategy := range map[string]ratelimit.Strategy{"token bucket": ratelimit.TokenBucket, "sliding window": ratelimit.SlidingWindow} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			l := ratelimit.New(c, ratelimit.WithPrefix(prefix), ratelimit.WithStrategy(strategy))
			// A long period, so no refill lands while the burst is in flight.
			lim := ratelimit.Limit{Rate: 10, Period: time.Minute}
			key := "hot"

			var allowed atomic.Int32
			var wg sync.WaitGroup
			for range 40 {
				wg.Go(func() {
					r, err := l.Allow(ctx, key, lim)
					if err != nil {
						t.Errorf("Allow: %v", err)
						return
					}
					if r.Allowed {
						allowed.Add(1)
					}
				})
			}
			wg.Wait()
			if n := allowed.Load(); n != 10 {
				t.Fatalf("%d of 40 concurrent requests allowed, want exactly 10", n)
			}

			if r, err := l.Allow(ctx, key, lim); err != nil || r.Allowed || r.RetryAfter <= 0 || r.RetryAfter > time.Minute {
				t.Fatalf("over the limit = %+v, %v; want denied with a retry-after within the period", r, err)
			}

			// Refill on the real server clock.
			short := ratelimit.Limit{Rate: 1, Period: 300 * time.Millisecond}
			if r, err := l.Allow(ctx, "refill", short); err != nil || !r.Allowed {
				t.Fatalf("first = %+v, %v", r, err)
			}
			r, err := l.Allow(ctx, "refill", short)
			if err != nil || r.Allowed || r.RetryAfter <= 0 || r.RetryAfter > 300*time.Millisecond {
				t.Fatalf("second = %+v, %v; want denied with a retry-after under 300ms", r, err)
			}
			time.Sleep(r.RetryAfter + 20*time.Millisecond)
			if r, err := l.Allow(ctx, "refill", short); err != nil || !r.Allowed {
				t.Fatalf("after RetryAfter = %+v, %v; want allowed", r, err)
			}
		})
	}
	t.Cleanup(func() {
		_ = c.Redis().Del(context.Background(), prefix+"tb:hot", prefix+"sw:hot", prefix+"tb:refill", prefix+"sw:refill").Err()
	})
}
