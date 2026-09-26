//go:build integration

package idempotency_test

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/idempotency"
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

// TestIntegrationIdempotency races concurrent duplicates against a real
// server and checks lease expiry frees a key left by a crashed caller. Run it
// with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./idempotency/
func TestIntegrationIdempotency(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()
	c := connect(t, redis.WithAddr(addr))
	prefix := fmt.Sprintf("it:idem:%d:", time.Now().UnixNano())
	t.Cleanup(func() { _ = c.Redis().Del(context.Background(), prefix+"once", prefix+"crash").Err() })

	s := idempotency.New[string](c, idempotency.WithPrefix(prefix), idempotency.WithWait(5*time.Second))

	var calls atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			got, err := s.Do(ctx, "once", time.Minute, func(context.Context) (string, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return "result", nil
			})
			if err != nil || got != "result" {
				t.Errorf("Do = %q, %v", got, err)
			}
		})
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times for 20 concurrent duplicates, want 1", n)
	}

	// A caller that crashed mid-run leaves a pending entry; with a short
	// lease the key frees itself and the next caller runs.
	short := idempotency.New[string](c, idempotency.WithPrefix(prefix), idempotency.WithLease(200*time.Millisecond))
	if err := c.Redis().Set(ctx, prefix+"crash", "pdead-token", 200*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := short.Do(ctx, "crash", time.Minute, func(context.Context) (string, error) { return "", nil }); !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("before the lease ran out: %v, want ErrInProgress", err)
	}
	time.Sleep(300 * time.Millisecond)
	got, err := short.Do(ctx, "crash", time.Minute, func(context.Context) (string, error) { return "recovered", nil })
	if err != nil || got != "recovered" {
		t.Fatalf("after the lease ran out: %q, %v", got, err)
	}
}
