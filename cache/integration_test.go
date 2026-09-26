//go:build integration

package cache_test

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
	"github.com/Bugs5382/go-redis/cache"
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

// TestIntegrationCache exercises get-or-set, stampede collapsing, TTLs and
// negative caching against a real server. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./cache/
func TestIntegrationCache(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()
	c := connect(t, redis.WithAddr(addr))
	prefix := fmt.Sprintf("it:cache:%d:", time.Now().UnixNano())
	users := cache.New[user](c, cache.WithPrefix(prefix), cache.WithJitter(0.1), cache.WithNegativeTTL(30*time.Second))
	t.Cleanup(func() { _ = users.Delete(context.Background(), "1", "404") })

	var calls atomic.Int32
	load := func(context.Context) (user, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return user{ID: 1, Name: "alice"}, nil
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if got, err := users.GetOrSet(ctx, "1", time.Minute, load); err != nil || got.Name != "alice" {
				t.Errorf("GetOrSet = %+v, %v", got, err)
			}
		})
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader ran %d times for 10 concurrent callers, want 1", n)
	}

	ttl, err := c.Redis().PTTL(ctx, prefix+"1").Result()
	if err != nil || ttl <= 50*time.Second || ttl > time.Minute {
		t.Fatalf("PTTL = %v, %v; want within the jittered minute", ttl, err)
	}

	notFound := func(context.Context) (user, error) { return user{}, cache.ErrNotFound }
	if _, err := users.GetOrSet(ctx, "404", time.Minute, notFound); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("GetOrSet on a missing value: %v", err)
	}
	if _, err := users.Get(ctx, "404"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get on the cached not-found: %v", err)
	}

	if err := users.Delete(ctx, "1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := users.Get(ctx, "1"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get after Delete: %v, want ErrMiss", err)
	}
}
