//go:build integration

package streams_test

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
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/streams"
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

// TestIntegrationStreams runs a consumer against a real server: a normal ack,
// a retry through XAUTOCLAIM, a dead letter, and recovery after the server
// kills the consumer's connection. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./streams/
func TestIntegrationStreams(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()
	c := connect(t, redis.WithAddr(addr))
	admin := connect(t, redis.WithAddr(addr))

	stream := fmt.Sprintf("it:streams:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = admin.Redis().Del(context.Background(), stream, stream+":dead").Err() })

	cfg := fast(stream)
	cfg.ClaimMinIdle = 200 * time.Millisecond
	cfg.ClaimInterval = 100 * time.Millisecond
	cfg.Block = 100 * time.Millisecond
	cfg.MaxDeliveries = 2

	var flaky atomic.Int32
	seen := make(chan string, 16)
	run(t, c, cfg, func(_ context.Context, msg streams.Message) error {
		kind := fmt.Sprint(msg.Values["kind"])
		seen <- fmt.Sprintf("%s#%d", kind, msg.Deliveries)
		switch kind {
		case "flaky":
			if flaky.Add(1) == 1 {
				return errors.New("transient")
			}
		case "poison":
			return errors.New("never works")
		}
		return nil
	})

	expect := func(want string) {
		t.Helper()
		deadline := time.After(10 * time.Second)
		for {
			select {
			case got := <-seen:
				if got == want {
					return
				}
			case <-deadline:
				t.Fatalf("never saw %s", want)
			}
		}
	}

	publish(t, c, stream, map[string]any{"kind": "ok"})
	expect("ok#1")
	publish(t, c, stream, map[string]any{"kind": "flaky"})
	expect("flaky#1")
	expect("flaky#2")
	publish(t, c, stream, map[string]any{"kind": "poison"})
	expect("poison#2")
	eventually(t, "the dead letter", func() bool {
		n, _ := admin.Redis().XLen(ctx, stream+":dead").Result()
		return n == 1
	})
	eventually(t, "every ack", func() bool { return pending(t, admin, stream, "workers") == 0 })

	// Drop every other client's connection; the consumer must reconnect.
	if err := admin.Redis().Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err(); err != nil {
		t.Fatalf("CLIENT KILL: %v", err)
	}
	publish(t, admin, stream, map[string]any{"kind": "after-kill"})
	expect("after-kill#1")
}
