//go:build integration

package pubsub_test

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
	"strings"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/pubsub"
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

// TestIntegrationResubscribe subscribes against a real server, has the server
// kill the subscriber's connection, and checks that both the channel and the
// pattern are subscribed again. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./pubsub/
func TestIntegrationResubscribe(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()
	c := connect(t, redis.WithAddr(addr))
	admin := connect(t, redis.WithAddr(addr))
	logs := newCaptureLog()

	prefix := fmt.Sprintf("it:pubsub:%d", time.Now().UnixNano())
	got := make(chan string, 16)
	s := subscribe(t, c, pubsub.Topics{Channels: []string{prefix + ":ch"}, Patterns: []string{prefix + ":p.*"}},
		func(_ context.Context, msg pubsub.Message) { got <- msg.Payload },
		fastOpts(pubsub.WithLogger(logs), pubsub.WithPingInterval(200*time.Millisecond))...)

	expect := func(channel, payload string) {
		t.Helper()
		// Publish until it lands: a message sent before the re-subscribe
		// completes is lost, which is the documented at-most-once behaviour.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if err := admin.Redis().Publish(ctx, channel, payload).Err(); err != nil {
				t.Fatalf("PUBLISH: %v", err)
			}
			select {
			case p := <-got:
				if p == payload {
					return
				}
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatalf("never received %q on %s; logs:\n%s", payload, channel, logs.dump())
	}

	expect(prefix+":ch", "one")
	expect(prefix+":p.x", "two")

	if err := admin.Redis().Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err(); err != nil {
		t.Fatalf("CLIENT KILL: %v", err)
	}
	eventually(t, "the re-subscribe", func() bool {
		return s.Healthy() && strings.Contains(logs.dump(), "subscribed again after reconnecting")
	})
	expect(prefix+":ch", "three")
	expect(prefix+":p.y", "four")
}
