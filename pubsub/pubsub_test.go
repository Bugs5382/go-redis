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
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/pubsub"
)

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func subscribe(t *testing.T, c *redis.Client, topics pubsub.Topics, h pubsub.Handler, opts ...pubsub.Option) *pubsub.Subscriber {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	s, err := pubsub.Subscribe(ctx, c, topics, h, opts...)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func fastOpts(extra ...pubsub.Option) []pubsub.Option {
	return append([]pubsub.Option{pubsub.WithBackoff(10*time.Millisecond, 50*time.Millisecond)}, extra...)
}

func TestSubscribeDeliversChannelsAndPatterns(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	got := make(chan pubsub.Message, 4)
	s := subscribe(t, c, pubsub.Topics{Channels: []string{"news"}, Patterns: []string{"orders.*"}},
		func(_ context.Context, msg pubsub.Message) { got <- msg }, fastOpts()...)
	if !s.Healthy() {
		t.Fatal("Healthy = false right after Subscribe returned")
	}

	ctx := context.Background()
	if err := c.Redis().Publish(ctx, "news", "hello").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Redis().Publish(ctx, "orders.created", "o-1").Err(); err != nil {
		t.Fatal(err)
	}
	want := map[string]pubsub.Message{
		"news":           {Channel: "news", Payload: "hello"},
		"orders.created": {Channel: "orders.created", Pattern: "orders.*", Payload: "o-1"},
	}
	for range 2 {
		select {
		case msg := <-got:
			if w := want[msg.Channel]; msg != w {
				t.Fatalf("message = %+v, want %+v", msg, w)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("message not delivered")
		}
	}
}

func TestDuplicateTopicsStillBecomeHealthy(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	s := subscribe(t, c, pubsub.Topics{Channels: []string{"a", "a", "b"}, Patterns: []string{"p.*", "p.*"}},
		func(context.Context, pubsub.Message) {}, fastOpts()...)
	if !s.Healthy() {
		t.Fatal("Healthy = false with duplicate topic names")
	}
}

func TestSubscribeValidates(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	h := func(context.Context, pubsub.Message) {}

	if _, err := pubsub.Subscribe(context.Background(), c, pubsub.Topics{}, h); !errors.Is(err, pubsub.ErrNoTopics) {
		t.Fatalf("no topics: err = %v, want ErrNoTopics", err)
	}
	if _, err := pubsub.Subscribe(context.Background(), c, pubsub.Topics{Channels: []string{"a"}}, nil); err == nil {
		t.Fatal("a nil handler should be refused")
	}
}

func TestResubscribesAfterRestart(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()), redis.WithRetry(0, 0, 0))
	logs := newCaptureLog()

	got := make(chan string, 16)
	s := subscribe(t, c, pubsub.Topics{Channels: []string{"events"}, Patterns: []string{"audit.*"}},
		func(_ context.Context, msg pubsub.Message) { got <- msg.Payload }, fastOpts(pubsub.WithLogger(logs))...)

	m.Close()
	eventually(t, "Healthy to turn false", func() bool { return !s.Healthy() })
	if err := m.Restart(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	eventually(t, "Healthy to turn true again", s.Healthy)

	// Both the channel and the pattern must be back.
	for _, ch := range []string{"events", "audit.login"} {
		if err := c.Redis().Publish(context.Background(), ch, "after-"+ch).Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case p := <-got:
			if p != "after-"+ch {
				t.Fatalf("got %q", p)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no delivery on %s after the restart; logs:\n%s", ch, logs.dump())
		}
	}
	out := logs.dump()
	for _, want := range []string{"warn", "subscribed again"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}

func TestCloseStops(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	var calls atomic.Int32
	s := subscribe(t, c, pubsub.Topics{Channels: []string{"x"}}, func(context.Context, pubsub.Message) { calls.Add(1) }, fastOpts()...)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after Close")
	}
	if s.Healthy() {
		t.Fatal("Healthy = true after Close")
	}
	_ = c.Redis().Publish(context.Background(), "x", "late").Err()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("handler ran after Close")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestContextCancelStops(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	ctx, cancel := context.WithCancel(context.Background())
	s, err := pubsub.Subscribe(ctx, c, pubsub.Topics{Channels: []string{"x"}}, func(context.Context, pubsub.Message) {}, fastOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after the context was cancelled")
	}
}

func TestSubscribeHonoursContext(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()), redis.WithRetry(0, 0, 0))
	m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := pubsub.Subscribe(ctx, c, pubsub.Topics{Channels: []string{"x"}}, func(context.Context, pubsub.Message) {}, fastOpts()...)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe with the server down: err = %v, want context.DeadlineExceeded", err)
	}
}

func TestHandlerPanicIsRecovered(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	got := make(chan string, 2)
	subscribe(t, c, pubsub.Topics{Channels: []string{"p"}}, func(_ context.Context, msg pubsub.Message) {
		if msg.Payload == "boom" {
			panic("bug")
		}
		got <- msg.Payload
	}, fastOpts(pubsub.WithLogger(logs))...)

	ctx := context.Background()
	_ = c.Redis().Publish(ctx, "p", "boom").Err()
	_ = c.Redis().Publish(ctx, "p", "fine").Err()
	select {
	case p := <-got:
		if p != "fine" {
			t.Fatalf("got %q", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("subscriber stopped after a handler panic:\n%s", logs.dump())
	}
	if !strings.Contains(logs.dump(), "panicked") {
		t.Fatalf("the panic should be logged:\n%s", logs.dump())
	}
}

func TestPingKeepsIdleSubscriptionHealthy(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	s := subscribe(t, c, pubsub.Topics{Channels: []string{"quiet"}}, func(context.Context, pubsub.Message) {},
		fastOpts(pubsub.WithPingInterval(30*time.Millisecond), pubsub.WithLogger(logs))...)
	time.Sleep(200 * time.Millisecond)
	if !s.Healthy() {
		t.Fatalf("an idle subscription with working pings went unhealthy:\n%s", logs.dump())
	}
	if !strings.Contains(logs.dump(), "pong") {
		t.Fatalf("pings should be answered while idle:\n%s", logs.dump())
	}
}

func TestLogsChannelsNotPayloads(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	got := make(chan struct{}, 1)
	subscribe(t, c, pubsub.Topics{Channels: []string{"secrets"}}, func(context.Context, pubsub.Message) { got <- struct{}{} },
		fastOpts(pubsub.WithLogger(logs))...)
	_ = c.Redis().Publish(context.Background(), "secrets", "hunter2").Err()
	<-got

	out := logs.dump()
	if strings.Contains(out, "hunter2") {
		t.Fatalf("a payload leaked into the logs:\n%s", out)
	}
	if !strings.Contains(out, "secrets") {
		t.Fatalf("logs should name the channel:\n%s", out)
	}
}
