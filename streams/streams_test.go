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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/streams"
	goredis "github.com/redis/go-redis/v9"
)

// fast returns a config with short timings so the tests run quickly.
func fast(stream string) streams.Config {
	return streams.Config{
		Stream:        stream,
		Group:         "workers",
		Consumer:      "c1",
		StartID:       "0",
		Block:         20 * time.Millisecond,
		ClaimInterval: 30 * time.Millisecond,
		ClaimMinIdle:  50 * time.Millisecond,
		RetryMin:      10 * time.Millisecond,
		RetryMax:      50 * time.Millisecond,
	}
}

// run starts Consume in the background and returns a stop function that
// cancels it and waits for it to return.
func run(t *testing.T, c *redis.Client, cfg streams.Config, h streams.Handler, opts ...streams.Option) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- streams.Consume(ctx, c, cfg, h, opts...) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				err = errors.New("Consume did not return within 5s of cancel")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

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

func pending(t *testing.T, c *redis.Client, stream, group string) int64 {
	t.Helper()
	p, err := c.Redis().XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	return p.Count
}

func publish(t *testing.T, c *redis.Client, stream string, values map[string]any) string {
	t.Helper()
	id, err := streams.Publish(context.Background(), c, stream, values)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return id
}

func TestConsumeHandlesAndAcks(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	got := make(chan streams.Message, 1)
	stop := run(t, c, fast("orders"), func(_ context.Context, msg streams.Message) error {
		got <- msg
		return nil
	})

	id := publish(t, c, "orders", map[string]any{"order": "42"})
	select {
	case msg := <-got:
		if msg.ID != id || msg.Stream != "orders" || msg.Values["order"] != "42" || msg.Deliveries != 1 {
			t.Fatalf("message = %+v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never ran")
	}
	eventually(t, "the ack", func() bool { return pending(t, c, "orders", "workers") == 0 })
	if err := stop(); err != nil {
		t.Fatalf("Consume returned %v on cancel, want nil", err)
	}
}

func TestConsumeCreatesGroupAndStream(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	run(t, c, fast("fresh"), func(context.Context, streams.Message) error { return nil })
	eventually(t, "the group", func() bool {
		groups, err := c.Redis().XInfoGroups(context.Background(), "fresh").Result()
		return err == nil && len(groups) == 1 && groups[0].Name == "workers"
	})
}

func TestHandlerErrorIsRetriedAfterMinIdle(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	var calls atomic.Int32
	deliveries := make(chan int64, 4)
	run(t, c, fast("jobs"), func(_ context.Context, msg streams.Message) error {
		deliveries <- msg.Deliveries
		if calls.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})

	publish(t, c, "jobs", map[string]any{"n": "1"})
	for want := int64(1); want <= 2; want++ {
		select {
		case d := <-deliveries:
			if d != want {
				t.Fatalf("delivery %d reported Deliveries=%d", want, d)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("delivery %d never happened", want)
		}
	}
	eventually(t, "the ack after the retry", func() bool { return pending(t, c, "jobs", "workers") == 0 })
}

func TestMaxDeliveriesDeadLetters(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	cfg := fast("pay")
	cfg.MaxDeliveries = 3
	var calls atomic.Int32
	run(t, c, cfg, func(context.Context, streams.Message) error {
		calls.Add(1)
		return errors.New("card declined")
	})

	id := publish(t, c, "pay", map[string]any{"payment": "p-7"})
	eventually(t, "the dead letter", func() bool {
		n, _ := c.Redis().XLen(context.Background(), "pay:dead").Result()
		return n == 1
	})
	if n := calls.Load(); n != 3 {
		t.Fatalf("handler ran %d times, want MaxDeliveries = 3", n)
	}
	eventually(t, "the ack of the original", func() bool { return pending(t, c, "pay", "workers") == 0 })

	dead, err := c.Redis().XRange(context.Background(), "pay:dead", "-", "+").Result()
	if err != nil || len(dead) != 1 {
		t.Fatalf("XRANGE dead = %v, %v", dead, err)
	}
	v := dead[0].Values
	if v["payment"] != "p-7" || v[streams.FieldSourceID] != id || v[streams.FieldSourceStream] != "pay" ||
		v[streams.FieldGroup] != "workers" || v[streams.FieldDeliveries] != "3" || !strings.Contains(fmt.Sprint(v[streams.FieldError]), "card declined") {
		t.Fatalf("dead letter = %v", v)
	}
}

func TestErrDeadLetterSkipsRetries(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	cfg := fast("inbox")
	cfg.DeadLetterStream = "inbox:poison"
	var calls atomic.Int32
	run(t, c, cfg, func(context.Context, streams.Message) error {
		calls.Add(1)
		return fmt.Errorf("unparseable: %w", streams.ErrDeadLetter)
	})

	publish(t, c, "inbox", map[string]any{"raw": "x"})
	eventually(t, "the dead letter", func() bool {
		n, _ := c.Redis().XLen(context.Background(), "inbox:poison").Result()
		return n == 1
	})
	time.Sleep(150 * time.Millisecond) // longer than ClaimMinIdle
	if n := calls.Load(); n != 1 {
		t.Fatalf("handler ran %d times, want 1 for ErrDeadLetter", n)
	}
	if p := pending(t, c, "inbox", "workers"); p != 0 {
		t.Fatalf("%d messages still pending", p)
	}
}

func TestHandlerPanicIsRetried(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	var calls atomic.Int32
	run(t, c, fast("p"), func(context.Context, streams.Message) error {
		if calls.Add(1) == 1 {
			panic("bug")
		}
		return nil
	})
	publish(t, c, "p", map[string]any{"k": "v"})
	eventually(t, "the retry after a panic", func() bool { return calls.Load() >= 2 && pending(t, c, "p", "workers") == 0 })
}

func TestReclaimsFromDeadConsumer(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	if err := c.Redis().XGroupCreateMkStream(ctx, "work", "workers", "0").Err(); err != nil {
		t.Fatal(err)
	}
	id := publish(t, c, "work", map[string]any{"task": "t-1"})
	// A consumer reads the message and dies without acking it.
	if err := c.Redis().XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "workers", Consumer: "crashed", Streams: []string{"work", ">"}}).Err(); err != nil {
		t.Fatal(err)
	}

	got := make(chan streams.Message, 1)
	run(t, c, fast("work"), func(_ context.Context, msg streams.Message) error {
		got <- msg
		return nil
	})
	select {
	case msg := <-got:
		if msg.ID != id || msg.Deliveries != 2 {
			t.Fatalf("reclaimed message = %+v, want ID %s with Deliveries 2", msg, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pending message was never reclaimed")
	}
	eventually(t, "the ack", func() bool { return pending(t, c, "work", "workers") == 0 })
}

func TestReclaimedOverLimitGoesStraightToDeadLetter(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	if err := c.Redis().XGroupCreateMkStream(ctx, "w2", "workers", "0").Err(); err != nil {
		t.Fatal(err)
	}
	id := publish(t, c, "w2", map[string]any{"task": "t-2"})
	// Two crashed consumers each took the message once.
	if err := c.Redis().XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "workers", Consumer: "a", Streams: []string{"w2", ">"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Redis().XClaim(ctx, &goredis.XClaimArgs{Stream: "w2", Group: "workers", Consumer: "b", Messages: []string{id}}).Err(); err != nil {
		t.Fatal(err)
	}

	cfg := fast("w2")
	cfg.MaxDeliveries = 2
	var calls atomic.Int32
	run(t, c, cfg, func(context.Context, streams.Message) error {
		calls.Add(1)
		return nil
	})
	eventually(t, "the dead letter", func() bool {
		n, _ := c.Redis().XLen(ctx, "w2:dead").Result()
		return n == 1
	})
	if n := calls.Load(); n != 0 {
		t.Fatalf("handler ran %d times for a message already over the limit", n)
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	started := make(chan struct{})
	release := make(chan struct{})
	var handlerCtxErr atomic.Value
	stop := run(t, c, fast("drain"), func(ctx context.Context, _ streams.Message) error {
		close(started)
		<-release
		handlerCtxErr.Store(fmt.Sprint(ctx.Err()))
		return nil
	})
	publish(t, c, "drain", map[string]any{"k": "v"})
	<-started

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case <-stopped:
		t.Fatal("Consume returned while a handler was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := handlerCtxErr.Load(); got != "<nil>" {
		t.Fatalf("an in-flight handler's context was cancelled by shutdown: %v", got)
	}
	if p := pending(t, c, "drain", "workers"); p != 0 {
		t.Fatalf("the drained message was not acked, %d pending", p)
	}
}

func TestConcurrency(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	cfg := fast("par")
	cfg.Concurrency = 4
	var inFlight, peak atomic.Int32
	release := make(chan struct{})
	run(t, c, cfg, func(context.Context, streams.Message) error {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return nil
	})
	for i := range 4 {
		publish(t, c, "par", map[string]any{"i": fmt.Sprint(i)})
	}
	eventually(t, "4 handlers in flight", func() bool { return peak.Load() == 4 })
	close(release)
	eventually(t, "all acks", func() bool { return pending(t, c, "par", "workers") == 0 })
}

func TestRetriesRedisErrors(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	got := make(chan string, 4)
	run(t, c, fast("r"), func(_ context.Context, msg streams.Message) error {
		got <- fmt.Sprint(msg.Values["n"])
		return nil
	}, streams.WithLogger(logs))

	publish(t, c, "r", map[string]any{"n": "before"})
	if v := <-got; v != "before" {
		t.Fatalf("got %q", v)
	}
	eventually(t, "the ack", func() bool { return pending(t, c, "r", "workers") == 0 })

	// Every command fails for a while, as during a failover. (miniredis cannot
	// restart with blocked readers, so the outage is simulated with SetError;
	// the integration test kills the real connection instead.)
	m.SetError("LOADING Redis is loading the dataset in memory")
	eventually(t, "a logged retry", func() bool { return strings.Contains(logs.dump(), "failed, retrying") })
	m.SetError("")

	publish(t, c, "r", map[string]any{"n": "after"})
	select {
	case v := <-got:
		if v != "after" {
			t.Fatalf("got %q after the outage", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no delivery after the outage ended; logs:\n%s", logs.dump())
	}
	eventually(t, "the recovery line", func() bool { return strings.Contains(logs.dump(), "reading again after failures") })
}

func TestRecreatesGroupAfterStreamIsLost(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	got := make(chan string, 4)
	run(t, c, fast("lost"), func(_ context.Context, msg streams.Message) error {
		got <- fmt.Sprint(msg.Values["n"])
		return nil
	})
	publish(t, c, "lost", map[string]any{"n": "1"})
	<-got

	// A failover to a replica that never saw the group, or a DEL, loses it.
	m.Del("lost")
	eventually(t, "the group to be recreated", func() bool {
		groups, err := c.Redis().XInfoGroups(context.Background(), "lost").Result()
		return err == nil && len(groups) == 1
	})
	publish(t, c, "lost", map[string]any{"n": "2"})
	select {
	case v := <-got:
		if v != "2" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery after the group was recreated")
	}
}

func TestPublishMaxLen(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	for i := range 20 {
		if _, err := streams.Publish(ctx, c, "capped", map[string]any{"i": i}, streams.WithMaxLen(5)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// MAXLEN ~ trims lazily on a real server; miniredis trims exactly.
	if n, _ := c.Redis().XLen(ctx, "capped").Result(); n > 5 {
		t.Fatalf("XLEN = %d, want at most 5", n)
	}
}

func TestPublishRejectsEmptyValues(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	if _, err := streams.Publish(context.Background(), c, "s", nil); err == nil {
		t.Fatal("Publish with no fields should fail")
	}
}

func TestConsumeValidatesConfig(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	h := func(context.Context, streams.Message) error { return nil }

	for name, cfg := range map[string]streams.Config{
		"no stream":                 {Group: "g", Consumer: "c"},
		"no group":                  {Stream: "s", Consumer: "c"},
		"no consumer":               {Stream: "s", Group: "g"},
		"dead letter is the stream": {Stream: "s", Group: "g", Consumer: "c", DeadLetterStream: "s"},
	} {
		if err := streams.Consume(context.Background(), c, cfg, h); !errors.Is(err, streams.ErrInvalidConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidConfig", name, err)
		}
	}
	if err := streams.Consume(context.Background(), c, fast("s"), nil); !errors.Is(err, streams.ErrInvalidConfig) {
		t.Errorf("nil handler: err = %v, want ErrInvalidConfig", err)
	}
}

func TestConsumeFailsOnWrongType(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	if err := m.Set("notastream", "x"); err != nil {
		t.Fatal(err)
	}
	err := streams.Consume(context.Background(), c, fast("notastream"), func(context.Context, streams.Message) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("err = %v, want the WRONGTYPE failure", err)
	}
}

func TestLogsIDsNotValues(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	cfg := fast("audit")
	cfg.MaxDeliveries = 1
	id := publish(t, c, "audit", map[string]any{"ssn": "123-45-6789"})
	run(t, c, cfg, func(context.Context, streams.Message) error { return errors.New("nope") }, streams.WithLogger(logs))
	eventually(t, "the dead letter", func() bool {
		n, _ := c.Redis().XLen(context.Background(), "audit:dead").Result()
		return n == 1
	})

	out := logs.dump()
	if strings.Contains(out, "123-45-6789") {
		t.Fatalf("a message value leaked into the logs:\n%s", out)
	}
	for _, want := range []string{id, "audit", "workers", "dead-letter"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}
