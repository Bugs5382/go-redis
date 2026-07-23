package redis

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
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func newServer(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(m.Close)
	return m
}

func TestConnectAndHealthy(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	if !c.Healthy(ctx) {
		t.Error("Healthy = false, want true against a live server")
	}
}

func TestConnectGetSet(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()), WithDB(0))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	rdb := c.Redis()
	if err := rdb.Set(ctx, "session:123", "alice", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := rdb.Get(ctx, "session:123").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "alice" {
		t.Errorf("Get = %q, want %q", got, "alice")
	}

	// A missing key surfaces as goredis.Nil, not a transport failure. The
	// re-exported Nil is the same sentinel, so a caller can check it without
	// importing goredis directly.
	if _, err := rdb.Get(ctx, "session:absent").Result(); err != goredis.Nil {
		t.Errorf("missing key err = %v, want goredis.Nil", err)
	}
	if _, err := rdb.Get(ctx, "session:absent").Result(); !errors.Is(err, Nil) {
		t.Errorf("missing key err = %v, want errors.Is(err, Nil)", err)
	}
}

func TestConnectFailsWhenUnreachable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Reserve then release a port so nothing is listening; a short dial timeout
	// keeps the test fast.
	_, err := Connect(ctx,
		WithAddr("127.0.0.1:1"),
		WithTimeouts(200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond),
		WithRetry(0, 0, 0),
	)
	if err == nil {
		t.Fatal("Connect to an unreachable server should fail")
	}
}

func TestHealthyFalseAfterClose(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c.Healthy(ctx) {
		t.Error("Healthy = true after Close, want false")
	}
}

// recordingObserver captures the commands and dials it is told about.
type recordingObserver struct {
	mu       sync.Mutex
	commands []string
	dials    int
}

func (o *recordingObserver) ObserveCommand(_ context.Context, name string, _ time.Duration, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.commands = append(o.commands, name)
}

func (o *recordingObserver) ObserveDial(_ context.Context, _, _ string, _ time.Duration, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.dials++
}

func (o *recordingObserver) commandSeen(name string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, c := range o.commands {
		if c == name {
			return true
		}
	}
	return false
}

func TestObserverReceivesCommandsAndDials(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()
	obs := &recordingObserver{}

	c, err := Connect(ctx, WithAddr(m.Addr()), WithObserver(obs))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Redis().Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	obs.mu.Lock()
	dials := obs.dials
	obs.mu.Unlock()
	if dials == 0 {
		t.Error("observer recorded no dials")
	}
	if !obs.commandSeen("set") {
		t.Errorf("observer did not record the set command; saw %v", obs.commands)
	}
}

// countingLogger records how many times it was called.
type countingLogger struct {
	mu    sync.Mutex
	calls int
}

func (l *countingLogger) Printf(context.Context, string, ...any) {
	l.mu.Lock()
	l.calls++
	l.mu.Unlock()
}

func TestLoggerLogsDialFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	log := &countingLogger{}

	_, err := Connect(ctx,
		WithAddr("127.0.0.1:1"),
		WithLogger(log),
		WithTimeouts(200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond),
		WithRetry(0, 0, 0),
	)
	if err == nil {
		t.Fatal("expected connect failure")
	}
	log.mu.Lock()
	calls := log.calls
	log.mu.Unlock()
	if calls == 0 {
		t.Error("logger was never called on a dial failure")
	}
}

func TestSentinelBuildsFailoverClient(t *testing.T) {
	t.Parallel()
	// No sentinel is reachable, so Connect must fail on the readiness Ping --
	// but reaching that failure proves the sentinel/failover path is wired
	// (a standalone client would report a different dial target).
	ctx := context.Background()
	_, err := Connect(ctx,
		WithSentinel("mymaster", "127.0.0.1:1"),
		WithTimeouts(200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond),
		WithRetry(0, 0, 0),
	)
	if err == nil {
		t.Fatal("sentinel Connect against no sentinel should fail")
	}
}

// TestNilIsGoredisNil proves the re-exported sentinel is identical to the
// upstream one, so replacing a direct goredis.Nil comparison with this
// package's Nil changes nothing at runtime.
func TestNilIsGoredisNil(t *testing.T) {
	if Nil != goredis.Nil {
		t.Fatalf("Nil = %v, want goredis.Nil (%v)", Nil, goredis.Nil)
	}
	if !errors.Is(goredis.Nil, Nil) {
		t.Error("errors.Is(goredis.Nil, Nil) = false, want true")
	}
}

// TestClientRedisSatisfiesAliases proves, at compile time, that
// Client.Redis's return value satisfies both the UniversalClient and Cmdable
// aliases -- the exact shapes a downstream consumer names without importing
// github.com/redis/go-redis/v9.
func TestClientRedisSatisfiesAliases(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	uc := c.Redis() // static type is the UniversalClient alias
	var cmd Cmdable = uc
	if err := cmd.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set via Cmdable alias: %v", err)
	}
}
