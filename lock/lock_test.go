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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/lock"
)

// scriptCounter is an Observer that counts EVALSHA and EVAL calls.
type scriptCounter struct{ n atomic.Int64 }

func (s *scriptCounter) ObserveCommand(_ context.Context, name string, _ time.Duration, _ error) {
	if name == "evalsha" || name == "eval" {
		s.n.Add(1)
	}
}

func (s *scriptCounter) ObserveDial(context.Context, string, string, time.Duration, error) {}

func TestAcquireAndRelease(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	l, err := lock.Acquire(ctx, c, "jobs:nightly", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if l.Key() != "jobs:nightly" || l.Token() == "" {
		t.Fatalf("Key = %q, Token = %q", l.Key(), l.Token())
	}
	if got, _ := m.Get("jobs:nightly"); got != l.Token() {
		t.Fatalf("stored %q, want the lock token", got)
	}
	if ttl := m.TTL("jobs:nightly"); ttl != 10*time.Second {
		t.Fatalf("TTL = %v, want 10s", ttl)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if m.Exists("jobs:nightly") {
		t.Fatal("key still present after Release")
	}
	if err := l.Release(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("second Release: err = %v, want ErrNotHeld", err)
	}
}

func TestAcquireFailsFastWhenHeld(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	if _, err := lock.Acquire(ctx, c, "k", time.Minute); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := lock.Acquire(ctx, c, "k", time.Minute); !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("second Acquire: err = %v, want ErrNotAcquired", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("the default Acquire should fail fast")
	}
}

func TestTokensAreUnique(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	seen := map[string]bool{}
	for i := range 50 {
		l, err := lock.Acquire(context.Background(), c, "u", time.Minute)
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		if seen[l.Token()] {
			t.Fatal("token repeated")
		}
		seen[l.Token()] = true
		if err := l.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReleaseNeverDeletesAnotherHoldersLock(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	a, err := lock.Acquire(ctx, c, "shared", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m.FastForward(2 * time.Second) // a's lease runs out while it still thinks it holds the lock
	b, err := lock.Acquire(ctx, c, "shared", time.Minute)
	if err != nil {
		t.Fatalf("b should acquire the expired lock: %v", err)
	}

	if err := a.Release(ctx); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("stale Release: err = %v, want ErrNotHeld", err)
	}
	if got, _ := m.Get("shared"); got != b.Token() {
		t.Fatal("a stale Release deleted the new holder's lock")
	}
	if err := a.Extend(ctx, time.Minute); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("stale Extend: err = %v, want ErrNotHeld", err)
	}
	if ttl := m.TTL("shared"); ttl != time.Minute {
		t.Fatalf("a stale Extend changed the TTL to %v", ttl)
	}
}

func TestExtend(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	l, err := lock.Acquire(ctx, c, "e", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Extend(ctx, 30*time.Second); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if ttl := m.TTL("e"); ttl != 30*time.Second {
		t.Fatalf("TTL after Extend = %v, want 30s", ttl)
	}
	m.FastForward(31 * time.Second)
	if err := l.Extend(ctx, time.Second); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("Extend after expiry: err = %v, want ErrNotHeld", err)
	}
}

func TestAcquireWaitsForRelease(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	held, err := lock.Acquire(ctx, c, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = held.Release(ctx)
	}()
	start := time.Now()
	l, err := lock.Acquire(ctx, c, "w", time.Minute, lock.WithWait(2*time.Second), lock.WithBackoff(5*time.Millisecond, 20*time.Millisecond))
	if err != nil {
		t.Fatalf("waiting Acquire: %v", err)
	}
	if time.Since(start) < 90*time.Millisecond {
		t.Fatal("Acquire returned before the holder released")
	}
	_ = l.Release(ctx)
}

func TestAcquireWaitGivesUp(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	ctx := context.Background()

	if _, err := lock.Acquire(ctx, c, "g", time.Minute); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := lock.Acquire(ctx, c, "g", time.Minute, lock.WithWait(100*time.Millisecond), lock.WithBackoff(5*time.Millisecond, 20*time.Millisecond))
	if !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("err = %v, want ErrNotAcquired", err)
	}
	if d := time.Since(start); d < 90*time.Millisecond || d > time.Second {
		t.Fatalf("gave up after %v, want about 100ms", d)
	}
}

func TestAcquireWaitHonoursContext(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	if _, err := lock.Acquire(context.Background(), c, "x", time.Minute); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := lock.Acquire(ctx, c, "x", time.Minute, lock.WithWait(time.Minute))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestAcquireRejectsBadTTL(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	if _, err := lock.Acquire(context.Background(), c, "t", 0); err == nil {
		t.Fatal("a zero TTL should be refused")
	}
	if _, err := lock.Acquire(context.Background(), c, "t", 500*time.Microsecond); err == nil {
		t.Fatal("a sub-millisecond TTL should be refused")
	}
}

func TestAutoExtendKeepsExtendingUntilRelease(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	obs := &scriptCounter{}
	c := connect(t, redis.WithAddr(m.Addr()), redis.WithObserver(obs))
	ctx := context.Background()

	l, err := lock.Acquire(ctx, c, "auto", time.Second, lock.WithAutoExtend(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if n := obs.n.Load(); n < 4 {
		t.Fatalf("%d extend scripts in 150ms at a 20ms interval, want at least 4", n)
	}
	select {
	case <-l.Lost():
		t.Fatal("Lost closed while the lock is held")
	default:
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	after := obs.n.Load()
	time.Sleep(100 * time.Millisecond)
	if n := obs.n.Load(); n != after {
		t.Fatalf("auto-extend kept running after Release (%d more calls)", n-after)
	}
}

func TestAutoExtendStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	obs := &scriptCounter{}
	c := connect(t, redis.WithAddr(m.Addr()), redis.WithObserver(obs))

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := lock.Acquire(ctx, c, "cancel", time.Second, lock.WithAutoExtend(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	cancel()
	time.Sleep(40 * time.Millisecond)
	before := obs.n.Load()
	time.Sleep(100 * time.Millisecond)
	if n := obs.n.Load(); n != before {
		t.Fatalf("auto-extend kept running after cancel (%d more calls)", n-before)
	}
	if !m.Exists("cancel") {
		t.Fatal("cancelling the context must not release the lock; it should expire on its own")
	}
}

func TestAutoExtendReportsLoss(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	l, err := lock.Acquire(context.Background(), c, "lost", time.Second,
		lock.WithAutoExtend(20*time.Millisecond), lock.WithLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	m.Del("lost") // someone deleted it, or it expired during a long pause
	select {
	case <-l.Lost():
	case <-time.After(2 * time.Second):
		t.Fatal("Lost never closed after the key disappeared")
	}
	if err := l.Release(context.Background()); !errors.Is(err, lock.ErrNotHeld) {
		t.Fatalf("Release after loss: err = %v, want ErrNotHeld", err)
	}
	if !strings.Contains(logs.dump(), "lost") {
		t.Fatalf("the loss should be logged:\n%s", logs.dump())
	}
}

func TestLostIsNeverClosedWithoutAutoExtend(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	l, err := lock.Acquire(context.Background(), c, "plain", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.Lost():
		t.Fatal("Lost closed on a normal Release")
	default:
	}
}

func TestMutualExclusion(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	var inside, maxInside atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			ctx := context.Background()
			l, err := lock.Acquire(ctx, c, "mutex", time.Minute, lock.WithWait(5*time.Second), lock.WithBackoff(time.Millisecond, 5*time.Millisecond))
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			n := inside.Add(1)
			for {
				p := maxInside.Load()
				if n <= p || maxInside.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inside.Add(-1)
			if err := l.Release(ctx); err != nil {
				t.Errorf("Release: %v", err)
			}
		})
	}
	wg.Wait()
	if n := maxInside.Load(); n != 1 {
		t.Fatalf("%d holders at once, want 1", n)
	}
}

func TestLogsKeyNotToken(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	l, err := lock.Acquire(context.Background(), c, "reports:daily", time.Minute, lock.WithLogger(logs))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := logs.dump()
	if strings.Contains(out, l.Token()) {
		t.Fatalf("the lock token leaked into the logs:\n%s", out)
	}
	for _, want := range []string{"reports:daily", "acquired", "released"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}
