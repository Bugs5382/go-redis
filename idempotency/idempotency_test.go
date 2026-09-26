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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/idempotency"
)

type receipt struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func TestDoRunsOnceAndReplays(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[receipt](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	var calls atomic.Int32
	fn := func(context.Context) (receipt, error) {
		calls.Add(1)
		return receipt{ID: "r-1", Amount: 500}, nil
	}
	for i := range 3 {
		got, err := s.Do(ctx, "pay:abc", time.Hour, fn)
		if err != nil || got != (receipt{ID: "r-1", Amount: 500}) {
			t.Fatalf("call %d: Do = %+v, %v", i, got, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times, want 1", n)
	}
	if ttl := m.TTL("idempotency:pay:abc"); ttl != time.Hour {
		t.Fatalf("result TTL = %v, want 1h", ttl)
	}
}

func TestDuplicateWhileRunningGetsErrInProgress(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
			close(started)
			<-release
			return "v", nil
		})
		done <- err
	}()
	<-started

	_, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
		t.Error("a duplicate must not run while the first is in flight")
		return "", nil
	})
	if !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("duplicate: err = %v, want ErrInProgress", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWithWaitReturnsTheFirstResult(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())), idempotency.WithWait(2*time.Second))
	ctx := context.Background()

	started := make(chan struct{})
	go func() {
		_, _ = s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
			close(started)
			time.Sleep(100 * time.Millisecond)
			return "first", nil
		})
	}()
	<-started
	got, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
		t.Error("a waiting duplicate must not run")
		return "second", nil
	})
	if err != nil || got != "first" {
		t.Fatalf("waiting duplicate = %q, %v; want the first result", got, err)
	}
}

func TestWithWaitGivesUp(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())), idempotency.WithWait(100*time.Millisecond))
	ctx := context.Background()

	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	go func() {
		_, _ = s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
			close(started)
			<-release
			return "v", nil
		})
	}()
	<-started
	start := time.Now()
	if _, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "", nil }); !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("err = %v, want ErrInProgress after the wait", err)
	}
	if d := time.Since(start); d < 90*time.Millisecond || d > time.Second {
		t.Fatalf("waited %v, want about 100ms", d)
	}
}

func TestWaiterTakesOverWhenTheFirstFails(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())), idempotency.WithWait(2*time.Second))
	ctx := context.Background()

	started := make(chan struct{})
	go func() {
		_, _ = s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
			close(started)
			time.Sleep(50 * time.Millisecond)
			return "", errors.New("first attempt failed")
		})
	}()
	<-started
	got, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "second", nil })
	if err != nil || got != "second" {
		t.Fatalf("waiter = %q, %v; want it to run once the key was released", got, err)
	}
}

func TestErrorReleasesTheKeyByDefault(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	boom := errors.New("gateway timeout")
	if _, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want fn's error", err)
	}
	if m.Exists("idempotency:k") {
		t.Fatal("ReleaseOnError should delete the key after a failure")
	}
	got, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "retried", nil })
	if err != nil || got != "retried" {
		t.Fatalf("retry = %q, %v", got, err)
	}
}

func TestKeepOnErrorReplaysTheFailure(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())), idempotency.WithFailurePolicy(idempotency.KeepOnError))
	ctx := context.Background()

	if _, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "", errors.New("card declined") }); err == nil {
		t.Fatal("want fn's error")
	}
	var calls atomic.Int32
	_, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
		calls.Add(1)
		return "", nil
	})
	if !errors.Is(err, idempotency.ErrFailed) || !strings.Contains(err.Error(), "card declined") {
		t.Fatalf("duplicate after a kept failure: err = %v, want ErrFailed with the message", err)
	}
	if calls.Load() != 0 {
		t.Fatal("KeepOnError must not run fn again")
	}
	if ttl := m.TTL("idempotency:k"); ttl != time.Hour {
		t.Fatalf("failure TTL = %v, want 1h", ttl)
	}
}

func TestLeaseExpiryLetsAnotherCallerRun(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())), idempotency.WithLease(time.Second))
	ctx := context.Background()

	// A caller claims the key and crashes (simulated by claiming and never
	// finishing: the pending entry is left behind).
	started, release := make(chan struct{}), make(chan struct{})
	firstDone := make(chan string, 1)
	go func() {
		v, _ := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) {
			close(started)
			<-release
			return "late", nil
		})
		firstDone <- v
	}()
	<-started
	if ttl := m.TTL("idempotency:k"); ttl != time.Second {
		t.Fatalf("pending TTL = %v, want the 1s lease", ttl)
	}
	m.FastForward(2 * time.Second)

	got, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "fresh", nil })
	if err != nil || got != "fresh" {
		t.Fatalf("after the lease ran out: %q, %v", got, err)
	}

	// The first caller finishes late: it keeps its own result but must not
	// overwrite the entry the second caller stored.
	close(release)
	if v := <-firstDone; v != "late" {
		t.Fatalf("late caller got %q", v)
	}
	replay, err := s.Do(ctx, "k", time.Hour, func(context.Context) (string, error) { return "third", nil })
	if err != nil || replay != "fresh" {
		t.Fatalf("stored result = %q, %v; the late finish overwrote it", replay, err)
	}
}

func TestPanicReleasesTheKey(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())))

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic should propagate")
			}
		}()
		_, _ = s.Do(context.Background(), "k", time.Hour, func(context.Context) (string, error) { panic("bug") })
	}()
	if m.Exists("idempotency:k") {
		t.Fatal("a panic must release the pending key")
	}
}

func TestPrefixAndCustomCodec(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())),
		idempotency.WithPrefix("orders:"), idempotency.WithCodec(rawString{}))
	if _, err := s.Do(context.Background(), "7", time.Hour, func(context.Context) (string, error) { return "shipped", nil }); err != nil {
		t.Fatal(err)
	}
	raw, err := m.Get("orders:7")
	if err != nil || !strings.HasSuffix(raw, "shipped") || strings.Contains(raw, `"`) {
		t.Fatalf("stored %q, %v; want the custom codec's bytes under the prefix", raw, err)
	}
}

// rawString stores strings as bytes, with no JSON quoting.
type rawString struct{}

func (rawString) Marshal(v any) ([]byte, error)      { return []byte(*v.(*string)), nil }
func (rawString) Unmarshal(data []byte, v any) error { *v.(*string) = string(data); return nil }

func TestCorruptEntryIsAnError(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[receipt](connect(t, redis.WithAddr(m.Addr())))
	if err := m.Set("idempotency:k", "garbage"); err != nil {
		t.Fatal(err)
	}
	_, err := s.Do(context.Background(), "k", time.Hour, func(context.Context) (receipt, error) {
		t.Error("fn must not run over an entry it cannot read")
		return receipt{}, nil
	})
	if err == nil {
		t.Fatal("want an error for an unreadable entry")
	}
}

func TestRejectsBadTTL(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())))
	if _, err := s.Do(context.Background(), "k", 0, func(context.Context) (string, error) { return "", nil }); err == nil {
		t.Fatal("a zero TTL should be refused")
	}
}

func TestLogsKeysNotValues(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	logs := newCaptureLog()
	s := idempotency.New[string](connect(t, redis.WithAddr(m.Addr())), idempotency.WithLogger(logs))
	ctx := context.Background()

	for range 2 {
		if _, err := s.Do(ctx, "charge:42", time.Hour, func(context.Context) (string, error) { return "tok_secret_value", nil }); err != nil {
			t.Fatal(err)
		}
	}
	out := logs.dump()
	if strings.Contains(out, "tok_secret_value") {
		t.Fatalf("a result leaked into the logs:\n%s", out)
	}
	for _, want := range []string{"idempotency:charge:42", "claimed", "replay"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}
