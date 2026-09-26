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
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/ratelimit"
	"github.com/alicebob/miniredis/v2"
)

// clock pins miniredis's TIME so refills are deterministic.
type clock struct {
	m   *miniredis.Miniredis
	now time.Time
}

func newClock(m *miniredis.Miniredis) *clock {
	c := &clock{m: m, now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	m.SetTime(c.now)
	return c
}

func (c *clock) advance(d time.Duration) {
	c.now = c.now.Add(d)
	c.m.SetTime(c.now)
}

func allow(t *testing.T, l *ratelimit.Limiter, key string, lim ratelimit.Limit) ratelimit.Result {
	t.Helper()
	r, err := l.Allow(context.Background(), key, lim)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return r
}

func TestTokenBucketBurstThenRefill(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	clk := newClock(m)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())))
	lim := ratelimit.Limit{Rate: 1, Period: time.Second, Burst: 3}

	for i, wantRemaining := range []int64{2, 1, 0} {
		r := allow(t, l, "api:alice", lim)
		if !r.Allowed || r.Remaining != wantRemaining || r.RetryAfter != 0 || r.Limit != 3 {
			t.Fatalf("request %d = %+v, want allowed with %d remaining", i+1, r, wantRemaining)
		}
	}
	r := allow(t, l, "api:alice", lim)
	if r.Allowed || r.Remaining != 0 || r.RetryAfter != time.Second {
		t.Fatalf("4th request = %+v, want denied with RetryAfter 1s", r)
	}

	clk.advance(500 * time.Millisecond)
	if r := allow(t, l, "api:alice", lim); r.Allowed || r.RetryAfter != 500*time.Millisecond {
		t.Fatalf("after 0.5s = %+v, want denied with RetryAfter 500ms", r)
	}
	clk.advance(500 * time.Millisecond)
	if r := allow(t, l, "api:alice", lim); !r.Allowed || r.Remaining != 0 {
		t.Fatalf("after 1s = %+v, want one token refilled and spent", r)
	}
}

func TestTokenBucketRefillIsCappedAtBurst(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	clk := newClock(m)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())))
	lim := ratelimit.PerSecond(5)

	allow(t, l, "k", lim)
	clk.advance(time.Hour)
	if r := allow(t, l, "k", lim); r.Remaining != 4 {
		t.Fatalf("after a long idle, Remaining = %d, want burst-1 = 4", r.Remaining)
	}
}

func TestTokenBucketClockGoingBackDoesNotMintTokens(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	clk := newClock(m)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())))
	lim := ratelimit.Limit{Rate: 1, Period: time.Second, Burst: 1}

	allow(t, l, "k", lim)
	clk.advance(-10 * time.Second) // a failover to a server with a slower clock
	if r := allow(t, l, "k", lim); r.Allowed {
		t.Fatalf("clock moved back and a token appeared: %+v", r)
	}
}

func TestAllowN(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	newClock(m)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())))
	lim := ratelimit.Limit{Rate: 10, Period: time.Second, Burst: 10}
	ctx := context.Background()

	r, err := l.AllowN(ctx, "bulk", lim, 7)
	if err != nil || !r.Allowed || r.Remaining != 3 {
		t.Fatalf("AllowN(7) = %+v, %v", r, err)
	}
	r, err = l.AllowN(ctx, "bulk", lim, 5)
	if err != nil || r.Allowed || r.Remaining != 3 || r.RetryAfter != 200*time.Millisecond {
		t.Fatalf("AllowN(5) with 3 left = %+v, %v; want denied, RetryAfter 200ms", r, err)
	}
	if _, err := l.AllowN(ctx, "bulk", lim, 11); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("AllowN above the burst: err = %v, want ErrInvalidLimit", err)
	}
	if _, err := l.AllowN(ctx, "bulk", lim, 0); !errors.Is(err, ratelimit.ErrInvalidLimit) {
		t.Fatalf("AllowN(0): err = %v, want ErrInvalidLimit", err)
	}
}

func TestInvalidLimits(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())))
	for name, lim := range map[string]ratelimit.Limit{
		"zero rate":      {Rate: 0, Period: time.Second},
		"zero period":    {Rate: 1},
		"tiny period":    {Rate: 1, Period: time.Microsecond},
		"negative burst": {Rate: 1, Period: time.Second, Burst: -1},
	} {
		if _, err := l.Allow(context.Background(), "k", lim); !errors.Is(err, ratelimit.ErrInvalidLimit) {
			t.Errorf("%s: err = %v, want ErrInvalidLimit", name, err)
		}
	}
}

func TestSlidingWindow(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	clk := newClock(m)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())), ratelimit.WithStrategy(ratelimit.SlidingWindow))
	lim := ratelimit.Limit{Rate: 3, Period: 10 * time.Second}

	for i, wantRemaining := range []int64{2, 1, 0} {
		r := allow(t, l, "login:bob", lim)
		if !r.Allowed || r.Remaining != wantRemaining || r.Limit != 3 {
			t.Fatalf("request %d = %+v, want allowed with %d remaining", i+1, r, wantRemaining)
		}
		clk.advance(time.Second)
	}
	// Now t0+3s: the window still holds requests from t0, t0+1s and t0+2s.
	r := allow(t, l, "login:bob", lim)
	if r.Allowed || r.RetryAfter != 7*time.Second {
		t.Fatalf("4th request = %+v, want denied with RetryAfter 7s", r)
	}
	clk.advance(7 * time.Second) // t0+10s: the t0 request leaves the window
	if r := allow(t, l, "login:bob", lim); !r.Allowed || r.Remaining != 0 {
		t.Fatalf("at t0+10s = %+v, want allowed", r)
	}
	if r := allow(t, l, "login:bob", lim); r.Allowed || r.RetryAfter != time.Second {
		t.Fatalf("next = %+v, want denied until the t0+1s request leaves", r)
	}
}

func TestSlidingWindowDeniedRequestsDoNotCount(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	clk := newClock(m)
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())), ratelimit.WithStrategy(ratelimit.SlidingWindow))
	lim := ratelimit.Limit{Rate: 1, Period: time.Second}

	allow(t, l, "k", lim)
	for range 5 {
		allow(t, l, "k", lim) // denied, and must not extend the window
	}
	clk.advance(time.Second)
	if r := allow(t, l, "k", lim); !r.Allowed {
		t.Fatalf("denied requests pushed the window out: %+v", r)
	}
}

func TestAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	for name, strategy := range map[string]ratelimit.Strategy{"token bucket": ratelimit.TokenBucket, "sliding window": ratelimit.SlidingWindow} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := newServer(t)
			newClock(m)
			l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())), ratelimit.WithStrategy(strategy))
			lim := ratelimit.Limit{Rate: 10, Period: time.Minute}

			var allowed atomic.Int32
			var wg sync.WaitGroup
			for range 50 {
				wg.Go(func() {
					r, err := l.Allow(context.Background(), "hot", lim)
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
				t.Fatalf("%d of 50 concurrent requests allowed, want exactly 10", n)
			}
		})
	}
}

func TestKeysArePrefixedPerStrategyAndExpire(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	newClock(m)
	c := connect(t, redis.WithAddr(m.Addr()))
	lim := ratelimit.Limit{Rate: 2, Period: time.Minute}

	allow(t, ratelimit.New(c), "u1", lim)
	allow(t, ratelimit.New(c, ratelimit.WithStrategy(ratelimit.SlidingWindow)), "u1", lim)
	allow(t, ratelimit.New(c, ratelimit.WithPrefix("rl:")), "u2", lim)

	for _, key := range []string{"ratelimit:tb:u1", "ratelimit:sw:u1", "rl:tb:u2"} {
		if !m.Exists(key) {
			t.Fatalf("missing %s; have %v", key, m.Keys())
		}
		if ttl := m.TTL(key); ttl <= 0 || ttl > 2*time.Minute {
			t.Errorf("%s TTL = %v, want a bounded expiry", key, ttl)
		}
	}
}

func TestWorksOnCluster(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	newClock(m)
	l := ratelimit.New(connect(t, redis.WithCluster(m.Addr())))
	if r := allow(t, l, "{tenant:9}:api", ratelimit.PerMinute(1)); !r.Allowed {
		t.Fatalf("first request on a cluster client = %+v", r)
	}
}

func TestLogsDecisions(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	newClock(m)
	logs := newCaptureLog()
	l := ratelimit.New(connect(t, redis.WithAddr(m.Addr())), ratelimit.WithLogger(logs))
	lim := ratelimit.Limit{Rate: 1, Period: time.Minute}

	allow(t, l, "ip:10.0.0.1", lim)
	allow(t, l, "ip:10.0.0.1", lim)
	out := logs.dump()
	for _, want := range []string{"ratelimit:tb:ip:10.0.0.1", "allowed", "denied"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}
