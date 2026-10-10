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
	"fmt"
	"io"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// redisErr is a server reply error the way go-redis reports one.
type redisErr string

func (e redisErr) Error() string { return string(e) }
func (redisErr) RedisError()     {}

func TestTransient(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"dns", &net.DNSError{Err: "server misbehaving", Name: "cache.example.org"}, true},
		{"wrapped refused", fmt.Errorf("redis: ping on connect: %w", syscall.ECONNREFUSED), true},
		{"eof", fmt.Errorf("read: %w", io.EOF), true},
		{"timeout", fmt.Errorf("dial: %w", context.DeadlineExceeded), true},
		{"loading", redisErr("LOADING Redis is loading the dataset in memory"), true},
		{"masterdown", redisErr("MASTERDOWN Link with MASTER is down"), true},
		{"tryagain", redisErr("TRYAGAIN Multiple keys request during rehashing"), true},
		{"clusterdown", redisErr("CLUSTERDOWN The cluster is down"), true},
		{"wrongpass", redisErr("WRONGPASS invalid username-password pair"), false},
		{"noauth", redisErr("NOAUTH Authentication required."), false},
		{"wrapped wrongpass", fmt.Errorf("redis: ping on connect: %w", redisErr("WRONGPASS invalid username-password pair")), false},
		{"closed", goredis.ErrClosed, false},
		{"canceled", context.Canceled, false},
		{"plain", errors.New("bad option"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Transient(c.err); got != c.want {
				t.Fatalf("Transient(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// What Connect really returns while the server is unreachable is transient.
func TestTransient_RealConnectErrors(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := "127.0.0.1:" + strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()
	for name, addr := range map[string]string{"refused": closed, "dns": "cache.invalid:6379"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := Connect(ctx, WithAddr(addr), WithRetry(0, 0, 0))
			if err == nil || !Transient(err) {
				t.Fatalf("Connect error %v: Transient = false, want true", err)
			}
		})
	}
}

func TestWaitFor_RetriesTransientUntilSuccess(t *testing.T) {
	calls := 0
	var hooked []int
	err := WaitFor(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return redisErr("LOADING Redis is loading the dataset in memory")
		}
		return nil
	}, WithWaitBackoff(time.Millisecond, 2*time.Millisecond), WithRetryHook(func(attempt int, _ error, delay time.Duration) {
		if delay <= 0 || delay > 2*time.Millisecond {
			t.Errorf("attempt %d delay %v", attempt, delay)
		}
		hooked = append(hooked, attempt)
	}))
	if err != nil || calls != 3 || len(hooked) != 2 {
		t.Fatalf("WaitFor = %v after %d calls, hooks %v", err, calls, hooked)
	}
}

func TestWaitFor_PermanentErrorReturnsAtOnce(t *testing.T) {
	calls := 0
	bad := redisErr("WRONGPASS invalid username-password pair")
	err := WaitFor(context.Background(), func(context.Context) error { calls++; return bad })
	if !errors.Is(err, bad) || calls != 1 {
		t.Fatalf("WaitFor = %v after %d calls", err, calls)
	}
}

func TestWaitFor_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := WaitFor(ctx, func(context.Context) error { return syscall.ECONNREFUSED }, WithWaitBackoff(5*time.Millisecond, 10*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("WaitFor = %v", err)
	}
}

func TestWaitDelay_JitterStaysInTheBackoffWindow(t *testing.T) {
	for attempt := 1; attempt < 40; attempt++ {
		d := waitDelay(attempt, 100*time.Millisecond, 2*time.Second)
		full := min(100*time.Millisecond<<min(attempt-1, 20), 2*time.Second)
		if d < full/2 || d > full {
			t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, full/2, full)
		}
	}
}
