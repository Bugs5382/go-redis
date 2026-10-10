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
	"math/rand/v2"
	"net"
	"strings"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Default delays between WaitFor attempts: the first wait is about
// defaultWaitBase, doubling up to defaultWaitMax.
const (
	defaultWaitBase = 500 * time.Millisecond
	defaultWaitMax  = 15 * time.Second
)

// transientReplies are the server replies that mean "not ready yet": a
// server loading its dataset, a replica without its primary, a cluster that
// is down or rehashing.
var transientReplies = []string{"LOADING", "MASTERDOWN", "TRYAGAIN", "CLUSTERDOWN"}

// Transient reports whether err means the server is not reachable or not
// ready yet, as opposed to a failure that retrying will not fix. It is true
// for network failures (refused or reset connections, failed DNS lookups,
// timeouts, a cut connection) and for the LOADING, MASTERDOWN, TRYAGAIN and
// CLUSTERDOWN replies. Any other reply, such as WRONGPASS or NOAUTH, is not
// transient, and neither is a closed client. It unwraps the chain, so the
// errors Connect returns qualify.
func Transient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, goredis.ErrClosed) {
		return false
	}
	var reply goredis.Error
	if errors.As(err, &reply) {
		msg := reply.Error()
		for _, p := range transientReplies {
			if strings.HasPrefix(msg, p) {
				return true
			}
		}
		return false
	}
	var ne net.Error
	switch {
	case errors.As(err, &ne):
		return true
	case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return true
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, context.DeadlineExceeded):
		return true
	}
	return false
}

// WaitOption configures WaitFor.
type WaitOption func(*waitConfig)

type waitConfig struct {
	base, max time.Duration
	hook      func(attempt int, err error, delay time.Duration)
}

// WithWaitBackoff sets the delay before the first retry (base) and its cap
// (max); each wait doubles up to max, with jitter. Non-positive values keep
// the defaults (500ms and 15s).
func WithWaitBackoff(base, max time.Duration) WaitOption {
	return func(c *waitConfig) {
		if base > 0 {
			c.base = base
		}
		if max > 0 {
			c.max = max
		}
	}
}

// WithRetryHook calls hook after each transient failure, before the wait:
// attempt is 1-based, err is the failure and delay the wait that follows. Use
// it to log one line per attempt.
func WithRetryHook(hook func(attempt int, err error, delay time.Duration)) WaitOption {
	return func(c *waitConfig) { c.hook = hook }
}

// WaitFor calls fn until it succeeds, retrying while its error is Transient,
// so a service can start before its Redis or Valkey answers instead of
// exiting. A non-transient error is returned at once. When ctx ends first,
// the error wraps both ctx's error and the last attempt's. Each wait is a
// capped exponential backoff with jitter. fn is typically a Connect call.
func WaitFor(ctx context.Context, fn func(context.Context) error, opts ...WaitOption) error {
	cfg := waitConfig{base: defaultWaitBase, max: defaultWaitMax}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.max < cfg.base {
		cfg.max = cfg.base
	}
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("redis: gave up waiting: %w (last error: %w)", ctx.Err(), err)
		}
		if !Transient(err) {
			return err
		}
		delay := waitDelay(attempt, cfg.base, cfg.max)
		if cfg.hook != nil {
			cfg.hook(attempt, err, delay)
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("redis: gave up waiting: %w (last error: %w)", ctx.Err(), err)
		case <-t.C:
		}
	}
}

// waitDelay is a random delay in the upper half of the attempt's capped
// exponential window, so the wait still grows but clients that failed
// together spread out.
func waitDelay(attempt int, base, max time.Duration) time.Duration {
	full := base
	for i := 1; i < attempt && full < max; i++ {
		full *= 2
	}
	if full > max || full <= 0 {
		full = max
	}
	half := full / 2
	return half + rand.N(full-half+1) //nolint:gosec // jitter, not security
}
