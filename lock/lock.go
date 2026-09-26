package lock

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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/script"
)

// ErrNotAcquired is returned by Acquire when another holder has the lock and
// it did not come free within the wait (or at once, without WithWait).
var ErrNotAcquired = errors.New("lock: not acquired, held elsewhere")

// ErrNotHeld is returned by Release and Extend when this Lock no longer owns
// the key: it expired, was released already, or was taken by someone else.
var ErrNotHeld = errors.New("lock: not held")

// Defaults for the wait backoff.
const (
	DefaultBackoffMin = 10 * time.Millisecond
	DefaultBackoffMax = 500 * time.Millisecond
)

// release deletes the key only if it still holds our token, so a holder whose
// lease ran out can never delete the next holder's lock.
var release = script.New(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`, script.WithName("lock.release"))

// extend resets the TTL only if the key still holds our token.
var extend = script.New(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0`, script.WithName("lock.extend"))

// options holds what an Option sets.
type options struct {
	wait       time.Duration
	backoffMin time.Duration
	backoffMax time.Duration
	autoExtend time.Duration
	log        log.Logger
}

// Option configures Acquire.
type Option func(*options)

// WithWait makes Acquire retry for up to d while the lock is held elsewhere,
// with exponential backoff (see WithBackoff), instead of failing at once. The
// context passed to Acquire still bounds the wait.
func WithWait(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.wait = d
		}
	}
}

// WithBackoff bounds the delay between attempts while waiting. Defaults
// DefaultBackoffMin and DefaultBackoffMax; non-positive values keep them.
func WithBackoff(minDelay, maxDelay time.Duration) Option {
	return func(o *options) {
		if minDelay > 0 {
			o.backoffMin = minDelay
		}
		if maxDelay > 0 {
			o.backoffMax = maxDelay
		}
	}
}

// WithAutoExtend keeps the lock alive while work runs: every interval it
// resets the TTL to the ttl given to Acquire. It stops on Release, when the
// context passed to Acquire ends, or when an extend finds the lock gone, in
// which case Lost is closed. Pick an interval well under the ttl, such as a
// third of it. A non-positive interval uses ttl/3.
func WithAutoExtend(interval time.Duration) Option {
	return func(o *options) {
		o.autoExtend = interval
		if interval <= 0 {
			o.autoExtend = -1 // resolved to ttl/3 in Acquire
		}
	}
}

// WithLogger sends diagnostics to l. Each call derives a logger from the
// call's context with l.Ctx, so trace and span IDs are attached. The default
// discards everything. The key is logged; the token never is.
func WithLogger(l log.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}

// Lock is a held lock. Release it when the work is done. A Lock is safe for
// concurrent use.
type Lock struct {
	client *redis.Client
	key    string
	token  string
	ttl    time.Duration
	log    log.Logger

	lost     chan struct{}
	lostOnce sync.Once
	stop     context.CancelFunc
	stopped  chan struct{}
}

// Acquire takes the lock on key for ttl with SET key token NX PX ttl. The
// token is 128 random bits, so only this Lock can release or extend it.
//
// Without WithWait it fails at once with ErrNotAcquired when the key is held.
// With WithWait it retries with backoff until the lock comes free, the wait
// runs out (ErrNotAcquired), or ctx ends (ctx's error). With WithAutoExtend,
// ctx also bounds the background extension: when ctx ends extension stops and
// the lock expires ttl later unless it is released first.
//
// The lock is only as safe as a single Redis primary: see the package docs
// for what that means under failover.
func Acquire(ctx context.Context, c *redis.Client, key string, ttl time.Duration, opts ...Option) (*Lock, error) {
	if ttl < time.Millisecond {
		return nil, fmt.Errorf("lock: ttl %v is below the 1ms minimum", ttl)
	}
	o := options{backoffMin: DefaultBackoffMin, backoffMax: DefaultBackoffMax, log: nopLogger{}}
	for _, opt := range opts {
		opt(&o)
	}
	o.backoffMax = max(o.backoffMax, o.backoffMin)
	if o.autoExtend < 0 {
		o.autoExtend = max(ttl/3, time.Millisecond)
	}
	l := o.log.Ctx(ctx).With(log.F("key", key))

	token, err := newToken()
	if err != nil {
		l.Error(err, "lock: could not generate a token")
		return nil, err
	}

	start := time.Now()
	deadline := start.Add(o.wait)
	delay := o.backoffMin
	for attempt := 1; ; attempt++ {
		ok, err := c.Redis().SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			l.Error(err, "lock: acquire failed", log.F("attempt", attempt))
			return nil, fmt.Errorf("lock: acquire %q: %w", key, err)
		}
		if ok {
			l.Debug("lock: acquired", log.F("ttl", ttl), log.F("attempts", attempt), log.F("waited", time.Since(start)))
			lk := &Lock{client: c, key: key, token: token, ttl: ttl, log: o.log, lost: make(chan struct{})}
			if o.autoExtend > 0 {
				lk.startAutoExtend(ctx, l, o.autoExtend)
			}
			return lk, nil
		}
		if o.wait == 0 || !time.Now().Add(delay).Before(deadline) {
			l.Debug("lock: held elsewhere, giving up", log.F("attempts", attempt), log.F("waited", time.Since(start)))
			return nil, ErrNotAcquired
		}
		l.Debug("lock: held elsewhere, waiting", log.F("attempt", attempt), log.F("backoff", delay))
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			l.Debug("lock: gave up waiting, context ended", log.F("attempts", attempt))
			return nil, ctx.Err()
		case <-t.C:
		}
		delay = min(delay*2, o.backoffMax)
	}
}

// Key returns the locked key.
func (lk *Lock) Key() string { return lk.key }

// Token returns the random value stored in the key. It identifies this holder
// and can serve as an opaque ID; treat it as a credential, since anyone with
// it can release the lock.
func (lk *Lock) Token() string { return lk.token }

// Lost is closed when auto-extend finds that the lock is no longer held (it
// expired during a pause, or someone deleted it). Work guarded by the lock
// should stop when it fires. Without WithAutoExtend it is never closed.
func (lk *Lock) Lost() <-chan struct{} { return lk.lost }

// Extend resets the lock's TTL to ttl if this Lock still holds it, and
// returns ErrNotHeld otherwise.
func (lk *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	l := lk.log.Ctx(ctx).With(log.F("key", lk.key))
	n, err := extend.Run(ctx, lk.client, []string{lk.key}, lk.token, ttl.Milliseconds()).Int64()
	if err != nil {
		l.Error(err, "lock: extend failed")
		return fmt.Errorf("lock: extend %q: %w", lk.key, err)
	}
	if n == 0 {
		l.Warn("lock: extend found the lock no longer held")
		return ErrNotHeld
	}
	l.Debug("lock: extended", log.F("ttl", ttl))
	return nil
}

// Release stops auto-extend and deletes the key if this Lock still holds it.
// It returns ErrNotHeld when the lock had already expired, been released, or
// been taken by another holder; the other holder's lock is left alone.
func (lk *Lock) Release(ctx context.Context) error {
	lk.stopAutoExtend()
	l := lk.log.Ctx(ctx).With(log.F("key", lk.key))
	n, err := release.Run(ctx, lk.client, []string{lk.key}, lk.token).Int64()
	if err != nil {
		l.Error(err, "lock: release failed")
		return fmt.Errorf("lock: release %q: %w", lk.key, err)
	}
	if n == 0 {
		l.Warn("lock: release found the lock no longer held")
		return ErrNotHeld
	}
	l.Debug("lock: released")
	return nil
}

// startAutoExtend runs the extension loop until Release, ctx ends, or the
// lock is lost.
func (lk *Lock) startAutoExtend(ctx context.Context, l log.Logger, every time.Duration) {
	actx, cancel := context.WithCancel(ctx)
	lk.stop = cancel
	lk.stopped = make(chan struct{})
	l.Debug("lock: auto-extend started", log.F("interval", every))
	go func() {
		defer close(lk.stopped)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-actx.Done():
				l.Debug("lock: auto-extend stopped")
				return
			case <-t.C:
			}
			err := lk.Extend(actx, lk.ttl)
			switch {
			case err == nil:
			case errors.Is(err, ErrNotHeld):
				l.Warn("lock: lost, auto-extend stopped")
				lk.lostOnce.Do(func() { close(lk.lost) })
				return
			case actx.Err() != nil:
				return
			default:
				// A transient error: keep trying until the lease runs out.
				l.Warn("lock: auto-extend attempt failed, retrying", log.F("error", err.Error()))
			}
		}
	}()
}

// stopAutoExtend stops the extension loop, if any, and waits for it.
func (lk *Lock) stopAutoExtend() {
	if lk.stop == nil {
		return
	}
	lk.stop()
	<-lk.stopped
}

// newToken returns 128 random bits in hex.
func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("lock: token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// nopLogger is the default Logger; it discards everything.
type nopLogger struct{}

func (nopLogger) Debug(string, ...log.Field)        {}
func (nopLogger) Info(string, ...log.Field)         {}
func (nopLogger) Warn(string, ...log.Field)         {}
func (nopLogger) Error(error, string, ...log.Field) {}
func (nopLogger) Fatal(error, string, ...log.Field) {}
func (n nopLogger) With(...log.Field) log.Logger    { return n }
func (n nopLogger) Ctx(context.Context) log.Logger  { return n }
