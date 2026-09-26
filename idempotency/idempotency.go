package idempotency

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
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/cache"
	"github.com/Bugs5382/go-redis/script"
	goredis "github.com/redis/go-redis/v9"
)

// ErrInProgress is returned by Do when another caller is running fn for the
// same key and has not finished (within the WithWait limit, if one is set).
var ErrInProgress = errors.New("idempotency: already in progress")

// ErrFailed is returned, wrapped with the original error text, when a key
// holds a failure kept by the KeepOnError policy.
var ErrFailed = errors.New("idempotency: previous attempt failed")

// Codec encodes stored results. It is the cache package's Codec, so any
// codec written for one works with the other. The default is cache.JSON.
type Codec = cache.Codec

// FailurePolicy says what happens to a key when fn returns an error.
type FailurePolicy int

const (
	// ReleaseOnError deletes the key when fn fails, so the next caller runs
	// fn again. It is the default and suits transient failures.
	ReleaseOnError FailurePolicy = iota
	// KeepOnError stores the failure for the key's TTL; later callers get
	// ErrFailed with the original error text instead of running fn. It suits
	// permanent failures that must not be retried, such as a declined card.
	KeepOnError
)

// Defaults for the wait polling.
const (
	DefaultPollMin = 10 * time.Millisecond
	DefaultPollMax = 200 * time.Millisecond
)

// Entry states, stored as the first byte of the value.
const (
	statePending = 'p'
	stateDone    = 'd'
	stateFailed  = 'e'
)

// claim returns the existing entry, or stores a pending entry and returns nil
// when the key is free.
var claim = script.New(`
local v = redis.call('GET', KEYS[1])
if v then return v end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return false`, script.WithName("idempotency.claim"))

// finish replaces our pending entry with the outcome, only if it is still ours.
var finish = script.New(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return 1
end
return 0`, script.WithName("idempotency.finish"))

// abandon deletes our pending entry, only if it is still ours.
var abandon = script.New(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`, script.WithName("idempotency.abandon"))

// config is what an Option sets.
type config struct {
	prefix  string
	codec   Codec
	lease   time.Duration
	wait    time.Duration
	policy  FailurePolicy
	pollMin time.Duration
	pollMax time.Duration
	log     log.Logger
}

// Option configures a Store.
type Option func(*config)

// WithPrefix namespaces keys: key k is stored at prefix+k. The default is
// "idempotency:".
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithCodec sets how results are encoded. The default is JSON; a nil codec
// keeps it.
func WithCodec(codec Codec) Option {
	return func(c *config) {
		if codec != nil {
			c.codec = codec
		}
	}
}

// WithLease bounds how long a claimed key stays "in progress". If the caller
// running fn crashes, the key frees itself after the lease and the next
// caller runs fn. Set it comfortably above fn's longest run. The default is
// the ttl passed to Do. Non-positive values keep the default.
func WithLease(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.lease = d
		}
	}
}

// WithWait makes a duplicate wait up to d for the running call to finish and
// then return its result, instead of failing at once with ErrInProgress. If
// the running call releases the key (it failed under ReleaseOnError, or its
// lease ran out), the waiter claims it and runs fn itself.
func WithWait(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.wait = d
		}
	}
}

// WithFailurePolicy sets what happens to a key when fn fails. The default is
// ReleaseOnError.
func WithFailurePolicy(p FailurePolicy) Option {
	return func(c *config) { c.policy = p }
}

// WithLogger sends diagnostics to l. Each call derives a logger from the
// call's context with l.Ctx, so trace and span IDs are attached. The default
// discards everything. Keys are logged; results never are.
func WithLogger(l log.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.log = l
		}
	}
}

// Store runs functions at most once per key and replays their results. It is
// safe for concurrent use.
type Store[T any] struct {
	client *redis.Client
	cfg    config
}

// New returns a Store for results of type T kept through c.
func New[T any](c *redis.Client, opts ...Option) *Store[T] {
	cfg := config{
		prefix: "idempotency:", codec: cache.JSON{}, policy: ReleaseOnError,
		pollMin: DefaultPollMin, pollMax: DefaultPollMax, log: nopLogger{},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Store[T]{client: c, cfg: cfg}
}

// Do runs fn at most once for key within ttl and returns its result.
//
// The first caller claims the key atomically and runs fn. When fn succeeds
// its result is stored for ttl, and every later call with the same key
// returns that result without running fn. A call that arrives while fn is
// still running returns ErrInProgress, or with WithWait, waits for the
// result. When fn fails, the failure policy decides: ReleaseOnError (the
// default) frees the key for a retry; KeepOnError stores the failure so later
// calls return ErrFailed.
//
// If fn panics the key is released and the panic continues. If the lease runs
// out while fn is still running, another caller may run fn too; the late
// finisher keeps its own result but does not overwrite the stored one.
func (s *Store[T]) Do(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	full := s.cfg.prefix + key
	l := s.cfg.log.Ctx(ctx).With(log.F("key", full))
	if ttl < time.Millisecond {
		return zero, fmt.Errorf("idempotency: ttl %v is below the 1ms minimum", ttl)
	}
	lease := s.cfg.lease
	if lease <= 0 {
		lease = ttl
	}

	token, err := newToken()
	if err != nil {
		return zero, err
	}
	pending := string(statePending) + token

	deadline := time.Now().Add(s.cfg.wait)
	poll := s.cfg.pollMin
	for attempt := 1; ; attempt++ {
		existing, err := claim.Run(ctx, s.client, []string{full}, pending, lease.Milliseconds()).Text()
		switch {
		case errors.Is(err, goredis.Nil):
			l.Debug("idempotency: claimed, running", log.F("lease", lease), log.F("attempt", attempt))
			return s.run(ctx, l, full, pending, ttl, fn)
		case err != nil:
			l.Error(err, "idempotency: claim failed")
			return zero, fmt.Errorf("idempotency: claim %q: %w", full, err)
		}

		if existing == "" {
			return zero, fmt.Errorf("idempotency: %q holds an empty entry", full)
		}
		switch existing[0] {
		case stateDone:
			var v T
			if err := s.cfg.codec.Unmarshal([]byte(existing[1:]), &v); err != nil {
				l.Error(err, "idempotency: stored result could not be decoded")
				return zero, fmt.Errorf("idempotency: decode %q: %w", full, err)
			}
			l.Debug("idempotency: replaying the stored result")
			return v, nil
		case stateFailed:
			l.Debug("idempotency: replaying a kept failure")
			return zero, fmt.Errorf("%w: %s", ErrFailed, existing[1:])
		case statePending:
		default:
			err := fmt.Errorf("idempotency: %q holds an entry this package did not write", full)
			l.Error(err, "idempotency: unreadable entry")
			return zero, err
		}

		remaining := time.Until(deadline)
		if s.cfg.wait == 0 || remaining <= 0 {
			l.Debug("idempotency: in progress elsewhere", log.F("attempts", attempt))
			return zero, ErrInProgress
		}
		// Sleep to the next poll, or to the deadline for one last look.
		nap := min(poll, remaining)
		l.Debug("idempotency: in progress elsewhere, waiting", log.F("attempt", attempt), log.F("poll", nap))
		t := time.NewTimer(nap)
		select {
		case <-ctx.Done():
			t.Stop()
			return zero, ctx.Err()
		case <-t.C:
		}
		poll = min(poll*2, s.cfg.pollMax)
	}
}

// run executes fn for a claimed key and records the outcome.
func (s *Store[T]) run(ctx context.Context, l log.Logger, full, pending string, ttl time.Duration, fn func(context.Context) (T, error)) (v T, err error) {
	settled := false
	defer func() {
		if !settled {
			// fn panicked: free the key and let the panic continue.
			s.release(context.WithoutCancel(ctx), l, full, pending, "fn panicked")
		}
	}()

	start := time.Now()
	v, err = fn(ctx)
	dur := time.Since(start)
	settled = true

	if err != nil {
		if s.cfg.policy == KeepOnError {
			l.Warn("idempotency: fn failed, keeping the failure", log.F("duration", dur))
			s.store(ctx, l, full, pending, string(stateFailed)+err.Error(), ttl)
		} else {
			l.Warn("idempotency: fn failed, releasing the key for a retry", log.F("duration", dur))
			s.release(ctx, l, full, pending, "fn failed")
		}
		return v, err
	}

	data, merr := s.cfg.codec.Marshal(&v)
	if merr != nil {
		l.Error(merr, "idempotency: result could not be encoded, releasing the key")
		s.release(ctx, l, full, pending, "encode failed")
		return v, fmt.Errorf("idempotency: encode %q: %w", full, merr)
	}
	l.Debug("idempotency: fn done", log.F("duration", dur), log.F("bytes", len(data)))
	s.store(ctx, l, full, pending, string(stateDone)+string(data), ttl)
	return v, nil
}

// store replaces our pending entry with value. A failure is logged, not
// returned: fn already ran, and its outcome belongs to the caller.
func (s *Store[T]) store(ctx context.Context, l log.Logger, full, pending, value string, ttl time.Duration) {
	n, err := finish.Run(ctx, s.client, []string{full}, pending, value, ttl.Milliseconds()).Int64()
	switch {
	case err != nil:
		l.Error(err, "idempotency: could not store the outcome; a duplicate may run fn again after the lease")
	case n == 0:
		l.Warn("idempotency: lease ran out before fn finished; the entry now belongs to another caller")
	default:
		l.Debug("idempotency: outcome stored", log.F("ttl", ttl))
	}
}

// release deletes our pending entry.
func (s *Store[T]) release(ctx context.Context, l log.Logger, full, pending, why string) {
	n, err := abandon.Run(ctx, s.client, []string{full}, pending).Int64()
	switch {
	case err != nil:
		l.Error(err, "idempotency: could not release the key; it frees itself when the lease runs out", log.F("reason", why))
	case n == 0:
		l.Warn("idempotency: key was no longer ours to release", log.F("reason", why))
	default:
		l.Debug("idempotency: key released", log.F("reason", why))
	}
}

// newToken returns 128 random bits in hex.
func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("idempotency: token: %w", err)
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
