package cache

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	goredis "github.com/redis/go-redis/v9"
)

// ErrMiss is returned by Get when the key holds no entry.
var ErrMiss = errors.New("cache: miss")

// ErrNotFound marks a value that does not exist at the source. A loader
// returns it (or an error wrapping it) to say "there is nothing to cache".
// With WithNegativeTTL set, GetOrSet remembers that answer for the negative
// TTL, and Get and GetOrSet then return ErrNotFound without calling the
// loader again.
var ErrNotFound = errors.New("cache: not found")

// notFoundMarker is what a negative entry stores. It starts with a zero byte,
// which no JSON document can, so it never collides with a cached value from
// the default codec.
var notFoundMarker = []byte("\x00go-redis/cache:not-found")

// Loader produces the value for a key on a cache miss. Return ErrNotFound (or
// an error wrapping it) when the value does not exist.
type Loader[T any] func(ctx context.Context) (T, error)

// config is the resolved configuration an Option mutates.
type config struct {
	prefix      string
	codec       Codec
	jitter      float64
	negativeTTL time.Duration
	log         log.Logger
}

// Option configures a Cache.
type Option func(*config)

// WithPrefix namespaces every key: the entry for key k is stored at prefix+k,
// for example WithPrefix("users:"). The default is no prefix.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithCodec sets how values are encoded. The default is JSON. A nil codec
// keeps the default.
func WithCodec(codec Codec) Option {
	return func(c *config) {
		if codec != nil {
			c.codec = codec
		}
	}
}

// WithJitter shortens each TTL by a random amount of up to fraction of it, so
// entries written together do not all expire in the same instant. With 0.1 a
// one-minute TTL becomes somewhere between 54 and 60 seconds. fraction is
// clamped to [0, 1]; the default is 0 (no jitter). A TTL of 0 (no expiry) is
// never jittered.
func WithJitter(fraction float64) Option {
	return func(c *config) { c.jitter = min(max(fraction, 0), 1) }
}

// WithNegativeTTL turns on negative caching: when a loader returns
// ErrNotFound, GetOrSet stores a not-found entry for ttl so repeated lookups
// of a missing value do not reach the source. A non-positive ttl leaves
// negative caching off, which is the default.
func WithNegativeTTL(ttl time.Duration) Option {
	return func(c *config) {
		if ttl > 0 {
			c.negativeTTL = ttl
		}
	}
}

// WithLogger sends the cache's diagnostics to l. Each call derives a logger
// from the call's context with l.Ctx, so trace and span IDs are attached. The
// default discards everything. Keys are logged; values never are.
func WithLogger(l log.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.log = l
		}
	}
}

// Cache is a typed view over a go-redis Client that stores values of type T.
// It is safe for concurrent use by multiple goroutines. Build one with New and
// share it: the stampede protection in GetOrSet works per Cache value.
type Cache[T any] struct {
	client *redis.Client
	cfg    config
	flight group[T]
}

// New returns a Cache for values of type T stored through c.
func New[T any](c *redis.Client, opts ...Option) *Cache[T] {
	cfg := config{codec: JSON{}, log: nopLogger{}}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Cache[T]{client: c, cfg: cfg}
}

// Get returns the cached value for key. It returns ErrMiss when there is no
// entry, ErrNotFound when a negative entry is cached, and a wrapped error when
// Redis fails or the entry cannot be decoded.
func (c *Cache[T]) Get(ctx context.Context, key string) (T, error) {
	full := c.cfg.prefix + key
	l := c.logger(ctx, full)
	start := time.Now()

	var zero T
	data, err := c.client.Redis().Get(ctx, full).Bytes()
	switch {
	case errors.Is(err, goredis.Nil):
		l.Debug("cache: miss", log.F("duration", time.Since(start)))
		return zero, ErrMiss
	case err != nil:
		l.Error(err, "cache: get failed", log.F("duration", time.Since(start)))
		return zero, fmt.Errorf("cache: get %q: %w", full, err)
	}
	if bytes.Equal(data, notFoundMarker) {
		l.Debug("cache: hit on a cached not-found", log.F("duration", time.Since(start)))
		return zero, ErrNotFound
	}
	var v T
	if err := c.cfg.codec.Unmarshal(data, &v); err != nil {
		l.Error(err, "cache: decode failed", log.F("bytes", len(data)))
		return zero, fmt.Errorf("cache: decode %q: %w", full, err)
	}
	l.Debug("cache: hit", log.F("bytes", len(data)), log.F("duration", time.Since(start)))
	return v, nil
}

// Set stores v under key for ttl, shortened by the jitter option if set. A ttl
// of 0 stores the entry without an expiry.
func (c *Cache[T]) Set(ctx context.Context, key string, v T, ttl time.Duration) error {
	full := c.cfg.prefix + key
	l := c.logger(ctx, full)

	data, err := c.cfg.codec.Marshal(&v)
	if err != nil {
		l.Error(err, "cache: encode failed")
		return fmt.Errorf("cache: encode %q: %w", full, err)
	}
	return c.write(ctx, l, full, data, ttl)
}

// Delete removes the entries for keys. Missing keys are not an error. Keys are
// deleted one command each, in a single pipeline, so keys in different
// cluster slots are fine.
func (c *Cache[T]) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = c.cfg.prefix + k
	}
	l := c.cfg.log.Ctx(ctx).With(log.F("keys", full))
	start := time.Now()

	_, err := c.client.Redis().Pipelined(ctx, func(p goredis.Pipeliner) error {
		for _, k := range full {
			p.Del(ctx, k)
		}
		return nil
	})
	if err != nil {
		l.Error(err, "cache: delete failed", log.F("duration", time.Since(start)))
		return fmt.Errorf("cache: delete: %w", err)
	}
	l.Debug("cache: deleted", log.F("duration", time.Since(start)))
	return nil
}

// GetOrSet returns the cached value for key, or calls load on a miss and
// caches its result for ttl.
//
// Concurrent calls for the same key on the same Cache share one load: the
// first caller runs load with its own context and the others wait for its
// result, each bounded by its own context. This protects the source from a
// stampede when a hot key expires; it does not coordinate across processes.
// Waiters receive the same value, so a T holding pointers, slices or maps is
// shared between them.
//
// A loader error is returned and nothing is cached. ErrNotFound from the
// loader is cached as a negative entry when WithNegativeTTL is set. If Redis
// itself fails, or the stored entry cannot be decoded, GetOrSet logs a warning
// and serves from the loader, so an outage degrades to uncached reads instead
// of errors. A failure to write the loaded value back is logged and does not
// fail the call.
func (c *Cache[T]) GetOrSet(ctx context.Context, key string, ttl time.Duration, load Loader[T]) (T, error) {
	full := c.cfg.prefix + key
	l := c.logger(ctx, full)

	v, err := c.Get(ctx, key)
	switch {
	case err == nil:
		return v, nil
	case errors.Is(err, ErrNotFound):
		return v, err
	case errors.Is(err, ErrMiss):
	case ctx.Err() != nil:
		return v, err
	default:
		l.Warn("cache: read failed, loading from the source", log.F("error", err.Error()))
	}

	v, err, shared := c.flight.do(ctx, full, func() (T, error) {
		return c.load(ctx, l, key, full, ttl, load)
	})
	if shared {
		l.Debug("cache: shared an in-flight load")
	}
	return v, err
}

// load runs the loader for one flight and writes its outcome back.
func (c *Cache[T]) load(ctx context.Context, l log.Logger, key, full string, ttl time.Duration, load Loader[T]) (T, error) {
	start := time.Now()
	l.Debug("cache: loading")
	v, err := load(ctx)
	dur := time.Since(start)

	switch {
	case err == nil:
		l.Debug("cache: loaded", log.F("duration", dur))
		if werr := c.Set(ctx, key, v, ttl); werr != nil {
			l.Warn("cache: could not store the loaded value", log.F("error", werr.Error()))
		}
		return v, nil
	case errors.Is(err, ErrNotFound):
		l.Debug("cache: source has no value", log.F("duration", dur))
		if c.cfg.negativeTTL > 0 {
			if werr := c.write(ctx, l, full, notFoundMarker, c.cfg.negativeTTL); werr != nil {
				l.Warn("cache: could not store the not-found entry", log.F("error", werr.Error()))
			}
		}
		return v, err
	default:
		l.Warn("cache: loader failed, nothing cached", log.F("duration", dur), log.F("error", err.Error()))
		return v, err
	}
}

// write stores data at the full key with a jittered ttl.
func (c *Cache[T]) write(ctx context.Context, l log.Logger, full string, data []byte, ttl time.Duration) error {
	ttl = c.jittered(ttl)
	start := time.Now()
	if err := c.client.Redis().Set(ctx, full, data, ttl).Err(); err != nil {
		l.Error(err, "cache: set failed", log.F("duration", time.Since(start)))
		return fmt.Errorf("cache: set %q: %w", full, err)
	}
	l.Debug("cache: stored", log.F("ttl", ttl), log.F("bytes", len(data)), log.F("duration", time.Since(start)))
	return nil
}

// jittered shortens ttl by up to the jitter fraction, never to zero.
func (c *Cache[T]) jittered(ttl time.Duration) time.Duration {
	if ttl <= 0 || c.cfg.jitter == 0 {
		return ttl
	}
	cut := time.Duration(rand.Float64() * c.cfg.jitter * float64(ttl)) // #nosec G404 -- expiry jitter needs no cryptographic randomness.
	return max(ttl-cut, time.Millisecond)
}

// logger returns the configured logger correlated with ctx and labelled with
// the full key.
func (c *Cache[T]) logger(ctx context.Context, full string) log.Logger {
	return c.cfg.log.Ctx(ctx).With(log.F("key", full))
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
