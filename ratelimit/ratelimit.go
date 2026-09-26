package ratelimit

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
	"math/rand/v2"
	"strconv"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/script"
)

// ErrInvalidLimit is returned for a Limit or request size that cannot work: a
// non-positive rate, a period under a millisecond, a negative burst, or n
// outside 1..capacity.
var ErrInvalidLimit = errors.New("ratelimit: invalid limit")

// Strategy selects the limiting algorithm.
type Strategy int

const (
	// TokenBucket (the default) allows bursts up to Burst and refills at
	// Rate per Period. State is one small hash per key.
	TokenBucket Strategy = iota
	// SlidingWindow allows at most Rate requests in any Period-long window.
	// It is exact and never bursts past Rate, at the cost of one sorted-set
	// entry per allowed request inside the window.
	SlidingWindow
)

func (s Strategy) String() string {
	if s == SlidingWindow {
		return "sliding-window"
	}
	return "token-bucket"
}

// keyTag keeps the two strategies' keys apart, since they store different
// Redis types.
func (s Strategy) keyTag() string {
	if s == SlidingWindow {
		return "sw:"
	}
	return "tb:"
}

// Limit is Rate requests per Period. Burst is the token-bucket capacity (how
// many requests can arrive at once after an idle spell); zero means Rate. The
// sliding window ignores Burst.
type Limit struct {
	Rate   int64
	Period time.Duration
	Burst  int64
}

// PerSecond returns a limit of n per second, bursting to n.
func PerSecond(n int64) Limit { return Limit{Rate: n, Period: time.Second} }

// PerMinute returns a limit of n per minute, bursting to n.
func PerMinute(n int64) Limit { return Limit{Rate: n, Period: time.Minute} }

// PerHour returns a limit of n per hour, bursting to n.
func PerHour(n int64) Limit { return Limit{Rate: n, Period: time.Hour} }

// Result is the outcome of one Allow.
type Result struct {
	// Allowed reports whether the request may go ahead.
	Allowed bool
	// Limit is the capacity: Burst for the token bucket, Rate for the
	// sliding window.
	Limit int64
	// Remaining is how many more requests would be allowed right now.
	Remaining int64
	// RetryAfter is how long to wait before the same request would be
	// allowed; zero when Allowed.
	RetryAfter time.Duration
}

// tokenBucket refills from the server clock (TIME) so every app server shares
// one notion of now, then spends n tokens if it can. Times are microseconds.
var tokenBucket = script.New(`
local rate = tonumber(ARGV[1])
local period = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local h = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(h[1])
local ts = tonumber(h[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
if now > ts then
  tokens = math.min(burst, tokens + (now - ts) * rate / period)
  ts = now
end
local allowed = 0
local retry = 0
if tokens >= n then
  tokens = tokens - n
  allowed = 1
else
  retry = math.ceil((n - tokens) * period / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * period / rate / 1000) + 1)
return {allowed, math.floor(tokens), retry}`, script.WithName("ratelimit.token-bucket"))

// slidingWindow keeps one sorted-set entry per allowed request, scored by the
// server clock, and counts the entries inside the window. Denied requests are
// not recorded. Times are microseconds.
var slidingWindow = script.New(`
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local nonce = ARGV[4]
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
if count + n <= limit then
  for i = 1, n do
    redis.call('ZADD', KEYS[1], now, t[1] .. t[2] .. '-' .. nonce .. '-' .. i)
  end
  redis.call('PEXPIRE', KEYS[1], math.ceil(window / 1000))
  return {1, limit - count - n, 0}
end
local oldest = redis.call('ZRANGE', KEYS[1], count + n - limit - 1, count + n - limit - 1, 'WITHSCORES')
local retry = 0
if oldest[2] then
  retry = math.max(0, math.ceil(tonumber(oldest[2]) + window - now))
end
return {0, math.max(0, limit - count), retry}`, script.WithName("ratelimit.sliding-window"))

// config is what an Option sets.
type config struct {
	strategy Strategy
	prefix   string
	log      log.Logger
}

// Option configures a Limiter.
type Option func(*config)

// WithStrategy selects the algorithm. The default is TokenBucket.
func WithStrategy(s Strategy) Option {
	return func(c *config) { c.strategy = s }
}

// WithPrefix namespaces keys. The key for k is prefix + "tb:" + k (token
// bucket) or prefix + "sw:" + k (sliding window). The default prefix is
// "ratelimit:".
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithLogger sends diagnostics to l. Each call derives a logger from the
// call's context with l.Ctx, so trace and span IDs are attached. The default
// discards everything. Keys and decisions are logged.
func WithLogger(l log.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.log = l
		}
	}
}

// Limiter checks requests against limits stored in Redis. Every decision is
// one atomic Lua script on one key, so it holds across any number of app
// servers and is cluster-safe. It is safe for concurrent use.
type Limiter struct {
	client *redis.Client
	cfg    config
}

// New returns a Limiter backed by c.
func New(c *redis.Client, opts ...Option) *Limiter {
	cfg := config{strategy: TokenBucket, prefix: "ratelimit:", log: nopLogger{}}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Limiter{client: c, cfg: cfg}
}

// Allow checks one request for key against limit.
func (l *Limiter) Allow(ctx context.Context, key string, limit Limit) (Result, error) {
	return l.AllowN(ctx, key, limit, 1)
}

// AllowN checks a request that costs n units, for example a batch of n items.
// It is all or nothing: a denied request spends nothing. n must be between 1
// and the limit's capacity.
func (l *Limiter) AllowN(ctx context.Context, key string, limit Limit, n int64) (Result, error) {
	full := l.cfg.prefix + l.cfg.strategy.keyTag() + key
	lg := l.cfg.log.Ctx(ctx).With(log.F("key", full), log.F("strategy", l.cfg.strategy.String()))

	capacity, err := validate(limit, n, l.cfg.strategy)
	if err != nil {
		lg.Error(err, "ratelimit: refused an invalid limit")
		return Result{}, err
	}

	periodUS := limit.Period.Microseconds()
	var vals []int64
	start := time.Now()
	switch l.cfg.strategy {
	case SlidingWindow:
		nonce := strconv.FormatUint(rand.Uint64(), 36) // #nosec G404 -- only keeps sorted-set members unique.
		vals, err = slidingWindow.Run(ctx, l.client, []string{full}, limit.Rate, periodUS, n, nonce).Int64Slice()
	default:
		vals, err = tokenBucket.Run(ctx, l.client, []string{full}, limit.Rate, periodUS, capacity, n).Int64Slice()
	}
	dur := time.Since(start)
	if err != nil {
		lg.Error(err, "ratelimit: check failed", log.F("duration", dur))
		return Result{}, fmt.Errorf("ratelimit: allow %q: %w", full, err)
	}
	if len(vals) != 3 {
		err := fmt.Errorf("ratelimit: script returned %d values, want 3", len(vals))
		lg.Error(err, "ratelimit: unexpected script reply")
		return Result{}, err
	}

	r := Result{
		Allowed:    vals[0] == 1,
		Limit:      capacity,
		Remaining:  vals[1],
		RetryAfter: time.Duration(vals[2]) * time.Microsecond,
	}
	if r.Allowed {
		lg.Debug("ratelimit: allowed", log.F("n", n), log.F("remaining", r.Remaining), log.F("duration", dur))
	} else {
		lg.Debug("ratelimit: denied", log.F("n", n), log.F("remaining", r.Remaining),
			log.F("retry_after", r.RetryAfter), log.F("duration", dur))
	}
	return r, nil
}

// validate checks limit and n, and returns the capacity.
func validate(limit Limit, n int64, s Strategy) (int64, error) {
	switch {
	case limit.Rate <= 0:
		return 0, fmt.Errorf("%w: rate %d must be positive", ErrInvalidLimit, limit.Rate)
	case limit.Period < time.Millisecond:
		return 0, fmt.Errorf("%w: period %v is below 1ms", ErrInvalidLimit, limit.Period)
	case limit.Burst < 0:
		return 0, fmt.Errorf("%w: burst %d is negative", ErrInvalidLimit, limit.Burst)
	}
	capacity := limit.Rate
	if s == TokenBucket && limit.Burst > 0 {
		capacity = limit.Burst
	}
	if n < 1 || n > capacity {
		return 0, fmt.Errorf("%w: n %d must be between 1 and the capacity %d", ErrInvalidLimit, n, capacity)
	}
	return capacity, nil
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
