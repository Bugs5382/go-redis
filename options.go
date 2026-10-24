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
	"crypto/tls"
	"runtime"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// mode selects which kind of underlying go-redis client Connect builds.
type mode int

const (
	modeStandalone mode = iota // single server (redis.NewClient)
	modeSentinel               // sentinel-managed failover master (redis.NewFailoverClient)
	modeCluster                // cluster (redis.NewClusterClient)
)

// Default configuration values. They mirror go-redis's own well-chosen
// production defaults but are set explicitly so callers can see, and rely on,
// the resilient baseline regardless of upstream changes.
const (
	defaultAddr            = "localhost:6379"
	defaultDialTimeout     = 5 * time.Second
	defaultReadTimeout     = 3 * time.Second
	defaultWriteTimeout    = 3 * time.Second
	defaultMaxRetries      = 3
	defaultMinRetryBackoff = 8 * time.Millisecond
	defaultMaxRetryBackoff = 512 * time.Millisecond

	// defaultDialAttempts and defaultDialRetryBackoff are go-redis's own dial
	// loop defaults, made explicit here: each new connection is tried up to
	// five times with 100ms between tries, so one failed dial costs at least
	// 400ms. That patience rides out a restart, but it is what makes every
	// call slow while the server is down. FastFail uses a single attempt.
	defaultDialAttempts     = 5
	defaultDialRetryBackoff = 100 * time.Millisecond

	// defaultFastFailTimeout is the per-operation budget of the FastFail
	// view: its dial, read, write and pool-wait timeouts. A healthy local or
	// in-cluster server answers in a few milliseconds, so this leaves room
	// for a slow moment without letting a down server hold a request.
	defaultFastFailTimeout = 200 * time.Millisecond
)

// defaultPoolSize returns the baseline connections-per-CPU pool go-redis uses,
// with a sane floor so a single-CPU runner still gets a usable pool.
func defaultPoolSize() int {
	if n := 10 * runtime.GOMAXPROCS(0); n > 10 {
		return n
	}
	return 10
}

// config is the resolved, internal configuration an Option mutates. It is never
// exposed; Connect turns it into the concrete go-redis options struct.
type config struct {
	mode       mode
	addrs      []string
	masterName string
	password   string
	db         int
	tlsConfig  *tls.Config

	poolSize     int
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	maxRetries      int
	minRetryBackoff time.Duration
	maxRetryBackoff time.Duration

	dialAttempts     int
	dialRetryBackoff time.Duration

	// poolTimeout is how long a call waits for a free pooled connection. Zero
	// keeps the go-redis default (ReadTimeout + 1s); only the FastFail view
	// sets it.
	poolTimeout time.Duration

	fastFailTimeout time.Duration

	logger   Logger
	observer Observer
}

// defaults returns a config carrying the resilient baseline. Every field has a
// usable value before any Option runs.
func defaults() config {
	return config{
		mode:            modeStandalone,
		addrs:           []string{defaultAddr},
		poolSize:        defaultPoolSize(),
		dialTimeout:     defaultDialTimeout,
		readTimeout:     defaultReadTimeout,
		writeTimeout:    defaultWriteTimeout,
		maxRetries:      defaultMaxRetries,
		minRetryBackoff: defaultMinRetryBackoff,
		maxRetryBackoff: defaultMaxRetryBackoff,

		dialAttempts:     defaultDialAttempts,
		dialRetryBackoff: defaultDialRetryBackoff,
		fastFailTimeout:  defaultFastFailTimeout,
	}
}

// fastFailConfig derives the FastFail view's configuration from the parent's:
// the same target, auth, TLS, database, pool size and hooks, but no command
// retries, a single dial attempt, and the fast-fail budget on every timeout.
func (c *config) fastFailConfig() config {
	ff := *c
	ff.addrs = append([]string(nil), c.addrs...)
	ff.maxRetries = 0
	ff.dialAttempts = 1
	ff.dialTimeout = c.fastFailTimeout
	ff.readTimeout = c.fastFailTimeout
	ff.writeTimeout = c.fastFailTimeout
	ff.poolTimeout = c.fastFailTimeout
	return ff
}

// Option configures a Client. Options are applied in order by Connect; a later
// Option wins over an earlier one.
type Option func(*config)

// WithAddr targets a standalone Redis server. The first address is used; extra
// addresses are ignored (use WithCluster for multiple nodes). Overrides the
// default localhost:6379.
func WithAddr(addrs ...string) Option {
	return func(c *config) {
		c.mode = modeStandalone
		if len(addrs) > 0 {
			c.addrs = addrs
		}
	}
}

// WithSentinel targets a Redis Sentinel deployment: masterName is the monitored
// master's name (commonly "mymaster") and addrs are the sentinel endpoints. This
// is the high-availability path -- go-redis discovers the current master through
// the sentinels and re-resolves it after a failover.
func WithSentinel(masterName string, addrs ...string) Option {
	return func(c *config) {
		c.mode = modeSentinel
		c.masterName = masterName
		if len(addrs) > 0 {
			c.addrs = addrs
		}
	}
}

// WithCluster targets a Redis Cluster. addrs are seed node endpoints; the client
// discovers the rest of the topology and reshards transparently. Note: cluster
// mode ignores WithDB (Redis Cluster supports only database 0).
func WithCluster(addrs ...string) Option {
	return func(c *config) {
		c.mode = modeCluster
		if len(addrs) > 0 {
			c.addrs = addrs
		}
	}
}

// WithPassword sets the AUTH password. Leave unset for an unauthenticated server.
func WithPassword(password string) Option {
	return func(c *config) { c.password = password }
}

// WithDB selects the logical database index. Ignored in cluster mode.
func WithDB(db int) Option {
	return func(c *config) { c.db = db }
}

// WithTLS enables TLS using the given configuration. Pass a non-nil
// *tls.Config (an empty &tls.Config{} uses the system roots and the server's
// hostname). A nil config leaves TLS disabled.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) { c.tlsConfig = cfg }
}

// WithPool sets the maximum number of socket connections in the pool. A
// non-positive size is ignored, keeping the resilient default.
func WithPool(size int) Option {
	return func(c *config) {
		if size > 0 {
			c.poolSize = size
		}
	}
}

// WithTimeouts overrides the dial, read, and write timeouts. A non-positive
// duration for any parameter leaves that timeout at its default.
func WithTimeouts(dial, read, write time.Duration) Option {
	return func(c *config) {
		if dial > 0 {
			c.dialTimeout = dial
		}
		if read > 0 {
			c.readTimeout = read
		}
		if write > 0 {
			c.writeTimeout = write
		}
	}
}

// WithRetry tunes command retries: attempts is the maximum number of retries
// after the first try (mapped to go-redis MaxRetries), and min/max bound the
// exponential backoff between them. A negative attempts count is ignored; pass 0
// to disable retries. Non-positive backoff bounds are left at their defaults.
func WithRetry(attempts int, min, max time.Duration) Option {
	return func(c *config) {
		if attempts >= 0 {
			c.maxRetries = attempts
		}
		if min > 0 {
			c.minRetryBackoff = min
		}
		if max > 0 {
			c.maxRetryBackoff = max
		}
	}
}

// WithDialRetry tunes how hard each new connection is dialed: attempts is the
// total number of dial tries (1 means a single try with no retry) and backoff
// is the pause between them. The defaults, 5 attempts and 100ms, are the
// go-redis values; together with WithRetry they decide how long a call takes
// to fail while the server is down (at least (attempts-1) x backoff per
// dial). Lowering them makes every call fail sooner but gives up on a server
// that is only restarting; for a per-call choice, use Client.FastFail instead.
// Non-positive values keep the defaults.
func WithDialRetry(attempts int, backoff time.Duration) Option {
	return func(c *config) {
		if attempts > 0 {
			c.dialAttempts = attempts
		}
		if backoff > 0 {
			c.dialRetryBackoff = backoff
		}
	}
}

// WithFastFail sets the per-operation budget of the Client.FastFail view: its
// dial, read, write and pool-wait timeouts (default 200ms). Pick a value
// above the server's normal worst-case latency; a call that needs longer
// fails on the view and should take the caller's fallback. A non-positive
// value keeps the default.
func WithFastFail(timeout time.Duration) Option {
	return func(c *config) {
		if timeout > 0 {
			c.fastFailTimeout = timeout
		}
	}
}

// WithLogger installs a Logger for connection-level diagnostics (dial failures).
// The default is a no-op; the core imposes no logging dependency.
func WithLogger(l Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithObserver installs an Observer for lightweight command and dial metrics.
// The default is a no-op; the core imposes no telemetry dependency.
func WithObserver(o Observer) Option {
	return func(c *config) { c.observer = o }
}

// Logger receives connection-level diagnostic messages. Implementations must be
// safe for concurrent use. The default is a no-op, so the core carries no
// logging dependency.
type Logger interface {
	// Printf logs a formatted message. ctx carries the call's context, which
	// may hold trace information.
	Printf(ctx context.Context, format string, args ...any)
}

// Observer receives lightweight per-command and per-dial signals, suitable for
// driving metrics without pulling a telemetry library into the core.
// Implementations must be safe for concurrent use and must not block. The
// default is a no-op. For full OpenTelemetry tracing and metrics, use the otel
// subpackage instead.
type Observer interface {
	// ObserveCommand is called after each command completes. name is the Redis
	// command name, dur is how long it took, and err is its result (nil on
	// success; note goredis.Nil signals a missing key, not a failure).
	ObserveCommand(ctx context.Context, name string, dur time.Duration, err error)
	// ObserveDial is called after each attempt to open a new connection.
	ObserveDial(ctx context.Context, network, addr string, dur time.Duration, err error)
}

// String names the mode for log lines.
func (m mode) String() string {
	switch m {
	case modeSentinel:
		return "sentinel"
	case modeCluster:
		return "cluster"
	default:
		return "standalone"
	}
}

// log returns the configured Logger, or the no-op one.
func (c *config) log() Logger {
	if c.logger == nil {
		return nopLogger{}
	}
	return c.logger
}

// obs returns the configured Observer, or the no-op one.
func (c *config) obs() Observer {
	if c.observer == nil {
		return nopObserver{}
	}
	return c.observer
}

// nopLogger is the default Logger; it discards everything.
type nopLogger struct{}

func (nopLogger) Printf(context.Context, string, ...any) {}

// nopObserver is the default Observer; it discards everything.
type nopObserver struct{}

func (nopObserver) ObserveCommand(context.Context, string, time.Duration, error)      {}
func (nopObserver) ObserveDial(context.Context, string, string, time.Duration, error) {}

// goRedisMaxRetries translates maxRetries into go-redis terms. go-redis reads
// MaxRetries 0 as "use the default of 3" and -1 as "no retries", so the 0 that
// WithRetry documents as "disable retries" must be sent as -1 (#20).
func (c *config) goRedisMaxRetries() int {
	if c.maxRetries == 0 {
		return -1
	}
	return c.maxRetries
}

// standaloneOptions maps the config onto go-redis standalone options.
func (c *config) standaloneOptions() *goredis.Options {
	addr := defaultAddr
	if len(c.addrs) > 0 {
		addr = c.addrs[0]
	}
	return &goredis.Options{
		Addr:            addr,
		Password:        c.password,
		DB:              c.db,
		TLSConfig:       c.tlsConfig,
		PoolSize:        c.poolSize,
		DialTimeout:     c.dialTimeout,
		ReadTimeout:     c.readTimeout,
		WriteTimeout:    c.writeTimeout,
		MaxRetries:      c.goRedisMaxRetries(),
		MinRetryBackoff: c.minRetryBackoff,
		MaxRetryBackoff: c.maxRetryBackoff,

		DialerRetries:      c.dialAttempts,
		DialerRetryTimeout: c.dialRetryBackoff,
		PoolTimeout:        c.poolTimeout,
	}
}

// failoverOptions maps the config onto go-redis sentinel failover options.
func (c *config) failoverOptions() *goredis.FailoverOptions {
	return &goredis.FailoverOptions{
		MasterName:      c.masterName,
		SentinelAddrs:   c.addrs,
		Password:        c.password,
		DB:              c.db,
		TLSConfig:       c.tlsConfig,
		PoolSize:        c.poolSize,
		DialTimeout:     c.dialTimeout,
		ReadTimeout:     c.readTimeout,
		WriteTimeout:    c.writeTimeout,
		MaxRetries:      c.goRedisMaxRetries(),
		MinRetryBackoff: c.minRetryBackoff,
		MaxRetryBackoff: c.maxRetryBackoff,

		DialerRetries:      c.dialAttempts,
		DialerRetryTimeout: c.dialRetryBackoff,
		PoolTimeout:        c.poolTimeout,
	}
}

// clusterOptions maps the config onto go-redis cluster options. Redis Cluster
// supports only database 0, so DB is intentionally not carried over.
func (c *config) clusterOptions() *goredis.ClusterOptions {
	return &goredis.ClusterOptions{
		Addrs:           c.addrs,
		Password:        c.password,
		TLSConfig:       c.tlsConfig,
		PoolSize:        c.poolSize,
		DialTimeout:     c.dialTimeout,
		ReadTimeout:     c.readTimeout,
		WriteTimeout:    c.writeTimeout,
		MaxRetries:      c.goRedisMaxRetries(),
		MinRetryBackoff: c.minRetryBackoff,
		MaxRetryBackoff: c.maxRetryBackoff,

		DialerRetries:      c.dialAttempts,
		DialerRetryTimeout: c.dialRetryBackoff,
		PoolTimeout:        c.poolTimeout,
	}
}
