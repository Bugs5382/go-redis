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
	}
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

// nopLogger is the default Logger; it discards everything.
type nopLogger struct{}

func (nopLogger) Printf(context.Context, string, ...any) {}

// nopObserver is the default Observer; it discards everything.
type nopObserver struct{}

func (nopObserver) ObserveCommand(context.Context, string, time.Duration, error)      {}
func (nopObserver) ObserveDial(context.Context, string, string, time.Duration, error) {}

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
		MaxRetries:      c.maxRetries,
		MinRetryBackoff: c.minRetryBackoff,
		MaxRetryBackoff: c.maxRetryBackoff,
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
		MaxRetries:      c.maxRetries,
		MinRetryBackoff: c.minRetryBackoff,
		MaxRetryBackoff: c.maxRetryBackoff,
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
		MaxRetries:      c.maxRetries,
		MinRetryBackoff: c.minRetryBackoff,
		MaxRetryBackoff: c.maxRetryBackoff,
	}
}
