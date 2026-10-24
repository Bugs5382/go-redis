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
	"fmt"
	"net"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Nil is the sentinel error go-redis returns when a command finds no result
// (for example GET on a missing key). It is re-exported here so a caller can
// write errors.Is(err, redis.Nil) without importing
// github.com/redis/go-redis/v9 directly.
var Nil = goredis.Nil

// Cmdable is the full go-redis command surface (Get, Set, pipelines, pub/sub,
// scripts, and so on), re-exported as a type alias so a caller can name it --
// for example to store the result of Client.Redis in a typed field or accept
// it as a function parameter -- without importing
// github.com/redis/go-redis/v9 directly.
type Cmdable = goredis.Cmdable

// UniversalClient is the client-mode-agnostic go-redis interface (standalone,
// sentinel-failover, or cluster) that Client wraps, re-exported as a type
// alias for the same reason as Cmdable. It embeds Cmdable and additionally
// exposes hooks, transactions, pub/sub, and pool stats.
type UniversalClient = goredis.UniversalClient

// Client is a thin, resilient wrapper over a go-redis UniversalClient. Build one
// with Connect; use Redis to reach the full go-redis command API, Healthy for a
// liveness probe, and Close to release the pool. A Client is safe for concurrent
// use by multiple goroutines.
type Client struct {
	uc goredis.UniversalClient
	// st sits behind a pointer so Client stays a comparable struct, as it
	// was before FastFail.
	st *clientState
}

// clientState is a Client's configuration and its FastFail bookkeeping.
type clientState struct {
	cfg config

	// parent is set on the FastFail view and nil on a client from Connect.
	parent *Client

	mu     sync.Mutex
	closed bool
	ff     *Client
}

// Connect builds a Redis client from the given options, applies the resilient
// defaults for anything left unset, and verifies readiness with a Ping before
// returning. The mode (standalone, sentinel, or cluster) is chosen by the
// WithAddr, WithSentinel, or WithCluster option; standalone localhost:6379 is
// the default. On a failed Ping the underlying client is closed and the error is
// returned, so a returned Client is always usable. The ctx bounds both the dial
// and the readiness Ping.
func Connect(ctx context.Context, opts ...Option) (*Client, error) {
	cfg := defaults()
	for _, opt := range opts {
		opt(&cfg)
	}

	uc := newUniversal(&cfg)
	if err := uc.Ping(ctx).Err(); err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("redis: ping on connect: %w", err)
	}
	return &Client{uc: uc, st: &clientState{cfg: cfg}}, nil
}

// newUniversal builds the go-redis client for cfg's mode and wires the
// instrumentation hook. It does not dial; go-redis connects on first use.
func newUniversal(cfg *config) goredis.UniversalClient {
	var uc goredis.UniversalClient
	switch cfg.mode {
	case modeSentinel:
		uc = goredis.NewFailoverClient(cfg.failoverOptions())
	case modeCluster:
		uc = goredis.NewClusterClient(cfg.clusterOptions())
	default:
		uc = goredis.NewClient(cfg.standaloneOptions())
	}

	// Only wire the instrumentation hook when the caller actually supplied a
	// Logger or Observer, so the default path adds no per-command overhead.
	if cfg.logger != nil || cfg.observer != nil {
		uc.AddHook(instHook{log: cfg.log(), obs: cfg.obs()})
	}
	return uc
}

// Redis returns the underlying client as the UniversalClient alias so callers
// can use the full command API (Get, Set, pipelines, pub/sub, scripts, and so
// on) while naming only this package's exported types. The returned value is
// owned by this Client; do not Close it directly -- use Client.Close.
func (c *Client) Redis() UniversalClient { return c.uc }

// Healthy reports whether the server answers a Ping within ctx. On a Client
// from Connect the Ping carries the normal retries, so while the server is
// down it can take a second or more to say false; for a readiness probe use
// c.FastFail().Healthy(ctx), which answers within the fast-fail budget.
func (c *Client) Healthy(ctx context.Context) bool {
	return c.uc.Ping(ctx).Err() == nil
}

// FastFail returns a view of the same server that gives up quickly instead of
// retrying: no command retries, a single dial attempt, and the WithFastFail
// budget (default 200ms) as its dial, read, write and pool-wait timeouts.
// While the server is down, a call through the view fails in about the time
// it takes to be refused (or the budget, for a host that does not answer)
// instead of the seconds the retrying path can spend.
//
// Use it where Redis is optional and the caller has a fallback: a cache read
// that falls back to the source, a rate-limit check that lets the request
// through, a readiness probe. The returned value is a *Client, so it plugs
// into the toolkit packages (cache, ratelimit and the rest) unchanged.
// Errors from the view are the usual go-redis errors; Transient reports true
// for the ones that mean the server is unreachable.
//
// The view has its own small connection pool, built on the first call and
// shared by every later call; it opens connections only when used. It inherits
// the Logger and Observer given to Connect. The parent owns it: Close on the
// view is a no-op, and closing the parent closes the view. FastFail on the
// view returns the view.
func (c *Client) FastFail() *Client {
	st := c.st
	if st.parent != nil {
		return c
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.ff != nil {
		return st.ff
	}
	ffCfg := st.cfg.fastFailConfig()
	view := &Client{uc: newUniversal(&ffCfg), st: &clientState{cfg: ffCfg, parent: c}}
	if st.closed {
		// Keep the "use after Close returns ErrClosed" contract for a view
		// asked for late, instead of handing out a live pool nobody closes.
		_ = view.uc.Close()
	}
	st.ff = view
	st.cfg.log().Printf(context.Background(),
		"redis: fast-fail view built: mode=%s addrs=%d timeout=%s closed=%t",
		st.cfg.mode, len(ffCfg.addrs), ffCfg.fastFailTimeout, st.closed)
	return view
}

// Close releases the connection pool, and the FastFail view's pool when one
// was built. It is safe to call once; further use of the Client after Close
// returns errors from go-redis. On the FastFail view, Close does nothing: the
// parent owns the view's pool.
func (c *Client) Close() error {
	st := c.st
	if st.parent != nil {
		return nil
	}
	st.mu.Lock()
	st.closed = true
	ff := st.ff
	st.mu.Unlock()

	err := c.uc.Close()
	if ff != nil {
		if ffErr := ff.uc.Close(); ffErr != nil && err == nil {
			err = ffErr
		}
	}
	st.cfg.log().Printf(context.Background(), "redis: client closed: fast-fail view=%t err=%v", ff != nil, err)
	return err
}

// instHook adapts the pluggable Logger and Observer onto the go-redis Hook
// interface, translating dials and commands into observer callbacks and logging
// dial failures. It is installed only when a Logger or Observer is configured.
type instHook struct {
	log Logger
	obs Observer
}

// DialHook times each connection dial, reports it to the Observer, and logs a
// failure.
func (h instHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()
		conn, err := next(ctx, network, addr)
		h.obs.ObserveDial(ctx, network, addr, time.Since(start), err)
		if err != nil {
			h.log.Printf(ctx, "redis: dial %s %s failed: %v", network, addr, err)
		}
		return conn, err
	}
}

// ProcessHook times each command and reports it to the Observer.
func (h instHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		h.obs.ObserveCommand(ctx, cmd.Name(), time.Since(start), cmd.Err())
		return err
	}
}

// ProcessPipelineHook times each pipeline and reports it to the Observer under
// the synthetic command name "pipeline".
func (h instHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		h.obs.ObserveCommand(ctx, "pipeline", time.Since(start), err)
		return err
	}
}
