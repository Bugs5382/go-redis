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
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Client is a thin, resilient wrapper over a go-redis UniversalClient. Build one
// with Connect; use Redis to reach the full go-redis command API, Healthy for a
// liveness probe, and Close to release the pool. A Client is safe for concurrent
// use by multiple goroutines.
type Client struct {
	uc goredis.UniversalClient
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
		log := cfg.logger
		if log == nil {
			log = nopLogger{}
		}
		obs := cfg.observer
		if obs == nil {
			obs = nopObserver{}
		}
		uc.AddHook(instHook{log: log, obs: obs})
	}

	if err := uc.Ping(ctx).Err(); err != nil {
		_ = uc.Close()
		return nil, fmt.Errorf("redis: ping on connect: %w", err)
	}
	return &Client{uc: uc}, nil
}

// Redis returns the underlying go-redis UniversalClient so callers can use the
// full command API (Get, Set, pipelines, pub/sub, scripts, and so on). The
// returned value is owned by this Client; do not Close it directly -- use
// Client.Close.
func (c *Client) Redis() goredis.UniversalClient { return c.uc }

// Healthy reports whether the server answers a Ping within ctx. It is cheap and
// suitable for readiness and liveness probes.
func (c *Client) Healthy(ctx context.Context) bool {
	return c.uc.Ping(ctx).Err() == nil
}

// Close releases the connection pool. It is safe to call once; further use of
// the Client after Close returns errors from go-redis.
func (c *Client) Close() error { return c.uc.Close() }

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
