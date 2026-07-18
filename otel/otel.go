package otel

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
	"fmt"

	redisx "github.com/Bugs5382/go-redis"
	"github.com/redis/go-redis/extra/redisotel/v9"
)

// config controls which signals Instrument enables. Both are on by default.
type config struct {
	tracing bool
	metrics bool
}

// Option toggles the signals Instrument enables.
type Option func(*config)

// WithoutTracing disables span creation, leaving metrics enabled.
func WithoutTracing() Option {
	return func(c *config) { c.tracing = false }
}

// WithoutMetrics disables metric recording, leaving tracing enabled.
func WithoutMetrics() Option {
	return func(c *config) { c.metrics = false }
}

// Instrument enables OpenTelemetry tracing and metrics on the client's
// underlying go-redis connection. By default both signals are wired against the
// global TracerProvider and MeterProvider, so configure those (for example with
// github.com/Bugs5382/go-otel) before calling. Use WithoutTracing or
// WithoutMetrics to enable only one. Call it once per Client, after Connect.
func Instrument(c *redisx.Client, opts ...Option) error {
	cfg := config{tracing: true, metrics: true}
	for _, opt := range opts {
		opt(&cfg)
	}

	rdb := c.Redis()
	if cfg.tracing {
		if err := redisotel.InstrumentTracing(rdb); err != nil {
			return fmt.Errorf("redis/otel: instrument tracing: %w", err)
		}
	}
	if cfg.metrics {
		if err := redisotel.InstrumentMetrics(rdb); err != nil {
			return fmt.Errorf("redis/otel: instrument metrics: %w", err)
		}
	}
	return nil
}
