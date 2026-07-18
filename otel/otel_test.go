package otel_test

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
	"testing"

	redis "github.com/Bugs5382/go-redis"
	redisotel "github.com/Bugs5382/go-redis/otel"
	"github.com/alicebob/miniredis/v2"
)

func connect(t *testing.T) *redis.Client {
	t.Helper()
	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(m.Close)

	c, err := redis.Connect(context.Background(), redis.WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestInstrumentDefault(t *testing.T) {
	t.Parallel()
	c := connect(t)
	if err := redisotel.Instrument(c); err != nil {
		t.Fatalf("Instrument: %v", err)
	}
	// Commands still work once instrumented against the global (no-op) providers.
	if err := c.Redis().Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after instrument: %v", err)
	}
}

func TestInstrumentTracingOnly(t *testing.T) {
	t.Parallel()
	c := connect(t)
	if err := redisotel.Instrument(c, redisotel.WithoutMetrics()); err != nil {
		t.Fatalf("Instrument tracing-only: %v", err)
	}
}

func TestInstrumentMetricsOnly(t *testing.T) {
	t.Parallel()
	c := connect(t)
	if err := redisotel.Instrument(c, redisotel.WithoutTracing()); err != nil {
		t.Fatalf("Instrument metrics-only: %v", err)
	}
}
