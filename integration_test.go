//go:build integration

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
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// redisAddr returns the server address from REDIS_ADDR and skips the test when
// it is unset, so a local run without a server still passes. CI also sets
// REDIS_TEST_REQUIRED (see .github/workflows/job-go-integration.yaml), which
// turns a missing address into a failure: broken service wiring must not pass
// as a run of skipped tests.
func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		if os.Getenv("REDIS_TEST_REQUIRED") != "" {
			t.Fatal("REDIS_TEST_REQUIRED is set but REDIS_ADDR is empty")
		}
		t.Skip("set REDIS_ADDR to run the integration test")
	}
	return addr
}

// TestIntegrationRoundTrip exercises the client against a real Redis. It is
// excluded from the default build. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./...
func TestIntegrationRoundTrip(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	if !c.Healthy(ctx) {
		t.Fatal("server not healthy")
	}

	rdb := c.Redis()
	if err := rdb.Set(ctx, "session:integration", "ok", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := rdb.Get(ctx, "session:integration").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "ok" {
		t.Fatalf("Get = %q, want %q", got, "ok")
	}
}

// cutProxy forwards TCP to a real server until cut, which closes the listener
// and every forwarded connection: from the client's side the server went away.
type cutProxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &cutProxy{ln: ln}
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			out, err := net.Dial("tcp", target)
			if err != nil {
				_ = in.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, in, out)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(out, in); _ = out.Close() }()
			go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		}
	}()
	t.Cleanup(p.cut)
	return p
}

func (p *cutProxy) addr() string { return p.ln.Addr().String() }

func (p *cutProxy) cut() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// TestIntegrationFastFail runs the FastFail view against a real server, then
// cuts the path to it and checks the view fails within its budget while the
// retrying path does not.
func TestIntegrationFastFail(t *testing.T) {
	target := redisAddr(t)
	ctx := context.Background()
	proxy := newCutProxy(t, target)

	c, err := Connect(ctx, WithAddr(proxy.addr()), WithFastFail(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	ff := c.FastFail()
	if err := ff.Redis().Set(ctx, "fastfail:integration", "ok", time.Minute).Err(); err != nil {
		t.Fatalf("FastFail Set: %v", err)
	}
	if got, err := ff.Redis().Get(ctx, "fastfail:integration").Result(); err != nil || got != "ok" {
		t.Fatalf("FastFail Get = %q, %v; want ok", got, err)
	}

	proxy.cut()

	start := time.Now()
	err = ff.Redis().Get(ctx, "fastfail:integration").Err()
	fastTook := time.Since(start)
	t.Logf("FastFail Get with the server cut off: %v (%v)", fastTook, err)
	if err == nil || errors.Is(err, Nil) {
		t.Fatalf("FastFail Get with the server cut off = %v, want a connection error", err)
	}
	if !Transient(err) {
		t.Errorf("FastFail error %v is not Transient", err)
	}
	if fastTook > 500*time.Millisecond {
		t.Errorf("FastFail Get took %v with the server cut off, want under 500ms", fastTook)
	}

	start = time.Now()
	healthy := c.Healthy(ctx)
	slowTook := time.Since(start)
	t.Logf("default Healthy with the server cut off: %v", slowTook)
	if healthy {
		t.Fatal("Healthy = true with the server cut off")
	}
	if slowTook < 300*time.Millisecond {
		t.Errorf("default Healthy took %v; the retrying path should take at least 300ms", slowTook)
	}
}
