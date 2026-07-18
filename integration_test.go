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
	"os"
	"testing"
	"time"
)

// TestIntegrationRoundTrip exercises the client against a real Redis. Run it
// with a live server:
//
//	REDIS_ADDR=localhost:6379 go test -tags=integration ./...
//
// It is excluded from the default build so CI stays green without a broker.
func TestIntegrationRoundTrip(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run the integration test")
	}
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

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
