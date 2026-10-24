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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// The bounds below are loose on purpose. A refused connection on loopback
// fails in well under a millisecond, so the fail-fast bound has two orders of
// magnitude of headroom for a busy CI runner. The default-path floor comes
// from the go-redis dial loop: five dial attempts with 100ms between them is
// at least 400ms per failed dial, before any command retry.
const (
	fastFailCeiling    = 150 * time.Millisecond
	defaultPathFloor   = 300 * time.Millisecond
	recoveryAllowance  = 5 * time.Second
	fastFailCallsInRow = 20
)

// TestFastFail_FailsFastWhileTheServerIsDown is the reason FastFail exists: a
// fail-open caller must not pay the default retry budget on every call while
// Redis is down.
func TestFastFail_FailsFastWhileTheServerIsDown(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	ff := c.FastFail()
	if !ff.Healthy(ctx) {
		t.Fatal("FastFail().Healthy = false against a live server")
	}
	if err := ff.Redis().Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("FastFail Set against a live server: %v", err)
	}

	m.Close()

	start := time.Now()
	ok := ff.Healthy(ctx)
	fastTook := time.Since(start)
	if ok {
		t.Fatal("FastFail().Healthy = true with the server stopped")
	}
	t.Logf("FastFail().Healthy with the server down: %v", fastTook)
	if fastTook > fastFailCeiling {
		t.Errorf("FastFail().Healthy took %v with the server down, want under %v", fastTook, fastFailCeiling)
	}

	start = time.Now()
	for i := 0; i < fastFailCallsInRow; i++ {
		err := ff.Redis().Get(ctx, "k").Err()
		if err == nil {
			t.Fatal("FastFail Get succeeded with the server stopped")
		}
		if !Transient(err) {
			t.Fatalf("FastFail Get error %v is not Transient; a fail-open caller could not tell it apart", err)
		}
	}
	t.Logf("%d FastFail Gets with the server down: %v", fastFailCallsInRow, time.Since(start))
	if took := time.Since(start); took > fastFailCeiling*fastFailCallsInRow/4 {
		t.Errorf("%d FastFail Gets took %v with the server down, want each well under %v",
			fastFailCallsInRow, took, fastFailCeiling)
	}

	start = time.Now()
	if c.Healthy(ctx) {
		t.Fatal("Healthy = true with the server stopped")
	}
	slowTook := time.Since(start)
	t.Logf("default Healthy with the server down: %v", slowTook)
	if slowTook < defaultPathFloor {
		t.Errorf("default Healthy took %v; expected the retrying path to take at least %v, "+
			"or this test no longer shows the difference", slowTook, defaultPathFloor)
	}
}

// TestFastFail_RecoversWhenTheServerReturns checks the view is not stuck once
// it has seen the server go away.
func TestFastFail_RecoversWhenTheServerReturns(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	addr := m.Addr()
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(addr))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()
	ff := c.FastFail()

	m.Close()
	for i := 0; i < fastFailCallsInRow; i++ {
		_ = ff.Healthy(ctx)
	}

	back := miniredis.NewMiniRedis()
	if err := back.StartAddr(addr); err != nil {
		t.Skipf("could not restart the server on %s: %v", addr, err)
	}
	t.Cleanup(back.Close)

	deadline := time.Now().Add(recoveryAllowance)
	for !ff.Healthy(ctx) {
		if time.Now().After(deadline) {
			t.Fatalf("FastFail().Healthy still false %v after the server came back", recoveryAllowance)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFastFail_ReturnsOneSharedView(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	ff := c.FastFail()
	if ff == nil {
		t.Fatal("FastFail returned nil")
	}
	if ff == c {
		t.Fatal("FastFail returned the parent client")
	}
	if again := c.FastFail(); again != ff {
		t.Error("FastFail built a second view; want one shared view per client")
	}
	if self := ff.FastFail(); self != ff {
		t.Error("FastFail on the view should return the view itself")
	}
}

func TestFastFail_ParentOwnsTheConnections(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()

	c, err := Connect(ctx, WithAddr(m.Addr()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ff := c.FastFail()

	if err := ff.Close(); err != nil {
		t.Fatalf("view Close: %v", err)
	}
	if !ff.Healthy(ctx) {
		t.Error("closing the view closed its pool; the parent owns it")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("parent Close: %v", err)
	}
	if err := ff.Redis().Ping(ctx).Err(); !errors.Is(err, goredis.ErrClosed) {
		t.Errorf("view Ping after parent Close = %v, want %v", err, goredis.ErrClosed)
	}
	if late := c.FastFail(); late.Redis().Ping(ctx).Err() == nil {
		t.Error("a view asked for after Close should be closed too")
	}
}

func TestFastFail_UsesTheParentsHooks(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	ctx := context.Background()
	obs := &recordingObserver{}

	c, err := Connect(ctx, WithAddr(m.Addr()), WithObserver(obs))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.FastFail().Redis().Incr(ctx, "n").Err(); err != nil {
		t.Fatalf("Incr: %v", err)
	}
	if !obs.commandSeen("incr") {
		t.Errorf("observer did not see the view's incr; saw %v", obs.commands)
	}
}
