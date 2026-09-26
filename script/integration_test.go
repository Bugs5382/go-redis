//go:build integration

package script_test

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
	"os"
	"slices"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/script"
)

// redisAddr returns the server address from REDIS_ADDR and skips the test when
// it is unset, so a local run without a server still passes. CI also sets
// REDIS_TEST_REQUIRED, which turns a missing address into a failure: broken
// service wiring must not pass as a run of skipped tests.
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

// TestIntegrationScriptFallback runs a script against a real server, flushes
// the script cache, and checks that the next run falls back to EVAL and
// succeeds. Run it with a live server:
//
//	docker run -d --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test -tags integration -run Integration ./script/
func TestIntegrationScriptFallback(t *testing.T) {
	addr := redisAddr(t)
	ctx := context.Background()
	obs := &recorder{}
	c := connect(t, redis.WithAddr(addr), redis.WithObserver(obs))

	key := fmt.Sprintf("it:script:%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = c.Redis().Del(context.Background(), key).Err() })

	// A unique source, so a copy cached by another run cannot hide the fallback.
	s := script.New(fmt.Sprintf("-- %s\nreturn redis.call('INCRBY', KEYS[1], ARGV[1])", key))
	if err := c.Redis().ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	obs.take()

	if got, err := s.Run(ctx, c, []string{key}, 2).Int64(); err != nil || got != 2 {
		t.Fatalf("first Run = %d, %v; want 2, nil", got, err)
	}
	if want := []string{"evalsha", "eval"}; !slices.Equal(obs.take(), want) {
		t.Fatalf("first run should fall back to EVAL, want %v", want)
	}
	if got, err := s.Run(ctx, c, []string{key}, 3).Int64(); err != nil || got != 5 {
		t.Fatalf("second Run = %d, %v; want 5, nil", got, err)
	}
	if want := []string{"evalsha"}; !slices.Equal(obs.take(), want) {
		t.Fatalf("second run should hit the cache, want %v", want)
	}

	sha, err := c.Redis().ScriptLoad(ctx, s.Source()).Result()
	if err != nil || sha != s.SHA() {
		t.Fatalf("SCRIPT LOAD = %s, %v; want %s", sha, err, s.SHA())
	}
}
