package cache_test

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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/cache"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestGetMissSetGet(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[user](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	if _, err := c.Get(ctx, "u:1"); !errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get on an empty cache: err = %v, want ErrMiss", err)
	}
	want := user{ID: 1, Name: "alice"}
	if err := c.Set(ctx, "u:1", want, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := c.Get(ctx, "u:1")
	if err != nil || got != want {
		t.Fatalf("Get = %+v, %v; want %+v, nil", got, err, want)
	}
	if ttl := m.TTL("u:1"); ttl != time.Minute {
		t.Fatalf("TTL = %v, want 1m", ttl)
	}
}

func TestSetZeroTTLNeverExpires(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())))
	if err := c.Set(context.Background(), "k", "v", 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ttl := m.TTL("k"); ttl != 0 {
		t.Fatalf("TTL = %v, want none", ttl)
	}
}

func TestPrefix(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())), cache.WithPrefix("users:"))
	ctx := context.Background()

	if err := c.Set(ctx, "42", "bob", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !m.Exists("users:42") {
		t.Fatalf("value should be stored under the prefixed key, have %v", m.Keys())
	}
	if m.Exists("42") {
		t.Fatal("value stored under the bare key")
	}
	if err := c.Delete(ctx, "42"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if m.Exists("users:42") {
		t.Fatal("Delete did not use the prefix")
	}
}

func TestJitterShortensTTLWithinBounds(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[int](connect(t, redis.WithAddr(m.Addr())), cache.WithJitter(0.2))
	ctx := context.Background()

	ttl := 100 * time.Second
	seen := map[time.Duration]bool{}
	for i := range 50 {
		key := fmt.Sprintf("j:%d", i)
		if err := c.Set(ctx, key, i, ttl); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got := m.TTL(key)
		if got > ttl || got < 80*time.Second {
			t.Fatalf("TTL %v outside [80s, 100s]", got)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Fatal("jitter should spread expiries, every TTL was the same")
	}
}

func TestGetOrSetLoadsOnceThenHits(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[user](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	var calls atomic.Int32
	load := func(context.Context) (user, error) {
		calls.Add(1)
		return user{ID: 7, Name: "carol"}, nil
	}
	for range 3 {
		got, err := c.GetOrSet(ctx, "u:7", time.Minute, load)
		if err != nil || got.Name != "carol" {
			t.Fatalf("GetOrSet = %+v, %v", got, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader ran %d times, want 1", n)
	}
	if ttl := m.TTL("u:7"); ttl != time.Minute {
		t.Fatalf("TTL = %v, want 1m", ttl)
	}
}

func TestGetOrSetCollapsesConcurrentLoads(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) (string, error) {
		calls.Add(1)
		<-release
		return "hot", nil
	}

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Go(func() {
			v, err := c.GetOrSet(ctx, "hot", time.Minute, load)
			if err == nil && v != "hot" {
				err = fmt.Errorf("got %q", v)
			}
			errs <- err
		})
	}
	// Let every goroutine reach the flight before the loader returns.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("GetOrSet: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times for %d concurrent callers, want 1", got, n)
	}
}

func TestGetOrSetWaiterHonoursItsContext(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())))

	release := make(chan struct{})
	started := make(chan struct{})
	leaderDone := make(chan error, 1)
	go func() {
		_, err := c.GetOrSet(context.Background(), "slow", time.Minute, func(context.Context) (string, error) {
			close(started)
			<-release
			return "v", nil
		})
		leaderDone <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.GetOrSet(ctx, "slow", time.Minute, func(context.Context) (string, error) {
		t.Error("a waiter must not run its own loader while a load is in flight")
		return "", nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter err = %v, want context.DeadlineExceeded", err)
	}
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader: %v", err)
	}
}

func TestGetOrSetLoaderErrorIsNotCached(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	boom := errors.New("upstream down")
	if _, err := c.GetOrSet(ctx, "k", time.Minute, func(context.Context) (string, error) { return "", boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the loader's error", err)
	}
	if m.Exists("k") {
		t.Fatal("a loader error must not be cached")
	}
	got, err := c.GetOrSet(ctx, "k", time.Minute, func(context.Context) (string, error) { return "ok", nil })
	if err != nil || got != "ok" {
		t.Fatalf("retry = %q, %v", got, err)
	}
}

func TestNegativeCaching(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[user](connect(t, redis.WithAddr(m.Addr())), cache.WithNegativeTTL(10*time.Second))
	ctx := context.Background()

	var calls atomic.Int32
	load := func(context.Context) (user, error) {
		calls.Add(1)
		return user{}, fmt.Errorf("user 9: %w", cache.ErrNotFound)
	}
	for range 3 {
		if _, err := c.GetOrSet(ctx, "u:9", time.Minute, load); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("GetOrSet: err = %v, want ErrNotFound", err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader ran %d times, want 1 (the not-found should be cached)", n)
	}
	if _, err := c.Get(ctx, "u:9"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Get on a cached not-found: err = %v, want ErrNotFound", err)
	}
	if ttl := m.TTL("u:9"); ttl != 10*time.Second {
		t.Fatalf("not-found TTL = %v, want the negative TTL of 10s", ttl)
	}

	// Once the negative entry expires the loader runs again.
	m.FastForward(11 * time.Second)
	_, _ = c.GetOrSet(ctx, "u:9", time.Minute, load)
	if n := calls.Load(); n != 2 {
		t.Fatalf("loader ran %d times after expiry, want 2", n)
	}
}

func TestNotFoundWithoutNegativeCachingIsNotStored(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[user](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	var calls atomic.Int32
	load := func(context.Context) (user, error) {
		calls.Add(1)
		return user{}, cache.ErrNotFound
	}
	for range 2 {
		if _, err := c.GetOrSet(ctx, "u:9", time.Minute, load); !errors.Is(err, cache.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	}
	if calls.Load() != 2 || m.Exists("u:9") {
		t.Fatal("without WithNegativeTTL a not-found must not be cached")
	}
}

func TestGetOrSetServesFromLoaderWhenRedisIsDown(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	logs := newCaptureLog()
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr()), redis.WithRetry(0, 0, 0)), cache.WithLogger(logs))
	m.Close()

	got, err := c.GetOrSet(context.Background(), "k", time.Minute, func(context.Context) (string, error) { return "fresh", nil })
	if err != nil || got != "fresh" {
		t.Fatalf("GetOrSet with Redis down = %q, %v; want the loader's value", got, err)
	}
	if !strings.Contains(logs.dump(), "warn") {
		t.Fatalf("the Redis failure should be logged as a warning:\n%s", logs.dump())
	}
}

func TestCorruptEntryIsReloaded(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[user](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	if err := m.Set("u:1", "{not json"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "u:1"); err == nil || errors.Is(err, cache.ErrMiss) {
		t.Fatalf("Get on a corrupt entry: err = %v, want a decode error", err)
	}
	got, err := c.GetOrSet(ctx, "u:1", time.Minute, func(context.Context) (user, error) { return user{ID: 1}, nil })
	if err != nil || got.ID != 1 {
		t.Fatalf("GetOrSet over a corrupt entry = %+v, %v", got, err)
	}
	if got, err := c.Get(ctx, "u:1"); err != nil || got.ID != 1 {
		t.Fatalf("entry should be overwritten, Get = %+v, %v", got, err)
	}
}

// upper is a Codec that stores strings upper-cased, to prove the codec is used.
type upper struct{}

func (upper) Marshal(v any) ([]byte, error) { return []byte(strings.ToUpper(*v.(*string))), nil }
func (upper) Unmarshal(data []byte, v any) error {
	*v.(*string) = strings.ToLower(string(data))
	return nil
}

func TestCustomCodec(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())), cache.WithCodec(upper{}))
	ctx := context.Background()

	if err := c.Set(ctx, "k", "hello", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if raw, _ := m.Get("k"); raw != "HELLO" {
		t.Fatalf("stored %q, want the codec's output HELLO", raw)
	}
	if got, err := c.Get(ctx, "k"); err != nil || got != "hello" {
		t.Fatalf("Get = %q, %v", got, err)
	}
}

func TestDeleteManyKeys(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[int](connect(t, redis.WithAddr(m.Addr())))
	ctx := context.Background()

	for i := range 3 {
		if err := c.Set(ctx, fmt.Sprint(i), i, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Delete(ctx, "0", "1", "2", "missing"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n := len(m.Keys()); n != 0 {
		t.Fatalf("%d keys left after Delete", n)
	}
	if err := c.Delete(ctx); err != nil {
		t.Fatalf("Delete with no keys: %v", err)
	}
}

func TestLogsKeysNotValues(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	logs := newCaptureLog()
	c := cache.New[string](connect(t, redis.WithAddr(m.Addr())), cache.WithPrefix("s:"), cache.WithLogger(logs))
	ctx := context.Background()

	_, err := c.GetOrSet(ctx, "token", time.Minute, func(context.Context) (string, error) { return "very-secret", nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "token"); err != nil {
		t.Fatal(err)
	}
	out := logs.dump()
	if strings.Contains(out, "very-secret") {
		t.Fatalf("a cached value leaked into the logs:\n%s", out)
	}
	for _, want := range []string{"s:token", "miss", "hit"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}

func TestWithJitterClamps(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := cache.New[int](connect(t, redis.WithAddr(m.Addr())), cache.WithJitter(5))
	if err := c.Set(context.Background(), "k", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	// A fraction above 1 is clamped to 1, so the TTL stays positive.
	if ttl := m.TTL("k"); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("TTL = %v, want within (0, 1m]", ttl)
	}
}
