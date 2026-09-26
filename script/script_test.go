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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/script"
	"github.com/alicebob/miniredis/v2"
)

// recorder is an Observer that keeps the name of every command the client runs.
type recorder struct {
	mu    sync.Mutex
	names []string
}

func (r *recorder) ObserveCommand(_ context.Context, name string, _ time.Duration, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.names = append(r.names, name)
}

func (r *recorder) ObserveDial(context.Context, string, string, time.Duration, error) {}

// take returns the commands seen since the last call and resets the record.
func (r *recorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.names
	r.names = nil
	return out
}

// entry is one captured log line.
type entry struct {
	level  string
	msg    string
	err    error
	fields []log.Field
}

// captureLog is a go-log Logger that records every line, shared across With
// and Ctx children.
type captureLog struct {
	mu      *sync.Mutex
	entries *[]entry
	with    []log.Field
}

func newCaptureLog() *captureLog {
	return &captureLog{mu: &sync.Mutex{}, entries: &[]entry{}}
}

func (c *captureLog) add(level string, err error, msg string, fields []log.Field) {
	c.mu.Lock()
	defer c.mu.Unlock()
	all := append(slices.Clone(c.with), fields...)
	*c.entries = append(*c.entries, entry{level: level, msg: msg, err: err, fields: all})
}

func (c *captureLog) Debug(msg string, f ...log.Field)            { c.add("debug", nil, msg, f) }
func (c *captureLog) Info(msg string, f ...log.Field)             { c.add("info", nil, msg, f) }
func (c *captureLog) Warn(msg string, f ...log.Field)             { c.add("warn", nil, msg, f) }
func (c *captureLog) Error(err error, msg string, f ...log.Field) { c.add("error", err, msg, f) }
func (c *captureLog) Fatal(err error, msg string, f ...log.Field) { c.add("fatal", err, msg, f) }
func (c *captureLog) Ctx(context.Context) log.Logger              { return c }
func (c *captureLog) With(f ...log.Field) log.Logger {
	return &captureLog{mu: c.mu, entries: c.entries, with: append(slices.Clone(c.with), f...)}
}

func (c *captureLog) all() []entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(*c.entries)
}

// dump renders every captured line, fields included, for value-leak checks.
func (c *captureLog) dump() string {
	var b strings.Builder
	for _, e := range c.all() {
		fmt.Fprintf(&b, "%s %s %v", e.level, e.msg, e.err)
		for _, f := range e.fields {
			fmt.Fprintf(&b, " %s=%v", f.Key, f.Val)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func newServer(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(m.Close)
	return m
}

func connect(t *testing.T, opts ...redis.Option) *redis.Client {
	t.Helper()
	c, err := redis.Connect(context.Background(), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

const incrBy = `return redis.call('INCRBY', KEYS[1], ARGV[1])`

func TestRunFallsBackToEvalOnNoScript(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	obs := &recorder{}
	c := connect(t, redis.WithAddr(m.Addr()), redis.WithObserver(obs))
	ctx := context.Background()
	obs.take()

	s := script.New(incrBy)

	// The server has never seen the script: EVALSHA answers NOSCRIPT and Run
	// falls back to EVAL, which also caches the script on the server.
	got, err := s.Run(ctx, c, []string{"counter"}, 2).Int64()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 2 {
		t.Fatalf("Run = %d, want 2", got)
	}
	if want := []string{"evalsha", "eval"}; !slices.Equal(obs.take(), want) {
		t.Fatalf("first run commands differ, want %v", want)
	}

	// Now the script is cached, so only EVALSHA runs.
	got, err = s.Run(ctx, c, []string{"counter"}, 3).Int64()
	if err != nil || got != 5 {
		t.Fatalf("Run = %d, %v; want 5, nil", got, err)
	}
	if want := []string{"evalsha"}; !slices.Equal(obs.take(), want) {
		t.Fatalf("second run commands differ, want %v", want)
	}
}

func TestRunRecoversAfterScriptFlush(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	obs := &recorder{}
	c := connect(t, redis.WithAddr(m.Addr()), redis.WithObserver(obs))
	ctx := context.Background()

	s := script.New(incrBy)
	if err := s.Load(ctx, c); err != nil {
		t.Fatalf("Load: %v", err)
	}
	obs.take()

	if _, err := s.Run(ctx, c, []string{"n"}, 1).Int64(); err != nil {
		t.Fatalf("Run after Load: %v", err)
	}
	if want := []string{"evalsha"}; !slices.Equal(obs.take(), want) {
		t.Fatalf("after Load, want only %v", want)
	}

	// A failover to a fresh replica or a SCRIPT FLUSH empties the cache.
	if err := c.Redis().ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	obs.take()

	got, err := s.Run(ctx, c, []string{"n"}, 1).Int64()
	if err != nil || got != 2 {
		t.Fatalf("Run after flush = %d, %v; want 2, nil", got, err)
	}
	if want := []string{"evalsha", "eval"}; !slices.Equal(obs.take(), want) {
		t.Fatalf("after flush, want %v", want)
	}
}

func TestSHAMatchesServer(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	s := script.New(incrBy)
	sha, err := c.Redis().ScriptLoad(context.Background(), incrBy).Result()
	if err != nil {
		t.Fatalf("SCRIPT LOAD: %v", err)
	}
	if s.SHA() != sha {
		t.Fatalf("SHA = %s, server says %s", s.SHA(), sha)
	}
	if s.Source() != incrBy {
		t.Fatal("Source does not return the script text")
	}
}

func TestRunNilResult(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	s := script.New(`return redis.call('GET', KEYS[1])`)
	_, err := s.Run(context.Background(), c, []string{"missing"}).Result()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("Run on a missing key: err = %v, want redis.Nil", err)
	}
}

func TestRunScriptErrorIsReturned(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	s := script.New(`return redis.error_reply('BOOM broken')`)
	_, err := s.Run(context.Background(), c, nil).Result()
	if err == nil || !strings.Contains(err.Error(), "BOOM") {
		t.Fatalf("Run: err = %v, want the script's error", err)
	}
}

func TestRunHonoursContext(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := script.New(incrBy).Run(ctx, c, []string{"n"}, 1).Result()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run with a cancelled context: err = %v, want context.Canceled", err)
	}
}

func TestRunClusterRejectsCrossSlotKeys(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	obs := &recorder{}
	c := connect(t, redis.WithCluster(m.Addr()), redis.WithObserver(obs))
	ctx := context.Background()

	s := script.New(`return redis.call('MSET', KEYS[1], ARGV[1], KEYS[2], ARGV[1])`)
	obs.take()
	_, err := s.Run(ctx, c, []string{"user:1", "user:2"}, "x").Result()
	if !errors.Is(err, script.ErrCrossSlot) {
		t.Fatalf("Run with keys in different slots: err = %v, want ErrCrossSlot", err)
	}
	if !strings.Contains(err.Error(), "user:1") || !strings.Contains(err.Error(), "user:2") {
		t.Fatalf("error %q should name the conflicting keys", err)
	}
	if got := obs.take(); len(got) != 0 {
		t.Fatalf("a cross-slot run must not reach the server, sent %v", got)
	}

	// A shared hash tag puts both keys in one slot.
	if _, err := s.Run(ctx, c, []string{"{user}:1", "{user}:2"}, "x").Result(); err != nil && !errors.Is(err, redis.Nil) {
		t.Fatalf("Run with a shared hash tag: %v", err)
	}
}

func TestRunStandaloneAllowsAnyKeys(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	s := script.New(`redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[1]); return 1`)
	if _, err := s.Run(context.Background(), c, []string{"user:1", "user:2"}, "x").Int64(); err != nil {
		t.Fatalf("standalone Run across slots: %v", err)
	}
}

func TestSlot(t *testing.T) {
	t.Parallel()
	// Known values from the Redis documentation and the CRC-16/XMODEM check
	// value (0x31C3 for "123456789"). miniredis answers CLUSTER KEYSLOT with a
	// constant, and a standalone server refuses it, so the slot rules are
	// checked against known values and hash-tag equalities instead.
	known := map[string]int{
		"foo":       12182,
		"hello":     866,
		"somekey":   11058,
		"123456789": 0x31C3,
	}
	for k, want := range known {
		if got := script.Slot(k); got != want {
			t.Errorf("Slot(%q) = %d, want %d", k, got, want)
		}
	}

	// Hash tag rules: only the first non-empty {...} counts.
	same := [][2]string{
		{"{user1000}.following", "{user1000}.followers"},
		{"{user1000}.following", "user1000"},
		{"foo{bar}{zap}", "bar"},
		{"foo{{bar}}zap", "{bar"},
		{"foo{}{bar}", "foo{}{bar}"},
	}
	for _, p := range same {
		if script.Slot(p[0]) != script.Slot(p[1]) {
			t.Errorf("Slot(%q) != Slot(%q)", p[0], p[1])
		}
	}
	// An empty tag means the whole key is hashed, not the empty string.
	if script.Slot("foo{}{bar}") == script.Slot("") {
		t.Error("an empty hash tag must hash the whole key")
	}
	if script.Slot("foo{}{bar}") == script.Slot("bar") {
		t.Error("only the first {...} is a hash tag")
	}
}

func TestRunLogsKeysNotValues(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))
	logs := newCaptureLog()

	s := script.New(`return redis.call('SET', KEYS[1], ARGV[1])`, script.WithName("store"), script.WithLogger(logs))
	if err := s.Run(context.Background(), c, []string{"session:42"}, "top-secret-value").Err(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	out := logs.dump()
	if strings.Contains(out, "top-secret-value") {
		t.Fatalf("an argument value leaked into the logs:\n%s", out)
	}
	for _, want := range []string{"session:42", "store", "falling back to EVAL"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs should mention %q:\n%s", want, out)
		}
	}
}

func TestRunDefaultLoggerIsQuiet(t *testing.T) {
	t.Parallel()
	m := newServer(t)
	c := connect(t, redis.WithAddr(m.Addr()))

	// No WithLogger: Run must not panic or write anywhere.
	if _, err := script.New(incrBy).Run(context.Background(), c, []string{"n"}, 1).Int64(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
