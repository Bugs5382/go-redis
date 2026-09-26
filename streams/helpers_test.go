package streams_test

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
	"slices"
	"strings"
	"sync"
	"testing"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	"github.com/alicebob/miniredis/v2"
)

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

// dump renders every captured line, fields included, for value-leak checks.
func (c *captureLog) dump() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for _, e := range *c.entries {
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
