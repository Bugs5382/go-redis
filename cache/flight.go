package cache

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
	"sync"
)

// errLoaderPanicked is what waiters receive when the loader they were waiting
// on panicked. The panic itself carries on in the goroutine that ran it.
var errLoaderPanicked = errors.New("cache: loader panicked")

// group collapses concurrent loads of the same key into one call, like
// golang.org/x/sync/singleflight, but typed and with waiters that honour their
// own context. It is kept in-package so the module takes no extra dependency.
type group[T any] struct {
	mu    sync.Mutex
	calls map[string]*call[T]
}

// call is one in-flight load.
type call[T any] struct {
	done chan struct{}
	val  T
	err  error
}

// do runs fn for key unless a call for key is already running, in which case
// it waits for that call's result or for ctx to end. shared reports whether
// the result came from another caller's run.
func (g *group[T]) do(ctx context.Context, key string, fn func() (T, error)) (v T, err error, shared bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*call[T])
	}
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-c.done:
			return c.val, c.err, true
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err(), true
		}
	}
	c := &call[T]{done: make(chan struct{}), err: errLoaderPanicked}
	g.calls[key] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		close(c.done)
	}()
	c.val, c.err = fn()
	return c.val, c.err, false
}
