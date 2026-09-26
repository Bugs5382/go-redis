package lock_test

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
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/lock"
	"github.com/alicebob/miniredis/v2"
)

// Take a lock for a job, keep it alive while the job runs, and show that a
// second worker is turned away until it is released. The example runs
// against an in-memory server so it is self-contained; point redis.WithAddr
// at a real server in production.
func Example() {
	srv, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	ctx := context.Background()
	client, err := redis.Connect(ctx, redis.WithAddr(srv.Addr()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	l, err := lock.Acquire(ctx, client, "jobs:nightly-report", 30*time.Second,
		lock.WithAutoExtend(10*time.Second))
	if err != nil {
		panic(err)
	}
	fmt.Println("worker 1 has the lock")

	_, err = lock.Acquire(ctx, client, "jobs:nightly-report", 30*time.Second)
	fmt.Println("worker 2 turned away:", errors.Is(err, lock.ErrNotAcquired))

	if err := l.Release(ctx); err != nil {
		panic(err)
	}
	l2, err := lock.Acquire(ctx, client, "jobs:nightly-report", 30*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("worker 2 has the lock after release")
	_ = l2.Release(ctx)
	// Output:
	// worker 1 has the lock
	// worker 2 turned away: true
	// worker 2 has the lock after release
}
