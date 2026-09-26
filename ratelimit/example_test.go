package ratelimit_test

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
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/ratelimit"
	"github.com/alicebob/miniredis/v2"
)

// Allow a burst of three, then turn the fourth request away with a
// retry-after. The example runs against an in-memory server so it is
// self-contained; point redis.WithAddr at a real server in production.
func Example() {
	srv, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	srv.SetTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) // a fixed clock keeps the output stable

	ctx := context.Background()
	client, err := redis.Connect(ctx, redis.WithAddr(srv.Addr()))
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	limiter := ratelimit.New(client)
	limit := ratelimit.Limit{Rate: 1, Period: 2 * time.Second, Burst: 3}

	for range 4 {
		r, err := limiter.Allow(ctx, "api:alice", limit)
		if err != nil {
			panic(err)
		}
		fmt.Printf("allowed=%v remaining=%d retry_after=%v\n", r.Allowed, r.Remaining, r.RetryAfter)
	}
	// Output:
	// allowed=true remaining=2 retry_after=0s
	// allowed=true remaining=1 retry_after=0s
	// allowed=true remaining=0 retry_after=0s
	// allowed=false remaining=0 retry_after=2s
}
