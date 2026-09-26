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
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/streams"
	"github.com/alicebob/miniredis/v2"
)

// Publish two orders and consume them in a group. The handler rejects the
// malformed one with ErrDeadLetter, which moves it to "orders:dead". The
// example runs against an in-memory server so it is self-contained; point
// redis.WithAddr at a real server in production.
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

	for _, order := range []string{"A-1", ""} {
		if _, err := streams.Publish(ctx, client, "orders", map[string]any{"order_id": order}, streams.WithMaxLen(10000)); err != nil {
			panic(err)
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	handled := make(chan struct{}, 2)
	done := make(chan error, 1)
	go func() {
		done <- streams.Consume(runCtx, client, streams.Config{
			Stream:   "orders",
			Group:    "billing",
			Consumer: "billing-1",
			StartID:  "0", // a new group reads the whole stream
			Block:    50 * time.Millisecond,
		}, func(_ context.Context, msg streams.Message) error {
			defer func() { handled <- struct{}{} }()
			id := msg.Values["order_id"]
			if id == "" {
				return fmt.Errorf("order without an ID: %w", streams.ErrDeadLetter)
			}
			fmt.Println("billed", id)
			return nil
		})
	}()

	<-handled
	<-handled
	stop()
	if err := <-done; err != nil {
		panic(err)
	}

	dead, err := client.Redis().XLen(ctx, "orders:dead").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("dead letters:", dead)
	// Output:
	// billed A-1
	// dead letters: 1
}
