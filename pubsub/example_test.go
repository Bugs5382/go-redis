package pubsub_test

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

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/pubsub"
	"github.com/alicebob/miniredis/v2"
)

// Subscribe to a channel and a pattern, then publish to both. Healthy is what
// a readiness probe would report. The example runs against an in-memory
// server so it is self-contained; point redis.WithAddr at a real server in
// production.
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

	got := make(chan pubsub.Message, 2)
	sub, err := pubsub.Subscribe(ctx, client, pubsub.Topics{
		Channels: []string{"config.reload"},
		Patterns: []string{"orders.*"},
	}, func(_ context.Context, msg pubsub.Message) {
		got <- msg
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = sub.Close() }()
	fmt.Println("healthy:", sub.Healthy())

	rdb := client.Redis()
	if err := rdb.Publish(ctx, "config.reload", "now").Err(); err != nil {
		panic(err)
	}
	fmt.Println((<-got).Channel)
	if err := rdb.Publish(ctx, "orders.created", "A-1").Err(); err != nil {
		panic(err)
	}
	msg := <-got
	fmt.Println(msg.Channel, "via", msg.Pattern)
	// Output:
	// healthy: true
	// config.reload
	// orders.created via orders.*
}
