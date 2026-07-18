package redis_test

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
	"log"
	"time"

	redis "github.com/Bugs5382/go-redis"
)

// Connect to a standalone server with the resilient defaults, then read and
// write through the underlying go-redis client.
func ExampleConnect() {
	ctx := context.Background()

	client, err := redis.Connect(ctx, redis.WithAddr("localhost:6379"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	rdb := client.Redis()
	if err := rdb.Set(ctx, "session:123", "alice", time.Minute).Err(); err != nil {
		log.Fatal(err)
	}
	name, err := rdb.Get(ctx, "session:123").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)
}

// Connect through Redis Sentinel for high availability: the client discovers the
// current master through the sentinels and re-resolves it after a failover.
func ExampleConnect_sentinel() {
	ctx := context.Background()

	client, err := redis.Connect(ctx,
		redis.WithSentinel("mymaster", "localhost:26379", "localhost:26380"),
		redis.WithPassword("s3cret"),
		redis.WithPool(50),
		redis.WithTimeouts(5*time.Second, 3*time.Second, 3*time.Second),
		redis.WithRetry(3, 8*time.Millisecond, 512*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	if client.Healthy(ctx) {
		fmt.Println("ready")
	}
}
