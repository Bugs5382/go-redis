package idempotency_test

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
	"github.com/Bugs5382/go-redis/idempotency"
	"github.com/alicebob/miniredis/v2"
)

type charge struct {
	ID     string
	Amount int
}

// A client retries a payment request with the same idempotency key. The
// charge runs once; the retry gets the stored receipt back. The example runs
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

	payments := idempotency.New[charge](client,
		idempotency.WithPrefix("payments:"),
		idempotency.WithLease(30*time.Second),
	)

	pay := func(ctx context.Context) (charge, error) {
		fmt.Println("charging the card")
		return charge{ID: "ch_1", Amount: 4200}, nil
	}

	for range 2 {
		c, err := payments.Do(ctx, "request-7f3a", 24*time.Hour, pay)
		if err != nil {
			panic(err)
		}
		fmt.Println(c.ID, c.Amount)
	}
	// Output:
	// charging the card
	// ch_1 4200
	// ch_1 4200
}
