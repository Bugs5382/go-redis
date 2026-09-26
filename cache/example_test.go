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
	"time"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/cache"
	"github.com/alicebob/miniredis/v2"
)

type product struct {
	SKU   string
	Price int
}

// Load a value once and serve it from Redis afterwards. A not-found answer is
// cached too, so a missing SKU does not hit the source on every request. The
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

	products := cache.New[product](client,
		cache.WithPrefix("products:"),
		cache.WithJitter(0.1),
		cache.WithNegativeTTL(30*time.Second),
	)

	load := func(sku string) cache.Loader[product] {
		return func(context.Context) (product, error) {
			fmt.Println("loading", sku)
			if sku != "A-1" {
				return product{}, cache.ErrNotFound
			}
			return product{SKU: sku, Price: 1299}, nil
		}
	}

	for range 2 {
		p, err := products.GetOrSet(ctx, "A-1", 5*time.Minute, load("A-1"))
		if err != nil {
			panic(err)
		}
		fmt.Println(p.SKU, p.Price)
	}
	for range 2 {
		_, err := products.GetOrSet(ctx, "Z-9", 5*time.Minute, load("Z-9"))
		fmt.Println(errors.Is(err, cache.ErrNotFound))
	}
	// Output:
	// loading A-1
	// A-1 1299
	// A-1 1299
	// loading Z-9
	// true
	// true
}
