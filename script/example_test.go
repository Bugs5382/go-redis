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
	"fmt"

	redis "github.com/Bugs5382/go-redis"
	"github.com/Bugs5382/go-redis/script"
	"github.com/alicebob/miniredis/v2"
)

// Build a Script once and run it as often as needed. The first run finds the
// script missing from the server's cache and falls back to EVAL; later runs
// use EVALSHA. The example runs against an in-memory server so it is
// self-contained; point redis.WithAddr at a real server in production.
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

	// Add to a counter and cap it, atomically.
	capped := script.New(`
local n = redis.call('INCRBY', KEYS[1], ARGV[1])
if n > tonumber(ARGV[2]) then
  redis.call('SET', KEYS[1], ARGV[2])
  return tonumber(ARGV[2])
end
return n`, script.WithName("capped-incr"))

	for range 3 {
		n, err := capped.Run(ctx, client, []string{"quota:42"}, 4, 10).Int64()
		if err != nil {
			panic(err)
		}
		fmt.Println(n)
	}
	// Output:
	// 4
	// 8
	// 10
}
