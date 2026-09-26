// Package cache adds typed get-or-set caching on top of a go-redis Client,
// with pluggable codecs, key namespacing, TTL jitter, negative caching and
// stampede protection.
//
// A Cache[T] stores values of one type:
//
//	users := cache.New[User](client, cache.WithPrefix("users:"), cache.WithJitter(0.1))
//	u, err := users.GetOrSet(ctx, id, 5*time.Minute, func(ctx context.Context) (User, error) {
//		return db.LoadUser(ctx, id)
//	})
//
// Get returns ErrMiss when there is no entry, so a miss is never confused with
// a failure. Set, Delete and GetOrSet round out the API.
//
// # Stampede protection
//
// When many requests miss the same key at once, GetOrSet runs the loader once
// and hands its result to every caller waiting on that key; each waiter still
// honours its own context. This collapsing is per Cache value, inside one
// process. WithJitter spreads the expiry of entries written together, so they
// do not all reload in the same instant.
//
// # Negative caching
//
// A loader returns ErrNotFound (or an error wrapping it) when the value does
// not exist. With WithNegativeTTL set, GetOrSet stores a not-found entry for
// that TTL and later lookups return ErrNotFound without calling the loader.
// Without it, not-found results are never stored.
//
// # Failure handling
//
// Loader errors are returned and never cached. If Redis is unreachable, or an
// entry cannot be decoded, GetOrSet logs a warning and serves from the loader,
// so an outage degrades to uncached reads. Get, Set and Delete return Redis
// errors to the caller. Errors are plain wrapped errors (this module does not
// use go-apperr); match ErrMiss and ErrNotFound with errors.Is.
//
// # Codecs
//
// Values are JSON by default. WithCodec plugs in any other encoding (for
// example MessagePack or protobuf) through the Codec interface.
//
// # Logging
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// debug lines for hits, misses, loads and writes (with durations), warnings
// when a failure is absorbed, and error lines for failed commands. The default
// logger discards everything. Keys are logged; values never are.
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
