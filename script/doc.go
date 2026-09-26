// Package script runs Lua scripts on a go-redis Client with EVALSHA, falling
// back to EVAL when the server answers NOSCRIPT.
//
// A server forgets its script cache after a restart, a failover to a replica
// that never saw the script, or SCRIPT FLUSH. Run hides that: it sends
// EVALSHA, and on NOSCRIPT it sends the full source once with EVAL, which
// caches it again. Callers build a Script once with New and call Run as often
// as they like.
//
//	incr := script.New(`return redis.call('INCRBY', KEYS[1], ARGV[1])`)
//	n, err := incr.Run(ctx, client, []string{"counter"}, 5).Int64()
//
// Keys are passed explicitly so the command routes to the right node. On a
// Redis Cluster client, Run checks that every key hashes to the same slot and
// fails with ErrCrossSlot before sending anything if they do not. Use a hash
// tag ("{order:7}:items", "{order:7}:total") to keep related keys together.
// Slot exposes the same slot calculation the server uses.
//
// EVALSHA and EVAL pass through the Client's hooks, so an Observer installed
// with redis.WithObserver, or the otel subpackage, sees every call.
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// debug lines for each run, an info line when the EVAL fallback fires, and an
// error line for each failure. The default logger discards everything. Keys,
// the script's name and its SHA are logged; argument values never are.
//
// Errors are plain wrapped errors (this module does not use go-apperr); match
// ErrCrossSlot with errors.Is, and redis.Nil for a script that returns nil.
package script

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
