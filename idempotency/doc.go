// Package idempotency runs a function at most once per key within a TTL and
// replays its result, built on a go-redis Client and the script package. It
// pairs with consumers that may redeliver (streams, queues, webhooks, client
// retries).
//
//	payments := idempotency.New[Receipt](client, idempotency.WithLease(30*time.Second))
//	r, err := payments.Do(ctx, requestID, 24*time.Hour, func(ctx context.Context) (Receipt, error) {
//		return charge(ctx, order)
//	})
//
// # How it works
//
// Each key holds one entry. The first caller claims it atomically with a Lua
// script that stores a pending marker (with a random token) only if the key
// is empty, then runs fn. On success the result is encoded with the codec
// (JSON by default; any cache.Codec works) and stored for the TTL, and later
// calls get it back without running fn. Finishing and releasing are
// token-checked scripts, so a caller whose lease ran out never overwrites or
// deletes an entry that another caller now owns.
//
// # Duplicates and failures
//
// A call that arrives while fn is running gets ErrInProgress, or with
// WithWait, polls until the result is stored and returns it. If fn fails, the
// FailurePolicy decides: ReleaseOnError (the default) deletes the entry so
// the next call retries, and a waiting duplicate takes over; KeepOnError
// stores the failure for the TTL and later calls get ErrFailed with the
// original error text. A panic in fn releases the key and continues.
//
// WithLease bounds how long a claimed key stays in progress (the TTL by
// default), so a caller that crashes mid-run frees the key when the lease
// runs out. Keep the lease above fn's longest run: if it expires while fn is
// still working, a duplicate can run fn as well.
//
// # Limits
//
// The at-most-once promise is as strong as the Redis primary holding the key:
// a failover that loses recent writes can lose a claim or a stored result.
// Stored results and kept error texts are written to Redis as-is, so do not
// put secrets in them unless the server is trusted with them.
//
// # Logging
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// debug lines for claims, replays, waits and stores, warnings for fn failures
// and lost leases, and error lines for failed commands. The default logger
// discards everything. Keys are logged; results never are.
//
// Errors are plain wrapped errors (this module does not use go-apperr); match
// ErrInProgress and ErrFailed with errors.Is.
package idempotency

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
