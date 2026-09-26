// Package lock is a Redis lock with safe release and optional auto-extend,
// built on a go-redis Client and the script package.
//
//	l, err := lock.Acquire(ctx, client, "jobs:nightly-report", 30*time.Second,
//		lock.WithWait(5*time.Second), lock.WithAutoExtend(10*time.Second))
//	if errors.Is(err, lock.ErrNotAcquired) {
//		return nil // another worker is on it
//	}
//	defer l.Release(ctx)
//
// Acquire stores a random 128-bit token with SET key token NX PX ttl. Release
// and Extend run small Lua scripts that act only if the key still holds that
// token, so a holder whose lease ran out can never delete or extend the next
// holder's lock; they return ErrNotHeld instead.
//
// # Waiting and extending
//
// By default Acquire fails fast with ErrNotAcquired. WithWait retries with
// exponential backoff (WithBackoff) up to a limit, always bounded by the
// context. WithAutoExtend resets the TTL on an interval while the work runs
// and stops on Release, when the context passed to Acquire ends, or when the
// lock turns out to be gone, in which case Lost is closed. Guarded work should
// watch Lost and stop when it fires.
//
// # Limits: a single-instance lock, not Redlock
//
// This is a lock on one Redis primary. It does not implement Redlock, the
// multi-node algorithm, and makes no claim to its guarantees. Know its limits:
//
//   - Failover. Redis replicates asynchronously. If the primary fails after
//     granting a lock but before the write reaches a replica, a sentinel or
//     cluster failover promotes a replica without the key, and a second
//     client can acquire the same lock. Two holders can then overlap.
//   - Pauses and clock drift. A holder can stall (a GC pause, a slow
//     network, a suspended VM) past its TTL and wake up believing it still
//     holds the lock. Auto-extend and Lost narrow the window but cannot close
//     it; Redlock has the same problem.
//   - Fencing. When overlapping holders would corrupt data, pass a fencing
//     token to the protected resource (for example a counter from INCR taken
//     while holding the lock) and have the resource reject stale tokens.
//
// Use it for efficiency (avoiding duplicate work) freely; use it for
// correctness only together with fencing or an idempotent downstream.
//
// # Logging
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// debug lines for acquire attempts, waits, extends and releases, warnings when
// a lock is found lost, and error lines for failed commands. The default
// logger discards everything. The key is logged; the token never is.
//
// Errors are plain wrapped errors (this module does not use go-apperr); match
// ErrNotAcquired and ErrNotHeld with errors.Is.
package lock

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
