// Package ratelimit is a distributed rate limiter backed by atomic Lua
// scripts, built on a go-redis Client and the script package.
//
//	limiter := ratelimit.New(client)
//	r, err := limiter.Allow(ctx, "api:"+userID, ratelimit.Limit{Rate: 100, Period: time.Minute, Burst: 20})
//	if err != nil {
//		return err
//	}
//	if !r.Allowed {
//		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(r.RetryAfter.Seconds()))))
//		w.WriteHeader(http.StatusTooManyRequests)
//		return nil
//	}
//
// Each decision returns whether the request is allowed, how many more would
// be allowed right now, and how long to wait before retrying. AllowN spends
// several units at once, all or nothing.
//
// # Strategies
//
// TokenBucket (the default) refills Rate tokens per Period up to Burst, so a
// client can burst after an idle spell and then settles to the rate. It keeps
// one small hash per key. SlidingWindow allows at most Rate requests in any
// Period-long window, exactly, with no bursting past the rate; it keeps one
// sorted-set entry per allowed request in the window, so prefer it for small
// limits such as login attempts. Pick one per key space: the two keep their
// keys apart with a "tb:" or "sw:" tag after the prefix.
//
// # Atomicity and clocks
//
// Each check is a single Lua script on a single key, so concurrent requests
// from any number of app servers cannot overshoot the limit, and the limiter
// works on Redis Cluster (use hash tags only if you need several limits on
// one slot). The scripts read the server clock with TIME, so app servers with
// drifting clocks still agree. This needs Redis 5 or newer (script effects
// replication). A failover to a replica whose clock is behind never mints
// extra tokens; the bucket just waits for the clock to catch up.
//
// Keys expire once they would be full again (token bucket) or once the window
// has passed (sliding window), so idle clients cost nothing.
//
// # Logging
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// a debug line per decision (allowed or denied, remaining, retry-after,
// duration) and error lines for invalid limits and failed commands. The
// default logger discards everything. Keys are logged, so do not build keys
// from secrets.
//
// Errors are plain wrapped errors (this module does not use go-apperr); match
// ErrInvalidLimit with errors.Is.
package ratelimit

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
