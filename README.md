# go-redis 🧰

> 🧷 Resilient Redis wiring for Go in one call — sensible pool, timeouts, and retries by default, with sentinel failover, cluster, and a health check built in, plus an optional toolkit for the patterns every service rebuilds.

`go-redis` wraps [`redis/go-redis/v9`](https://github.com/redis/go-redis) so a service stops re-deriving the same pool sizes, timeouts, and retry policy in every codebase. `Connect` returns a client that has already Pinged the server, so a successful call means you are ready to serve.

## ✨ Highlights

- 🛟 **Resilient by default** — pool, timeouts, retries with backoff, sentinel failover and cluster from one `Connect`.
- 🧰 **A toolkit on top** — Lua scripts, caching, streams, pub/sub, locks, idempotency keys and rate limiting, each in its own package.
- 🪶 **Pay for what you import** — the core has no logging or telemetry dependency; the toolkit and `otel` live in subpackages.
- 🔇 **Quiet unless asked** — every tool takes a go-log logger for generous diagnostics and discards everything by default.
- 🧪 **Tested against real Redis** — unit tests on miniredis, integration tests on `redis:7` in CI.

## 📦 Install

```bash
go get github.com/Bugs5382/go-redis
```

## 🚀 Usage

`Connect` applies resilient defaults, dials, and verifies readiness with a Ping.
`Redis()` hands you the full go-redis command API.

```go
client, err := redis.Connect(ctx, redis.WithAddr("localhost:6379"))
if err != nil {
	log.Fatal(err)
}
defer client.Close()

rdb := client.Redis()
rdb.Set(ctx, "session:123", "alice", time.Minute)
name, _ := rdb.Get(ctx, "session:123").Result()
```

## 🧩 Depend on go-redis only

`Nil`, `Cmdable`, and `UniversalClient` re-export the handful of go-redis
names a caller needs in order to *name* the value `Redis()` returns, so a
wrapper library can depend on `go-redis` alone and never import
`github.com/redis/go-redis/v9` directly:

```go
var cmd redis.Cmdable = client.Redis() // or redis.UniversalClient

_, err := cmd.Get(ctx, "session:missing").Result()
if errors.Is(err, redis.Nil) {
	// cache miss
}
```

## 🛟 High availability

Point at Redis Sentinel for automatic failover — the client discovers the
current master through the sentinels and re-resolves it after a switchover.
Redis Cluster is one option away.

```go
// Sentinel-managed failover master.
client, _ := redis.Connect(ctx,
	redis.WithSentinel("mymaster", "localhost:26379", "localhost:26380"),
)

// Redis Cluster.
client, _ := redis.Connect(ctx,
	redis.WithCluster("localhost:7000", "localhost:7001", "localhost:7002"),
)
```

## 🎛 Options

Every knob is a functional option, and every one has a resilient default.

```go
client, _ := redis.Connect(ctx,
	redis.WithAddr("localhost:6379"),
	redis.WithPassword("s3cret"),
	redis.WithDB(0),
	redis.WithTLS(&tls.Config{}),
	redis.WithPool(50),
	redis.WithTimeouts(5*time.Second, 3*time.Second, 3*time.Second),
	redis.WithRetry(3, 8*time.Millisecond, 512*time.Millisecond),
)
```

## 💓 Health

`Healthy` is a cheap Ping — wire it straight into a readiness or liveness probe.

```go
if !client.Healthy(ctx) {
	// fail the probe
}
```

## 🔌 Logging & metrics

A minimal `Logger` (dial diagnostics) and `Observer` (per-command and per-dial
signals) are pluggable and default to no-ops, so the core drags in no logging or
telemetry dependency.

```go
client, _ := redis.Connect(ctx,
	redis.WithAddr("localhost:6379"),
	redis.WithObserver(myObserver),
)
```

## 📊 OpenTelemetry

The optional [`otel`](./otel) subpackage turns on tracing and metrics in one
call against the global providers, keeping the dependency out of the core.

```go
import redisotel "github.com/Bugs5382/go-redis/otel"

client, _ := redis.Connect(ctx, redis.WithAddr("localhost:6379"))
redisotel.Instrument(client) // spans + metrics per command
```

## 🧰 Toolkit

Each tool is optional, builds only on `*redis.Client`, and lives in its own
package, so importing one pulls in nothing else. They all take a
`context.Context`, log through a [go-log](https://github.com/Bugs5382/go-log)
`Logger` passed with `WithLogger` (keys and IDs only, never values; silent by
default), and return plain wrapped errors you match with `errors.Is`.

| Package | For |
| --- | --- |
| [`script`](./script) | Lua scripts with EVALSHA and a transparent EVAL fallback |
| [`cache`](./cache) | Typed get-or-set with codecs, jitter, negative caching and stampede protection |
| [`streams`](./streams) | A resilient consumer-group consumer with retries and dead letters |
| [`pubsub`](./pubsub) | A subscriber that re-subscribes after reconnects |
| [`lock`](./lock) | A lock with token-checked release and auto-extend |
| [`idempotency`](./idempotency) | Run a function once per key and replay its result |
| [`ratelimit`](./ratelimit) | Token-bucket and sliding-window rate limiting |

## 📜 Lua scripts

`script.New` hashes the source once; `Run` sends EVALSHA and, when the server
answers NOSCRIPT (first use, after a failover or `SCRIPT FLUSH`), falls back to
EVAL, which caches it again. The lock, idempotency and rate-limit tools are
built on it.

```go
import "github.com/Bugs5382/go-redis/script"

incr := script.New(`return redis.call('INCRBY', KEYS[1], ARGV[1])`)
n, err := incr.Run(ctx, client, []string{"counter"}, 5).Int64()
```

Pass every key the script touches in `keys`. On a cluster client, keys that
hash to different slots fail with `ErrCrossSlot` before anything is sent, so
use a hash tag such as `{order:7}:items` to keep related keys together.

## 🗄️ Cache

A typed `Cache[T]` with `Get`, `Set`, `GetOrSet` and `Delete`. A miss is
`ErrMiss`, never confused with a failure.

```go
import "github.com/Bugs5382/go-redis/cache"

users := cache.New[User](client,
	cache.WithPrefix("users:"),
	cache.WithJitter(0.1),              // spread expiries
	cache.WithNegativeTTL(time.Minute), // remember "not found"
)
u, err := users.GetOrSet(ctx, id, 5*time.Minute, func(ctx context.Context) (User, error) {
	return db.LoadUser(ctx, id) // return cache.ErrNotFound when there is no such user
})
```

Concurrent `GetOrSet` calls for one key share a single load, within one
process. If Redis is down, `GetOrSet` logs a warning and serves from the
loader. Values are JSON unless you pass another `Codec`.

## 🌊 Streams

`streams.Consume` runs one consumer in a consumer group: it creates the group,
reads with XREADGROUP, acks what the handler accepts, reclaims entries stuck
with dead consumers (XAUTOCLAIM), and moves messages that keep failing to a
dead-letter stream.

```go
import "github.com/Bugs5382/go-redis/streams"

_, _ = streams.Publish(ctx, client, "orders", map[string]any{"order_id": "A-1"}, streams.WithMaxLen(100000))

err := streams.Consume(ctx, client, streams.Config{
	Stream:        "orders",
	Group:         "billing",
	Consumer:      podName,
	MaxDeliveries: 5, // then to "orders:dead"
}, func(ctx context.Context, msg streams.Message) error {
	return bill(ctx, msg.Values["order_id"]) // an error retries after ClaimMinIdle
})
```

Delivery is **at least once**: a message is handled again if its ack is lost,
so handlers must be idempotent (the idempotency tool below pairs well).
Redis errors back off and retry, and cancelling the context drains in-flight
handlers before `Consume` returns.

## 📣 Pub/sub

`pubsub.Subscribe` returns once every channel and pattern is confirmed, then
re-subscribes after a dropped connection or failover. `Healthy` backs a
readiness probe.

```go
import "github.com/Bugs5382/go-redis/pubsub"

sub, err := pubsub.Subscribe(ctx, client, pubsub.Topics{
	Channels: []string{"config.reload"},
	Patterns: []string{"orders.*"},
}, func(ctx context.Context, msg pubsub.Message) {
	route(ctx, msg.Channel, msg.Payload)
})
if err != nil {
	return err
}
defer sub.Close()
```

Redis pub/sub is **at most once**: messages published while the subscriber is
disconnected are lost. Use streams when every message must arrive. The
handler runs one message at a time, so hand slow work to your own goroutines.

## 🔒 Lock

`lock.Acquire` stores a random token with `SET NX PX`; `Release` and `Extend`
only act while the key still holds that token, so an expired holder can never
free the next holder's lock.

```go
import "github.com/Bugs5382/go-redis/lock"

l, err := lock.Acquire(ctx, client, "jobs:nightly-report", 30*time.Second,
	lock.WithWait(5*time.Second),        // retry with backoff instead of failing fast
	lock.WithAutoExtend(10*time.Second), // keep it alive while the job runs
)
if errors.Is(err, lock.ErrNotAcquired) {
	return nil // another worker has it
}
defer l.Release(ctx)
// stop work if <-l.Lost() fires
```

This is a **single-instance lock, not Redlock**. Redis replicates
asynchronously, so a failover can hand the same lock to a second client, and a
long pause can outlive the TTL. Use it freely to avoid duplicate work; when
overlapping holders would corrupt data, add a fencing token or make the
downstream idempotent.

## 🔁 Idempotency

`Store[T].Do` runs a function at most once per key within a TTL and replays
the stored result to duplicates, which is what a retried request or a
redelivered message needs.

```go
import "github.com/Bugs5382/go-redis/idempotency"

payments := idempotency.New[Receipt](client, idempotency.WithLease(30*time.Second))
r, err := payments.Do(ctx, requestID, 24*time.Hour, func(ctx context.Context) (Receipt, error) {
	return charge(ctx, order)
})
// a duplicate while charge runs gets idempotency.ErrInProgress (or waits, with WithWait)
```

On failure the key is released for a retry by default; `KeepOnError` stores
the failure instead. The lease frees keys left by a caller that crashed mid-run,
so keep it above the function's longest run. Like the lock, the guarantee is
as strong as the Redis primary holding the key.

## 🚦 Rate limiting

`ratelimit` decides each request with one atomic Lua script per key, timed by
the Redis server clock, so every app server agrees. It works on cluster.

```go
import "github.com/Bugs5382/go-redis/ratelimit"

limiter := ratelimit.New(client) // token bucket; ratelimit.WithStrategy(ratelimit.SlidingWindow) for an exact window
r, err := limiter.Allow(ctx, "api:"+userID, ratelimit.Limit{Rate: 100, Period: time.Minute, Burst: 20})
if err != nil {
	return err
}
if !r.Allowed {
	return tooManyRequests(r.RetryAfter)
}
```

The token bucket allows bursts up to `Burst` and refills at `Rate` per
`Period`; the sliding window never admits more than `Rate` in any `Period`.
It needs Redis 5 or newer (the scripts call `TIME`).

## 🛠 Develop

```bash
task build    # go build ./...
task test     # go test ./...
task ci       # build + vet + race tests + linters
task license  # verify MIT headers (golic)
```

Integration tests run against a live server and are excluded from the default build:

```bash
REDIS_ADDR=localhost:6379 task test-integration
```

## ⚖️ License

MIT © 2026 Shane
