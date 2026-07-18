# go-redis 🧰

> Resilient Redis wiring for Go in one call — sensible pool, timeouts, and retries by default, with sentinel failover, cluster, and a health check built in.

`go-redis` wraps [`redis/go-redis/v9`](https://github.com/redis/go-redis) so a service stops re-deriving the same pool sizes, timeouts, and retry policy in every codebase. `Connect` returns a client that has already Pinged the server, so a successful call means you are ready to serve.

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
