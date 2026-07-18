# AGENTS.md - go-redis

Guide for AI agents working in this repository. Pair with `CLAUDE.md` (the working agreement and
hook-enforced rules). Keep this file current when the build, layout, or public API changes.

## What this is

A small, dependency-light Redis client library for Go. It wraps
`github.com/redis/go-redis/v9` so a service gets resilient wiring -- a sensible
pool, bounded timeouts, a few retries with backoff, sentinel failover, and a
readiness Ping -- from a single `Connect` call, instead of re-deriving that
configuration in every codebase.

## Using go-redis

The public surface is small and additive; keep it stable:

- `Connect(ctx, ...Option) (*Client, error)` -- builds the underlying go-redis client (standalone,
  sentinel, or cluster), applies resilient defaults, and Pings to verify readiness before returning.
- `(*Client).Redis() redis.UniversalClient` -- the underlying go-redis client for the full command
  API. `(*Client).Healthy(ctx) bool` -- a cheap Ping for probes. `(*Client).Close() error`.
- Options: `WithAddr`, `WithSentinel(master, addrs...)` (the HA path), `WithCluster`, `WithPassword`,
  `WithDB`, `WithTLS`, `WithPool`, `WithTimeouts(dial, read, write)`, `WithRetry(attempts, min, max)`,
  `WithLogger`, `WithObserver`. Non-positive tuning values keep the default; the last mode option wins.
- `Logger` and `Observer` are minimal, default to no-ops, and impose no logging/telemetry dependency.
- The `otel` subpackage adds OpenTelemetry tracing and metrics via `Instrument`, keeping the
  OpenTelemetry dependency out of the core.

## Layout

- `redis.go` - `Client`, `Connect`, the `Redis`/`Healthy`/`Close` methods, and the go-redis hook that
  drives `Logger`/`Observer`.
- `options.go` - the `Option` type, all `With*` options, the resilient defaults, and the mapping onto
  go-redis's standalone/failover/cluster option structs.
- `otel/` - the optional OpenTelemetry adapter (`Instrument`), a separate import path.
- `doc.go` - package doc.
- `*_test.go` - unit tests using `alicebob/miniredis` (no live Redis); `integration_test.go` is behind
  `//go:build integration` and reads `REDIS_ADDR`.

## Build, test, lint

- Build: `task build`
- Test: `task test` (no external service; tests use miniredis) / `task test-integration` (needs a server)
- Full gate: `task ci` (build + vet + race tests + gofmt + golangci-lint + yamllint)
- License headers: `task license` (verify) / `task license:fix` (inject)

## Conventions and gotchas

- See `CLAUDE.md` for the branch/commit/PR rules; they are enforced by the git hooks in
  `.claude/hooks` (run `bash .claude/hooks/install.sh` once per clone).
- Keep the `Connect`/`Client` surface stable; add capabilities additively.
- The core must stay free of any logging or telemetry dependency -- new observability goes through
  the `Logger`/`Observer` seams or the `otel` subpackage.
- Cluster mode does not carry `WithDB` (Redis Cluster supports only database 0); keep that mapping in
  `clusterOptions`.
