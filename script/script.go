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

import (
	"context"
	"crypto/sha1" // #nosec G505 -- Redis names scripts by SHA-1; it is an identifier, not a security control.
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	goredis "github.com/redis/go-redis/v9"
)

// ErrCrossSlot is returned by Run on a Redis Cluster client when the keys do
// not all hash to the same slot. A script can only touch keys on one node, so
// the call is refused before anything is sent. Put the keys under a shared
// hash tag, such as "{user:42}:a" and "{user:42}:b", to keep them together.
var ErrCrossSlot = errors.New("script: keys hash to different cluster slots")

// Script is a Lua script that runs with EVALSHA and transparently falls back
// to EVAL when the server answers NOSCRIPT, which happens the first time a
// server sees the script and again after a failover or SCRIPT FLUSH. EVAL
// caches the script, so the next run goes back to EVALSHA.
//
// Build one with New, once, and reuse it: a Script is immutable and safe for
// concurrent use by multiple goroutines.
type Script struct {
	src  string
	sha  string
	name string
	log  log.Logger
}

// Option configures a Script.
type Option func(*Script)

// WithName labels the script in log lines, for example "lock.release". The
// default is the first 12 characters of the script's SHA-1.
func WithName(name string) Option {
	return func(s *Script) {
		if name != "" {
			s.name = name
		}
	}
}

// WithLogger sends the script's diagnostics to l. Each call derives a logger
// from the call's context with l.Ctx, so trace and span IDs are attached. The
// default discards everything. Keys and the script's name and SHA are logged;
// argument values never are.
func WithLogger(l log.Logger) Option {
	return func(s *Script) {
		if l != nil {
			s.log = l
		}
	}
}

// New returns a Script for the Lua source src. It computes the SHA-1 the
// server will use to name the script, but sends nothing: the script is loaded
// lazily by the first Run, or eagerly by Load.
func New(src string, opts ...Option) *Script {
	sum := sha1.Sum([]byte(src)) // #nosec G401 -- see the import comment.
	sha := hex.EncodeToString(sum[:])
	s := &Script{src: src, sha: sha, name: sha[:12], log: nopLogger{}}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// SHA returns the script's SHA-1 in hex, as the server names it.
func (s *Script) SHA() string { return s.sha }

// Source returns the script's Lua source.
func (s *Script) Source() string { return s.src }

// Name returns the label used in log lines.
func (s *Script) Name() string { return s.name }

// Load sends the script to the server with SCRIPT LOAD so the first Run skips
// the NOSCRIPT round trip. On a cluster client go-redis sends it to every
// master. Load is optional; Run works without it.
func (s *Script) Load(ctx context.Context, c *redis.Client) error {
	l := s.logger(ctx)
	start := time.Now()
	l.Debug("script: loading")
	if err := c.Redis().ScriptLoad(ctx, s.src).Err(); err != nil {
		l.Error(err, "script: load failed", log.F("duration", time.Since(start)))
		return fmt.Errorf("script: load %s: %w", s.name, err)
	}
	l.Debug("script: loaded", log.F("duration", time.Since(start)))
	return nil
}

// Run executes the script against c with the given keys and arguments. It
// tries EVALSHA first and, on NOSCRIPT, retries once with EVAL. Pass every key
// the script touches in keys (never build key names inside the script) so the
// command routes correctly and stays cluster-safe. On a cluster client, keys
// that hash to different slots fail with ErrCrossSlot before anything is sent.
//
// The result is the go-redis command, so the usual typed accessors work:
// Int64, Text, Slice, Result, Err and so on. A script that returns nil reports
// redis.Nil, as a GET on a missing key does. Both commands pass through the
// Client's hooks, so an Observer set with redis.WithObserver sees each one
// as "evalsha" or "eval".
func (s *Script) Run(ctx context.Context, c *redis.Client, keys []string, args ...any) *goredis.Cmd {
	l := s.logger(ctx)
	rdb := c.Redis()

	if _, cluster := rdb.(*goredis.ClusterClient); cluster {
		if err := sameSlot(keys); err != nil {
			l.Error(err, "script: refused cross-slot keys", log.F("keys", keys))
			cmd := goredis.NewCmd(ctx)
			cmd.SetErr(err)
			return cmd
		}
	}

	start := time.Now()
	l.Debug("script: run", log.F("keys", keys), log.F("args", len(args)))

	cmd := rdb.EvalSha(ctx, s.sha, keys, args...)
	if goredis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
		l.Info("script: not cached on the server, falling back to EVAL", log.F("keys", keys))
		cmd = rdb.Eval(ctx, s.src, keys, args...)
	}

	dur := time.Since(start)
	switch err := cmd.Err(); {
	case err == nil:
		l.Debug("script: done", log.F("keys", keys), log.F("duration", dur))
	case errors.Is(err, goredis.Nil):
		l.Debug("script: done with a nil result", log.F("keys", keys), log.F("duration", dur))
	default:
		l.Error(err, "script: run failed", log.F("keys", keys), log.F("duration", dur))
	}
	return cmd
}

// logger returns the configured logger correlated with ctx and labelled with
// the script's name and SHA.
func (s *Script) logger(ctx context.Context) log.Logger {
	return s.log.Ctx(ctx).With(log.F("script", s.name), log.F("sha", s.sha))
}

// sameSlot returns an error wrapping ErrCrossSlot when keys span more than one
// cluster slot.
func sameSlot(keys []string) error {
	if len(keys) < 2 {
		return nil
	}
	first := Slot(keys[0])
	for _, k := range keys[1:] {
		if Slot(k) != first {
			return fmt.Errorf("%w: %q is in slot %d, %q is in slot %d", ErrCrossSlot, keys[0], first, k, Slot(k))
		}
	}
	return nil
}

// nopLogger is the default Logger; it discards everything.
type nopLogger struct{}

func (nopLogger) Debug(string, ...log.Field)        {}
func (nopLogger) Info(string, ...log.Field)         {}
func (nopLogger) Warn(string, ...log.Field)         {}
func (nopLogger) Error(error, string, ...log.Field) {}
func (nopLogger) Fatal(error, string, ...log.Field) {}
func (n nopLogger) With(...log.Field) log.Logger    { return n }
func (n nopLogger) Ctx(context.Context) log.Logger  { return n }
