package pubsub

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
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	goredis "github.com/redis/go-redis/v9"
)

// ErrNoTopics is returned by Subscribe when Topics names no channel and no
// pattern.
var ErrNoTopics = errors.New("pubsub: no channels or patterns to subscribe to")

// Defaults for the options.
const (
	DefaultPingInterval = 15 * time.Second
	DefaultBackoffMin   = 100 * time.Millisecond
	DefaultBackoffMax   = 5 * time.Second
)

// Topics lists what to subscribe to: exact channel names (SUBSCRIBE) and glob
// patterns such as "orders.*" (PSUBSCRIBE). At least one is required.
type Topics struct {
	Channels []string
	Patterns []string
}

// Message is one published message.
type Message struct {
	// Channel is the channel the message was published to.
	Channel string
	// Pattern is the pattern that matched, for a pattern subscription; empty
	// for a channel subscription.
	Pattern string
	// Payload is the message body.
	Payload string
}

// Handler receives each message, one at a time, on the subscriber's
// goroutine. A slow handler delays the messages behind it, and a server that
// sees a subscriber fall too far behind drops the connection; hand long work
// off to your own goroutines. A panic is recovered and logged.
type Handler func(ctx context.Context, msg Message)

// options holds what an Option sets.
type options struct {
	log          log.Logger
	ping         time.Duration
	backoffMin   time.Duration
	backoffMax   time.Duration
	expectedAcks int
}

// Option configures Subscribe.
type Option func(*options)

// WithLogger sends diagnostics to l. Each call derives a logger from the
// subscription's context with l.Ctx, so trace and span IDs are attached. The
// default discards everything. Channel and pattern names are logged;
// payloads never are.
func WithLogger(l log.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}

// WithPingInterval sets how long the subscription may sit idle before a PING
// checks the connection. A dead connection is found within about this long.
// Default DefaultPingInterval; non-positive values keep it.
func WithPingInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.ping = d
		}
	}
}

// WithBackoff bounds the exponential delay between reconnect attempts.
// Defaults DefaultBackoffMin and DefaultBackoffMax; non-positive values keep
// them.
func WithBackoff(minDelay, maxDelay time.Duration) Option {
	return func(o *options) {
		if minDelay > 0 {
			o.backoffMin = minDelay
		}
		if maxDelay > 0 {
			o.backoffMax = maxDelay
		}
	}
}

// Subscriber is a live subscription. It re-subscribes to all of its channels
// and patterns after a reconnect or failover. It is safe for concurrent use.
type Subscriber struct {
	ps      *goredis.PubSub
	topics  Topics
	h       Handler
	opts    options
	cancel  context.CancelFunc
	done    chan struct{}
	ready   chan struct{}
	healthy atomic.Bool
	close   sync.Once
}

// Subscribe subscribes c to topics and delivers each message to h until ctx
// is cancelled or Close is called. It returns once the server has confirmed
// every channel and pattern, or with ctx's error if that does not happen
// before ctx ends.
//
// After a dropped connection or a failover the subscriber reconnects with
// exponential backoff and subscribes to everything again; Healthy reports
// false in between. Messages published while it is disconnected are lost:
// Redis pub/sub delivers at most once. Use the streams package when every
// message must arrive.
func Subscribe(ctx context.Context, c *redis.Client, topics Topics, h Handler, opts ...Option) (*Subscriber, error) {
	if len(topics.Channels) == 0 && len(topics.Patterns) == 0 {
		return nil, ErrNoTopics
	}
	if h == nil {
		return nil, errors.New("pubsub: handler is nil")
	}
	o := options{
		log:        nopLogger{},
		ping:       DefaultPingInterval,
		backoffMin: DefaultBackoffMin,
		backoffMax: DefaultBackoffMax,
	}
	for _, opt := range opts {
		opt(&o)
	}
	o.backoffMax = max(o.backoffMax, o.backoffMin)
	// The server confirms each name once per SUBSCRIBE, and go-redis replays a
	// de-duplicated set after a reconnect, so count unique names.
	topics = Topics{Channels: unique(topics.Channels), Patterns: unique(topics.Patterns)}
	o.expectedAcks = len(topics.Channels) + len(topics.Patterns)

	life, cancel := context.WithCancel(ctx)
	l := o.log.Ctx(life).With(log.F("channels", topics.Channels), log.F("patterns", topics.Patterns))
	l.Info("pubsub: subscribing")

	rdb := c.Redis()
	ps := rdb.Subscribe(life)
	s := &Subscriber{
		ps: ps, topics: topics, h: h, opts: o, cancel: cancel,
		done: make(chan struct{}), ready: make(chan struct{}),
	}
	// A blocked receive ignores ctx, so closing the PubSub is what wakes it.
	context.AfterFunc(life, func() { _ = ps.Close() })

	if err := s.send(life); err != nil {
		// The loop reconnects and re-sends SUBSCRIBE, so only log it.
		l.Warn("pubsub: initial subscribe failed, retrying", log.F("error", err.Error()))
	}
	go s.loop(life, l)

	select {
	case <-s.ready:
		return s, nil
	case <-ctx.Done():
		_ = s.Close()
		l.Warn("pubsub: gave up before the subscription was confirmed", log.F("error", ctx.Err().Error()))
		return nil, fmt.Errorf("pubsub: subscribe: %w", ctx.Err())
	}
}

// send issues SUBSCRIBE and PSUBSCRIBE. go-redis remembers them and sends
// them again on every new connection.
func (s *Subscriber) send(ctx context.Context) error {
	var errs []error
	if len(s.topics.Channels) > 0 {
		errs = append(errs, s.ps.Subscribe(ctx, s.topics.Channels...))
	}
	if len(s.topics.Patterns) > 0 {
		errs = append(errs, s.ps.PSubscribe(ctx, s.topics.Patterns...))
	}
	return errors.Join(errs...)
}

// Healthy reports whether the subscription is live: the server has confirmed
// every channel and pattern since the last connection error. Use it in a
// readiness probe.
func (s *Subscriber) Healthy() bool { return s.healthy.Load() }

// Done is closed once the subscriber has stopped, after Close or when its
// context ends.
func (s *Subscriber) Done() <-chan struct{} { return s.done }

// Close unsubscribes, closes the connection and waits for the handler to
// return. It is safe to call more than once.
func (s *Subscriber) Close() error {
	s.close.Do(func() {
		s.cancel()
		_ = s.ps.Close()
	})
	<-s.done
	return nil
}

// loop receives until the subscription ends.
func (s *Subscriber) loop(ctx context.Context, l log.Logger) {
	defer close(s.done)
	defer s.healthy.Store(false)
	defer l.Info("pubsub: subscriber stopped")

	b := backoff{min: s.opts.backoffMin, max: s.opts.backoffMax}
	acks := 0
	everReady := false
	for {
		raw, err := s.ps.ReceiveTimeout(ctx, s.opts.ping)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if isTimeout(err) {
				l.Debug("pubsub: idle, sending ping")
				if perr := s.ps.Ping(ctx); perr != nil {
					s.lost(l, &acks, perr)
				}
				continue
			}
			s.lost(l, &acks, err)
			wait := b.next()
			l.Warn("pubsub: receive failed, reconnecting",
				log.F("error", err.Error()), log.F("attempt", b.attempt), log.F("backoff", wait))
			if !sleep(ctx, wait) {
				return
			}
			continue
		}

		switch m := raw.(type) {
		case *goredis.Subscription:
			if m.Kind != "subscribe" && m.Kind != "psubscribe" {
				l.Debug("pubsub: subscription change", log.F("kind", m.Kind), log.F("topic", m.Channel))
				continue
			}
			acks++
			l.Debug("pubsub: subscription confirmed", log.F("kind", m.Kind), log.F("topic", m.Channel), log.F("count", m.Count))
			if acks == s.opts.expectedAcks && !s.healthy.Load() {
				s.healthy.Store(true)
				if everReady {
					l.Info("pubsub: subscribed again after reconnecting", log.F("attempts", b.attempt))
				} else {
					l.Info("pubsub: subscribed")
					everReady = true
					close(s.ready)
				}
				b.reset()
			}
		case *goredis.Message:
			s.deliver(ctx, l, Message{Channel: m.Channel, Pattern: m.Pattern, Payload: m.Payload})
		case *goredis.Pong:
			l.Debug("pubsub: pong")
		default:
			l.Debug("pubsub: ignoring an unexpected reply", log.F("type", fmt.Sprintf("%T", raw)))
		}
	}
}

// lost marks the subscription unhealthy after a connection error. go-redis
// reconnects on the next receive and replays SUBSCRIBE and PSUBSCRIBE, whose
// confirmations turn it healthy again.
func (s *Subscriber) lost(l log.Logger, acks *int, err error) {
	*acks = 0
	if s.healthy.Swap(false) {
		l.Warn("pubsub: subscription lost", log.F("error", err.Error()))
	}
}

// deliver runs the handler for one message, recovering a panic.
func (s *Subscriber) deliver(ctx context.Context, l log.Logger, msg Message) {
	ml := l.With(log.F("channel", msg.Channel), log.F("pattern", msg.Pattern))
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			ml.Error(fmt.Errorf("pubsub: handler panicked: %v", r), "pubsub: handler panicked")
			return
		}
		ml.Debug("pubsub: handled", log.F("bytes", len(msg.Payload)), log.F("duration", time.Since(start)))
	}()
	s.h(ctx, msg)
}

// unique returns names without duplicates, keeping the first occurrence.
func unique(names []string) []string {
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, n := range names {
		if _, ok := seen[n]; !ok {
			seen[n] = struct{}{}
			out = append(out, n)
		}
	}
	return out
}

// isTimeout reports whether err is a read timeout, meaning the connection was
// simply idle.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// backoff is a doubling delay between min and max.
type backoff struct {
	min, max time.Duration
	attempt  int
}

func (b *backoff) next() time.Duration {
	d := b.min << min(b.attempt, 30)
	if d <= 0 || d > b.max {
		d = b.max
	}
	b.attempt++
	return d
}

func (b *backoff) reset() { b.attempt = 0 }

// sleep waits for d or until ctx ends, reporting whether the full wait passed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
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
