package streams

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
	"maps"
	"strconv"
	"sync"
	"time"

	log "github.com/Bugs5382/go-log"
	redis "github.com/Bugs5382/go-redis"
	goredis "github.com/redis/go-redis/v9"
)

// ErrInvalidConfig is returned by Consume when the Config or handler cannot
// work: a missing stream, group or consumer name, a dead-letter stream equal
// to the source stream, or a nil handler.
var ErrInvalidConfig = errors.New("streams: invalid config")

// ErrDeadLetter tells Consume that a message can never succeed. A handler
// returns it (or an error wrapping it) to move the message to the dead-letter
// stream at once, without waiting for MaxDeliveries.
var ErrDeadLetter = errors.New("streams: dead-letter this message")

// Fields added to each dead-lettered entry, next to the original fields.
const (
	FieldSourceStream = "dlq_source_stream" // the stream the message came from
	FieldSourceID     = "dlq_source_id"     // its ID in that stream
	FieldGroup        = "dlq_group"         // the consumer group that gave up on it
	FieldDeliveries   = "dlq_deliveries"    // how many times it was delivered
	FieldError        = "dlq_error"         // the last handler error, or why it was moved
)

// Defaults applied by Consume to zero Config fields.
const (
	DefaultBatch         = 10
	DefaultBlock         = 2 * time.Second
	DefaultClaimInterval = 30 * time.Second
	DefaultClaimMinIdle  = time.Minute
	DefaultMaxDeliveries = 5
	DefaultRetryMin      = 100 * time.Millisecond
	DefaultRetryMax      = 5 * time.Second
)

// Message is one stream entry handed to a Handler.
type Message struct {
	// ID is the entry ID, such as "1700000000000-0".
	ID string
	// Stream is the stream the entry was read from.
	Stream string
	// Values holds the entry's fields. Values read back from Redis are strings.
	Values map[string]any
	// Deliveries counts how many times the group has delivered this entry,
	// this delivery included: 1 the first time, more after a retry or a claim
	// from another consumer.
	Deliveries int64
}

// Handler processes one message. Return nil to acknowledge it. Return an error
// to leave it pending: it is retried once it has been idle for ClaimMinIdle,
// and moved to the dead-letter stream when it reaches MaxDeliveries. Return
// ErrDeadLetter (or wrap it) to dead-letter it straight away. A panic is
// recovered and treated as an error.
//
// The context passed to a handler is not cancelled when Consume shuts down, so
// in-flight work can finish; bound long work with your own timeout.
type Handler func(ctx context.Context, msg Message) error

// Config describes one consumer in a consumer group.
type Config struct {
	// Stream is the stream to read. Required.
	Stream string
	// Group is the consumer group name. It is created (with the stream, if
	// needed) when missing. Required.
	Group string
	// Consumer names this consumer within the group. Give every running
	// process its own name, such as the pod name. Required.
	Consumer string
	// StartID is where a newly created group starts reading: "$" (the
	// default) for entries added from now on, or "0" for the whole stream. It
	// has no effect on a group that already exists.
	StartID string

	// Batch is the most entries read per XREADGROUP or XAUTOCLAIM call.
	// Default DefaultBatch.
	Batch int64
	// Block is how long one XREADGROUP waits for new entries. It also bounds
	// how quickly Consume notices a cancelled context. Default DefaultBlock.
	Block time.Duration
	// Concurrency is how many handlers run at once. Default 1.
	Concurrency int

	// ClaimInterval is how often pending entries are checked for reclaiming.
	// Default DefaultClaimInterval.
	ClaimInterval time.Duration
	// ClaimMinIdle is how long an entry must sit unacknowledged before it is
	// reclaimed with XAUTOCLAIM, whether its consumer died or its handler
	// failed. It is the retry delay. Default DefaultClaimMinIdle.
	ClaimMinIdle time.Duration

	// MaxDeliveries is how many deliveries a message gets before it is moved
	// to the dead-letter stream. Default DefaultMaxDeliveries; a negative
	// value never dead-letters on count.
	MaxDeliveries int64
	// DeadLetterStream receives messages that ran out of deliveries or were
	// rejected with ErrDeadLetter. Default Stream + ":dead".
	DeadLetterStream string

	// RetryMin and RetryMax bound the exponential backoff after a Redis
	// error, such as during a failover. Defaults DefaultRetryMin and
	// DefaultRetryMax.
	RetryMin, RetryMax time.Duration
}

// withDefaults validates cfg and fills in the defaults.
func (cfg Config) withDefaults() (Config, error) {
	switch {
	case cfg.Stream == "":
		return cfg, fmt.Errorf("%w: Stream is required", ErrInvalidConfig)
	case cfg.Group == "":
		return cfg, fmt.Errorf("%w: Group is required", ErrInvalidConfig)
	case cfg.Consumer == "":
		return cfg, fmt.Errorf("%w: Consumer is required", ErrInvalidConfig)
	}
	if cfg.StartID == "" {
		cfg.StartID = "$"
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultBatch
	}
	if cfg.Block <= 0 {
		cfg.Block = DefaultBlock
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.ClaimInterval <= 0 {
		cfg.ClaimInterval = DefaultClaimInterval
	}
	if cfg.ClaimMinIdle <= 0 {
		cfg.ClaimMinIdle = DefaultClaimMinIdle
	}
	if cfg.MaxDeliveries == 0 {
		cfg.MaxDeliveries = DefaultMaxDeliveries
	}
	if cfg.DeadLetterStream == "" {
		cfg.DeadLetterStream = cfg.Stream + ":dead"
	}
	if cfg.DeadLetterStream == cfg.Stream {
		return cfg, fmt.Errorf("%w: DeadLetterStream must differ from Stream", ErrInvalidConfig)
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = DefaultRetryMin
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = DefaultRetryMax
	}
	cfg.RetryMax = max(cfg.RetryMax, cfg.RetryMin)
	return cfg, nil
}

// options holds what an Option sets.
type options struct {
	log    log.Logger
	maxLen int64
}

// Option configures Consume and Publish.
type Option func(*options)

// WithLogger sends diagnostics to l. Each call derives a logger from the
// call's context with l.Ctx, so trace and span IDs are attached. The default
// discards everything. Stream, group and consumer names and entry IDs are
// logged; field values never are.
func WithLogger(l log.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.log = l
		}
	}
}

// WithMaxLen caps a stream at about n entries with XADD MAXLEN ~ n, which lets
// Redis trim in whole nodes for speed, so the length can briefly exceed n.
// Publish applies it to the stream it writes; Consume applies it to the
// dead-letter stream. A non-positive n means no cap, the default.
func WithMaxLen(n int64) Option {
	return func(o *options) {
		if n > 0 {
			o.maxLen = n
		}
	}
}

func resolve(opts []Option) options {
	o := options{log: nopLogger{}}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Publish appends values to stream with XADD and returns the new entry's ID.
func Publish(ctx context.Context, c *redis.Client, stream string, values map[string]any, opts ...Option) (string, error) {
	o := resolve(opts)
	l := o.log.Ctx(ctx).With(log.F("stream", stream))
	if len(values) == 0 {
		err := errors.New("streams: publish needs at least one field")
		l.Error(err, "streams: publish refused")
		return "", err
	}

	args := &goredis.XAddArgs{Stream: stream, Values: values}
	if o.maxLen > 0 {
		args.MaxLen, args.Approx = o.maxLen, true
	}
	start := time.Now()
	id, err := c.Redis().XAdd(ctx, args).Result()
	if err != nil {
		l.Error(err, "streams: publish failed", log.F("duration", time.Since(start)))
		return "", fmt.Errorf("streams: publish to %q: %w", stream, err)
	}
	l.Debug("streams: published", log.F("id", id), log.F("fields", len(values)), log.F("duration", time.Since(start)))
	return id, nil
}

// Consume reads cfg.Stream as cfg.Consumer in cfg.Group and passes each entry
// to h until ctx is cancelled.
//
// It creates the group (and the stream) if missing, reads new entries with
// XREADGROUP, acknowledges each one the handler accepts with XACK, and every
// ClaimInterval takes over entries that have been pending for ClaimMinIdle
// with XAUTOCLAIM, whether their consumer died or their handler failed. A
// message that reaches MaxDeliveries, or whose handler returns ErrDeadLetter,
// is copied to the dead-letter stream with the Field* metadata and
// acknowledged.
//
// Redis errors, such as a dropped connection or a failover, are logged and
// retried with exponential backoff between RetryMin and RetryMax. If the
// group disappears (the stream was deleted, or a failover promoted a replica
// that never had it) it is created again at StartID.
//
// On cancel, Consume stops reading, lets in-flight handlers finish, and
// returns nil. Entries it read but had not started are left pending and are
// reclaimed later. It returns an error only for an invalid Config or a
// permanent failure such as the key holding another type.
//
// Delivery is at least once: a message can be handled again if its ack is
// lost, so handlers should be idempotent.
func Consume(ctx context.Context, c *redis.Client, cfg Config, h Handler, opts ...Option) error {
	if h == nil {
		return fmt.Errorf("%w: handler is nil", ErrInvalidConfig)
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return err
	}
	k := &consumer{rdb: c.Redis(), cfg: cfg, h: h, opts: resolve(opts), claimFrom: "0-0"}
	return k.run(ctx)
}

// consumer is the running state of one Consume call.
type consumer struct {
	rdb       redis.UniversalClient
	cfg       Config
	h         Handler
	opts      options
	claimFrom string
}

func (k *consumer) run(ctx context.Context) error {
	l := k.opts.log.Ctx(ctx).With(
		log.F("stream", k.cfg.Stream), log.F("group", k.cfg.Group), log.F("consumer", k.cfg.Consumer))
	l.Info("streams: consumer starting",
		log.F("concurrency", k.cfg.Concurrency), log.F("batch", k.cfg.Batch),
		log.F("claim_min_idle", k.cfg.ClaimMinIdle), log.F("max_deliveries", k.cfg.MaxDeliveries),
		log.F("dead_letter_stream", k.cfg.DeadLetterStream))

	if err := k.ensureGroup(ctx, l); err != nil {
		l.Error(err, "streams: consumer cannot start")
		return err
	}

	// Handlers get a context that survives shutdown so in-flight work drains.
	hctx := context.WithoutCancel(ctx)
	work := make(chan Message)
	var wg sync.WaitGroup
	for range k.cfg.Concurrency {
		wg.Go(func() {
			for msg := range work {
				k.handle(hctx, l, msg)
			}
		})
	}

	err := k.readLoop(ctx, l, work)
	close(work)
	l.Debug("streams: waiting for in-flight handlers")
	wg.Wait()
	if err != nil {
		l.Error(err, "streams: consumer stopped on a permanent error")
		return err
	}
	l.Info("streams: consumer stopped")
	return nil
}

// ensureGroup creates the group, retrying transient errors until ctx ends. It
// returns nil when the group exists or ctx is cancelled, and an error only for
// a permanent failure.
func (k *consumer) ensureGroup(ctx context.Context, l log.Logger) error {
	b := k.backoff()
	for {
		err := k.rdb.XGroupCreateMkStream(ctx, k.cfg.Stream, k.cfg.Group, k.cfg.StartID).Err()
		switch {
		case err == nil:
			l.Info("streams: created consumer group", log.F("start_id", k.cfg.StartID))
			return nil
		case goredis.HasErrorPrefix(err, "BUSYGROUP"):
			l.Debug("streams: consumer group already exists")
			return nil
		case goredis.HasErrorPrefix(err, "WRONGTYPE"):
			return fmt.Errorf("streams: create group %q on %q: %w", k.cfg.Group, k.cfg.Stream, err)
		case ctx.Err() != nil:
			return nil
		}
		wait := b.next()
		l.Warn("streams: could not create consumer group, retrying",
			log.F("error", err.Error()), log.F("attempt", b.attempt), log.F("backoff", wait))
		if !sleep(ctx, wait) {
			return nil
		}
	}
}

// readLoop feeds work until ctx is cancelled or a permanent error occurs.
func (k *consumer) readLoop(ctx context.Context, l log.Logger, work chan<- Message) error {
	b := k.backoff()
	nextClaim := time.Now()
	for ctx.Err() == nil {
		var (
			batch []Message
			err   error
			what  string
		)
		if !time.Now().Before(nextClaim) {
			what = "claim"
			batch, err = k.claim(ctx, l)
			nextClaim = time.Now().Add(k.cfg.ClaimInterval)
		}
		if err == nil && len(batch) == 0 {
			what = "read"
			batch, err = k.read(ctx)
		}

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if goredis.HasErrorPrefix(err, "NOGROUP") {
				l.Warn("streams: consumer group is gone, creating it again", log.F("start_id", k.cfg.StartID))
				if gerr := k.ensureGroup(ctx, l); gerr != nil {
					return gerr
				}
				continue
			}
			wait := b.next()
			l.Warn("streams: "+what+" failed, retrying",
				log.F("error", err.Error()), log.F("attempt", b.attempt), log.F("backoff", wait))
			if !sleep(ctx, wait) {
				return nil
			}
			continue
		}
		if b.attempt > 0 {
			l.Info("streams: reading again after failures", log.F("failures", b.attempt))
			b.reset()
		}

		for _, msg := range batch {
			select {
			case work <- msg:
			case <-ctx.Done():
				l.Debug("streams: stopping with unstarted messages left pending", log.F("id", msg.ID))
				return nil
			}
		}
	}
	return nil
}

// read fetches new entries for this consumer.
func (k *consumer) read(ctx context.Context) ([]Message, error) {
	res, err := k.rdb.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    k.cfg.Group,
		Consumer: k.cfg.Consumer,
		Streams:  []string{k.cfg.Stream, ">"},
		Count:    k.cfg.Batch,
		Block:    k.cfg.Block,
	}).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Message
	for _, s := range res {
		for _, m := range s.Messages {
			out = append(out, Message{ID: m.ID, Stream: s.Stream, Values: m.Values, Deliveries: 1})
		}
	}
	return out, nil
}

// claim takes over entries idle for ClaimMinIdle, looks up their delivery
// counts, and dead-letters those already over the limit.
func (k *consumer) claim(ctx context.Context, l log.Logger) ([]Message, error) {
	msgs, next, err := k.rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   k.cfg.Stream,
		Group:    k.cfg.Group,
		Consumer: k.cfg.Consumer,
		MinIdle:  k.cfg.ClaimMinIdle,
		Start:    k.claimFrom,
		Count:    k.cfg.Batch,
	}).Result()
	if err != nil {
		return nil, err
	}
	k.claimFrom = next
	if len(msgs) == 0 {
		return nil, nil
	}

	cmds := make([]*goredis.XPendingExtCmd, len(msgs))
	if _, err := k.rdb.Pipelined(ctx, func(p goredis.Pipeliner) error {
		for i, m := range msgs {
			cmds[i] = p.XPendingExt(ctx, &goredis.XPendingExtArgs{
				Stream: k.cfg.Stream, Group: k.cfg.Group, Start: m.ID, End: m.ID, Count: 1,
			})
		}
		return nil
	}); err != nil {
		return nil, err
	}

	var out []Message
	for i, m := range msgs {
		msg := Message{ID: m.ID, Stream: k.cfg.Stream, Values: m.Values, Deliveries: 1}
		if p, _ := cmds[i].Result(); len(p) == 1 {
			msg.Deliveries = p[0].RetryCount
		}
		ml := l.With(log.F("id", msg.ID), log.F("deliveries", msg.Deliveries))
		if msg.Values == nil {
			// The entry was deleted from the stream while pending.
			ml.Info("streams: reclaimed an entry that no longer exists, acknowledging it")
			k.ack(ctx, ml, msg)
			continue
		}
		if k.cfg.MaxDeliveries > 0 && msg.Deliveries > k.cfg.MaxDeliveries {
			ml.Warn("streams: reclaimed message is over the delivery limit")
			k.deadLetter(ctx, ml, msg, "exceeded max deliveries while pending")
			continue
		}
		ml.Info("streams: reclaimed pending message")
		out = append(out, msg)
	}
	l.Debug("streams: claim pass done", log.F("claimed", len(msgs)), log.F("to_handle", len(out)))
	return out, nil
}

// handle runs the handler for one message and settles it.
func (k *consumer) handle(ctx context.Context, l log.Logger, msg Message) {
	ml := l.With(log.F("id", msg.ID), log.F("deliveries", msg.Deliveries))
	ml.Debug("streams: handling message")
	start := time.Now()
	err := k.call(ctx, msg)
	dur := time.Since(start)

	switch {
	case err == nil:
		ml.Debug("streams: handled", log.F("duration", dur))
		k.ack(ctx, ml, msg)
	case errors.Is(err, ErrDeadLetter):
		ml.Warn("streams: handler rejected the message", log.F("error", err.Error()), log.F("duration", dur))
		k.deadLetter(ctx, ml, msg, err.Error())
	case k.cfg.MaxDeliveries > 0 && msg.Deliveries >= k.cfg.MaxDeliveries:
		ml.Warn("streams: handler failed on the last delivery", log.F("error", err.Error()), log.F("duration", dur))
		k.deadLetter(ctx, ml, msg, err.Error())
	default:
		ml.Warn("streams: handler failed, leaving the message pending for a retry",
			log.F("error", err.Error()), log.F("duration", dur), log.F("retry_after", k.cfg.ClaimMinIdle))
	}
}

// call runs the handler, turning a panic into an error.
func (k *consumer) call(ctx context.Context, msg Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("streams: handler panicked: %v", r)
		}
	}()
	return k.h(ctx, msg)
}

// ack acknowledges msg. A failed ack leaves it pending, so it is delivered
// again later.
func (k *consumer) ack(ctx context.Context, l log.Logger, msg Message) {
	if err := k.rdb.XAck(ctx, k.cfg.Stream, k.cfg.Group, msg.ID).Err(); err != nil {
		l.Error(err, "streams: ack failed, the message will be delivered again")
		return
	}
	l.Debug("streams: acknowledged")
}

// deadLetter copies msg to the dead-letter stream and acknowledges it. If the
// copy fails the message stays pending and is tried again later.
func (k *consumer) deadLetter(ctx context.Context, l log.Logger, msg Message, reason string) {
	values := maps.Clone(msg.Values)
	values[FieldSourceStream] = msg.Stream
	values[FieldSourceID] = msg.ID
	values[FieldGroup] = k.cfg.Group
	values[FieldDeliveries] = strconv.FormatInt(msg.Deliveries, 10)
	values[FieldError] = reason

	args := &goredis.XAddArgs{Stream: k.cfg.DeadLetterStream, Values: values}
	if k.opts.maxLen > 0 {
		args.MaxLen, args.Approx = k.opts.maxLen, true
	}
	id, err := k.rdb.XAdd(ctx, args).Result()
	if err != nil {
		l.Error(err, "streams: dead-letter write failed, the message stays pending",
			log.F("dead_letter_stream", k.cfg.DeadLetterStream))
		return
	}
	l.Warn("streams: moved message to the dead-letter stream",
		log.F("dead_letter_stream", k.cfg.DeadLetterStream), log.F("dead_letter_id", id))
	k.ack(ctx, l, msg)
}

func (k *consumer) backoff() *backoff {
	return &backoff{min: k.cfg.RetryMin, max: k.cfg.RetryMax}
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
