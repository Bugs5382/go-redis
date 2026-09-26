// Package streams is a resilient Redis Streams consumer-group consumer, with
// a matching Publish, built on a go-redis Client.
//
// Consume runs one consumer in a consumer group until its context is
// cancelled:
//
//	err := streams.Consume(ctx, client, streams.Config{
//		Stream:   "orders",
//		Group:    "billing",
//		Consumer: podName,
//	}, func(ctx context.Context, msg streams.Message) error {
//		return bill(ctx, msg.Values["order_id"])
//	})
//
// It creates the group (and the stream) when missing, reads new entries with
// XREADGROUP, and acknowledges each entry the handler accepts with XACK.
//
// # Retries, reclaiming and dead letters
//
// A handler error leaves the entry pending. Every ClaimInterval the consumer
// runs XAUTOCLAIM, taking over entries that have been pending for at least
// ClaimMinIdle, whether their handler failed or their consumer died, and
// handles them again with Message.Deliveries counting up. When a message
// reaches MaxDeliveries, or its handler returns ErrDeadLetter, it is copied to
// the dead-letter stream (Stream + ":dead" by default) with the Field*
// metadata fields added, and acknowledged. A handler panic counts as an
// error.
//
// # Resilience
//
// Redis errors, such as a dropped connection or a sentinel failover, are
// logged and retried with exponential backoff between RetryMin and RetryMax;
// the Client reconnects underneath. If the group disappears because the
// stream was deleted or a failover promoted a replica that never had it,
// Consume creates it again at StartID. Batch and Block bound each read, and
// Concurrency sets how many handlers run at once.
//
// # Shutdown
//
// Cancelling the context stops reading. In-flight handlers keep a context
// that is not cancelled, so they can finish and be acknowledged; Consume
// returns nil once they have. Entries read but not yet started stay pending
// and are reclaimed later.
//
// # Delivery guarantees
//
// Delivery is at least once. An entry is handled again if its ack is lost, and
// a crash between the dead-letter write and the ack can leave a duplicate in
// the dead-letter stream. Make handlers idempotent.
//
// # Publishing
//
// Publish appends an entry with XADD; WithMaxLen caps the stream with
// MAXLEN ~. Passed to Consume, WithMaxLen caps the dead-letter stream.
//
// # Logging
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// info for start, stop, group creation and reclaims, debug for each message
// and ack, warnings for handler failures, retries and dead letters, and error
// lines for failed commands. The default logger discards everything. Stream,
// group and consumer names and entry IDs are logged; field values never are.
//
// Errors are plain wrapped errors (this module does not use go-apperr); match
// ErrInvalidConfig and ErrDeadLetter with errors.Is.
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
