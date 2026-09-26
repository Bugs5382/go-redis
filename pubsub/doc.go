// Package pubsub is a Redis pub/sub subscriber that re-subscribes to its
// channels and patterns after a reconnect or failover, with its health
// visible to readiness probes.
//
//	sub, err := pubsub.Subscribe(ctx, client, pubsub.Topics{
//		Channels: []string{"config.reload"},
//		Patterns: []string{"orders.*"},
//	}, func(ctx context.Context, msg pubsub.Message) {
//		route(ctx, msg.Channel, msg.Payload)
//	})
//	defer sub.Close()
//
// Subscribe returns once the server has confirmed every channel and pattern.
// The subscription then runs until its context is cancelled or Close is
// called; Done reports when it has stopped.
//
// # Reconnects and health
//
// When the connection drops or a sentinel failover moves the master, the
// subscriber reconnects with exponential backoff (WithBackoff) and subscribes
// to everything again. Healthy reports false from the moment the connection
// is lost until the server confirms every channel and pattern again, so it
// can back a readiness probe. An idle subscription sends a PING every
// WithPingInterval to find a dead connection.
//
// # Delivery guarantees
//
// Redis pub/sub is at most once: a message published while the subscriber is
// disconnected, or while the server is dropping a subscriber that fell too
// far behind, is lost. Use the streams package when every message must
// arrive. The handler runs on the subscriber's goroutine, one message at a
// time; hand slow work to your own goroutines. A handler panic is recovered
// and logged.
//
// # Logging
//
// Diagnostics go to a github.com/Bugs5382/go-log Logger set with WithLogger:
// info when subscribing, re-subscribing and stopping, debug for each
// confirmation, ping and message, warnings for lost connections and retries,
// and an error line for a handler panic. The default logger discards
// everything. Channel and pattern names are logged; payloads never are.
//
// Errors are plain wrapped errors (this module does not use go-apperr); match
// ErrNoTopics with errors.Is.
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
