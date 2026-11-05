// Package ratelimit slows reconciliation down when a server expresses a rate limit, instead of
// requiring an operator to hand-tune worker counts and resync intervals per installation.
//
// Design: docs/design/rate-limit-governor.md. The shape is shared verbatim with
// provider-runtime (its #33) so the two runtimes cannot grow divergent vocabularies for the same
// state; the declarations are deliberately DUPLICATED rather than moved to a shared module, with a
// contract test in each repo — see the design's open question 1 for why.
//
// THE CONSTRAINT THAT SHAPES THIS PACKAGE: the runtime makes no outbound HTTP calls. Measured —
// net/http appears in three non-test files in pkg/, all of them its own metrics endpoint, health
// server and wiring. So a 429 is visible only to the handler, while the controls that could act on
// it (how many reconciles are in flight, how soon an item is retried) live here. This package is
// the channel between them, and the handler is the only thing that can fill it.
package ratelimit

import (
	"context"
	"time"
)

// Signal is one observation of an endpoint's rate-limit state, as the handler saw it.
type Signal struct {
	// Key identifies the rate-limited scope — normally the endpoint host, because that is what a
	// server-side limit is a property of. Never empty: a signal with no key is dropped, because
	// applying it globally would let one endpoint throttle every reconcile in the process.
	Key string

	// RetryAfter is how long the server asked us to wait, when it said so explicitly. Zero when
	// the server gave an instant instead.
	RetryAfter time.Duration

	// ResetAt is when the window resets, when the server gave an instant rather than a delay.
	// When both this and RetryAfter are set, the LATER of the two wins: the server has given two
	// statements about the same window and honouring the shorter one is how a client gets banned
	// rather than throttled.
	ResetAt time.Time

	// Remaining and Limit are the quota counters when the server publishes them, used only for the
	// advisory path. HasQuota distinguishes "Remaining is genuinely 0" from "we were not told",
	// which an int cannot.
	Remaining, Limit int
	HasQuota         bool

	// Throttled separates "the server refused this request" from "the server is telling us we are
	// close". Both slow us down; only the first means the call did not happen.
	Throttled bool
}

// deadline returns the instant this signal says to wait until, or the zero time when it names none.
func (s Signal) deadline(now time.Time) time.Time {
	var d time.Time
	if s.RetryAfter > 0 {
		d = now.Add(s.RetryAfter)
	}
	if s.ResetAt.After(d) {
		d = s.ResetAt
	}
	return d
}

// Sink receives signals. The runtime installs one per reconcile; a handler reports into it.
type Sink interface {
	Observe(Signal)
}

type sinkKey struct{}

// WithSink returns a context carrying sink, for the runtime to install before calling a handler.
func WithSink(ctx context.Context, s Sink) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, sinkKey{}, s)
}

// Report records a signal from inside a handler.
//
// SAFE ON A CONTEXT WITH NO SINK, and that is what makes the whole mechanism opt-in: a handler
// that reports into a runtime with no governor configured is a no-op, so adopting this on the
// handler side cannot change anyone's behaviour until the runtime side is enabled too. It also
// means a handler can report unconditionally without asking whether anyone is listening.
//
// WHY A CONTEXT SINK RATHER THAN A TYPED ERROR. A typed error checked with errors.As is the pattern
// this codebase uses elsewhere and it cannot work here: Azure DevOps publishes its advisory delay
// on a SUCCESSFUL response — it says it is delaying you before it starts refusing you, and
// reacting to that is the entire value of automatic slow-down. An error-only channel can only react
// after throttling has begun. A field on ExternalObservation covers success and not errors, which
// splits one invariant across two places that must agree. This is one mechanism on both paths, and
// it needs no change to the ExternalClient interface.
func Report(ctx context.Context, s Signal) {
	if s.Key == "" {
		return
	}
	if sink, ok := ctx.Value(sinkKey{}).(Sink); ok && sink != nil {
		sink.Observe(s)
	}
}

// DeferReason says WHY a call was not allowed, and the distinction is load-bearing for what the
// caller may claim. See Deferral.Until.
type DeferReason string

const (
	// DeferThrottled: the server refused us or told us to wait. Until is set, and it is the
	// server's own deadline.
	DeferThrottled DeferReason = "Throttled"

	// DeferAtCapacity: the governor is holding us below its own ceiling. Until is ZERO — we have
	// no deadline from anyone, and inventing one would be presented to a consumer as the server's.
	DeferAtCapacity DeferReason = "AtCapacity"
)

// Deferral says a call must not be made now.
type Deferral struct {
	Key    string
	Reason DeferReason

	// Until is the server's OWN deadline, set only for DeferThrottled. Zero for DeferAtCapacity,
	// where the caller requeues with a short jittered delay instead of claiming a deadline.
	Until time.Time
}

// DeferredError is what a reconcile pass returns when the governor declined to let it call the
// handler. It is an error only so it can travel the worker's existing return path; it is NOT a
// failure, and every consumer of the error must treat it as one of the following, never as the
// other:
//
//	requeue   with After as the delay, keeping the item's priority, WITHOUT the rate-limited add —
//	          the failure limiter would count it toward maxRetries and grow its backoff, which
//	          conflates "this item is failing" with "the endpoint is busy"
//	metrics   as a deferral, never as a reconcile failure — counting it as one would show a
//	          failure spike on every dashboard during rate limiting, telling an operator things are
//	          breaking when nothing is
//	Forget    NOT called: the item's genuine failure history must survive a deferral, which is
//	          what the worker already does for any non-nil error since #52
type DeferredError struct {
	Deferral *Deferral
	After    time.Duration
}

func (e *DeferredError) Error() string {
	if e.Deferral == nil {
		return "reconcile deferred"
	}
	return "reconcile deferred: " + e.Deferral.Key + " " + string(e.Deferral.Reason)
}
