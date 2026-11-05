package ratelimit

import (
	cryptorand "crypto/rand"
	"math/big"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Governor decides how much concurrency a key currently gets.
type Governor interface {
	// TryAcquire NEVER BLOCKS. It either grants capacity — returning a release func — or returns a
	// Deferral saying why and, when a deadline exists, when to come back.
	//
	// NON-BLOCKING IS NOT AN OPTIMISATION, it is what keeps this design from contradicting itself.
	// Keys exist so one throttled endpoint cannot slow reconciles against every other endpoint. A
	// blocking acquire with a fixed worker pool lets every worker park on the one throttled key,
	// and then nothing processes the healthy keys either — reproducing exactly the global stall
	// keying exists to prevent. The first draft of the design blocked here and argued against
	// itself two sections later; the provider-runtime session caught it.
	TryAcquire(key string) (release func(), d *Deferral)

	// Observe folds a handler's signal into the state for its key.
	Observe(Signal)

	// Capacity reports the current effective concurrency for a key, for metrics and tests. It is
	// the number an operator used to set by hand.
	Capacity(key string) int

	// RequeueAfter is how long a deferred item should wait before it is retried. On the interface
	// rather than only on the concrete type because the WORKER is what requeues, and it holds a
	// Governor. The first version had it as a method on *aimd alone, so nothing that held the
	// interface could reach it — found while wiring the worker, before any caller existed.
	RequeueAfter(d *Deferral) time.Duration
}

// RateLimitKeyer lets a handler name its rate-limit scope BEFORE a call is made. Optional: a
// handler that does not implement it is never gated.
//
// THIS CLOSES A CIRCULAR DEPENDENCY in the first draft of the design, found by the
// provider-runtime session. TryAcquire needs the key before the call; a Signal carries the key only
// once the handler has made the call. Both halves were written and neither could be satisfied. It
// was then written into the design document and not into this package, so it stayed open until the
// worker was wired and needed it.
//
// It must do NO I/O: it runs before every gated call, and a keyer that made a request to decide
// whether a request is allowed would be a rate-limit problem of its own.
//
// AN ERROR FAILS OPEN: the pass runs ungated. That is safe ONLY because the gate is a throttle and
// not a security control. A handler must not use RateLimitKey to refuse a call — a host it may not
// talk to, a credential it may not send — because the refusal is then silently dropped. Enforce
// those on the request path itself, where they hold whether or not a keyer ran (rest-dynamic-
// controller checks its host policy at request time and on every redirect hop, and fails closed
// with no policy — which is why its fail-open here is safe; the oasgen-provider review).
type RateLimitKeyer interface {
	RateLimitKey(mg *unstructured.Unstructured) (string, error)
}

// Options configures an AIMD governor.
type Options struct {
	// Ceiling is the maximum concurrent handler calls per key. Normally the controller's worker
	// count: the governor only ever holds us BELOW the pool size, never above it.
	Ceiling int

	// RecoveryInterval is how long a key must go without a signal before capacity rises by one.
	// Additive so recovery cannot itself produce the burst that caused the limit.
	RecoveryInterval time.Duration

	// Now and Jitter are injectable for tests. Nil means time.Now and a real jitter.
	Now    func() time.Time
	Jitter func(time.Duration) time.Duration
}

const (
	defaultRecoveryInterval = 30 * time.Second
	// capacityFloor is 1, not 0. A key is never reduced to "no calls ever": progress at one call
	// at a time is how the governor learns the limit has lifted, and a floor of 0 would need a
	// separate mechanism to ever probe again.
	capacityFloor = 1
)

type keyState struct {
	capacity  int
	inFlight  int
	holdUntil time.Time
	lastCut   time.Time
	lastRaise time.Time
}

type aimd struct {
	mu   sync.Mutex
	keys map[string]*keyState
	opts Options
}

// New returns an AIMD governor. A Ceiling below 1 is raised to 1 rather than rejected: a governor
// that could grant nothing would wedge every reconcile, and silently doing nothing would be worse.
func New(o Options) Governor {
	if o.Ceiling < 1 {
		o.Ceiling = 1
	}
	if o.RecoveryInterval <= 0 {
		o.RecoveryInterval = defaultRecoveryInterval
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Jitter == nil {
		o.Jitter = jitter
	}
	return &aimd{keys: map[string]*keyState{}, opts: o}
}

// state returns the key's state, creating it at full capacity. A key is unthrottled until something
// says otherwise — the governor never slows a key it has heard nothing about.
func (g *aimd) state(key string) *keyState {
	st, ok := g.keys[key]
	if !ok {
		st = &keyState{capacity: g.opts.Ceiling}
		g.keys[key] = st
	}
	return st
}

func (g *aimd) TryAcquire(key string) (func(), *Deferral) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.opts.Now()
	st := g.state(key)

	// Recovery is evaluated on ACQUIRE rather than on a timer. No goroutine to leak, no ticker to
	// stop, and a key nobody is reconciling does not need its capacity maintained.
	g.maybeRaiseLocked(st, now)

	if now.Before(st.holdUntil) {
		return nil, &Deferral{Key: key, Reason: DeferThrottled, Until: st.holdUntil}
	}
	if st.inFlight >= st.capacity {
		// NO Until. We are the ones holding this back, not the server, and a deadline we invented
		// would be read by a consumer as the server's.
		return nil, &Deferral{Key: key, Reason: DeferAtCapacity}
	}

	st.inFlight++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if s, ok := g.keys[key]; ok && s.inFlight > 0 {
				s.inFlight--
			}
		})
	}, nil
}

func (g *aimd) Observe(s Signal) {
	if s.Key == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.opts.Now()
	st := g.state(s.Key)

	// ONLY A CUT RESTARTS THE RECOVERY CLOCK. A server that publishes counters sends them on EVERY
	// response — GitHub does — so stamping every signal here meant a busy key never went one
	// RecoveryInterval without one, and capacity cut once never came back.
	switch {
	case s.Throttled:
		// THE SERVER'S OWN DEADLINE, NOT A HALVING. It has told us a specific instant; guessing a
		// shorter delay than the one we were given is how a client gets banned rather than
		// throttled. Capacity also drops to the floor so that when the hold lifts we resume at one
		// call rather than at the ceiling.
		st.capacity = capacityFloor
		st.lastCut = now
		if d := s.deadline(now); d.After(st.holdUntil) {
			st.holdUntil = d
		}

	case s.HasQuota && s.Limit > 0 && s.Remaining*4 <= s.Limit:
		// ADVISORY: under a quarter of the quota left and not yet refused. Halve and keep flowing.
		// This is the state that distinguishes this design from a retry loop — a retry loop can
		// only discover a limit by hitting it.
		st.capacity = max(capacityFloor, st.capacity/2)
		st.lastCut = now

	case s.HasQuota:
		// HEALTHY QUOTA: counters present and above a quarter. Nothing to do — and it must be an
		// arm of its own, ahead of the delay arm below. GitHub sends X-RateLimit-Reset, always in
		// the future, on every response including Remaining=4900/5000; falling through to the
		// delay arm read that window boundary as Azure's delay and halved capacity on every
		// successful call, pinning a GitHub key at the floor.

	case s.deadline(now).After(now):
		// A delay with no quota counters and no refusal — Azure's advisory shape. Treated as
		// advisory rather than throttled: the call SUCCEEDED, so closing the gate entirely would
		// stop work the server was still willing to do.
		st.capacity = max(capacityFloor, st.capacity/2)
		st.lastCut = now
	}
}

// maybeRaiseLocked implements the additive half. Caller holds the lock.
func (g *aimd) maybeRaiseLocked(st *keyState, now time.Time) {
	if st.capacity >= g.opts.Ceiling {
		return
	}
	if now.Before(st.holdUntil) {
		return // still held; raising now would just be undone
	}
	// RECOVERY COUNTS FROM THE LATER OF THE LAST CUT AND THE END OF THE HOLD. Counting from the cut
	// alone meant a hold longer than RecoveryInterval raised capacity the instant it lifted — the
	// first acquire after a 30s Retry-After went straight to 2 with no clean interval at the floor
	// to show the server had actually recovered.
	since := st.lastCut
	if st.holdUntil.After(since) {
		since = st.holdUntil
	}
	if !since.IsZero() && now.Sub(since) < g.opts.RecoveryInterval {
		return
	}
	// The cooldown is separate from lastCut so capacity rises at most once per interval even
	// when signals stopped long ago — otherwise a key idle for an hour would jump straight to the
	// ceiling on its next acquire, which is the burst recovery is meant to avoid.
	if !st.lastRaise.IsZero() && now.Sub(st.lastRaise) < g.opts.RecoveryInterval {
		return
	}
	st.capacity++
	st.lastRaise = now
}

func (g *aimd) Capacity(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state(key).capacity
}

// jitter returns a delay uniformly in [d/2, 3d/2).
//
// crypto/rand RATHER THAN math/rand, and not because jitter is security-sensitive — it is not. gosec
// flags math/rand and math/rand/v2 alike (G404), and the two ways to make the check pass are a
// `#nosec` suppression or a source that genuinely satisfies it. A suppression is an exception every
// later reviewer has to re-justify, and it silences the scanner for the whole line rather than for
// the reason given. crypto/rand costs eight bytes per deferral, which is nothing next to the
// round-trip it is delaying.
//
// cryptorand.Int RATHER THAN READING BYTES AND REDUCING MODULO d, and the first version of this
// function did the latter. It drew 8 bytes, took them mod uint64(d) and converted back with
// time.Duration(...), which gosec then flagged as G115 (integer overflow conversion uint64 ->
// int64). The conversion could not actually overflow — the result is in [0, d) and d fits in int64
// — but the reduction had a real flaw the scanner did not name: `x % d` is not uniform unless d
// divides 2^64, so it carried a slight bias toward small values. cryptorand.Int returns a uniform
// value in [0, d) by rejection sampling, which removes the bias AND the conversion, so the scanner
// is satisfied on the merits rather than by a suppression.
//
// On a read failure it returns d — the full delay, never a shorter one. Failing short is the
// dangerous direction for a backoff: it is how a client comes back early, and early is how it gets
// banned rather than throttled.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	n, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(d)))
	if err != nil {
		return d
	}
	return d/2 + time.Duration(n.Int64())
}

// RequeueAfter is how long a caller should wait before retrying a deferred item.
//
// For DeferThrottled it is the server's deadline. For DeferAtCapacity it is a short JITTERED delay:
// short because capacity may free up at any moment, and jittered because a fixed delay would make
// every deferred item for a key come back in the same instant and re-defer together.
func (g *aimd) RequeueAfter(d *Deferral) time.Duration {
	if d == nil {
		return 0
	}
	if d.Reason == DeferThrottled && !d.Until.IsZero() {
		if wait := d.Until.Sub(g.opts.Now()); wait > 0 {
			return wait
		}
		return 0
	}
	return g.opts.Jitter(2 * time.Second)
}
