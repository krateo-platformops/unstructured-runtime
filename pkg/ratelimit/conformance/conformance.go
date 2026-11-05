// Package conformance lets a HANDLER prove it reports rate-limit signals.
//
// WHY THIS LIVES HERE AND NOT IN THE RUNTIME'S OWN TESTS. The runtime cannot assert that a handler
// reports; it hands over a context and gets back an observation. A handler that never calls
// ratelimit.Report is indistinguishable from an endpoint that is never rate-limited, and the
// governor then does nothing while looking configured — the design's known weakness, stated in its
// §2. The only place that gap can be closed is inside the handler's own test suite, so the check
// ships as something a handler imports rather than something the runtime pretends to verify.
//
// Usage, in a handler's package:
//
//	func TestObserveReportsRateLimits(t *testing.T) {
//	    conformance.AssertReportsThrottle(t, func(ctx context.Context) error {
//	        _, err := myHandler.Observe(ctx, objectAgainst(server429.URL))
//	        return err
//	    })
//	}
package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
)

// Recorder collects what a handler reported.
type Recorder struct{ Signals []ratelimit.Signal }

func (r *Recorder) Observe(s ratelimit.Signal) { r.Signals = append(r.Signals, s) }

// Capture runs call with a recording sink installed and returns what was reported.
func Capture(ctx context.Context, call func(context.Context) error) (*Recorder, error) {
	rec := &Recorder{}
	return rec, call(ratelimit.WithSink(ctx, rec))
}

// AssertReportsThrottle fails t unless call reported at least one THROTTLED signal carrying a key
// and a deadline.
//
// A deadline is required because the governor cannot hold a key without one: a throttled signal
// with no RetryAfter and no ResetAt tells the governor to stop and never says for how long, which
// it can only treat as advisory. A handler that drops the server's Retry-After has reported
// something unusable, and that is worth failing on rather than warning about.
func AssertReportsThrottle(t testing.TB, call func(context.Context) error) {
	t.Helper()
	rec, _ := Capture(context.Background(), call)
	// The error is deliberately ignored: a throttled call SHOULD normally fail, and asserting on
	// its error would make this check also an assertion about the handler's error mapping, which
	// is its own business.

	if len(rec.Signals) == 0 {
		t.Fatalf("the handler reported NO rate-limit signal for a throttled response. The runtime " +
			"cannot see the response, so an unreported throttle is invisible: the governor stays " +
			"at full capacity while looking configured, and reconciles keep hammering a server " +
			"that is refusing them")
	}

	var throttled []ratelimit.Signal
	for _, s := range rec.Signals {
		if s.Throttled {
			throttled = append(throttled, s)
		}
	}
	if len(throttled) == 0 {
		t.Fatalf("the handler reported %d signal(s) but none with Throttled=true. An advisory "+
			"signal only halves capacity; a refusal must close the gate until the server's "+
			"deadline", len(rec.Signals))
	}

	for _, s := range throttled {
		if s.Key == "" {
			t.Error("a throttled signal carries no Key. Keyless signals are DROPPED, because the " +
				"empty key is not a scope — so this report has no effect at all")
		}
		if s.RetryAfter <= 0 && s.ResetAt.IsZero() {
			t.Errorf("the throttled signal for key %q carries neither RetryAfter nor ResetAt. The "+
				"governor cannot hold a key for an unknown duration, so it can only treat this as "+
				"advisory — the server told you how long to wait and the handler dropped it", s.Key)
		}
	}
}

// AssertReportsAdvisory fails t unless call reported a non-throttled signal with quota counters or
// a delay.
//
// THIS IS THE ARM MOST HANDLERS WILL FAIL FIRST, and it is the one worth having. Azure DevOps
// publishes its delay on a SUCCESSFUL response — it says it is delaying you before it refuses you.
// A handler that only reports on errors misses it entirely, and with it the whole point of
// automatic slow-down: reacting before being throttled rather than after.
func AssertReportsAdvisory(t testing.TB, call func(context.Context) error) {
	t.Helper()
	rec, err := Capture(context.Background(), call)
	if err != nil {
		t.Fatalf("the advisory call failed (%v). This check is about a SUCCESSFUL response that "+
			"carries rate-limit information, so a failing call means the fixture is wrong rather "+
			"than the handler", err)
	}

	for _, s := range rec.Signals {
		if s.Throttled {
			continue
		}
		if s.HasQuota || s.RetryAfter > 0 || !s.ResetAt.IsZero() {
			return // conforms
		}
	}
	t.Fatalf("the handler reported no advisory signal for a successful response carrying "+
		"rate-limit information (%d signal(s) seen, none advisory with quota or delay). An "+
		"error-only reporter can react only after throttling has begun", len(rec.Signals))
}

// AssertHealthyDoesNotSlowDown fails t if what call reported for a HEALTHY response would slow a
// governor down.
//
// THE CONTROL, and without it the two asserts above are satisfied by a handler that reports a
// throttle on every single response, which would pin capacity to the floor forever while passing
// conformance.
//
// IT ASSERTS THE EFFECT, NOT SILENCE. Its first version required zero signals, and that is wrong
// for exactly the vendors this exists for: GitHub publishes its counters on every response, and a
// handler that forwards them is correct — the advisory arm depends on them. So the reported signals
// are replayed into a fresh governor and the check is that it neither cut capacity nor closed the
// gate. A healthy quota report passes; a throttle, a low quota or a bare delay fails.
func AssertHealthyDoesNotSlowDown(t testing.TB, call func(context.Context) error) {
	t.Helper()
	rec, err := Capture(context.Background(), call)
	if err != nil {
		t.Fatalf("the healthy call failed (%v); the fixture is wrong", err)
	}

	const ceiling = 8
	g := ratelimit.New(ratelimit.Options{Ceiling: ceiling, RecoveryInterval: time.Hour})
	keys := map[string]bool{}
	for _, s := range rec.Signals {
		g.Observe(s)
		keys[s.Key] = true
	}
	// Capacity alone is the whole check: a throttle drops capacity to the floor as well as holding
	// the key, so a closed gate always shows up here first. A separate TryAcquire check could never
	// fail on its own, and a mutant removing it proved that.
	for key := range keys {
		if c := g.Capacity(key); c != ceiling {
			t.Errorf("a HEALTHY response cut capacity for key %q to %d of %d. Signals reported: %+v. "+
				"A handler that slows down on success throttles itself for no reason",
				key, c, ceiling, rec.Signals)
		}
	}
}

// Elapsed is a convenience for a handler converting an absolute reset header into a Signal.
// Returns zero for an instant already past, so a stale header cannot produce a negative delay that
// a governor would read as "no deadline".
func Elapsed(resetAt, now time.Time) time.Duration {
	if d := resetAt.Sub(now); d > 0 {
		return d
	}
	return 0
}
