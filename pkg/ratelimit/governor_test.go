package ratelimit

import (
	"context"
	"testing"
	"time"
)

// clock is a settable clock. The governor's decisions are all relative to "now", so a real clock
// would make every deadline assertion a sleep and every recovery assertion flaky.
type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func testGovernor(ceiling int, recovery time.Duration) (*aimd, *clock) {
	c := &clock{t: time.Date(2026, 11, 4, 23, 0, 0, 0, time.UTC)}
	g := New(Options{
		Ceiling:          ceiling,
		RecoveryInterval: recovery,
		Now:              c.now,
		// Fixed jitter so the at-capacity requeue is assertable; the real one is randomised.
		Jitter: func(d time.Duration) time.Duration { return d / 2 },
	}).(*aimd)
	return g, c
}

// Capacity must actually bound concurrency. Without this the governor is a gauge rather than a
// gate, and every other assertion here is about bookkeeping nobody enforces.
func TestCapacityBoundsConcurrency(t *testing.T) {
	g, _ := testGovernor(2, time.Minute)

	r1, d1 := g.TryAcquire("h")
	r2, d2 := g.TryAcquire("h")
	if d1 != nil || d2 != nil {
		t.Fatalf("the first two acquires were deferred (%v, %v) with a ceiling of 2", d1, d2)
	}

	r3, d3 := g.TryAcquire("h")
	if d3 == nil {
		t.Fatal("a third concurrent acquire was GRANTED with a ceiling of 2, so capacity bounds " +
			"nothing and the gate is decorative")
	}
	if r3 != nil {
		t.Error("a deferred acquire returned a non-nil release func, which a caller would defer()")
	}
	if d3.Reason != DeferAtCapacity {
		t.Errorf("reason = %q, want %q", d3.Reason, DeferAtCapacity)
	}

	// Releasing must give the capacity back, or the gate closes permanently after Ceiling calls.
	r1()
	if _, d := g.TryAcquire("h"); d != nil {
		t.Errorf("after a release, an acquire was still deferred (%v) — capacity is not returned", d)
	}
	r2()
}

// AT CAPACITY CARRIES NO DEADLINE. This is the honesty arm: we are the ones holding the call back,
// and a deadline we invented would be read by a consumer as the server's.
func TestAtCapacityDeferralClaimsNoDeadline(t *testing.T) {
	g, _ := testGovernor(1, time.Minute)
	release, _ := g.TryAcquire("h")
	defer release()

	_, d := g.TryAcquire("h")
	if d == nil {
		t.Fatal("expected a deferral at capacity 1 with one call in flight")
	}
	if !d.Until.IsZero() {
		t.Errorf("an AtCapacity deferral carries Until=%v. Nobody told us to wait until then — "+
			"presenting our own backpressure as a server deadline misreports who is throttling "+
			"whom", d.Until)
	}
	if got := g.RequeueAfter(d); got <= 0 || got > 2*time.Second {
		t.Errorf("RequeueAfter = %v for an at-capacity deferral; want a short positive delay, "+
			"because capacity may free at any moment", got)
	}
}

// THE SERVER'S DEADLINE IS HONOURED, NOT GUESSED. Shortening it is how a client gets banned rather
// than throttled.
func TestThrottledSignalHoldsUntilTheServersDeadline(t *testing.T) {
	g, c := testGovernor(4, time.Minute)

	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: 30 * time.Second})

	_, d := g.TryAcquire("h")
	if d == nil {
		t.Fatal("a throttled key granted capacity immediately")
	}
	if d.Reason != DeferThrottled {
		t.Errorf("reason = %q, want %q", d.Reason, DeferThrottled)
	}
	want := c.now().Add(30 * time.Second)
	if d.Until.Before(want) {
		t.Errorf("Until = %v, which is EARLIER than the server's own deadline %v. Retrying before "+
			"the instant we were given is how a client gets banned rather than throttled",
			d.Until, want)
	}
	if got := g.RequeueAfter(d); got < 29*time.Second {
		t.Errorf("RequeueAfter = %v, want ~30s — the requeue must not be shorter than the hold", got)
	}

	// And the hold must LIFT. A gate that never reopens is worse than no gate.
	c.add(31 * time.Second)
	if _, d := g.TryAcquire("h"); d != nil {
		t.Errorf("still deferred (%v) after the server's deadline passed", d)
	}
}

// When the server gives BOTH a delay and a reset instant, the LATER wins — two statements about one
// window, and honouring the shorter is the destructive choice.
func TestBothDelayAndResetTakeTheLater(t *testing.T) {
	g, c := testGovernor(4, time.Minute)
	later := c.now().Add(90 * time.Second)

	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: 10 * time.Second, ResetAt: later})

	_, d := g.TryAcquire("h")
	if d == nil {
		t.Fatal("expected a deferral")
	}
	if d.Until.Before(later) {
		t.Errorf("Until = %v, want >= %v (the later of RetryAfter and ResetAt)", d.Until, later)
	}
}

// A THROTTLED KEY MUST NOT SLOW ANOTHER KEY. This is the reason keys exist, and the property a
// blocking gate would have destroyed.
func TestOneThrottledKeyDoesNotAffectAnother(t *testing.T) {
	g, _ := testGovernor(4, time.Minute)

	g.Observe(Signal{Key: "github.example.com", Throttled: true, RetryAfter: time.Minute})

	if _, d := g.TryAcquire("github.example.com"); d == nil {
		t.Fatal("the throttled key was not deferred, so this test cannot show isolation")
	}
	release, d := g.TryAcquire("dev.azure.com")
	if d != nil {
		t.Fatalf("an unrelated key was deferred (%v) because a DIFFERENT key is throttled. That is "+
			"the global slow-down keying exists to prevent — one noisy provider would throttle "+
			"every reconcile in the process", d)
	}
	release()

	if got := g.Capacity("dev.azure.com"); got != 4 {
		t.Errorf("the unrelated key's capacity is %d, want the ceiling 4", got)
	}
}

// ADVISORY halves and keeps flowing. The advisory path is the whole difference from a retry loop,
// which can only discover a limit by hitting it.
func TestAdvisoryQuotaHalvesCapacityWithoutClosingTheGate(t *testing.T) {
	g, _ := testGovernor(8, time.Minute)

	g.Observe(Signal{Key: "h", HasQuota: true, Limit: 100, Remaining: 10})

	if got := g.Capacity("h"); got != 4 {
		t.Errorf("capacity = %d, want 4 (halved from 8)", got)
	}
	release, d := g.TryAcquire("h")
	if d != nil {
		t.Fatalf("an advisory signal CLOSED the gate (%v). The call succeeded — the server is "+
			"still willing to serve us, just less", d)
	}
	release()
}

// THE CONTROL for the advisory arm: plenty of quota left must change nothing, or the governor
// halves on every successful response and throughput collapses for no reason.
func TestHealthyQuotaDoesNotReduceCapacity(t *testing.T) {
	g, _ := testGovernor(8, time.Minute)

	g.Observe(Signal{Key: "h", HasQuota: true, Limit: 100, Remaining: 90})

	if got := g.Capacity("h"); got != 8 {
		t.Errorf("capacity = %d after a HEALTHY quota report, want the ceiling 8. Reducing here "+
			"would slow every endpoint that publishes counters at all", got)
	}
}

// RECOVERY IS ADDITIVE, and asserted as "not a jump" rather than only as "it rises" — a jump
// straight to the ceiling is the burst that recovery is supposed to avoid.
func TestRecoveryIsAdditiveNotAJump(t *testing.T) {
	g, c := testGovernor(4, 10*time.Second)

	g.Observe(Signal{Key: "h", Throttled: true, RetryAfter: time.Second})
	if got := g.Capacity("h"); got != 1 {
		t.Fatalf("capacity = %d after a throttle, want the floor 1", got)
	}

	// One recovery interval: +1, and no more.
	c.add(11 * time.Second)
	release, d := g.TryAcquire("h")
	if d != nil {
		t.Fatalf("deferred (%v) after the hold expired", d)
	}
	release()
	if got := g.Capacity("h"); got != 2 {
		t.Errorf("capacity = %d after ONE recovery interval, want 2. A jump to the ceiling is the "+
			"burst that caused the limit in the first place", got)
	}

	// Two more intervals, two more steps — and it must stop at the ceiling.
	for range 5 {
		c.add(11 * time.Second)
		r, _ := g.TryAcquire("h")
		if r != nil {
			r()
		}
	}
	if got := g.Capacity("h"); got != 4 {
		t.Errorf("capacity = %d after several intervals, want the ceiling 4 (and never above)", got)
	}
}

// A signal with no Key is DROPPED — asserted as "it creates no state", which is the invariant the
// guard actually provides.
//
// THE FIRST VERSION OF THIS TEST WAS VACUOUS AND A MUTANT PROVED IT. It observed a keyless signal
// and then asserted an UNRELATED key was still fast. That passes whether or not the guard exists,
// because a keyless signal lands on the "" key rather than globally — so removing the guard
// survived the whole file. The claim in the old comment ("applying it everywhere turns a missing
// field into a process-wide slow-down") was simply not what the code does, and the test was written
// to match the claim instead of the code.
//
// What removing the guard DOES do is create and throttle a phantom "" key: state that nothing will
// ever clean up, and a hold that would defer any caller who passes an empty key. That is the thing
// to assert.
func TestKeylessSignalCreatesNoState(t *testing.T) {
	g, _ := testGovernor(4, time.Minute)

	g.Observe(Signal{Throttled: true, RetryAfter: time.Hour})

	g.mu.Lock()
	n := len(g.keys)
	g.mu.Unlock()
	if n != 0 {
		t.Errorf("a keyless signal created %d key state(s). It must be dropped: the empty key is "+
			"not a scope, state for it is never cleaned up, and a hold on it would defer any "+
			"caller that passes an empty key", n)
	}

	// AND the control from the old version, kept because it is still worth asserting even though
	// it does not discriminate on its own: an unrelated key must remain at full speed.
	release, d := g.TryAcquire("h")
	if d != nil {
		t.Fatalf("an unrelated key was deferred (%v) after a keyless signal", d)
	}
	release()
}

// Report must be safe with no sink installed, because that is what makes handler-side adoption
// free: a handler can report unconditionally before any runtime enables the governor.
func TestReportIsANoOpWithoutASink(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Report panicked on a context with no sink: %v", r)
		}
	}()
	Report(context.Background(), Signal{Key: "h", Throttled: true})
}

type recordingSink struct{ got []Signal }

func (r *recordingSink) Observe(s Signal) { r.got = append(r.got, s) }

func TestReportReachesAnInstalledSinkAndDropsKeylessSignals(t *testing.T) {
	sink := &recordingSink{}
	ctx := WithSink(context.Background(), sink)

	Report(ctx, Signal{Key: "h", Throttled: true})
	Report(ctx, Signal{Throttled: true}) // keyless — dropped

	if len(sink.got) != 1 {
		t.Fatalf("sink received %d signals, want 1 (the keyless one must be dropped)", len(sink.got))
	}
	if sink.got[0].Key != "h" {
		t.Errorf("sink received key %q, want %q", sink.got[0].Key, "h")
	}
}

// A key nobody has reported on must start at the ceiling. The governor must never slow a key it has
// heard nothing about — otherwise enabling it would slow every endpoint on day one.
func TestUnknownKeyStartsAtTheCeiling(t *testing.T) {
	g, _ := testGovernor(6, time.Minute)
	if got := g.Capacity("never-seen"); got != 6 {
		t.Errorf("an unreported key has capacity %d, want the ceiling 6", got)
	}
}
