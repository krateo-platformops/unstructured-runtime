package ratelimit

import (
	"context"
	"testing"
	"time"
)

// #66, layer 2: the local capacity inside the governor. Mirrors provider-runtime v1.9.1's governor
// tests for the CPU input.

type fakeReader struct {
	r     reading
	reads int
}

func (f *fakeReader) read() reading { f.reads++; return f.r }

type satClock struct{ t time.Time }

func (c *satClock) now() time.Time { return c.t }

func satGovernor(t *testing.T, ceiling int) (*aimd, *fakeReader, *satClock) {
	t.Helper()
	c := &satClock{t: time.Unix(1000, 0)}
	g, err := NewWithSaturation(Options{Ceiling: ceiling, RecoveryInterval: 30 * time.Second, Now: c.now,
		Jitter: func(d time.Duration) time.Duration { return d }}, SaturationOptions{Enabled: true, CgroupDir: fixture("healthy")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a := g.(*aimd)
	fr := &fakeReader{r: clean}
	a.sat.reader = fr
	return a, fr, c
}

// Provider-runtime's second trap: a governor whose sampler was never started stays at 1. That must
// show as a real deferral with a cause, never as the quiet ramp.
func TestUnstartedSamplerIsNotAQuietRamp(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	if _, d := g.TryAcquire(""); d != nil {
		t.Fatalf("the first pass must get the single local slot, got %+v", d)
	}
	_, d := g.TryAcquire("")
	if d == nil || !d.Local || d.Reason != DeferAtCapacity || d.Cause != causeSamplerNotStarted {
		t.Fatalf("want a local AtCapacity deferral naming the unstarted sampler, got %+v", d)
	}
}

func TestRampingIsADistinctReasonUntilTheFirstCut(t *testing.T) {
	g, fr, c := satGovernor(t, 4)
	g.samplerStarted.Store(true)
	g.sample() // starts the clock
	if _, d := g.TryAcquire(""); d != nil {
		t.Fatalf("first pass: %+v", d)
	}
	_, d := g.TryAcquire("")
	if d == nil || d.Reason != DeferRamping || !d.Local {
		t.Fatalf("on the startup ramp a full local capacity is DeferRamping, got %+v", d)
	}
	// A hot sample cuts: from now on a full local capacity is Saturated, never Ramping.
	fr.r = hot
	c.t = c.t.Add(time.Second)
	g.sample()
	_, d = g.TryAcquire("")
	if d == nil || d.Reason != DeferSaturated || d.Cause == "" {
		t.Fatalf("after a cut with CPU over the threshold, want DeferSaturated with a cause, got %+v", d)
	}
	// Clean again: the cause clears, but the ramp is over: a full capacity is plain AtCapacity.
	fr.r = clean
	c.t = c.t.Add(time.Second)
	g.sample()
	_, d = g.TryAcquire("")
	if d == nil || d.Reason != DeferAtCapacity || d.Cause != "" {
		t.Fatalf("after the first cut a full local capacity is AtCapacity, not Ramping, got %+v", d)
	}
}

func TestRampingEndsAtTheCeiling(t *testing.T) {
	g, _, c := satGovernor(t, 2)
	g.samplerStarted.Store(true)
	g.sample()
	c.t = c.t.Add(2 * time.Second)
	g.sample() // capacity 2 = ceiling
	if got := g.LocalCapacity(); got != 2 {
		t.Fatalf("setup: local capacity %d", got)
	}
	g.TryAcquire("")
	g.TryAcquire("")
	_, d := g.TryAcquire("")
	if d == nil || d.Reason != DeferAtCapacity || !d.Local {
		t.Fatalf("at the ceiling the ramp is over; want local AtCapacity, got %+v", d)
	}
}

func TestRampingRequeueIsBoundedByTheRampLeft(t *testing.T) {
	g, _, _ := satGovernor(t, 3)
	g.samplerStarted.Store(true)
	g.sample()
	g.TryAcquire("")
	_, d := g.TryAcquire("")
	if d == nil || d.Reason != DeferRamping {
		t.Fatalf("setup: %+v", d)
	}
	// Capacity 1 of 3, step 2s: the ramp needs 4s, well under the 30s recovery interval.
	if got := g.RequeueAfter(d); got != 4*time.Second {
		t.Errorf("a ramping deferral must requeue within the ramp left (4s), got %s", got)
	}
	sat := &Deferral{Local: true, Reason: DeferSaturated}
	if got := g.RequeueAfter(sat); got != 30*time.Second {
		t.Errorf("a saturated deferral requeues after the recovery interval, got %s", got)
	}
}

// Provider-runtime's fourth trap: the local slot bounds EVERY gated pass, with or without a key.
func TestKeylessCallNeedsOnlyALocalSlot(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	release, d := g.TryAcquire("")
	if d != nil {
		t.Fatalf("a keyless pass with a free local slot was declined: %+v", d)
	}
	if _, ok := g.keys[""]; ok {
		t.Error("a keyless pass created per-key state for the empty key")
	}
	if g.localInFlight != 1 {
		t.Fatalf("the keyless pass did not take the local slot (inFlight=%d)", g.localInFlight)
	}
	release()
	release() // idempotent
	if g.localInFlight != 0 {
		t.Errorf("release did not return the local slot exactly once (inFlight=%d)", g.localInFlight)
	}
	// Saturation off: a keyless pass is ungated and touches nothing.
	off := New(Options{Ceiling: 4}).(*aimd)
	if rel, d := off.TryAcquire(""); d != nil || rel == nil {
		t.Errorf("without saturation a keyless pass must pass ungated, got %+v", d)
	}
	if _, ok := off.keys[""]; ok {
		t.Error("without saturation a keyless pass created per-key state")
	}
}

// A pass declined by its KEY must not take a local slot, or a throttled endpoint would starve every
// other object of local capacity.
func TestAKeyDeclinedPassTakesNoLocalSlot(t *testing.T) {
	g, _, c := satGovernor(t, 4)
	g.Observe(Signal{Key: "api", Throttled: true, RetryAfter: time.Minute})
	_ = c
	if _, d := g.TryAcquire("api"); d == nil || d.Reason != DeferThrottled {
		t.Fatalf("setup: want the key throttled, got %+v", d)
	}
	if g.localInFlight != 0 {
		t.Errorf("a key-declined pass took a local slot (inFlight=%d)", g.localInFlight)
	}
}

func TestTryAcquireDoesNoIO(t *testing.T) {
	g, fr, _ := satGovernor(t, 4)
	for i := 0; i < 10; i++ {
		if rel, _ := g.TryAcquire(""); rel != nil {
			rel()
		}
	}
	if fr.reads != 0 {
		t.Errorf("TryAcquire read the cgroup files %d times; only the sampler may", fr.reads)
	}
}

func TestStartStopsWithItsContextAndOffReturnsAtOnce(t *testing.T) {
	g, fr, _ := satGovernor(t, 4)
	g.sat.opts.SamplePeriod = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Start(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Start returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
	if fr.reads == 0 || !g.samplerStarted.Load() {
		t.Errorf("Start did not sample (reads=%d started=%v)", fr.reads, g.samplerStarted.Load())
	}
	off := New(Options{Ceiling: 4}).(*aimd)
	if err := off.Start(context.Background()); err != nil {
		t.Errorf("Start without saturation must return nil at once, got %v", err)
	}
}

func TestNewWithSaturationRejectsOutOfRangeSettings(t *testing.T) {
	if _, err := NewWithSaturation(Options{Ceiling: 4}, SaturationOptions{Enabled: true, CPUPressureThreshold: 150}, nil); err == nil {
		t.Error("an out-of-range threshold must be an error, not a silent cap")
	}
	g, err := NewWithSaturation(Options{Ceiling: 4}, SaturationOptions{}, nil)
	if err != nil || g.(*aimd).sat != nil {
		t.Errorf("Enabled=false must be plain New (err %v)", err)
	}
}
