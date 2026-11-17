package ratelimit

import (
	"context"
	"testing"
	"time"
)

func acquireWithin(t *testing.T, g Governor, d time.Duration) (func(), bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return g.(LocalGate).AcquireLocal(ctx)
}

// A Shared governor (core-provider#561) enforces Ceiling as the process total even with saturation
// off. Control: the same Ceiling on an unshared governor enforces nothing locally, as today.
func TestSharedGovernorEnforcesItsCeilingWithoutSaturation(t *testing.T) {
	g := New(Options{Ceiling: 2, Shared: true})
	r1, ok1 := acquireWithin(t, g, time.Second)
	r2, ok2 := acquireWithin(t, g, time.Second)
	if !ok1 || !ok2 {
		t.Fatal("the first two slots of a ceiling of 2 were not granted")
	}
	if _, ok := acquireWithin(t, g, 50*time.Millisecond); ok {
		t.Fatal("a third slot was granted over a shared ceiling of 2")
	}
	r1()
	r3, ok := acquireWithin(t, g, time.Second)
	if !ok {
		t.Fatal("a freed slot was not granted to the next waiter")
	}
	r2()
	r3()
	if !g.(SharedLocalGate).SharedLocal() {
		t.Error("a Shared governor does not report SharedLocal")
	}

	unshared := New(Options{Ceiling: 2})
	for i := 0; i < 5; i++ {
		if _, ok := acquireWithin(t, unshared, 50*time.Millisecond); !ok {
			t.Fatalf("control: an unshared governor without saturation refused local slot %d", i+1)
		}
	}
	if unshared.(SharedLocalGate).SharedLocal() {
		t.Error("control: an unshared governor reports SharedLocal")
	}
}

// A Shared governor's sampler runs ONCE while at least one sharing controller runs: two Starts give one
// sampler, the first ending leaves it running, the last ending stops it. Control: the unshared
// governor's sampler lives exactly as long as its one Start.
func TestSharedGovernorRunsOneSampler(t *testing.T) {
	newSat := func(shared bool) *aimd {
		g, err := NewWithSaturation(Options{Ceiling: 4, Shared: shared},
			SaturationOptions{Enabled: true, CgroupDir: fixture("healthy"), SamplePeriod: time.Hour}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return g.(*aimd)
	}
	eventually := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	g := newSat(true)
	ctxA, stopA := context.WithCancel(context.Background())
	ctxB, stopB := context.WithCancel(context.Background())
	defer stopA()
	defer stopB()
	go func() { _ = g.Start(ctxA) }()
	go func() { _ = g.Start(ctxB) }()
	eventually("one sampler", func() bool { return g.samplers.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	if n := g.samplers.Load(); n != 1 {
		t.Fatalf("%d samplers for two starts of a shared governor, want 1", n)
	}
	stopA()
	time.Sleep(50 * time.Millisecond)
	if n := g.samplers.Load(); n != 1 {
		t.Fatalf("%d samplers after the first starter ended, want 1: the other still runs", n)
	}
	stopB()
	eventually("the sampler to stop after the last starter", func() bool { return g.samplers.Load() == 0 })

	// Control, SEQUENTIAL on purpose: two concurrent samplers on one unshared governor race in
	// saturation.read (the race detector shows it), which is the hazard Shared removes for a pool. Here
	// the instrument is shown to count: one Start is one sampler, and it ends with its Start.
	u := newSat(false)
	ctxC, stopC := context.WithCancel(context.Background())
	defer stopC()
	go func() { _ = u.Start(ctxC) }()
	eventually("control: one sampler for an unshared Start", func() bool { return u.samplers.Load() == 1 })
	stopC()
	eventually("control: the unshared sampler to stop with its Start", func() bool { return u.samplers.Load() == 0 })
}
