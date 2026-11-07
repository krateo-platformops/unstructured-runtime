package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestAcquireLocal_OffReturnsAtOnce(t *testing.T) {
	off := New(Options{Ceiling: 1}).(*aimd)
	for i := 0; i < 5; i++ { // more than the ceiling: with saturation off nothing bounds this
		if _, ok := off.AcquireLocal(context.Background()); !ok {
			t.Fatal("without saturation a worker must never wait")
		}
	}
}

func TestAcquireLocal_ReleaseIsExactlyOnce(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	release, ok := g.AcquireLocal(context.Background())
	if !ok || g.localInFlight != 1 {
		t.Fatalf("want one slot taken, ok=%v inFlight=%d", ok, g.localInFlight)
	}
	release()
	release()
	if g.localInFlight != 0 {
		t.Fatalf("release did not return the slot exactly once (inFlight=%d)", g.localInFlight)
	}
}

// The waiter must be woken by each of the three things that can let it go.
func TestAcquireLocal_ReleaseWakesAWaiter(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	release, _ := g.AcquireLocal(context.Background())
	got := waitInBackground(g, context.Background())
	assertStillWaiting(t, got)
	release()
	assertAcquired(t, got)
}

func TestAcquireLocal_ACapacityRiseWakesAWaiter(t *testing.T) {
	g, _, c := satGovernor(t, 4)
	g.sample()
	if _, ok := g.AcquireLocal(context.Background()); !ok {
		t.Fatal("setup: no slot")
	}
	got := waitInBackground(g, context.Background())
	assertStillWaiting(t, got)

	c.t = c.t.Add(2 * time.Second) // a ramp step
	g.sample()
	if g.LocalCapacity() < 2 {
		t.Fatalf("setup: capacity did not rise, %d", g.LocalCapacity())
	}
	assertAcquired(t, got)
}

func TestAcquireLocal_ContextEndsTheWait(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	if _, ok := g.AcquireLocal(context.Background()); !ok {
		t.Fatal("setup: no slot")
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := waitInBackground(g, ctx)
	assertStillWaiting(t, got)
	cancel()
	select {
	case ok := <-got:
		if ok {
			t.Fatal("a cancelled wait must not report a slot")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled wait did not return: a drain would hang here")
	}
}

// A sampler that never started stays at capacity 1. That used to appear on the object as a deferral
// cause. It now shows on the input gauge: never read, so reported disabled rather than healthy.
func TestUnstartedSamplerShowsAsADisabledInput(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	if got := g.LocalCapacity(); got != 1 {
		t.Fatalf("an unstarted sampler has capacity %d", got)
	}
	if g.LocalInput().Enabled {
		t.Fatal("an input nobody has read must not report enabled")
	}
}

func TestRampEndsAtTheCeiling(t *testing.T) {
	g, _, c := satGovernor(t, 2)
	g.sample()
	c.t = c.t.Add(2 * time.Second)
	g.sample()
	if got := g.LocalCapacity(); got != 2 {
		t.Fatalf("local capacity %d, want the ceiling", got)
	}
	if g.local.Load().ramping {
		t.Fatal("still ramping at the ceiling")
	}
}

// A key is unrelated to local capacity now, so neither can take from the other.
func TestAKeyedPassTouchesNoLocalSlot(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	release, d := g.TryAcquire("api")
	if d != nil {
		t.Fatalf("a free key was declined: %+v", d)
	}
	if g.localInFlight != 0 {
		t.Fatalf("a keyed pass took a local slot (inFlight=%d)", g.localInFlight)
	}
	release()
}

func TestAKeylessPassCreatesNoKeyState(t *testing.T) {
	g, _, _ := satGovernor(t, 4)
	release, d := g.TryAcquire("")
	if d != nil {
		t.Fatalf("a keyless pass was declined: %+v", d)
	}
	release()
	if _, ok := g.keys[""]; ok {
		t.Fatal("a keyless pass created per-key state for the empty key")
	}
}

func waitInBackground(g *aimd, ctx context.Context) <-chan bool {
	got := make(chan bool, 1)
	go func() {
		_, ok := g.AcquireLocal(ctx)
		got <- ok
	}()
	return got
}

func assertStillWaiting(t *testing.T, got <-chan bool) {
	t.Helper()
	select {
	case <-got:
		t.Fatal("a slot was granted past the capacity")
	case <-time.After(50 * time.Millisecond):
	}
}

func assertAcquired(t *testing.T, got <-chan bool) {
	t.Helper()
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("the waiter gave up")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the waiter was never woken")
	}
}
