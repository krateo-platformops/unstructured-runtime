package workqueue

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func newTestLimiter(base, max time.Duration) (*TypedItemExponentialTimedFailureRateLimiter[string], *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	l := NewExponentialTimedFailureRateLimiter[string](base, max).(*TypedItemExponentialTimedFailureRateLimiter[string])
	l.now = c.now
	return l, c
}

func TestNewExponentialTimedFailureRateLimiter(t *testing.T) {
	if NewExponentialTimedFailureRateLimiter[string](time.Second, 10*time.Second) == nil {
		t.Errorf("Expected non-nil limiter")
	}
}

func TestWhenBacksOffExponentiallyToTheCap(t *testing.T) {
	l, _ := newTestLimiter(time.Second, 5*time.Second)
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got := l.When("item"); got != want {
			t.Errorf("failure %d: delay %v, want %v", i+1, got, want)
		}
	}
}

// THE DEFECT. After a gap over 2*maxDelay, When used to return 0 WITHOUT recording the attempt, so
// every later call took the same branch: zero backoff and a frozen count, forever — maxRetries could
// never fire. Every failure must be counted and backed off, however long since the last one.
func TestWhenKeepsCountingAfterALongGap(t *testing.T) {
	l, c := newTestLimiter(time.Second, 5*time.Second)
	l.When("item")
	l.When("item")

	c.add(11 * time.Second) // over 2*maxDelay
	const more = 5
	for i := range more {
		before := l.NumRequeues("item")
		if d := l.When("item"); d == 0 {
			t.Fatalf("failure %d after a long gap returned ZERO delay: a hot loop with no backoff", i+1)
		}
		if got := l.NumRequeues("item"); got != before+1 {
			t.Fatalf("failure %d after a long gap: NumRequeues %d -> %d, want +1. A frozen count means "+
				"maxRetries never fires", i+1, before, got)
		}
		c.add(11 * time.Second) // every retry slow, the case that froze
	}
	if got := l.NumRequeues("item"); got != 2+more {
		t.Errorf("NumRequeues = %d, want %d: every failure counted", got, 2+more)
	}
}

func TestNumRequeues(t *testing.T) {
	l, _ := newTestLimiter(time.Second, 10*time.Second)
	l.When("item")
	l.When("item")
	if got := l.NumRequeues("item"); got != 2 {
		t.Errorf("Expected numRequeues %v, got %v", 2, got)
	}
}

// Forget is called on success only (#52), so it must clear the history at once. The old idle-time
// guard kept a just-recovered item's entry, so its next failure resumed the old backoff and the map
// leaked one entry per recovered event instance.
func TestForgetClearsHistoryImmediately(t *testing.T) {
	l, _ := newTestLimiter(time.Second, 3*time.Second)
	l.When("item")
	l.When("item")

	l.Forget("item") // no idle period

	if got := l.NumRequeues("item"); got != 0 {
		t.Errorf("NumRequeues = %d after Forget, want 0", got)
	}
	if got := len(l.failures); got != 0 {
		t.Errorf("%d entries remain after Forget; a recovered item must not stay in the map", got)
	}
	// CONTROL: the next failure starts from the base again, which is what recovery means.
	if d := l.When("item"); d != time.Second {
		t.Errorf("first failure after Forget: delay %v, want the base 1s", d)
	}
}
