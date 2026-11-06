package priorityqueue

import (
	"fmt"
	"testing"
	"time"
)

// drainSaturated plays a worker pool that is always busy: every item handed out is replaced by a new
// undelayed one, so the queue never empties. That is a controller whose resync demand exceeds what it
// can do. It returns how many items were handed out before want, or -1 if want never came.
func drainSaturated(t *testing.T, q PriorityQueue[string], want string, priority, limit int) int {
	t.Helper()
	for i := 0; i < limit; i++ {
		got, _, _ := q.GetWithPriority()
		q.Done(got)
		if got == want {
			return i
		}
		q.AddWithOpts(AddOpts{Priority: priority}, fmt.Sprintf("resync-%d", i))
	}
	return -1
}

func saturated(t *testing.T, delayedPriority int) PriorityQueue[string] {
	t.Helper()
	q := New[string]("starvation")
	t.Cleanup(q.ShutDown)
	for i := 0; i < 8; i++ {
		q.AddWithOpts(AddOpts{Priority: -100}, fmt.Sprintf("backlog-%d", i))
	}
	q.AddWithOpts(AddOpts{After: time.Millisecond, Priority: delayedPriority}, "retry")
	time.Sleep(20 * time.Millisecond) // its delay is over
	return q
}

// A delay holds an item back until it expires, and no longer. An item whose delay has passed used to
// sort behind every undelayed item, whatever its priority, so it was reached only when nothing else
// was queued. In a controller whose queue never empties that is never: the 1s requeue of a lost
// status race, every rate-limited retry and every deferral starved, and the digest they left stored
// made each later resync of that object a duplicate. The object then went silent until a restart,
// with no error and every condition green.
func TestExpiredDelay_IsNotStarvedAtEqualPriority(t *testing.T) {
	q := saturated(t, -100)
	if n := drainSaturated(t, q, "retry", -100, 200); n < 0 {
		t.Fatal("a delayed item never came out of a queue that never empties")
	}
}

func TestExpiredDelay_KeepsItsPriority(t *testing.T) {
	q := saturated(t, 100)
	n := drainSaturated(t, q, "retry", -100, 200)
	if n != 0 {
		t.Fatalf("a high-priority item waited behind %d low-priority items", n)
	}
}

// Its turn comes when it is due, in order, so fixing the starvation must not turn a delay into a
// queue jump over items that were already waiting at the same priority.
func TestExpiredDelay_DoesNotJumpItsPriority(t *testing.T) {
	q := saturated(t, -100)
	if n := drainSaturated(t, q, "retry", -100, 200); n != 8 {
		t.Fatalf("want it after the 8 items queued before it, got position %d", n)
	}
}

// The control: a delay that has not passed still holds, however saturated the queue is.
func TestUnexpiredDelay_StillHolds(t *testing.T) {
	q := New[string]("starvation")
	t.Cleanup(q.ShutDown)
	q.AddWithOpts(AddOpts{After: time.Hour, Priority: 100}, "later")
	q.AddWithOpts(AddOpts{Priority: -100}, "now")

	if n := drainSaturated(t, q, "later", -100, 50); n != -1 {
		t.Fatalf("an item delayed by an hour was handed out at position %d", n)
	}
}
