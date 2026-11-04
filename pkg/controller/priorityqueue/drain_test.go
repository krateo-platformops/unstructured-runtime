package priorityqueue

import (
	"testing"
	"time"
)

// TestShutDown_WakesParkedWorker proves the graceful-drain foundation: a worker parked in
// GetWithPriority with no items available must be woken (return shutdown=true) by ShutDown().
// Before the fix, GetWithPriority blocked on a bare <-w.get with no shutdown branch, so a bounded
// drain would hang on every shutdown until its timeout.
func TestShutDown_WakesParkedWorker(t *testing.T) {
	q := New[int]("")

	got := make(chan bool, 1)
	go func() {
		_, _, shutdown := q.GetWithPriority()
		got <- shutdown
	}()

	// Let the goroutine reach the parked select before shutting down (target the wakeup path,
	// not the entry-time shutdown check).
	time.Sleep(50 * time.Millisecond)
	q.ShutDown()

	select {
	case shutdown := <-got:
		if !shutdown {
			t.Fatalf("parked GetWithPriority returned shutdown=false; want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked GetWithPriority was not woken by ShutDown — a bounded drain would hang")
	}
}

// TestShutDown_NoWedge_WhenSpinMidHandoff reproduces the teardown race the adversarial review
// caught: a worker parked in GetWithPriority, an item being handed out by spin() (a blocking send),
// and a concurrent ShutDown. Before the fix the parked worker could wake on w.done instead of
// receiving, leaving spin() blocked forever on the send while holding BOTH queue mutexes — which
// then hangs every later Len()/Done() and the whole drain. With the selectable send, ShutDown must
// never wedge: Len()/Done() must return promptly.
func TestShutDown_NoWedge_WhenSpinMidHandoff(t *testing.T) {
	for i := 0; i < 300; i++ {
		q := New[int]("")

		go func() { _, _, _ = q.GetWithPriority() }() // park a waiter
		time.Sleep(50 * time.Microsecond)             // let it park so spin will attempt a hand-out
		q.Add(i)                                      // spin picks the item and attempts the send
		q.ShutDown()                                  // race: close(w.done) vs spin's send

		done := make(chan struct{})
		go func() {
			_ = q.Len() // needs w.lock
			q.Done(i)   // needs w.lockedLock
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Len()/Done() blocked — spin() wedged the queue locks on shutdown", i)
		}
	}
}

// TestShutDown_Idempotent proves ShutDown can be called more than once (the drain path calls it
// explicitly and a deferred cleanup calls it again) without a close-of-closed-channel panic.
func TestShutDown_Idempotent(t *testing.T) {
	q := New[int]("")
	q.ShutDown()
	q.ShutDown() // must not panic (CAS-guarded close of w.done)

	_, _, shutdown := q.GetWithPriority()
	if !shutdown {
		t.Fatalf("GetWithPriority after ShutDown returned shutdown=false")
	}
}
