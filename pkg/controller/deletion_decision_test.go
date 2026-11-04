package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
)

// #47. handleObserve takes TWO reads of the object: `el` before the handler runs, and `e` on the
// error path as the write target for the conditions. The deletion decision used to come from the
// SECOND one, so a deletion timestamp landing between them made the pass internally inconsistent —
// the dispatch was chosen for a live object and the condition written below described a deleting
// one, with nothing to reconcile the halves.
//
// WHAT THIS TEST CAN AND CANNOT DO. It cannot drive handleObserve with two reads that disagree
// without a fake client that returns different objects on successive Gets, which this package's
// fixtures do not provide. What it CAN pin is the property the fix rests on: the two objects are
// genuinely distinguishable by meta.WasDeleted, so reading the wrong one is a real difference and
// not a distinction without one. Without this, "the decision comes from `el`" is unfalsifiable
// here — the two could have agreed in every reachable state and the fix would be inert.
//
// Stated plainly rather than dressed up: this is a necessary condition for the defect, not a
// reproduction of it. The reproduction needs the two-read fixture, and that is recorded on #47.
func TestWasDeleted_DistinguishesTheTwoReadsThePassCanSee(t *testing.T) {
	live := &unstructured.Unstructured{Object: map[string]any{}}
	live.SetName("fw")
	live.SetNamespace("krateo-system")

	deleting := live.DeepCopy()
	now := metav1.NewTime(time.Now())
	deleting.SetDeletionTimestamp(&now)

	if meta.WasDeleted(live) {
		t.Fatal("an object with no deletionTimestamp reads as deleted, so the two reads this fix " +
			"distinguishes are indistinguishable and the fix measures nothing")
	}
	if !meta.WasDeleted(deleting) {
		t.Fatal("an object WITH a deletionTimestamp reads as live; same problem in the other " +
			"direction")
	}

	// AND THE DEEP COPY MUST NOT CARRY IT BACK. The two reads in handleObserve are separate Gets,
	// but a fixture built by copying would share state silently — which would make a future
	// two-read test pass for the wrong reason.
	if meta.WasDeleted(live) {
		t.Error("setting the deletionTimestamp on the copy changed the original: a fixture built " +
			"this way cannot represent two reads that disagree")
	}
}
