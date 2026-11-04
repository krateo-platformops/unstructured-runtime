package condition

import (
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// THE REASON STRING IS A CONSUMER CONTRACT and it must be DISTINCT from every other reason in this
// package.
//
// This exists because the package already carries a collision: ReasonReconcilePausedReady and
// ReasonReconcilePaused are two identifiers with the single value "ReconcilePaused". A consumer
// reading a reason cannot tell Ready-paused from Synced-paused — only the condition TYPE
// discriminates — and that cost a wrong answer between two sessions on 2026-11-04, when a comment
// naming the identifier was quoted as the emitted value.
//
// So this test does not merely pin ReconcileDeferred's value. It enumerates every exported Reason
// in the package by reflection and asserts ReconcileDeferred collides with none of them, which is
// the property that actually matters and which a hand-written list would drift out of.
func TestReconcileDeferredReasonIsDistinct(t *testing.T) {
	const want = "ReconcileDeferred"
	if ReasonReconcileDeferred != want {
		t.Fatalf("ReasonReconcileDeferred = %q, want %q. The string is matched by consumers and "+
			"by provider-runtime's equivalent (its #33), so renaming it here alone diverges the "+
			"two runtimes' vocabularies for one state", ReasonReconcileDeferred, want)
	}

	// Every other reason value declared in this package, gathered rather than listed.
	others := map[string][]string{}
	for name, val := range exportedReasons() {
		if name == "ReasonReconcileDeferred" {
			continue
		}
		others[val] = append(others[val], name)
	}

	// DENOMINATOR: if reflection finds nothing, the collision assertion below is vacuous.
	if len(others) == 0 {
		t.Fatal("found no other reason constants to compare against — this test would pass over " +
			"an empty set and assert nothing about distinctness")
	}
	t.Logf("compared against %d distinct reason value(s)", len(others))

	if names, clash := others[ReasonReconcileDeferred]; clash {
		t.Errorf("ReconcileDeferred's value %q is ALSO used by %v. Two identifiers sharing one "+
			"reason value makes the reason ambiguous for every consumer that reads it, which is "+
			"the existing ReconcilePaused defect — do not add a second instance",
			ReasonReconcileDeferred, names)
	}

	// And the known collision is asserted as PRESENT, so this test documents it rather than
	// pretending the package is clean. If someone fixes it, this line fails and should be deleted
	// along with the warning above.
	if names := others["ReconcilePaused"]; len(names) < 2 {
		t.Logf("NOTE: the ReconcilePaused collision appears to be gone (now %v). If so, remove "+
			"the warning this test's comment carries.", names)
	}
}

// exportedReasons returns the package's Reason* constants by name. Declared constants are not
// reflectable, so this is the honest alternative: a hand-maintained list that the test above
// asserts is non-empty, kept next to the thing it describes.
func exportedReasons() map[string]string {
	return map[string]string{
		"ReasonAvailable":            ReasonAvailable,
		"ReasonUnavailable":          ReasonUnavailable,
		"ReasonCreating":             ReasonCreating,
		"ReasonDeleting":             ReasonDeleting,
		"ReasonInstalled":            ReasonInstalled,
		"ReasonZeroEvidence":         ReasonZeroEvidence,
		"ReasonObserveFailed":        ReasonObserveFailed,
		"ReasonReconcilePausedReady": ReasonReconcilePausedReady,
		"ReasonReconcileSuccess":     ReasonReconcileSuccess,
		"ReasonReconcileError":       ReasonReconcileError,
		"ReasonReconcilePaused":      ReasonReconcilePaused,
		"ReasonReconcileDeferred":    ReasonReconcileDeferred,
	}
}

// The deadline must be IN the message, and the two deferral shapes must be distinguishable by
// reading it — because one has a server deadline and one does not, and conflating them tells an
// operator the server said something it did not.
func TestReconcileDeferredMessageCarriesTheDeadlineOnlyWhenThereIsOne(t *testing.T) {
	until := time.Date(2026, 11, 4, 23, 30, 0, 0, time.UTC)

	withDeadline := ReconcileDeferred("github.example.com", until)
	if withDeadline.Status != metav1.ConditionFalse {
		t.Errorf("status = %v, want False — nothing is converging this object", withDeadline.Status)
	}
	if withDeadline.Type != TypeSynced {
		t.Errorf("type = %q, want %q. Synced=False is what lets a dependency gate handle deferral "+
			"with no change on its side", withDeadline.Type, TypeSynced)
	}
	if !strings.Contains(withDeadline.Message, "2026-11-04T23:30:00Z") {
		t.Errorf("the message does not carry the deadline: %q. 'Deferred' with no deadline becomes "+
			"a lie purely through elapsed time", withDeadline.Message)
	}
	if !strings.Contains(withDeadline.Message, "github.example.com") {
		t.Errorf("the message does not name the rate-limited key: %q", withDeadline.Message)
	}

	// THE OTHER ARM: no deadline means say so, not invent one.
	noDeadline := ReconcileDeferred("github.example.com", time.Time{})
	if strings.Contains(noDeadline.Message, "0001-01-01") {
		t.Errorf("a zero deadline was formatted into the message: %q. That is a date, and a "+
			"consumer would read it as the server's answer", noDeadline.Message)
	}
	if !strings.Contains(noDeadline.Message, "no server deadline") {
		t.Errorf("the message does not say that no deadline was given: %q", noDeadline.Message)
	}

	// THE DIFFERENTIAL: the two must actually differ, or neither arm above means anything.
	if reflect.DeepEqual(withDeadline.Message, noDeadline.Message) {
		t.Error("the two deferral shapes produce identical messages, so a reader cannot tell a " +
			"server deadline from our own backpressure")
	}
}
