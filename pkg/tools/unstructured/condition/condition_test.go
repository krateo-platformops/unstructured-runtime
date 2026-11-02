package condition

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUnavailable(t *testing.T) {
	expected := metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonUnavailable,
	}

	result := Unavailable()

	if result.Type != expected.Type {
		t.Errorf("Expected Type to be %s, but got %s", expected.Type, result.Type)
	}

	if result.Status != expected.Status {
		t.Errorf("Expected Status to be %s, but got %s", expected.Status, result.Status)
	}

	if result.Reason != expected.Reason {
		t.Errorf("Expected Reason to be %s, but got %s", expected.Reason, result.Reason)
	}
}
func TestCreating(t *testing.T) {
	expected := metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonCreating,
	}

	result := Creating()

	if result.Type != expected.Type {
		t.Errorf("Expected Type to be %s, but got %s", expected.Type, result.Type)
	}

	if result.Status != expected.Status {
		t.Errorf("Expected Status to be %s, but got %s", expected.Status, result.Status)
	}

	if result.Reason != expected.Reason {
		t.Errorf("Expected Reason to be %s, but got %s", expected.Reason, result.Reason)
	}
}
func TestFailWithReason(t *testing.T) {
	reason := "SomeReason"
	expected := metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
	}

	result := FailWithReason(reason)

	if result.Type != expected.Type {
		t.Errorf("Expected Type to be %s, but got %s", expected.Type, result.Type)
	}

	if result.Status != expected.Status {
		t.Errorf("Expected Status to be %s, but got %s", expected.Status, result.Status)
	}

	if result.Reason != expected.Reason {
		t.Errorf("Expected Reason to be %s, but got %s", expected.Reason, result.Reason)
	}
}
func TestDeleting(t *testing.T) {
	expected := metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonDeleting,
	}

	result := Deleting()

	if result.Type != expected.Type {
		t.Errorf("Expected Type to be %s, but got %s", expected.Type, result.Type)
	}

	if result.Status != expected.Status {
		t.Errorf("Expected Status to be %s, but got %s", expected.Status, result.Status)
	}

	if result.Reason != expected.Reason {
		t.Errorf("Expected Reason to be %s, but got %s", expected.Reason, result.Reason)
	}
}
func TestAvailable(t *testing.T) {
	expected := metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonAvailable,
	}

	result := Available()

	if result.Type != expected.Type {
		t.Errorf("Expected Type to be %s, but got %s", expected.Type, result.Type)
	}

	if result.Status != expected.Status {
		t.Errorf("Expected Status to be %s, but got %s", expected.Status, result.Status)
	}

	if result.Reason != expected.Reason {
		t.Errorf("Expected Reason to be %s, but got %s", expected.Reason, result.Reason)
	}
}
func TestUpsert(t *testing.T) {
	conds := []metav1.Condition{
		{
			Type:   "Condition1",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Condition2",
			Status: metav1.ConditionFalse,
		},
	}

	co := metav1.Condition{
		Type:   "Condition1",
		Status: metav1.ConditionFalse,
	}

	Upsert(&conds, co)

	found := false
	for _, c := range conds {
		if c.Type == co.Type {
			if c.Status != co.Status {
				t.Errorf("Expected Status to be %s, but got %s", co.Status, c.Status)
			}
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Condition %s not found", co.Type)
	}
}
func TestJoin(t *testing.T) {
	conds := []metav1.Condition{
		{
			Type:   "Condition1",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Condition2",
			Status: metav1.ConditionFalse,
		},
	}

	all := []metav1.Condition{
		{
			Type:   "Condition1",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Condition3",
			Status: metav1.ConditionUnknown,
		},
	}

	// Condition3 USED TO BE COMMENTED OUT HERE, with the count asserted as 2.
	//
	// That is the clearest evidence the bug was known: someone wrote Join intending it to add
	// Condition3, observed that it did not, and commented out the expectation rather than fixing
	// the function. The test then passed, documenting the defect as the contract (#36).
	//
	// Join ignored `all` entirely -- its body iterated *conds and upserted each element back into
	// itself. Nothing called it, so nothing noticed; but it is EXPORTED, so the next caller would
	// have got silence instead of an error.
	expected := []metav1.Condition{
		{
			Type:   "Condition1",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Condition2",
			Status: metav1.ConditionFalse,
		},
		{
			Type:   "Condition3",
			Status: metav1.ConditionUnknown,
		},
	}

	Join(&conds, all)

	if len(conds) != len(expected) {
		t.Errorf("Expected %d conditions, but got %d", len(expected), len(conds))
	}

	// Indexed over EXPECTED, not conds: the original indexed expected[i] while ranging conds, so a
	// conds longer than expected panicked with an index-out-of-range instead of failing the
	// assertion above. A test that panics on the failure it is meant to report is a test that
	// hides its own message.
	for i := range expected {
		if i >= len(conds) {
			break
		}
		if conds[i].Type != expected[i].Type {
			t.Errorf("Expected Type to be %s, but got %s", expected[i].Type, conds[i].Type)
		}

		if conds[i].Status != expected[i].Status {
			t.Errorf("Expected Status to be %s, but got %s", expected[i].Status, conds[i].Status)
		}
	}
}
func TestRemove(t *testing.T) {
	conds := []metav1.Condition{
		{
			Type:   "Condition1",
			Status: metav1.ConditionTrue,
		},
		{
			Type:   "Condition2",
			Status: metav1.ConditionFalse,
		},
		{
			Type:   "Condition3",
			Status: metav1.ConditionTrue,
		},
	}

	typ := "Condition2"

	Remove(&conds, typ)

	found := false
	for _, c := range conds {
		if c.Type == typ {
			found = true
			break
		}
	}

	if found {
		t.Errorf("Condition %s should have been removed", typ)
	}

	if len(conds) != 2 {
		t.Errorf("Expected 2 conditions, but got %d", len(conds))
	}
}

// #36 — Upsert must not re-stamp LastTransitionTime when the status has not transitioned.
//
// Before this, Upsert assigned the incoming condition wholesale, so every reconcile that merely
// RE-ASSERTED a condition moved its timestamp. A timestamp that moves without a transition is not
// slightly wrong — it is the opposite of what lastTransitionTime means, and an operator reading it
// concluded "became Synced just now" for a resource Synced for hours.
func TestUpsert_PreservesLastTransitionTimeWhenStatusUnchanged(t *testing.T) {
	began := metav1.NewTime(time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC))
	later := metav1.NewTime(time.Date(2026, 11, 2, 15, 30, 0, 0, time.UTC))

	for _, tc := range []struct {
		name      string
		stored    metav1.Condition
		incoming  metav1.Condition
		wantStamp metav1.Time
		because   string
	}{
		{
			name:      "same status, identical reason: keep the original instant",
			stored:    metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "Available", LastTransitionTime: began},
			incoming:  metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "Available", LastTransitionTime: later},
			wantStamp: began,
			because:   "re-asserting an unchanged condition is not a transition",
		},
		{
			name:      "same status, DIFFERENT reason: still not a transition",
			stored:    metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "Available", LastTransitionTime: began},
			incoming:  metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "AvailablePartialCoverage", LastTransitionTime: later},
			wantStamp: began,
			because: "per the Kubernetes API convention lastTransitionTime tracks STATUS changes; a " +
				"better explanation of an unchanged state is not a transition",
		},
		{
			name:      "status CHANGED: take the new instant",
			stored:    metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "Available", LastTransitionTime: began},
			incoming:  metav1.Condition{Type: "Synced", Status: metav1.ConditionFalse, Reason: "Unavailable", LastTransitionTime: later},
			wantStamp: later,
			because:   "this IS a transition, and preserving the old stamp would be the mirror-image bug",
		},
		{
			name:      "stored stamp is ZERO: do not preserve the absence",
			stored:    metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "Available"},
			incoming:  metav1.Condition{Type: "Synced", Status: metav1.ConditionTrue, Reason: "Available", LastTransitionTime: later},
			wantStamp: later,
			because: "carrying a zero timestamp forward is the plausible-value failure: 0001-01-01 " +
				"looks like a real instant rather than like missing data",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conds := []metav1.Condition{tc.stored}
			Upsert(&conds, tc.incoming)

			if len(conds) != 1 {
				t.Fatalf("expected the condition to be REPLACED not appended, got %d conditions — "+
					"the assertion below would otherwise read the wrong element", len(conds))
			}
			if !conds[0].LastTransitionTime.Equal(&tc.wantStamp) {
				t.Errorf("LastTransitionTime = %v, want %v: %s",
					conds[0].LastTransitionTime, tc.wantStamp, tc.because)
			}
			// The rest of the condition must still be replaced. Without this, "preserve the
			// timestamp" could be implemented as "ignore the incoming condition entirely", which
			// would pass every assertion above and lose the new reason and message.
			if conds[0].Reason != tc.incoming.Reason {
				t.Errorf("Reason = %q, want %q: the condition's CONTENT must still be updated; only "+
					"the transition instant is preserved", conds[0].Reason, tc.incoming.Reason)
			}
		})
	}
}
