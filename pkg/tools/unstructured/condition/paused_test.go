package condition

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// THE CASE THE CHANGE EXISTS FOR (core-provider#241). A paused resource must be DISTINGUISHABLE on
// Ready, not merely on Synced.
//
// Before this, ReconcilePaused set only Synced, so Ready kept whatever the handler wrote before the
// pause. Measured on krateo-057: a composition carrying krateo.io/paused reported
// Ready=True/Available beside Synced=False/ReconcilePaused. 66 of 185 compositions on one production
// estate are in that state, and a consumer reading Ready could not tell them from converging ones.
func TestReadyPaused_IsReadyTypedAndNamesThePause(t *testing.T) {
	c := ReadyPaused()

	if c.Type != TypeReady {
		t.Errorf("must be a Ready condition or it cannot displace the stale one, got Type=%q", c.Type)
	}
	if c.Reason == ReasonAvailable {
		t.Error("the reason must not be Available — that is exactly the value being replaced")
	}
	if c.Reason != ReasonReconcilePausedReady {
		t.Errorf("Reason = %q, want %q", c.Reason, ReasonReconcilePausedReady)
	}
}

// THE CONTROL, and it is the whole of the design decision. Status stays TRUE.
//
// A paused resource is still available; it is simply not converging. Flipping it to False would make
// a deliberate pause look like an outage and could stall every dependent — which is the verdict
// change #241 leaves open and this deliberately is not. Without this assertion, "make pause
// visible" slides into "make pause not-ready" and nothing catches it.
func TestReadyPaused_StaysTrueBecauseAPauseIsNotAnOutage(t *testing.T) {
	if got := ReadyPaused().Status; got != metav1.ConditionTrue {
		t.Errorf("Status = %q, want True — a paused resource is available but not converging, and "+
			"reporting False would make a deliberate pause indistinguishable from a failure", got)
	}
}

// The two conditions describe different axes and must coexist: Synced says "I am not reconciling",
// Ready says "what I last observed was healthy, and I am not refreshing it". Asserting both are
// emitted together is what stops a later change from dropping one.
func TestPausedEmitsBothAxes(t *testing.T) {
	synced, ready := ReconcilePaused(), ReadyPaused()

	if synced.Type == ready.Type {
		t.Fatalf("the two conditions must be on different types, both are %q", synced.Type)
	}
	if synced.Type != TypeSynced {
		t.Errorf("ReconcilePaused must be Synced-typed, got %q", synced.Type)
	}
	if synced.Status != metav1.ConditionFalse {
		t.Errorf("Synced must be False while paused, got %q", synced.Status)
	}
	// Same reason string on both axes is intentional: it is the same fact reported about two
	// different questions, and an operator grepping for it finds both.
	if synced.Reason != ready.Reason {
		t.Errorf("both axes should name the pause identically for grep-ability, got %q and %q",
			synced.Reason, ready.Reason)
	}
}
