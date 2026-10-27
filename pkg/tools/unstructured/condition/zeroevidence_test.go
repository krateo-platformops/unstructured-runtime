package condition

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ZeroEvidence must be Unknown, not False. That is the whole point: "I could not assess this" is a
// third answer, and collapsing it into either of the other two asserts something unobserved.
//
// False is the damaging collapse. Consumers gating on readiness wait on False, and absence of
// evidence is normally a steady state rather than a transient one, so they would wait forever on a
// resource that is, as far as anyone knows, fine.
func TestZeroEvidence_IsUnknownAndNotFalse(t *testing.T) {
	c := ZeroEvidence()

	if c.Type != TypeReady {
		t.Errorf("Type = %q, want %q", c.Type, TypeReady)
	}
	if c.Status != metav1.ConditionUnknown {
		t.Errorf("Status = %q, want Unknown — False asserts unhealth we did not observe", c.Status)
	}
	if c.Reason != ReasonZeroEvidence {
		t.Errorf("Reason = %q, want %q — consumers key on the reason, so it is the contract",
			c.Reason, ReasonZeroEvidence)
	}
	if c.Reason != "ZeroEvidence" {
		t.Errorf("Reason string = %q; the installer chart's Ready gate matches the literal "+
			"\"ZeroEvidence\", so renaming it silently breaks that gate", c.Reason)
	}
}

// It must be distinguishable from every other Ready condition, because a consumer's ability to
// special-case it rests entirely on telling them apart — and on the REASON, since Creating and
// ZeroEvidence can both appear as a non-True Ready.
func TestZeroEvidence_IsDistinctFromEveryOtherReadyCondition(t *testing.T) {
	z := ZeroEvidence()

	for _, other := range []struct {
		name string
		cond metav1.Condition
	}{
		{"Available", Available()},
		{"Unavailable", Unavailable()},
		{"Creating", Creating()},
		{"Deleting", Deleting()},
	} {
		if z.Status == other.cond.Status && z.Reason == other.cond.Reason {
			t.Errorf("ZeroEvidence is indistinguishable from %s (status %q, reason %q)",
				other.name, z.Status, z.Reason)
		}
	}

	// The one it must differ from most sharply. Unavailable means observed-and-sick; this means
	// not observed. Sharing a status would make them one signal to anything reading status alone.
	if z.Status == Unavailable().Status {
		t.Error("ZeroEvidence shares Unavailable's status: 'not observed' and 'observed sick' " +
			"would be the same signal to any consumer that reads status without reading reason")
	}

	// Control arm: these fields must be capable of matching, or the comparisons above are vacuous.
	if a, b := Available(), Available(); a.Status != b.Status || a.Reason != b.Reason {
		t.Fatal("control arm: a constructor does not match itself, so the inequalities above prove nothing")
	}
}

// Upsert is how a condition reaches an object's status. ZeroEvidence must REPLACE an existing Ready
// rather than sit beside it — two Ready conditions is the contradictory status #119 was about.
func TestZeroEvidence_ReplacesAnExistingReadyCondition(t *testing.T) {
	conds := []metav1.Condition{Available()}
	Upsert(&conds, ZeroEvidence())

	if len(conds) != 1 {
		t.Fatalf("got %d conditions, want 1: a second Ready leaves the object self-contradictory", len(conds))
	}
	if conds[0].Reason != ReasonZeroEvidence {
		t.Errorf("Ready reason = %q, want %q", conds[0].Reason, ReasonZeroEvidence)
	}
	if conds[0].Status != metav1.ConditionUnknown {
		t.Errorf("Ready status = %q, want Unknown", conds[0].Status)
	}
}
