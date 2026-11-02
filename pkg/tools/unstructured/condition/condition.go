package condition

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types.
const (
	// TypeReady resources are believed to be ready to handle work.
	TypeReady string = "Ready"

	// TypeSynced resources are believed to be in sync with the
	// Kubernetes resources that manage their lifecycle.
	TypeSynced string = "Synced"
)

// Reasons a resource is or is not ready.
const (
	ReasonAvailable   string = "Available"
	ReasonUnavailable string = "Unavailable"
	ReasonCreating    string = "Creating"
	ReasonDeleting    string = "Deleting"
	ReasonInstalled   string = "Installed"
	// ReasonZeroEvidence: readiness could not be assessed AT ALL — every signal the controller
	// knows how to read was absent, not negative. See ZeroEvidence.
	ReasonZeroEvidence string = "ZeroEvidence"
	// ReasonReconcilePausedReady: Ready is being reported for a resource whose reconciliation is
	// PAUSED, so the value is the last one observed and is not being refreshed. Status stays True —
	// a paused resource is still available, it just is not converging (core-provider#241).
	ReasonReconcilePausedReady string = "ReconcilePaused"
)

// Reasons a resource is or is not synced.
const (
	ReasonReconcileSuccess string = "ReconcileSuccess"
	ReasonReconcileError   string = "ReconcileError"
	ReasonReconcilePaused  string = "ReconcilePaused"
)

func Unavailable() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonUnavailable,
	}
}

// Creating returns a condition that indicates the resource is currently
// being created.
func Creating() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonCreating,
	}
}

// ReadyPaused returns the Ready condition for a resource whose reconciliation is paused.
//
// Status stays TRUE: a paused resource is still available, it simply is not converging, and flipping
// it to False would make a deliberate pause look like an outage and could stall every dependent.
// What changes is the REASON, so that a consumer able to read it can tell the two apart
// (core-provider#241).
//
// This exists because the paused path is the one place the runtime must speak for Ready. Everywhere
// else the handler owns the types it returns and the runtime owns only Synced — but when paused the
// runtime short-circuits BEFORE calling the handler, so there is no handler verdict to defer to and
// Ready would otherwise keep whatever value was written before the pause. Measured on krateo-057: a
// composition carrying krateo.io/paused reported Ready=True/Available with Synced=False/
// ReconcilePaused beside it, because ReconcilePaused only ever set Synced.
//
// That stale Available is not a wrong computation — it is a correct value that stopped being
// refreshed, which is indistinguishable from a healthy one from outside.
func ReadyPaused() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcilePausedReady,
	}
}

func FailWithReason(reason string) metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
	}
}

// Deleting returns a condition that indicates the resource is currently
// being deleted.
func Deleting() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonDeleting,
	}
}

// Available returns a condition that indicates the resource is
// currently observed to be available for use.
func Available() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonAvailable,
	}
}

// ZeroEvidence returns a condition that indicates readiness could not be assessed at all: every
// signal the controller knows how to read was ABSENT, so neither True nor False is supportable and
// the status is Unknown.
//
// It is NOT "unhealthy" and NOT a milder Unavailable. Unavailable means the resource was observed
// and found sick; this means it was not observed. The two call for opposite handling: a consumer
// gating on readiness must wait on Unavailable, while waiting on ZeroEvidence would wait forever,
// because absence of evidence is normally a steady state rather than a transient one.
//
// The trap it exists to avoid, and the reason the status is Unknown rather than False: the
// composition child-health rollup counts children whose kind it cannot interpret and, when EVERY
// child is uninterpretable, has been reporting Available over nothing at all
// (krateo-platformops/core-provider#165). Reporting False instead would be just as untrue and far
// more damaging — consumers gating on readiness would block indefinitely on resources that are, as
// far as anyone knows, fine.
//
// The REASON is the contract, not the status. A consumer that can distinguish this case may treat
// it as passable; one that cannot keeps testing for True and is no worse off than before, which is
// what makes this additive rather than a change to readiness semantics. Consumers must key on the
// reason — never on bare Unknown, since a genuinely converging resource also reports Unknown and
// must keep blocking, and never on the message text, which is prose.
func ZeroEvidence() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionUnknown,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonZeroEvidence,
	}
}

// Upsert replaces the condition of co's Type, or appends it.
//
// IT PRESERVES LastTransitionTime WHEN Status IS UNCHANGED (#36). Before this it assigned co
// wholesale, so every reconcile that merely RE-ASSERTED a condition re-stamped the timestamp — and
// a timestamp that moves without a transition is not a slightly-wrong timestamp, it is the opposite
// of what the field means. An operator reading it concluded "became Synced just now" for a resource
// that had been Synced for hours.
//
// Keyed on Status alone, deliberately, per the Kubernetes API convention: lastTransitionTime is
// "the last time the condition transitioned from one status to another". A reason or message change
// at the SAME status is not a transition — it is a better explanation of an unchanged state — so it
// updates the text and leaves the timestamp where the transition actually happened.
//
// This does not make a caller's write idempotent. The condition is still replaced, so the object
// still changes and resourceVersion still moves; what stops is the LIE about when the state began.
// Avoiding the write entirely is the caller's job, and a caller that compares reason/message/status
// before writing (core-provider's setReadyCondition) is the complementary half — see
// core-provider#298, where three phases supplied three messages for one state so that guard could
// never fire.
func Upsert(conds *[]metav1.Condition, co metav1.Condition) {
	for idx, el := range *conds {
		if el.Type != co.Type {
			continue
		}
		if el.Status == co.Status && !el.LastTransitionTime.IsZero() {
			// Same status: this is not a transition. Keep the moment it became true.
			//
			// The IsZero guard matters — a stored condition whose lastTransitionTime failed to
			// parse, or was never set, must NOT have that absence preserved. Carrying a zero
			// timestamp forward would be the "plausible value" failure: it looks like a real
			// instant (0001-01-01) rather than like missing data, and nothing downstream can
			// tell the difference.
			co.LastTransitionTime = el.LastTransitionTime
		}
		(*conds)[idx] = co
		return
	}
	*conds = append(*conds, co)
}

// Join upserts every condition in all into conds.
//
// IT USED TO IGNORE `all` ENTIRELY: the body iterated *conds and upserted each element back into
// itself, so it was a no-op with an unused parameter. Nothing in this module called it (control:
// Upsert has 2 call sites), so the bug was invisible — but the function is EXPORTED, and the next
// caller would have got silence rather than an error.
//
// Fixed rather than deleted: removing a published symbol breaks any consumer I cannot see, while
// making it do what its name and signature already promise breaks nobody.
func Join(conds *[]metav1.Condition, all []metav1.Condition) {
	for _, el := range all {
		Upsert(conds, el)
	}
}

func Remove(conds *[]metav1.Condition, typ string) {
	for idx, el := range *conds {
		if el.Type == typ {
			*conds = append((*conds)[:idx], (*conds)[idx+1:]...)
			return
		}
	}
}

// ReconcileSuccess returns a condition indicating that the provider successfully
// completed the most recent reconciliation of the resource.
func ReconcileSuccess() metav1.Condition {
	return metav1.Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcileSuccess,
	}
}

// ReconcileError returns a condition indicating that the provider encountered an
// error while reconciling the resource. This could mean that the provider was
// unable to update the resource to reflect its desired state, or that
// was unable to determine the current actual state of the resource.
func ReconcileError(err error) metav1.Condition {
	return metav1.Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcileError,
		Message:            err.Error(),
	}
}

// ReconcilePaused returns a condition that indicates reconciliation on
// the managed resource is paused via the pause annotation.
func ReconcilePaused() metav1.Condition {
	return metav1.Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcilePaused,
	}
}
