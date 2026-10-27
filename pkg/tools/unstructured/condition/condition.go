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

func Upsert(conds *[]metav1.Condition, co metav1.Condition) {
	for idx, el := range *conds {
		if el.Type == co.Type {
			(*conds)[idx] = co
			return
		}
	}
	*conds = append(*conds, co)
}

func Join(conds *[]metav1.Condition, all []metav1.Condition) {
	for _, el := range *conds {
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
