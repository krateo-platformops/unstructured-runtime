package condition

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
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
	// THE STRING IS A CONSUMER CONTRACT, not an internal label. Renaming it is a BREAKING CHANGE
	// for at least two gates that match it exactly and fail closed on anything else:
	//
	//   krateo-platformops/installer  chart/templates/_helpers.tpl:299
	//     inst.readyFromConditions accepts Ready=True, OR Unknown with reason EXACTLY
	//     "ZeroEvidence", with a strict else. A different Unknown reason is read as not-ready and
	//     inst.depsReady holds back every dependent.
	//   krateo-platformops/installer  hack/ready-classify.sh:25-31
	//     same acceptance, same strict else.
	//
	// Failing closed is the behaviour those gates want — a converging child also reports Unknown
	// and MUST block. The consequence is that this string cannot be changed unilaterally: a rename
	// turns every zero-evidence composition into a blocked dependency chain, silently, until
	// someone notices dependents never appeared.
	//
	// So: if this is ever renamed, the installer gates change in the same change, and the installer
	// session gets told before it ships. Recorded at their request after they confirmed both call
	// sites.
	ReasonZeroEvidence string = "ZeroEvidence"

	// ReasonObserveFailed: the observation itself did not complete, so nothing assessed this
	// resource's health. Sibling in meaning to ZeroEvidence — both mean "we do not know" — but
	// distinct in cause: ZeroEvidence is "we looked and found nothing to read", ObserveFailed is
	// "we could not look".
	ReasonObserveFailed string = "ObserveFailed"
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

	// ReasonReconcileDeferred: the runtime CHOSE NOT TO CALL the handler this pass, because a
	// server expressed a rate limit for this object's endpoint. Nothing about the resource was
	// learned, so this is not an assessment — and it is not silence either.
	//
	// WHY NOT ObserveFailed OR ReconcileError. Those mean "we looked and it went badly". The
	// handler did not run, so either would be a false statement about state. And writing NOTHING
	// would be worse in a quieter way: a deferral that records nothing is indistinguishable from a
	// pass that ran and found everything fine, so on a long backoff the conditions go stale while
	// reporting healthy. core-provider hit that shape three separate times (#273, #331, #337).
	//
	// THE MESSAGE CARRIES THE DEADLINE, and that is not decoration. "Deferred" with no deadline
	// becomes a lie purely through elapsed time, which is the most common way a status field stops
	// being true without anything changing.
	//
	// THE STRING IS A CONSUMER CONTRACT and it is DISTINCT from every other reason here on
	// purpose. This package already carries a collision — ReasonReconcilePausedReady and
	// ReasonReconcilePaused are two identifiers with the single value "ReconcilePaused", so a
	// consumer reading a reason cannot tell Ready-paused from Synced-paused and only the condition
	// TYPE discriminates. That cost a wrong answer between two sessions on 2026-11-04. A new reason
	// must not add a second instance; see TestReconcileDeferredReasonIsDistinct.
	ReasonReconcileDeferred string = "ReconcileDeferred"
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

// WithMessage returns c with its Message set, leaving Type, Status and Reason untouched.
//
// WHY A MESSAGE ON Ready AT ALL. The runtime's failure paths write Ready (Creating/Deleting) beside
// Synced=ReconcileError. Synced carried the whole diagnostic and Ready carried an EMPTY message, so
// a reader who stopped at Ready — and Ready is the condition people watch, the one
// `kubectl wait --for=condition=Ready` names — saw `reason=Creating` and concluded "still working"
// about a create that would never succeed. Measured by the oasgen-provider session on 0.50.0: a
// ComputeFlavor wedged on a refused cross-host redirect, retrying forever, with the refused host
// and the permitted set stated in full on Synced and nowhere on Ready.
//
// WHY THE REASON MUST NOT CHANGE INSTEAD, which is the obvious alternative. IsConditionSet compares
// Type && Status && Reason (unstructured.go:274), and consumers use it as a LATCH GUARD —
// oasgen's restResources.go reads IsConditionSet(mg, condition.Creating()) for exactly that.
// Changing the reason breaks the guard and reintroduces their #347: a create rejected 422 left
// Creating set, a later branch read its absence as grounds to assume the resource was fine, set
// Available, and latched a CR permanently Ready=True for a resource that was never created. A
// silently-wrong Ready=True is worse than a confusing Ready=False.
//
// Message is NOT compared, so attaching one is free on that axis. The obstacle was never the field.
func WithMessage(c metav1.Condition, message string) metav1.Condition {
	c.Message = message
	return c
}

// ObserveFailed is the READINESS half of a failed observation: Ready=UNKNOWN.
//
// Before this, an Observe failure wrote Synced=ReconcileError and NOTHING to Ready, so Ready kept
// whatever it last held — including True/Available. A CR then reported ready for a resource nobody
// had successfully observed, indefinitely, for as long as the observation kept failing. That is a
// correct value that stopped being refreshed, which from outside is indistinguishable from a
// healthy one.
//
// UNKNOWN, NOT FALSE, and the distinction is the whole point. We did not observe the resource
// becoming unhealthy — we failed to observe it at all. False would report an outage of the resource
// when the outage is in our ability to look, and it would move every readiness-gating consumer on
// what may be a transient apiserver blip. Unknown says the true thing: nobody knows.
//
// WHY ObserveFailed AND NOT NotObserved, which provider-runtime uses for the same situation in its
// own (entirely separate) runtime: oasgen-provider already publishes an `Observed` condition whose
// False reason is NoResponseBody, meaning "the observe SUCCEEDED and returned no body". Two
// conditions about observation whose names agree and whose meanings do not would be read as one
// claim, and the narrower one would be lost. ObserveFailed names the failure — "we tried and could
// not" — which composes with "we succeeded and got nothing" instead of colliding with it.
//
// CONTRACT FOR CALLERS: write this BESIDE Synced=ReconcileError, never instead of it. Consumers
// read Synced's reason to decide whether the last reconcile errored (oasgen's lastReconcileErrored
// feeds their not-exists branch); dropping it would remove a guard that currently fires for exactly
// this condition. The two conditions are not redundant — they answer different questions.
func ObserveFailed(message string) metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionUnknown,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonObserveFailed,
		Message:            message,
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

// ReconcileDeferred returns the Synced condition for a pass the runtime chose not to make, because
// key's endpoint expressed a rate limit.
//
// Synced=False is correct and load-bearing: nothing is converging this object right now, which is
// the same semantic as the pause conditions — so a dependency gate that requires Synced=True
// already handles deferral with no change on its side.
//
// READY IS NOT TOUCHED by this helper, and callers must not touch it either. Readiness is the
// handler's finding and the handler did not run; overwriting it would repeat #45, where
// ObserveFailed overwrote a handler's own finding.
//
// until may be the zero time, which means "we are holding this back ourselves and nobody gave us a
// deadline". The message then says so rather than inventing one — a deadline we made up is read by
// a consumer as the server's.
func ReconcileDeferred(key string, until time.Time) metav1.Condition {
	// Two different statements, worded so neither can be read as the other. At capacity nothing has
	// refused us — the runtime is pacing itself — so "rate-limited" would send an operator looking
	// for a server-side limit that does not exist.
	msg := fmt.Sprintf("reconciliation deferred: the runtime is holding calls to %s below its "+
		"concurrency limit; no server deadline was given", key)
	if !until.IsZero() {
		// ROUNDED UP to the second. RFC3339 drops sub-second precision, and truncating would print an
		// instant up to a second BEFORE the server's deadline — a time at which a retry is still
		// refused.
		next := until.UTC().Truncate(time.Second)
		if next.Before(until) {
			next = next.Add(time.Second)
		}
		msg = fmt.Sprintf("reconciliation deferred: %s is rate-limited by the server; next attempt %s",
			key, next.Format(time.RFC3339))
	}
	return metav1.Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcileDeferred,
		Message:            msg,
	}
}

// LocalReconcileDeferred returns the Synced condition for a pass the runtime did not make because the
// controller's OWN local capacity was full (#66): its CPU is saturated, or the capacity has not yet
// risen back after a cut. The reason is ReconcileDeferred like the rate-limit case, so a gate that
// already handles deferral needs no change; the message says it is the controller itself, not a
// server, so nobody goes looking for a remote limit. Ready is not touched, for the same reason as
// ReconcileDeferred.
//
// cause is the saturated reading, the unstarted-sampler explanation, or empty when the capacity is
// simply full and recovering.
func LocalReconcileDeferred(capacity int, cause string) metav1.Condition {
	msg := fmt.Sprintf("reconciliation deferred: the controller's own local capacity (%d) is full and "+
		"rises as its CPU allows; no server limit is involved", capacity)
	if cause != "" {
		msg = fmt.Sprintf("reconciliation deferred: the controller's own local capacity (%d) is full: %s", capacity, cause)
	}
	return metav1.Condition{
		Type:               TypeSynced,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonReconcileDeferred,
		Message:            msg,
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
