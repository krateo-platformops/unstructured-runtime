package controller

import "errors"

// ExistenceUndetermined wraps an existence-check error to declare it TRANSIENT: the check could not
// answer on THIS pass, and a later pass may.
//
// # Why the handler has to say, and the runtime cannot work it out
//
// processItem's incomplete-create branch writes Ready=False/CreateOutcomeUnknown whenever the existence
// check errors. That reason is correct for a refusal the handler can never resolve -- it names a state a
// human must act on -- and wrong for one that resolves itself on the next pass.
//
// The runtime cannot tell them apart, because the distinction is a property of the HANDLER's own
// configuration. Measured on rest-dynamic-controller, whose Exists refuses for sixteen distinct reasons:
//
//	STRUCTURAL   observation is delegated to a RESTAction; the kind declares no verb to read with;
//	             a path parameter is sourced from status and was never recorded.
//	             None can change without an author editing the RestDefinition.
//	TRANSIENT    an async operation is still in flight; the resource is Pending; the read failed with
//	             a 5xx or a timeout; a list succeeded and matched nothing.
//
// All sixteen arrive here as a plain error. So the opt-in belongs with the handler, which is the only
// layer that knows which of its own refusals it just returned.
//
// # Why the DEFAULT stays structural
//
// An unmarked error behaves exactly as before: CreateOutcomeUnknown, which the cdc rolls up as failed.
// The opposite default is tempting and wrong -- a kind that genuinely cannot be observed would then
// requeue forever with no condition explaining why, which is precisely the defect #93 added
// CreateOutcomeUnknown to fix. Marking is an opt-in to a WEAKER claim, so it must be deliberate.
//
// # Why this is a refinement rather than a fix for an outage
//
// A transient refusal does not wedge the resource today: the digest dedupe is cleared at the top of
// processItem (worker.go, the only production items.Delete), so the periodic resync enqueues another
// Observe and the check runs again. An unmarked transient refusal is therefore a WRONG CONDITION for
// about one resync interval, not a permanent state.
//
// It is worth marking anyway, because the condition is what an operator acts on: a Ready=False naming a
// cause that reads as unrecoverable, on a resource that is about to converge by itself, invites a
// destructive intervention. The 180s default is long enough for someone to reach for the annotation.
//
// The wrapped error is returned unchanged to every other caller: errors.Is, errors.As and Error() all
// behave as if it were not wrapped, so declaring the opt-in costs a handler nothing but the wrap.
func ExistenceUndetermined(err error) error {
	if err == nil {
		return nil
	}
	return existenceUndeterminedError{err}
}

// IsExistenceUndetermined reports whether err (or anything it wraps) carries the opt-in.
func IsExistenceUndetermined(err error) bool {
	var target existenceUndeterminedError
	return errors.As(err, &target)
}

type existenceUndeterminedError struct{ error }

func (e existenceUndeterminedError) Unwrap() error { return e.error }
