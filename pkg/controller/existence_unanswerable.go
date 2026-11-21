package controller

import "errors"

// ErrExistenceUnanswerable is the PERMANENT counterpart of ExistenceUndetermined (#119): a handler
// returns it, wrapped with ExistenceUnanswerable, when "does the external resource exist?" can NEVER be
// answered for this object, on this pass or any later one.
//
// # When a handler may return it
//
// Only when there is no way to ask at all, and nothing short of a change to the object or to the
// handler's own configuration could create one. The case that motivated it (rest-dynamic-controller,
// measured on two vendors in #119): the kind's read addresses the resource by a server-assigned id, the
// create's response that carried the id was never recorded, and the kind declares no lookup (findby)
// to find it another way. No request can be built, so no request is sent, ever.
//
// # When it must NOT
//
// Anything that could answer on a later pass is TRANSIENT and is ExistenceUndetermined (or an unmarked
// error): a 5xx, a timeout, a transport failure, an asynchronous operation still in flight, a resource
// reported Pending, a list that succeeded and matched nothing. Marking one of those unanswerable would,
// on deletion, release the finalizer of an object whose external resource was about to become
// checkable.
//
// # What the runtime does with it (Diego, 2026-11-21, #119)
//
//   - not being deleted: exactly what it does for any unanswered check. The create-pending marker
//     stays, no create is retried (so no duplicate can be made), and Ready=False/CreateOutcomeUnknown
//     says the outcome can never be determined and names the two ways out: verify by hand and remove
//     krateo.io/external-create-pending, or delete the object.
//   - being deleted: the finalizer is released WITHOUT calling the handler's Delete, the external
//     resource is left untouched (it could not be addressed for a Delete anyway), and a Warning event
//     names the kind and the cause. That is the outcome a deletion-policy of orphan already produces.
//
// A transient or unmarked error keeps today's behaviour on both paths: wait.
var ErrExistenceUnanswerable = errors.New("existence of the external resource can never be determined for this object")

// ExistenceUnanswerable wraps an existence-check error to declare it PERMANENT (see
// ErrExistenceUnanswerable for exactly when). errors.Is(result, ErrExistenceUnanswerable) is true, and
// the handler's own error stays reachable with errors.Is and errors.As.
func ExistenceUnanswerable(err error) error {
	if err == nil {
		return nil
	}
	return existenceUnanswerableError{err}
}

// IsExistenceUnanswerable reports whether err (or anything it wraps) declares the existence check
// permanently unanswerable.
func IsExistenceUnanswerable(err error) bool {
	return errors.Is(err, ErrExistenceUnanswerable)
}

type existenceUnanswerableError struct{ error }

func (e existenceUnanswerableError) Unwrap() []error {
	return []error{ErrExistenceUnanswerable, e.error}
}

// Error keeps the handler's own message: declaring the refusal permanent must not change what the
// logs and conditions say its cause is.
func (e existenceUnanswerableError) Error() string { return e.error.Error() }
