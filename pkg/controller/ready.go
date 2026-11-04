package controller

import "errors"

// ReadyAssessed wraps err to declare that the handler set the Ready condition ITSELF during this
// call, so the runtime must leave it alone.
//
// # Why an explicit opt-out rather than a before/after comparison
//
// The runtime writes Ready=Unknown/ObserveFailed when an observation fails, because a stale
// Ready=True for a resource nobody successfully observed is silently wrong. But a handler may
// already have assessed the resource and written a FINDING before returning its error — and
// replacing a finding with an absence of finding is strictly worse information, in the one
// direction an operator cannot recover from.
//
// Measured in core-provider's cdc, which is this runtime's largest consumer: two sites in its
// Observe write Ready=False/Unavailable WITH a diagnostic message, persist it, and then return the
// error. One of them is the RBAC-rotation path whose entire value is that the message names the
// cause — a binding left pointing at a replaced ServiceAccount, which once wedged a composition for
// 22 hours and whose first symptom reads as a missing CRD. Overwriting that with "the external
// resource could not be observed" sends the reader somewhere else entirely.
//
// A SNAPSHOT CANNOT DETECT IT. The obvious implementation is to record Ready before calling the
// handler and compare after. That fails on a same-value write: a composition failing RBAC
// repeatedly writes Ready=False/Unavailable over an identical Ready=False/Unavailable, and no
// comparison of before against after can tell that the handler spoke. provider-runtime reached the
// same conclusion independently, from providers that set Ready and then errored; their equivalent
// is reconciler.ReadyAssessed, and this mirrors it deliberately so the two runtimes' contracts read
// the same way even though they share no code.
//
// The wrapped error is returned unchanged to every other caller: errors.Is, errors.As and Error()
// all behave as if it were not wrapped, so declaring the opt-out costs a handler nothing but the
// wrap.
func ReadyAssessed(err error) error {
	if err == nil {
		return nil
	}
	return readyAssessedError{err}
}

// IsReadyAssessed reports whether err (or anything it wraps) carries the opt-out.
func IsReadyAssessed(err error) bool {
	var target readyAssessedError
	return errors.As(err, &target)
}

type readyAssessedError struct{ error }

func (e readyAssessedError) Unwrap() error { return e.error }
