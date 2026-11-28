package controller

import (
	"fmt"

	"github.com/krateo-platformops/unstructured-runtime/pkg/errors"
)

// ErrCreateNotCommitted marks a create error the HANDLER knows the external system did not commit:
// a validation refusal (a 4xx), a request that never left the process. Only such an error carries the
// guarantee that the CREATE did not happen: the vendor refused, so nothing was created.
//
// THE RETRY IS NOT BLIND, since v1.34.0 (#130). This used to say a classified refusal was the only
// error that could be retried without looking first, and that was true when written. It no longer is:
// processItem now confirms the external resource is still absent BEFORE retrying, and ADOPTS it if
// it appeared in the meantime (a route this controller did not take can create it between the
// refusal and the retry). So "safe to retry" holds THROUGH that confirm, not unconditionally. Read
// this together with meta.ExternalCreateRefusedUnconfirmed, the predicate that arms it, and whose
// ties resolve in favour of "done" because the markers are second-granular.
//
// THE CONFIRM ADOPTS ONLY ON A DEFINITE YES. It is a top-level statement in processItem, after the
// rate-limit gate, guarded by !ExternalCreateIncomplete and that same predicate. If the
// existence check errors or cannot answer, it falls through to the previous blind-retry behaviour,
// by design: that is the original intent, a classified refusal keeps retrying directly rather than
// stopping where nothing can be observed. On a yes it stamps external-create-succeeded and returns
// nil, which ENDS the retry series rather than rescheduling it.
//
// WHY THE DISTINCTION (Diego, 2026-11-16). Any other error is OUTCOME-UNKNOWN: a 5xx, a timeout or a
// connection reset can arrive after the vendor committed. The runtime used to stamp every create
// error as external-create-failed, which made the failure the newest create marker, so
// ExternalCreateIncomplete was false, processItem's incomplete-create recovery was never asked, and
// the re-delivered Create POSTed again with no existence check in between. Measured with a fake
// Zendesk that stores the request and then answers 500: SIX tickets from one CR (acmp, against
// rest-dynamic-controller on unstructured-runtime v1.18.0; the path was unchanged at v1.21.1).
//
// Now an unclassified error leaves external-create-pending the newest marker, and the next pass
// confirms the negative before any retry: the resource exists → recorded as created, no second
// create; absent → re-observed through the recovery grace period, then created; the check cannot
// answer → refused with Ready=False/CreateOutcomeUnknown. A handler that can tell "not committed"
// returns CreateNotCommitted(err) so such failures keep retrying directly instead of waiting out the
// grace, or stopping where nothing can be observed.
var ErrCreateNotCommitted = errors.New("the external system did not commit the create")

// CreateNotCommitted wraps err as a create the external system did not commit (see
// ErrCreateNotCommitted). errors.Is(result, ErrCreateNotCommitted) is true, and err stays reachable
// through errors.Is/As.
func CreateNotCommitted(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrCreateNotCommitted, err)
}

// createCommittedNot reports whether a create error is classified as not committed.
func createCommittedNot(err error) bool { return errors.Is(err, ErrCreateNotCommitted) }
