package controller

import (
	"fmt"

	"github.com/krateo-platformops/unstructured-runtime/pkg/errors"
)

// ErrCreateNotCommitted marks a create error the HANDLER knows the external system did not commit:
// a validation refusal (a 4xx), a request that never left the process. Only such an error is safe to
// retry blindly.
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
