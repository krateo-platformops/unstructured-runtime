package controller

import (
	"fmt"
	"net/http"
)

// ReadyzCheck reports whether this controller can see the resource it reconciles. It returns nil
// once the informer has completed its FIRST SUCCESSFUL LIST of the controller's GVR, and an error
// before that.
//
// That one fact is what tells a working controller from a blind one. A controller whose list keeps
// failing ("failed to list *unstructured.Unstructured: the server could not find the requested
// resource", e.g. because its CRD version is no longer served) never reaches it, so it stays
// not-ready for as long as the list fails, instead of reporting Ready because its process started
// (krateo-platformops/core-provider#333).
//
// The signature is func(*http.Request) error, the shape of controller-runtime's healthz.Checker, so
// a binary can mount it under any health handler without an adapter. The runtime deliberately does
// NOT serve it: which port, which path, and whether to serve at all are part of the contract between
// a binary and the Deployment that probes it, and neither of those lives here.
//
// It is safe to call at any time, from any goroutine, including before Run: the informer is not
// running yet then, so the check reports not-ready, which is the truth.
//
// What it does NOT answer:
//
//   - Liveness. A failing list or watch is a recoverable state (a CRD version briefly unserved
//     mid-bump), and restarting the process does not repair it. Do not mount this as a liveness
//     check; a liveness endpoint should answer from the process alone.
//   - Whether the resource is STILL listable. The informer's synced state latches: once the first
//     list has succeeded it stays true, so a resource that stops being served after that point
//     leaves this check passing while the reflector logs "Failed to watch". A restart then lists
//     afresh and does report it.
func (c *Controller) ReadyzCheck(_ *http.Request) error {
	if c.informer.HasSynced() {
		return nil
	}
	return fmt.Errorf("informer for %s has not completed its first successful list", c.gvr.String())
}
