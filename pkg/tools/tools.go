package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	contextutils "github.com/krateo-platformops/unstructured-runtime/pkg/context"
	"github.com/krateo-platformops/unstructured-runtime/pkg/pluralizer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

type UpdateOptions struct {
	Pluralizer    pluralizer.PluralizerInterface
	DynamicClient dynamic.Interface
}

const (
	ErrNilPluralizer = "pluralizer cannot be nil"
	ErrNilDynamic    = "dynamic client cannot be nil"
)

// Update updates the given unstructured object using the provided dynamic client and pluralizer. It returns the updated object or an error if the update fails.
// In case of error the original object is returned along with the error.
func Update(ctx context.Context, el *unstructured.Unstructured, opts UpdateOptions) (*unstructured.Unstructured, error) {
	if opts.Pluralizer == nil {
		return el, fmt.Errorf(ErrNilPluralizer)
	}
	if opts.DynamicClient == nil {
		return el, fmt.Errorf(ErrNilDynamic)
	}
	gvr, err := opts.Pluralizer.GVKtoGVR(el.GroupVersionKind())
	if err != nil {
		return el, err
	}

	res, err := opts.DynamicClient.Resource(gvr).
		Namespace(el.GetNamespace()).
		Update(ctx, el, metav1.UpdateOptions{
			FieldValidation: "Ignore",
		})
	if err != nil {
		return el, err
	}

	return res, nil
}

// ErrStatusConflict reports that a status write was refused because the object changed since the
// caller read it. The status the caller computed is therefore based on a stale read, and must not
// land. Match it with errors.Is; the apiserver's conflict error is wrapped alongside it, so
// apierrors.IsConflict still matches too.
var ErrStatusConflict = errors.New("status write conflicted with a newer write")

// UpdateStatus writes the status subresource ONCE. It returns the updated object, or `el` and an
// error.
//
// A CONFLICT IS RETURNED AS ErrStatusConflict, NEVER RETRIED WITH THE CALLER'S BODY (core-provider
// #359). It used to retry (#30): on a 409 it re-read the object and put the caller's INTENDED status
// onto it. That is last-writer-wins with a stale writer. The caller computed that status from a read
// the conflict had just proven stale, so re-submitting it replaced whatever the newer write had
// recorded. Measured on a snowplow bench cluster: a concurrent pass that read a composition before
// Create recorded its inventory re-submitted its stale status after the 409, and wiped
// status.managed (57 children).
//
// #30's GOAL STAYS, AT THE RIGHT LAYER. The retry existed because a returned 409 was recorded on the
// object as a ReconcileError, turning a transient race into a visible error (core-provider#258).
// The controller now handles ErrStatusConflict like a deferral: a prompt requeue, with no
// ReconcileError, no failure count, and no maxRetries budget spent. The next pass re-reads the object
// and computes a status that includes the newer write.
func UpdateStatus(ctx context.Context, el *unstructured.Unstructured, opts UpdateOptions) (*unstructured.Unstructured, error) {
	if opts.Pluralizer == nil {
		return el, fmt.Errorf(ErrNilPluralizer)
	}
	if opts.DynamicClient == nil {
		return el, fmt.Errorf(ErrNilDynamic)
	}
	gvr, err := opts.Pluralizer.GVKtoGVR(el.GroupVersionKind())
	if err != nil {
		return el, err
	}

	updated, err := opts.DynamicClient.Resource(gvr).Namespace(el.GetNamespace()).
		UpdateStatus(ctx, el, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsConflict(err) {
			return el, fmt.Errorf("%w: %w", ErrStatusConflict, err)
		}
		return el, err
	}
	return updated, nil
}

// PatchStatusConditions writes ONLY `status.conditions`, as a JSON merge patch on the status
// subresource. It is the write the RUNTIME should use; `UpdateStatus` stays for handlers, which own
// whole status stanzas and must keep the ability to REMOVE a status field.
//
// WHY THIS EXISTS (issue #33). The runtime owns exactly one status field: `status.conditions` —
// `worker.go`'s only status mutation is `SetConditions`. Everything else in the stanza (`managed`,
// `managedDigest`, `digest`, `previousManagedCount`, and every blueprint `statusDataTemplate`
// field) belongs to the handler. But every runtime status write did read-stanza -> set-conditions ->
// write-WHOLE-stanza, so a handler write that landed between the fetch and the write was reverted:
// the runtime resent a stanza in which the handler's field simply was not there. There is no
// per-object single-flight to prevent the interleaving — `priorityqueue.go` keys its `locked` set on
// the whole Event struct (EventType and QueuedAt included), so an Observe and a Create for the same
// object are distinct keys and run concurrently on different workers.
//
// A patch body containing only `conditions` CANNOT CARRY THE ABSENCE of a sibling field, so the
// interleaving stops mattering for siblings instead of merely being made less likely by a re-fetch.
//
// NO CONFLICT RETRY HERE, deliberately. A merge patch carries no resourceVersion precondition, so
// this call cannot return a 409 and there is nothing to retry — the retry that `UpdateStatus` needs
// (#30) is a consequence of optimistic concurrency on a full-object write, and that same retry is
// what widened this defect: on conflict it re-reads live and re-applies the stanza captured BEFORE
// the other writer. `UpdateStatus` keeps that behaviour unchanged.
//
// Within `conditions` this is still last-writer-wins: JSON merge patch replaces arrays wholesale.
// That is not a regression — the full-stanza write replaced the array too. The blast radius shrinks
// from "all of status" to "status.conditions" and never grows. Server-Side Apply would be the
// textbook fix and is unavailable: the conditions schema plumbing's crdgen emits declares neither
// `x-kubernetes-list-type: map` nor `x-kubernetes-list-map-keys`, so the array is atomic to SSA and
// it would additionally conflict with the handler's field manager.
func PatchStatusConditions(ctx context.Context, el *unstructured.Unstructured, opts UpdateOptions) (*unstructured.Unstructured, error) {
	if opts.Pluralizer == nil {
		return el, fmt.Errorf(ErrNilPluralizer)
	}
	if opts.DynamicClient == nil {
		return el, fmt.Errorf(ErrNilDynamic)
	}
	gvr, err := opts.Pluralizer.GVKtoGVR(el.GroupVersionKind())
	if err != nil {
		return el, err
	}

	// Read the RAW slice rather than going through unstructuredtools.GetConditions, which parses into
	// []metav1.Condition. Raw is both faithful — it sends back exactly what SetConditions wrote — and
	// it sidesteps the parse hazard that makes the guard below necessary in the first place.
	conditions, _, err := unstructured.NestedSlice(el.Object, "status", "conditions")
	if err != nil {
		return el, fmt.Errorf("reading status.conditions to patch: %w", err)
	}

	// THE GUARD. An empty slice must NEVER be patched, because `{"status":{"conditions":null}}` and
	// `{"status":{"conditions":[]}}` both DESTROY the conditions already on the object: in a JSON
	// merge patch an explicit null deletes the key and an array is replaced wholesale.
	//
	// This is not hypothetical. unstructuredtools.GetConditions returns nil for ALL conditions when
	// ANY SINGLE ONE of them has a missing or non-RFC3339 `lastTransitionTime` (documented on that
	// function in pkg/tools/unstructured/unstructured.go). SetConditions is itself read-modify-write
	// over GetConditions, so one malformed timestamp anywhere in the array can reduce what this
	// function is handed to nothing — and a body built from that without this check would wipe the
	// array on the server. Skipping is the only safe answer: a condition set that cannot be computed
	// is not a condition set that should be deleted. `el` is returned unchanged so callers that keep
	// using it (handleDelete clears finalizers off it immediately after) are unaffected.
	if len(conditions) == 0 {
		// Logged at Info, not Debug: worker.go always calls SetConditions immediately before this,
		// so an empty slice here means the runtime's conditions silently did not land, and this log
		// line is the only trace of it.
		contextutils.Logger(ctx).Info("Skipping the status.conditions patch: nothing to write",
			"name", el.GetName(), "namespace", el.GetNamespace(), "kind", el.GetKind(),
			"reason", "an empty conditions slice would delete the array on the server")
		return el, nil
	}

	body, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"conditions": conditions,
		},
	})
	if err != nil {
		return el, fmt.Errorf("marshalling the status.conditions patch: %w", err)
	}

	res, err := opts.DynamicClient.Resource(gvr).
		Namespace(el.GetNamespace()).
		Patch(ctx, el.GetName(), types.MergePatchType, body, metav1.PatchOptions{}, "status")
	if err != nil {
		return el, err
	}

	return res, nil
}
