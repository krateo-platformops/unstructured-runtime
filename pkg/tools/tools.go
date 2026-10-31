package tools

import (
	"context"
	"fmt"

	"github.com/krateo-platformops/unstructured-runtime/pkg/pluralizer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
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

// UpdateStatus updates the status subresource of the given unstructured object using the provided dynamic client and pluralizer. It returns the updated object or an error if the update fails.
// In case of error the original object is returned along with the error.
//
// RETRIES ON CONFLICT (#30). A conflict on a status write is the ordinary case in a controller with
// more than one writer, not a condition worth surfacing: this runtime does
// fetch -> Observe -> SetConditions -> UpdateStatus, so anything that writes the object between the
// fetch and the write leaves `el` at a stale resourceVersion. Without a retry that 409 was returned
// to the caller, which records it on the object as a ReconcileError — so a transient race became a
// PERMANENT error condition for as long as the race kept being won (core-provider#258, and the
// controller.go doc comment on Conditions describes the same path).
//
// On conflict it re-fetches and re-applies the caller's `status`, keeping the live object's
// resourceVersion. That is last-writer-wins for the status stanza, which is what UpdateStatus
// already meant — the caller passed a complete status and asked for it to land. The retry does not
// widen that: without it the same write simply failed. What it deliberately does NOT do is merge
// field-by-field, because a caller must still be able to REMOVE a status field, and a merge would
// make removal impossible while hiding which writer won.
//
// metadata is taken from the live object rather than from `el`, so a label or annotation written by
// someone else between the fetch and the retry is not reverted by a status write.
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

	client := opts.DynamicClient.Resource(gvr).Namespace(el.GetNamespace())

	// The status the caller asked to land, captured before any retry re-reads the object.
	intended, hasStatus, err := unstructured.NestedMap(el.Object, "status")
	if err != nil {
		return el, fmt.Errorf("reading the status to write: %w", err)
	}

	var res *unstructured.Unstructured
	attempt := el
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		updated, uerr := client.UpdateStatus(ctx, attempt, metav1.UpdateOptions{})
		if uerr == nil {
			res = updated
			return nil
		}
		if !apierrors.IsConflict(uerr) {
			return uerr
		}

		// Conflict: somebody wrote the object since it was fetched. Re-read it and put the intended
		// status onto THAT object, so the write carries a current resourceVersion.
		live, gerr := client.Get(ctx, el.GetName(), metav1.GetOptions{})
		if gerr != nil {
			// Report the conflict rather than the GET failure: the conflict is what RetryOnConflict
			// must see to retry, and a GET error here is almost always the same transient cause.
			return uerr
		}
		if hasStatus {
			if serr := unstructured.SetNestedMap(live.Object, intended, "status"); serr != nil {
				return serr
			}
		} else {
			unstructured.RemoveNestedField(live.Object, "status")
		}
		attempt = live
		return uerr
	})
	if err != nil {
		return el, err
	}

	return res, nil
}
