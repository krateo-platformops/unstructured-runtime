// Package meta contains functions for dealing with Kubernetes object metadata.
package meta

import (
	"time"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// AnnotationKeyExternalName is the key in the annotations map of a
	// resource for the name of the resource as it appears on provider's
	// systems.
	AnnotationKeyExternalName = "krateo.io/external-name"

	// AnnotationKeyTraceparent carries the W3C traceparent of the reconcile that created or
	// updated this object, so the controller that reconciles it continues the distributed
	// trace across the compose-of-compositions tree.
	AnnotationKeyTraceparent = "krateo.io/traceparent"

	// AnnotationKeyTracestate carries the W3C tracestate alongside AnnotationKeyTraceparent.
	AnnotationKeyTracestate = "krateo.io/tracestate"

	// AnnotationKeyExternalCreatePending is the key in the annotations map
	// of a resource that indicates the last time creation of the external
	// resource was pending (i.e. about to happen). Its value must be an
	// RFC3999 timestamp.
	AnnotationKeyExternalCreatePending = "krateo.io/external-create-pending"

	// AnnotationKeyExternalCreateSucceeded is the key in the annotations
	// map of a resource that represents the last time the external resource
	// was created successfully. Its value must be an RFC3339 timestamp,
	// which can be used to determine how long ago a resource was created.
	// This is useful for eventually consistent APIs that may take some time
	// before the API called by Observe will report that a recently created
	// external resource exists.
	AnnotationKeyExternalCreateSucceeded = "krateo.io/external-create-succeeded"

	// AnnotationKeyExternalCreateFailed is the key in the annotations map
	// of a resource that indicates the last time creation of the external
	// resource failed. Its value must be an RFC3999 timestamp.
	AnnotationKeyExternalCreateFailed = "krateo.io/external-create-failed"

	// AnnotationKeyExternalCreateNotRequested is the key in the annotations map of a resource that
	// indicates the last time a create pass ended WITHOUT requesting a create (a ResultCreator
	// returned Requested=false: no create verb, a referent not yet present, a refused credential).
	// Its value must be an RFC3339 timestamp. It closes the pending marker the way succeeded and failed
	// do, without deleting it: deleting external-create-pending fires the builder's OnDelete trigger,
	// which enqueues a Create at once and turned every not-requested pass into the next one
	// (oasgen-provider#674).
	AnnotationKeyExternalCreateNotRequested = "krateo.io/external-create-not-requested"

	// AnnotationKeyReconciliationPaused is the key in the annotations map
	// of a resource that indicates that further reconciliations on the
	// resource are paused. All create/update/delete/generic events on
	// the resource will be filtered and thus no further reconcile requests
	// will be queued for the resource.
	AnnotationKeyReconciliationPaused = "krateo.io/paused"

	// AnnotationKeyConnectorVerbose is the key in the annotations map
	// of a resource that indicates that the external client has verbose info enabled.
	AnnotationKeyConnectorVerbose = "krateo.io/connector-verbose"

	// AnnotationKeyManagementPolicy is the key in the annotations map
	// of a resource to instruct the provider to manage resources in a fine-grained way.
	// default: The provider can fully manage the resource.
	//          This is the default policy.
	// observe-create-update: The provider can observe, create, or update the
	//                        resource, but can not delete it.
	// observe-delete: The provider can observe or delete the resource, but
	//                 can not create and update it.
	// observe: The provider can only observe the resource.
	//          This maps to the read-only scenario where the resource is fully controlled by third party application.
	// Since #110 (D1) the value may also be a comma-separated SET of verbs, e.g. "observe,create";
	// observe is always implied (D3), and a value that does not parse acts on nothing (D2). The
	// four names above are aliases for their sets. The definition is plumbing's managementpolicy.
	AnnotationKeyManagementPolicy = managementpolicy.AnnotationKey

	// AnnotationKeyDeletionPolicy is the key in the annotations map
	// of a resource that determines what should happen to the underlying external
	// resource when a managed resource is deleted
	AnnotationKeyDeletionPolicy = managementpolicy.DeletionAnnotationKey
)

const (
	// ManagementPolicyDefault means the provider can fully manage the resource.
	ManagementPolicyDefault = "default"
	// ManagementPolicyObserveCreateUpdate means the provider can observe, create,
	// or update the resource, but can not delete it.
	ManagementPolicyObserveCreateUpdate = "observe-create-update"
	// ManagementPolicyObserveDelete means the provider can observe or delete
	// the resource, but can not create and update it.
	ManagementPolicyObserveDelete = "observe-delete"
	// ManagementPolicyObserve means the provider can only observe the resource.
	ManagementPolicyObserve = "observe"
	// DeletionPolicyOrphan means the external resource will orphaned when its managed
	// resource is deleted.
	DeletionPolicyOrphan = managementpolicy.DeletionOrphan

	// DeletionPolicyDelete means both the  external resource will be deleted when its
	// managed resource is deleted.
	DeletionPolicyDelete = managementpolicy.DeletionDelete

	// ActionCreate means to create an Object
	ActionCreate = "create"
	// ActionUpdate means to update an Object
	ActionUpdate = "update"
	// ActionDelete means to delete an Object
	ActionDelete = "delete"
)

const (
	ReleaseNameLabel = "krateo.io/release-name"
)

// AddFinalizer to the supplied Kubernetes object's metadata.
func AddFinalizer(o metav1.Object, finalizer string) {
	f := o.GetFinalizers()
	for _, e := range f {
		if e == finalizer {
			return
		}
	}
	o.SetFinalizers(append(f, finalizer))
}

// RemoveFinalizer from the supplied Kubernetes object's metadata.
func RemoveFinalizer(o metav1.Object, finalizer string) {
	f := o.GetFinalizers()
	for i, e := range f {
		if e == finalizer {
			f = append(f[:i], f[i+1:]...)
		}
	}
	o.SetFinalizers(f)
}

// FinalizerExists checks whether given finalizer is already set.
func FinalizerExists(o metav1.Object, finalizer string) bool {
	f := o.GetFinalizers()
	for _, e := range f {
		if e == finalizer {
			return true
		}
	}
	return false
}

// AddLabels to the supplied object.
func AddLabels(o metav1.Object, labels map[string]string) {
	l := o.GetLabels()
	if l == nil {
		o.SetLabels(labels)
		return
	}
	for k, v := range labels {
		l[k] = v
	}
	o.SetLabels(l)
}

// RemoveLabels with the supplied keys from the supplied object.
func RemoveLabels(o metav1.Object, labels ...string) {
	l := o.GetLabels()
	if l == nil {
		return
	}
	for _, k := range labels {
		delete(l, k)
	}
	o.SetLabels(l)
}

// AddAnnotations to the supplied object.
func AddAnnotations(o metav1.Object, annotations map[string]string) {
	a := o.GetAnnotations()
	if a == nil {
		o.SetAnnotations(annotations)
		return
	}
	for k, v := range annotations {
		a[k] = v
	}
	o.SetAnnotations(a)
}

// RemoveAnnotations with the supplied keys from the supplied object.
func RemoveAnnotations(o metav1.Object, annotations ...string) {
	a := o.GetAnnotations()
	if a == nil {
		return
	}
	for _, k := range annotations {
		delete(a, k)
	}
	o.SetAnnotations(a)
}

// WasDeleted returns true if the supplied object was deleted from the API server.
func WasDeleted(o metav1.Object) bool {
	return !o.GetDeletionTimestamp().IsZero()
}

// WasCreated returns true if the supplied object was created in the API server.
func WasCreated(o metav1.Object) bool {
	// This looks a little different from WasDeleted because DeletionTimestamp
	// returns a reference while CreationTimestamp returns a value.
	t := o.GetCreationTimestamp()
	return !t.IsZero()
}

// GetExternalName returns the external name annotation value on the resource.
func GetExternalName(o metav1.Object) string {
	return o.GetAnnotations()[AnnotationKeyExternalName]
}

// SetExternalName sets the external name annotation of the resource.
func SetExternalName(o metav1.Object, name string) {
	AddAnnotations(o, map[string]string{AnnotationKeyExternalName: name})
}

// GetExternalCreatePending returns the time at which the external resource
// was most recently pending creation.
func GetExternalCreatePending(o metav1.Object) time.Time {
	a := o.GetAnnotations()[AnnotationKeyExternalCreatePending]
	t, err := time.Parse(time.RFC3339, a)
	if err != nil {
		return time.Time{}
	}
	return t
}

// SetExternalCreatePending sets the time at which the external resource was
// most recently pending creation to the supplied time.
// markerTime is how every external-create marker is written: RFC3339 in UTC, at second precision.
// UTC because a process with TZ set used to write its local offset (2026-11-18T19:50:00+01:00), so a
// downstream tool that compared marker STRINGS could order a latched create wrongly (measured by
// aruba-provider-kog; provider-runtime#110 is the same fix there). Second precision is kept on
// purpose (Diego, 2026-11-18): ExternalCreateIncomplete's tie rule is second-granular. Readers must
// still compare PARSED times, never strings: values written by an earlier runtime carry an offset.
func markerTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func SetExternalCreatePending(o metav1.Object, t time.Time) {
	AddAnnotations(o, map[string]string{AnnotationKeyExternalCreatePending: markerTime(t)})
}

// GetExternalCreateSucceeded returns the time at which the external resource
// was most recently created.
func GetExternalCreateSucceeded(o metav1.Object) time.Time {
	a := o.GetAnnotations()[AnnotationKeyExternalCreateSucceeded]
	t, err := time.Parse(time.RFC3339, a)
	if err != nil {
		return time.Time{}
	}
	return t
}

// SetExternalCreateSucceeded sets the time at which the external resource was
// most recently created to the supplied time.
func SetExternalCreateSucceeded(o metav1.Object, t time.Time) {
	AddAnnotations(o, map[string]string{AnnotationKeyExternalCreateSucceeded: markerTime(t)})
}

// GetExternalCreateFailed returns the time at which the external resource
// recently failed to create.
func GetExternalCreateFailed(o metav1.Object) time.Time {
	a := o.GetAnnotations()[AnnotationKeyExternalCreateFailed]
	t, err := time.Parse(time.RFC3339, a)
	if err != nil {
		return time.Time{}
	}
	return t
}

// SetExternalCreateFailed sets the time at which the external resource most
// recently failed to create.
func SetExternalCreateFailed(o metav1.Object, t time.Time) {
	AddAnnotations(o, map[string]string{AnnotationKeyExternalCreateFailed: markerTime(t)})
}

// GetExternalCreateNotRequested returns the time at which a create pass most recently ended without
// requesting a create.
func GetExternalCreateNotRequested(o metav1.Object) time.Time {
	a := o.GetAnnotations()[AnnotationKeyExternalCreateNotRequested]
	t, err := time.Parse(time.RFC3339, a)
	if err != nil {
		return time.Time{}
	}
	return t
}

// SetExternalCreateNotRequested sets the time at which a create pass most recently ended without
// requesting a create.
func SetExternalCreateNotRequested(o metav1.Object, t time.Time) {
	AddAnnotations(o, map[string]string{AnnotationKeyExternalCreateNotRequested: markerTime(t)})
}

// ExternalCreateIncomplete returns true if creation of the external resource
// appears to be incomplete. We deem creation to be incomplete if the 'external
// create pending' annotation is the newest of all tracking annotations that are
// set (i.e. pending, succeeded, failed, and not-requested). Strictly newer: the
// timestamps are second-granular, and a pass that ends in the same second it
// started is complete.
func ExternalCreateIncomplete(o metav1.Object) bool {
	pending := GetExternalCreatePending(o)

	// If creation never started it can't be incomplete.
	if pending.IsZero() {
		return false
	}

	latest := GetExternalCreateSucceeded(o)
	for _, t := range []time.Time{GetExternalCreateFailed(o), GetExternalCreateNotRequested(o)} {
		if t.After(latest) {
			latest = t
		}
	}
	return pending.After(latest)
}

// ExternalCreateRefusedUnconfirmed reports that a create was attempted, the handler classified its
// failure as not-committed (so the runtime stamped failed), and nothing has confirmed the external
// resource's existence since.
//
// SEPARATE FROM ExternalCreateIncomplete, and deliberately so. That predicate means "a create pass
// started and did not finish", which is false here: this pass finished, with a refusal. What this one
// means is narrower -- "the last create was refused, and we have not looked since" -- and it exists
// because a refusal correctly classified as not-committed is safe to retry blindly AT THE MOMENT OF
// CLASSIFICATION and not two seconds later. The resource can appear by a route this controller did
// not take, and then the retry duplicates it.
func ExternalCreateRefusedUnconfirmed(o metav1.Object) bool {
	pending := GetExternalCreatePending(o)
	if pending.IsZero() {
		return false
	}
	failed := GetExternalCreateFailed(o)
	if failed.Before(pending) {
		// Not the outcome of the most recent attempt.
		return false
	}
	// TIES RESOLVE IN FAVOUR OF "DONE", as ExternalCreateIncomplete's do and for the same reason: the
	// timestamps are second-granular, so an adoption recorded in the same second as the refusal it
	// supersedes must not read as still-unconfirmed. Written with After() first, this returned true
	// immediately after a successful adoption and the next pass confirmed all over again.
	for _, t := range []time.Time{GetExternalCreateSucceeded(o), GetExternalCreateNotRequested(o)} {
		if !t.IsZero() && !t.Before(failed) {
			return false
		}
	}
	return true
}

// ExternalCreateSucceededDuring returns true if creation of the external
// resource that corresponds to the supplied managed resource succeeded within
// the supplied duration.
func ExternalCreateSucceededDuring(o metav1.Object, d time.Duration) bool {
	t := GetExternalCreateSucceeded(o)
	if t.IsZero() {
		return false
	}
	return time.Since(t) < d
}

// ExternalCreatePendingDuring returns true if creation of the external resource was ATTEMPTED
// (the pending marker was set) within the supplied duration. It is used to confirm-the-negative
// during incomplete-create recovery: an external Create that actually landed may not yet be
// visible in an eventually-consistent API, so a still-recent pending marker means "wait and
// re-observe" rather than "the create never happened, recreate it" (which would duplicate).
func ExternalCreatePendingDuring(o metav1.Object, d time.Duration) bool {
	t := GetExternalCreatePending(o)
	if t.IsZero() {
		return false
	}
	return time.Since(t) < d
}

// IsPaused returns true if the object has the AnnotationKeyReconciliationPaused
// annotation set to `true`.
func IsPaused(o metav1.Object) bool {
	return o.GetAnnotations()[AnnotationKeyReconciliationPaused] == "true"
}

// IsVerbose returns true if the object has the AnnotationKeyConnectorVerbose
// annotation set to `true`.
func IsVerbose(o metav1.Object) bool {
	return o.GetAnnotations()[AnnotationKeyConnectorVerbose] == "true"
}

// ManagementDecision is what the runtime may do to o's external resource, read from its
// krateo.io/management-policy and krateo.io/deletion-policy annotations by plumbing's
// managementpolicy package, the one definition both runtimes share (#110, D9). The worker consults
// it; nothing in this runtime parses either annotation itself.
//
// It reads no declared verbs (D8): a handler that declares its API's verbs is a later step, and nil
// means "no verb information", so the policy alone decides, as before.
func ManagementDecision(o metav1.Object) managementpolicy.Decision {
	return managementpolicy.For(o.GetAnnotations(), nil)
}

// IsActionAllowed determines if action is allowed to be performed on Object.
//
// Deprecated: nothing in this runtime calls it, and it disagrees with what the runtime does: for
// delete it ignores krateo.io/deletion-policy, which ShouldDelete honours. A probe that grepped for it
// found 0 call sites and nearly reported the management-policy gate as dead (aruba-provider-kog,
// #110). Use ManagementDecision, which is what the worker consults. It now reads the policy through
// managementpolicy (so a verb set such as "observe,create" is understood), and for delete it still
// ignores the deletion policy, as it always has.
func IsActionAllowed(o metav1.Object, action string) bool {
	return ManagementDecision(o).Policy.Allows(managementpolicy.Verb(action))
}

// ShouldDelete reports whether the external resource is deleted with o: the policy allows delete
// and the deletion policy keeps nothing. krateo.io/deletion-policy: orphan always wins (D5), and a
// deletion policy that is neither delete nor orphan keeps the resource too.
//
// Deprecated: use ManagementDecision(o).May(managementpolicy.Delete).
func ShouldDelete(o metav1.Object) bool {
	return ManagementDecision(o).May(managementpolicy.Delete)
}

// ShouldOnlyObserve returns true if the policy allows observe and nothing else: the "observe"
// preset, the set "observe", or a value that does not parse (D2: an invalid policy acts on nothing).
//
// Deprecated: read ManagementDecision(o).Policy.
func ShouldOnlyObserve(o metav1.Object) bool {
	p := ManagementDecision(o).Policy
	return !p.Allows(managementpolicy.Create) && !p.Allows(managementpolicy.Update) && !p.Allows(managementpolicy.Delete)
}

// ShouldCreate returns true if the Create action is allowed.
//
// Deprecated: use ManagementDecision(o).May(managementpolicy.Create).
func ShouldCreate(o metav1.Object) bool {
	return ManagementDecision(o).May(managementpolicy.Create)
}

// ShouldUpdate returns true if the Update action is allowed.
//
// Deprecated: use ManagementDecision(o).May(managementpolicy.Update).
func ShouldUpdate(o metav1.Object) bool {
	return ManagementDecision(o).May(managementpolicy.Update)
}
