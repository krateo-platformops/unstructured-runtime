package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Writing Ready=Unknown/ObserveFailed on an observation failure is right when nothing assessed the
// resource. It is WRONG in two cases, both found by the provider-runtime session reviewing the
// merged change against failure modes their own runtime had already paid for:
//
//  1. the handler already assessed it and wrote a FINDING before returning its error, so
//     overwriting replaces a finding with an absence of finding;
//  2. the object is being deleted, so "nobody knows" is false about an object whose fate is certain.
//
// Both were real. Case 1 was measured in core-provider's cdc: two Observe sites write
// Ready=False/Unavailable WITH a diagnostic message, persist it, and then return the error — one of
// them the RBAC-rotation path whose whole value is that the message names the cause.

// assessingClient is a handler that behaves the way the cdc does: it PERSISTS a Ready finding and
// then returns an error. Persisting matters — handleObserve re-fetches the object after the handler
// errors, so a handler that only mutated its in-memory copy would lose the write and the test would
// be about nothing.
type assessingClient struct {
	dyn     dynamic.Interface
	gvr     schema.GroupVersionResource
	ready   metav1.Condition
	wrapErr bool // wrap the returned error with ReadyAssessed
	err     error
}

func (a *assessingClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	live, err := a.dyn.Resource(a.gvr).Namespace(mg.GetNamespace()).
		Get(ctx, mg.GetName(), metav1.GetOptions{})
	if err != nil {
		return ExternalObservation{}, err
	}
	if err := unstructuredtools.SetConditions(live, a.ready); err != nil {
		return ExternalObservation{}, err
	}
	if _, err := a.dyn.Resource(a.gvr).Namespace(mg.GetNamespace()).
		UpdateStatus(ctx, live, metav1.UpdateOptions{}); err != nil {
		return ExternalObservation{}, err
	}
	if a.wrapErr {
		return ExternalObservation{}, ReadyAssessed(a.err)
	}
	return ExternalObservation{}, a.err
}

func (a *assessingClient) Create(context.Context, *unstructured.Unstructured) error { return nil }
func (a *assessingClient) Update(context.Context, *unstructured.Unstructured) error { return nil }
func (a *assessingClient) Delete(context.Context, *unstructured.Unstructured) error { return nil }

func readyOf(t *testing.T, u *unstructured.Unstructured) metav1.Condition {
	t.Helper()
	for _, c := range unstructuredtools.GetConditions(u) {
		if c.Type == condition.TypeReady {
			return c
		}
	}
	return metav1.Condition{}
}

func syncedOf(t *testing.T, u *unstructured.Unstructured) metav1.Condition {
	t.Helper()
	for _, c := range unstructuredtools.GetConditions(u) {
		if c.Type == condition.TypeSynced {
			return c
		}
	}
	return metav1.Condition{}
}

// countStatusUpdates is the DENOMINATOR. provider-runtime's equivalent test asserts it, and without
// it the test passes against an implementation that writes twice and happens to end on the right
// value — which is exactly the same-value-write case a before/after comparison cannot see.
func countStatusUpdates(actions []k8stesting.Action) int {
	n := 0
	for _, a := range actions {
		if a.GetVerb() == "update" && a.GetSubresource() == "status" {
			n++
		}
	}
	return n
}

func TestHandleObserve_ReadyAssessedKeepsTheHandlersFinding(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("assessed-1", opts.Namespace)
	require.NoError(t, unstructuredtools.SetConditions(obj, condition.Available()))
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	// The cdc's shape: Unavailable WITH the diagnostic in the message.
	finding := condition.Unavailable()
	finding.Message = "derived RBAC could not be applied: clusterrolebindings.rbac.authorization.k8s.io " +
		"\"portal-krateo-rbac\" names a ServiceAccount that does not exist"
	boom := errors.New("applying derived RBAC")

	ctrl.SetExternalClient(&assessingClient{
		dyn: opts.Client, gvr: opts.GVR, ready: finding, wrapErr: true, err: boom,
	})

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}
	require.Error(t, ctrl.handleObserve(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	ready, synced := readyOf(t, updated), syncedOf(t, updated)

	// THE SUBJECT: the handler's finding survives.
	require.Equal(t, condition.ReasonUnavailable, ready.Reason,
		"the handler's Ready finding was overwritten. Replacing Unavailable (with a diagnostic "+
			"naming the cause) by ObserveFailed (\"nobody knows\") is worse information in the one "+
			"direction an operator cannot recover from")
	require.Contains(t, ready.Message, "portal-krateo-rbac",
		"the finding's MESSAGE is the whole point -- it names which binding is broken")

	// THE CONTRACT: Synced must still record the error regardless of which Ready was chosen.
	require.Equal(t, condition.ReasonReconcileError, synced.Reason,
		"Synced=ReconcileError must be written in ALL cases; consumers read it to decide whether "+
			"the last reconcile errored and that guard must not depend on the Ready choice")
}

// THE SAME-VALUE WRITE, which is why the opt-out is explicit rather than a before/after snapshot.
// The object already reads Unavailable, the handler writes Unavailable AGAIN, and no comparison of
// before against after could tell that the handler spoke.
//
// The DENOMINATOR is the assertion that makes this meaningful: exactly one status update, the
// handler's. A runtime that also wrote ObserveFailed and then had it corrected would end on a
// different value; a runtime that wrote ObserveFailed over an identical-looking state would be
// invisible without counting.
func TestHandleObserve_ReadyAssessedSurvivesASameValueWrite(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	finding := condition.Unavailable()
	finding.Message = "managed children not healthy: githubproviderkogs/krateo-system/github-provider-kog"

	obj := createTestUnstructured("assessed-same", opts.Namespace)
	// PREMISE: the object ALREADY carries the exact condition the handler will write.
	require.NoError(t, unstructuredtools.SetConditions(obj, finding))
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	seeded, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, condition.ReasonUnavailable, readyOf(t, seeded).Reason,
		"premise: the seed must already be Unavailable, or this is not a same-value write")

	ctrl.SetExternalClient(&assessingClient{
		dyn: opts.Client, gvr: opts.GVR, ready: finding, wrapErr: true,
		err: errors.New("children still unhealthy"),
	})

	// Reset the recorded actions so the count below is about the reconcile, not the setup.
	fake := opts.Client.(*dynamicfake.FakeDynamicClient)
	fake.ClearActions()

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}
	require.Error(t, ctrl.handleObserve(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, condition.ReasonUnavailable, readyOf(t, updated).Reason,
		"a same-value handler write must still be respected")

	// THE DENOMINATOR.
	if got := countStatusUpdates(fake.Actions()); got != 1 {
		t.Errorf("status updates = %d, want exactly 1 (the handler's). More than one means the "+
			"runtime also wrote Ready, which a value comparison could never have detected on a "+
			"same-value write", got)
	}
}

// A DELETED object gets Deleting, not ObserveFailed. handleObserve has no deletion guard -- the
// Create branch drops deleted objects explicitly, this path does not -- so a composition carrying a
// deletion timestamp reaches the failure path, and "nobody knows" is false about an object whose
// fate is certain.
func TestHandleObserve_DeletedObjectGetsDeletingNotObserveFailed(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("deleting-1", opts.Namespace)
	require.NoError(t, unstructuredtools.SetConditions(obj, condition.Available()))
	now := metav1.Now()
	obj.SetDeletionTimestamp(&now)
	obj.SetFinalizers([]string{"composition.krateo.io/finalizer"})
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	live, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	// PREMISE: the deletion timestamp survived the fake's create, or this measures the wrong thing.
	require.NotNil(t, live.GetDeletionTimestamp(),
		"premise: the object must carry a deletion timestamp")

	ctrl.SetExternalClient(&fakeExternalClient{ObserveErr: errors.New("vendor unreachable")})

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}
	require.Error(t, ctrl.handleObserve(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	ready := readyOf(t, updated)
	require.Equal(t, condition.ReasonDeleting, ready.Reason,
		"a deleting object must report Deleting, not ObserveFailed: its fate is certain, so "+
			"\"nobody knows\" is false, and every other deletion path on this object says Deleting")
	require.Equal(t, condition.ReasonReconcileError, syncedOf(t, updated).Reason,
		"Synced must still carry the error on the deletion path too")
}

// THE SECOND HALF OF THE DISJUNCT: a Ready that ALREADY reads Deleting must survive an Observe
// failure even when the object we re-read carries no deletion timestamp.
//
// provider-runtime tests `meta.WasDeleted(mg) || Reason == ReasonDeleting` (pkg/reconciler/
// ready.go:64); this runtime tested only the first half, which was an asymmetry rather than a
// decision — found by the provider-runtime and oasgen-provider sessions reading the tagged v1.7.6
// code side by side.
//
// It matters because `e` is a FRESH RE-FETCH: the copy the failure path writes onto is read back
// from the apiserver, so a deletion timestamp that has not yet landed in that read would lose the
// Deleting a previous reconcile correctly wrote and replace it with ObserveFailed — a durable lie
// about an object whose fate is certain. provider-runtime's own reason for writing Deleting is the
// reason to keep it sticky: it is true whatever the external system answers.
//
// The timestamp is deliberately ABSENT here. With one present this would pass through the first
// disjunct and say nothing about the second — which is exactly how the gap survived.
func TestHandleObserve_DeletingReasonSurvivesWithoutATimestamp(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("deleting-sticky", opts.Namespace)
	require.NoError(t, unstructuredtools.SetConditions(obj, condition.Deleting()))
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	live, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	// PREMISE, both halves. Without the first this would be the timestamp case again; without the
	// second there would be no Deleting to preserve and the arm would pass against any behaviour.
	require.Nil(t, live.GetDeletionTimestamp(),
		"premise: this arm exists for the case where NO timestamp is visible")
	require.Equal(t, condition.ReasonDeleting, readyOf(t, live).Reason,
		"premise: Ready must already read Deleting for there to be anything to preserve")

	ctrl.SetExternalClient(&fakeExternalClient{ObserveErr: errors.New("vendor unreachable")})

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}
	require.Error(t, ctrl.handleObserve(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	require.Equal(t, condition.ReasonDeleting, readyOf(t, updated).Reason,
		"an object whose Ready already read Deleting must keep it: the re-fetch may not yet show "+
			"the timestamp, and overwriting with ObserveFailed is a durable lie about an object "+
			"whose fate is certain")
	require.Equal(t, condition.ReasonReconcileError, syncedOf(t, updated).Reason,
		"CONTROL: Synced must still carry the error, or this arm would also pass against a path "+
			"that wrote no conditions at all")
}
