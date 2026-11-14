package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// resultCreator is a ResultCreator whose CreateWithResult first persists a Ready condition on the
// object, as a provider that explains itself does (rest-dynamic-controller's AuthorizationDenied),
// then returns result and err. Its plain Create must never be called.
type resultCreator struct {
	fakeExternalClient
	t           *testing.T
	dyn         dynamic.Interface
	gvr         schema.GroupVersionResource
	result      CreateResult
	err         error
	readyReason string
	createCalls int
}

func (r *resultCreator) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	r.createCalls++
	return nil
}

func (r *resultCreator) CreateWithResult(ctx context.Context, mg *unstructured.Unstructured) (CreateResult, error) {
	if r.readyReason != "" {
		live, err := r.dyn.Resource(r.gvr).Namespace(mg.GetNamespace()).Get(ctx, mg.GetName(), metav1.GetOptions{})
		require.NoError(r.t, err)
		require.NoError(r.t, unstructuredtools.SetConditions(live, metav1.Condition{
			Type: condition.TypeReady, Status: metav1.ConditionFalse, Reason: r.readyReason,
			Message: "the credential does not grant create", LastTransitionTime: metav1.Now()}))
		_, err = r.dyn.Resource(r.gvr).Namespace(mg.GetNamespace()).UpdateStatus(ctx, live, metav1.UpdateOptions{})
		require.NoError(r.t, err)
	}
	return r.result, r.err
}

type createPass struct {
	obj    *unstructured.Unstructured
	err    error
	events []string // reasons, both recorders
}

// runCreate runs handleCreate once for a fresh object with client ec, and returns the object as
// read back and every event reason recorded.
func runCreate(t *testing.T, name string, ec func(Options) ExternalClient) createPass {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	rec, thr := &mockEventRecorder{}, &mockEventRecorder{}
	opts.Recorder, opts.ThrottledRecorder = rec, thr
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured(name, opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()}

	ctrl.SetExternalClient(ec(opts))
	herr := ctrl.handleCreate(context.TODO(), ref)

	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	var reasons []string
	for _, m := range []*mockEventRecorder{rec, thr} {
		m.mu.Lock()
		for _, e := range m.events {
			reasons = append(reasons, string(e.Reason))
		}
		m.mu.Unlock()
	}
	return createPass{obj: got, err: herr, events: reasons}
}

// The defect this exists for: a client that requested nothing must not be recorded as having
// created anything, and the Ready it persisted must survive.
func TestHandleCreate_NotRequested_RecordsNoCreate(t *testing.T) {
	var rc *resultCreator
	pass := runCreate(t, "not-requested", func(o Options) ExternalClient {
		rc = &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: false, Reason: "AuthorizationDenied"}, readyReason: "AuthorizationDenied"}
		return rc
	})
	require.NoError(t, pass.err)
	require.Zero(t, rc.createCalls, "the runtime called Create on a ResultCreator")

	ann := pass.obj.GetAnnotations()
	require.NotContains(t, ann, meta.AnnotationKeyExternalCreateSucceeded, "a create that was not requested is recorded as succeeded")
	require.NotContains(t, ann, meta.AnnotationKeyExternalCreatePending, "no create is in flight, yet external-create-pending is left set")
	require.False(t, meta.ExternalCreateIncomplete(pass.obj))
	require.NotContains(t, pass.events, string(reasonCreated), "CreatedExternalResource emitted for a create that was not requested")

	ready := conditionOf(t, pass.obj, condition.TypeReady)
	require.NotNil(t, ready)
	require.Equal(t, "AuthorizationDenied", ready.Reason, "the Ready the client persisted was overwritten")
	synced := conditionOf(t, pass.obj, condition.TypeSynced)
	require.NotNil(t, synced)
	require.Equal(t, metav1.ConditionTrue, synced.Status)
	require.Equal(t, string(condition.ReasonReconcileSuccess), synced.Reason)
}

// Requested through CreateWithResult is exactly a nil Create.
func TestHandleCreate_RequestedThroughResultCreator_IsANilCreate(t *testing.T) {
	pass := runCreate(t, "requested", func(o Options) ExternalClient {
		return &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: true}}
	})
	require.NoError(t, pass.err)
	require.Contains(t, pass.obj.GetAnnotations(), meta.AnnotationKeyExternalCreateSucceeded)
	require.Contains(t, pass.events, string(reasonCreated))
}

// The control: a plain client's nil Create still means requested, unchanged.
func TestHandleCreate_PlainNilCreate_StillMeansRequested(t *testing.T) {
	pass := runCreate(t, "plain", func(Options) ExternalClient { return &fakeExternalClient{} })
	require.NoError(t, pass.err)
	require.Contains(t, pass.obj.GetAnnotations(), meta.AnnotationKeyExternalCreateSucceeded)
	require.Contains(t, pass.events, string(reasonCreated))
}

// An error wins over the result: the create-error path, whatever Requested says.
func TestHandleCreate_NotRequestedWithAnError_TakesTheErrorPath(t *testing.T) {
	pass := runCreate(t, "not-requested-error", func(o Options) ExternalClient {
		return &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: false, Reason: "x"}, err: errors.New("create boom")}
	})
	require.Error(t, pass.err)
	require.Contains(t, pass.obj.GetAnnotations(), meta.AnnotationKeyExternalCreateFailed)
	require.NotContains(t, pass.obj.GetAnnotations(), meta.AnnotationKeyExternalCreateSucceeded)
	require.NotContains(t, pass.events, string(reasonCreated))
}
