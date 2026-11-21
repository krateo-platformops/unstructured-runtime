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
	// Closed, not deleted: deleting pending fires the builder's OnDelete Create trigger (#674).
	require.Contains(t, ann, meta.AnnotationKeyExternalCreatePending, "pending was deleted, which fires the OnDelete Create trigger")
	require.Contains(t, ann, meta.AnnotationKeyExternalCreateNotRequested, "the not-requested pass did not close pending")
	require.False(t, meta.ExternalCreateIncomplete(pass.obj), "a not-requested pass must not read as an incomplete create")
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

// A permanent refusal states its own Synced through CreateResult.Conditions, and the runtime does not
// replace it with its default (the Observe rule, core-provider#241). Without this, a resource that
// will never exist would read Synced=True.
func TestHandleCreate_NotRequested_ClientSyncedIsNotOverridden(t *testing.T) {
	refusal := []metav1.Condition{
		{Type: condition.TypeReady, Status: metav1.ConditionFalse, Reason: "NoCreateVerbDeclared", Message: "the definition declares no create verb", LastTransitionTime: metav1.Now()},
		{Type: condition.TypeSynced, Status: metav1.ConditionFalse, Reason: "NoCreateVerbDeclared", Message: "the definition declares no create verb", LastTransitionTime: metav1.Now()},
	}
	pass := runCreate(t, "refused-synced", func(o Options) ExternalClient {
		return &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: false, Reason: "NoCreateVerbDeclared", Conditions: refusal}}
	})
	require.NoError(t, pass.err)
	for _, typ := range []string{condition.TypeReady, condition.TypeSynced} {
		c := conditionOf(t, pass.obj, typ)
		require.NotNil(t, c, typ)
		require.Equal(t, metav1.ConditionFalse, c.Status, typ)
		require.Equal(t, "NoCreateVerbDeclared", c.Reason, typ)
	}
	require.NotContains(t, pass.obj.GetAnnotations(), meta.AnnotationKeyExternalCreateSucceeded)
}

// The same rule on a requested create: the client's Conditions are applied, and a client Synced wins.
func TestHandleCreate_Requested_ClientConditionsApplied(t *testing.T) {
	pass := runCreate(t, "requested-conds", func(o Options) ExternalClient {
		return &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: true, Conditions: []metav1.Condition{
			{Type: condition.TypeSynced, Status: metav1.ConditionFalse, Reason: "AsyncCreateAccepted", LastTransitionTime: metav1.Now()},
		}}}
	})
	require.NoError(t, pass.err)
	c := conditionOf(t, pass.obj, condition.TypeSynced)
	require.NotNil(t, c)
	require.Equal(t, "AsyncCreateAccepted", c.Reason)
	require.Contains(t, pass.obj.GetAnnotations(), meta.AnnotationKeyExternalCreateSucceeded)
}

// A FAILED create's own conditions are written (oasgen-provider: a create the vendor has refused for an
// hour, Ready=False/CreateRefusedPersistently, never reached the apiserver because this path returned
// before applying them). Its Ready replaces the runtime's Creating; its Synced is ignored, so Synced stays
// ReconcileError. CONTROL: a failed create that returns no conditions keeps Ready=Creating.
func TestHandleCreate_FailedCreate_ClientConditionsApplied_SyncedStaysTheRuntimes(t *testing.T) {
	refused := []metav1.Condition{
		{Type: condition.TypeReady, Status: metav1.ConditionFalse, Reason: "CreateRefusedPersistently", Message: "the vendor has refused this create for 1h", LastTransitionTime: metav1.Now()},
		{Type: condition.TypeSynced, Status: metav1.ConditionTrue, Reason: "HandlerSaysFine", LastTransitionTime: metav1.Now()},
		// A type the runtime does not write on this path is ADDED, not dropped (oasgen-provider routes
		// several reports through here).
		{Type: "ResolverDigestRecorded", Status: metav1.ConditionTrue, Reason: "Recorded", LastTransitionTime: metav1.Now()},
	}
	pass := runCreate(t, "failed-with-conds", func(o Options) ExternalClient {
		return &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: true, Conditions: refused}, err: errors.New("409 from the vendor")}
	})
	require.Error(t, pass.err, "premise: the create failed")
	ready := conditionOf(t, pass.obj, condition.TypeReady)
	require.NotNil(t, ready)
	require.Equal(t, "CreateRefusedPersistently", ready.Reason, "the client's Ready must reach the apiserver on a failed create")
	synced := conditionOf(t, pass.obj, condition.TypeSynced)
	require.NotNil(t, synced)
	require.Equal(t, metav1.ConditionFalse, synced.Status)
	require.Equal(t, condition.ReasonReconcileError, synced.Reason, "a failed pass keeps the runtime's Synced, not the client's")
	extra := conditionOf(t, pass.obj, "ResolverDigestRecorded")
	require.NotNil(t, extra, "a client condition of a type the runtime does not write must be added")
	require.Equal(t, "Recorded", extra.Reason)

	control := runCreate(t, "failed-no-conds", func(o Options) ExternalClient {
		return &resultCreator{t: t, dyn: o.Client, gvr: o.GVR, result: CreateResult{Requested: true}, err: errors.New("409 from the vendor")}
	})
	require.Error(t, control.err)
	require.Equal(t, condition.ReasonCreating, conditionOf(t, control.obj, condition.TypeReady).Reason, "control: no client conditions, Ready stays Creating")
}
