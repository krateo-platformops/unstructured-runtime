package controller

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// core-provider#258. A handler cannot report a condition by writing one: Observe receives a copy
// (handlers deep-copy to keep Observe a read phase), so anything it writes is discarded — and a
// handler that wrote status anyway would leave `el` stale and make the runtime's own UpdateStatus
// 409 on every reconcile, turning a missing condition into a permanent ReconcileError.
//
// So ExternalObservation carries Conditions, and the runtime applies them before its own status
// write. These pin that contract.

type conditionReturningClient struct {
	fakeExternalClient
	conds []metav1.Condition
}

func (c *conditionReturningClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	// Deliberately mutates the argument as well, to prove the runtime does NOT rely on that: a
	// real handler deep-copies, so a write here must not be what makes the condition appear.
	_ = unstructuredtools.SetConditions(mg, metav1.Condition{
		Type: "WrittenDirectly", Status: metav1.ConditionTrue, Reason: "ShouldNotPersist",
		LastTransitionTime: metav1.Now(),
	})
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true, Conditions: c.conds}, nil
}

func TestObservationConditionsAreApplied(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()

	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = &conditionReturningClient{conds: []metav1.Condition{{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "ReconcileGracefullyPaused",
		Message: "Composition is gracefully paused.", LastTransitionTime: metav1.Now(),
	}}}

	obj := createTestUnstructured("cond-sample", opts.Namespace)
	// Seed a CONTRADICTING Ready, so the test cannot pass by the condition simply being absent
	// before — the returned one has to replace it.
	require.NoError(t, unstructuredtools.SetConditions(obj, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionFalse, Reason: "Unavailable",
		Message: "boom", LastTransitionTime: metav1.Now(),
	}))
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	require.NoError(t, ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	}))

	// Read back from the cluster, not from the in-memory object: the whole defect was a condition
	// that existed in memory and never reached the apiserver.
	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	var ready, written *metav1.Condition
	for i, c := range unstructuredtools.GetConditions(got) {
		cc := unstructuredtools.GetConditions(got)[i]
		switch c.Type {
		case "Ready":
			ready = &cc
		case "WrittenDirectly":
			written = &cc
		}
	}

	require.NotNil(t, ready, "no Ready condition persisted")
	require.Equal(t, "ReconcileGracefullyPaused", ready.Reason,
		"the condition returned by the handler must REPLACE the contradicting one, or a consumer "+
			"gating on it never fires (core-provider#258)")
	require.Equal(t, metav1.ConditionTrue, ready.Status)

	require.Nil(t, written,
		"a condition the handler wrote onto its argument reached the apiserver — the runtime must "+
			"not depend on handler-side writes, because a real handler deep-copies and that write "+
			"would be silently discarded")
}

// Control: a handler returning no conditions must behave exactly as before, which is what every
// existing implementor does.
func TestObservationWithoutConditionsIsUnchanged(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = &conditionReturningClient{conds: nil}

	obj := createTestUnstructured("cond-none", opts.Namespace)
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	require.NoError(t, ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	}))

	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	for _, c := range unstructuredtools.GetConditions(got) {
		require.NotEqual(t, "WrittenDirectly", c.Type,
			"a handler-side write must not persist even when Conditions is nil")
	}
}
