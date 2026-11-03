package controller

// The runtime offers handlers an API — "return Conditions in your ExternalObservation and we will
// record them" — and honoured it on exactly one of three paths (core-provider#241).
//
//	ResourceExists=false      conditions set on an in-memory object, then `return nil`. NEVER WRITTEN.
//	ResourceUpToDate=false    same.
//	both true                 written, but Synced was OVERWRITTEN by the runtime's own
//	                          ReconcileSuccess() a few lines later.
//
// The third one made a documented state unreachable. The cdc's GRACEFUL pause returns
// ResourceExists=true, ResourceUpToDate=true and a paused condition, so it lands on that path and
// anything it said about Synced was replaced on the same reconcile. The runtime's own ANNOTATION
// pause escapes only because it returns earlier — so two pause mechanisms had inconsistent
// condition shapes, which nobody chose and which is why core-provider#241 could not be implemented
// in the cdc at all.

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// gracefullyPausedClient stands in for the cdc's graceful pause: up to date, exists, and saying so
// on BOTH conditions — which is the shape the runtime's own annotation pause uses.
type gracefullyPausedClient struct {
	fakeExternalClient
}

func (c *gracefullyPausedClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	return ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: true,
		Conditions: []metav1.Condition{
			{
				Type:               condition.TypeSynced,
				Status:             metav1.ConditionFalse,
				Reason:             "ReconcileGracefullyPaused",
				LastTransitionTime: metav1.Now(),
			},
			{
				Type:               condition.TypeReady,
				Status:             metav1.ConditionTrue,
				Reason:             "ReconcileGracefullyPaused",
				LastTransitionTime: metav1.Now(),
			},
		},
	}, nil
}

func conditionOf(t *testing.T, mg *unstructured.Unstructured, typ string) *metav1.Condition {
	t.Helper()
	for _, c := range unstructuredtools.GetConditions(mg) {
		if c.Type == typ {
			return &c
		}
	}
	return nil
}

// THE REGRESSION. A handler that says Synced=False must not have it replaced by the runtime's
// default Synced=True on the same reconcile.
func TestObserve_HandlerSyncedConditionIsNotOverridden(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()

	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = &gracefullyPausedClient{}

	obj := createTestUnstructured("paused-obj", opts.Namespace)
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

	synced := conditionOf(t, got, condition.TypeSynced)
	require.NotNil(t, synced,
		"no Synced condition was persisted at all — the assertions below would pass over a "+
			"reconcile that wrote nothing, so this is a calibration and not a nicety")

	require.Equal(t, metav1.ConditionFalse, synced.Status,
		"the runtime's default ReconcileSuccess (Synced=True) overwrote the handler's "+
			"Synced=False. That is what made core-provider#241 unimplementable in the cdc: the "+
			"graceful pause lands on the up-to-date path and could not keep its own condition")
	require.Equal(t, "ReconcileGracefullyPaused", synced.Reason,
		"the handler's REASON must survive too; a preserved status with the runtime's reason "+
			"would still lose the information an operator needs")

	// Ready must still be True. The whole point of the chosen semantics is that readiness and
	// liveness are different axes — if this flips, every readiness consumer moves at once, which is
	// the change #241 explicitly did NOT choose.
	ready := conditionOf(t, got, condition.TypeReady)
	require.NotNil(t, ready, "Ready must be present")
	require.Equal(t, metav1.ConditionTrue, ready.Status,
		"Ready must stay True: a paused composition is not a failed one, and flipping it moves "+
			"all four readiness consumers")
}

// THE OTHER TWO PATHS. A handler's conditions must be persisted even when the observation routes to
// a Create or an Update, where the runtime previously returned without any status write.
func TestObserve_HandlerConditionsSurviveTheEarlyReturns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		exists   bool
		upToDate bool
	}{
		{name: "routes to Create (ResourceExists=false)", exists: false, upToDate: false},
		{name: "routes to Update (ResourceUpToDate=false)", exists: true, upToDate: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sid, err := shortid.New(1, shortid.DefaultABC, 2342)
			require.NoError(t, err)
			opts := createTestOptions()

			ctrl, err := New(sid, opts)
			require.NoError(t, err)
			ctrl.externalClient = &earlyReturnConditionClient{exists: tc.exists, upToDate: tc.upToDate}

			obj := createTestUnstructured("early-return-obj", opts.Namespace)
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

			c := conditionOf(t, got, "HandlerSaidSo")
			require.NotNil(t, c,
				"the handler's condition was discarded: it was applied to an in-memory object and "+
					"the function returned before the only status write. The runtime documents that "+
					"a handler may return Conditions and we will record them — that held on one of "+
					"three paths")
			require.Equal(t, "BecauseItDid", c.Reason)
		})
	}
}

// earlyReturnConditionClient returns a condition on an observation that routes elsewhere.
type earlyReturnConditionClient struct {
	fakeExternalClient
	exists   bool
	upToDate bool
}

func (c *earlyReturnConditionClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	return ExternalObservation{
		ResourceExists:   c.exists,
		ResourceUpToDate: c.upToDate,
		Conditions: []metav1.Condition{{
			Type:               "HandlerSaidSo",
			Status:             metav1.ConditionTrue,
			Reason:             "BecauseItDid",
			LastTransitionTime: metav1.Now(),
		}},
	}, nil
}
