package controller

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// #28. The recovery block wrote with a bare tools.Update on the object fetched BEFORE
// externalResourceExists ran — and that call falls through to the client's Observe, which for a
// status-writing client bumps resourceVersion server-side. The write then conflicted on every
// pass, and because the error returns before the dispatch, no Create/Update ever ran: the
// resource stayed external-create-pending forever with its spec never pushed. Measured on
// rest-dynamic-controller as 51 consecutive conflicts and 0 recoveries in 20 minutes.
//
// A fake that never rejects on resourceVersion passes against the unfixed code, so these force a
// genuine Conflict with a reactor. That is the only thing that distinguishes the two.

// conflictOnce rejects the first update with a Conflict and lets later ones through, which is
// exactly the shape of losing a race to the Observe that just ran.
func conflictOnce(cli *dynamicfake.FakeDynamicClient, gvr schema.GroupVersionResource) *int {
	n := 0
	cli.PrependReactor("update", gvr.Resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "" {
			return false, nil, nil // let status updates through
		}
		n++
		if n == 1 {
			return true, nil, apierrors.NewConflict(
				a.GetResource().GroupResource(), "conflicted",
				errConflict{})
		}
		return false, nil, nil
	})
	return &n
}

type errConflict struct{}

func (errConflict) Error() string { return "the object has been modified" }

func TestExternalCreateRecovery_RetriesOnConflict(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()

	fake, ok := opts.Client.(*dynamicfake.FakeDynamicClient)
	require.True(t, ok, "test fake must be a FakeDynamicClient to install a reactor")
	attempts := conflictOnce(fake, opts.GVR)

	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	// Exists returns true, so recovery takes the "create actually succeeded" branch and stamps the
	// success marker — one of the two branches that reaches the write.
	ctrl.externalClient = &existenceCheckerClient{
		fakeExternalClient: fakeExternalClient{ObserveExists: true, ObserveUpToDate: true},
		exists:             true,
	}

	obj := createTestUnstructured("recover-conflict", opts.Namespace)
	meta.SetExternalCreatePending(obj, time.Now().Add(-24*time.Hour)) // well past the grace period
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	// PREMISE: the recovery block must actually be entered, or this asserts nothing.
	require.True(t, meta.ExternalCreateIncomplete(created),
		"premise: the object must look like an incomplete external create")

	err = ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	})
	require.NoError(t, err,
		"a single Conflict must be retried, not returned: returning it skips the dispatch "+
			"entirely, so the spec is never pushed and the resource wedges (#28)")

	require.Greater(t, *attempts, 1,
		"only one update was attempted, so no retry happened — the conflict was returned")

	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, meta.ExternalCreateIncomplete(got),
		"the pending marker survived: the annotation reconcile never landed, which is the wedge")
}

// Control: with no conflict, behaviour is unchanged and exactly one update is attempted — so the
// retry cannot be masking a double-write in the happy path.
func TestExternalCreateRecovery_NoConflictNoRetry(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()

	fake, ok := opts.Client.(*dynamicfake.FakeDynamicClient)
	require.True(t, ok)
	n := 0
	fake.PrependReactor("update", opts.GVR.Resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() == "" {
			n++
		}
		return false, nil, nil
	})

	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = &existenceCheckerClient{
		fakeExternalClient: fakeExternalClient{ObserveExists: true, ObserveUpToDate: true},
		exists:             true,
	}

	obj := createTestUnstructured("recover-clean", opts.Namespace)
	meta.SetExternalCreatePending(obj, time.Now().Add(-24*time.Hour))
	// Pre-seed the finalizer so the finalizer block below the recovery does NOT also write.
	// Without this the count conflates two unrelated updates and the control asserts nothing
	// about the recovery write — which is what it is here to measure.
	obj.SetFinalizers([]string{finalizerName})
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
	require.Equal(t, 1, n,
		"the happy path must attempt exactly ONE non-status update: more would mean the retry "+
			"wrapper is re-running a write that already succeeded")
}

var _ = unstructured.Unstructured{}
