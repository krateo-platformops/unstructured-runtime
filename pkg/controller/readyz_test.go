package controller

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// listGate makes the fake API server fail every LIST of the controller's resource while failing is
// set, with the error a real API server returns for a CRD version it no longer serves, and counts
// both outcomes so a test can prove a list was actually ATTEMPTED (and failed) before it reads the
// readiness answer. Without that count, "not ready" could just mean the informer had not got round
// to listing yet, and the before-half of the transition would pass without exercising anything.
type listGate struct {
	failing   atomic.Bool
	failures  atomic.Int32
	successes atomic.Int32
}

func (g *listGate) install(t *testing.T, opts Options) {
	t.Helper()
	fc, ok := opts.Client.(*dynamicfake.FakeDynamicClient)
	require.True(t, ok, "test options must carry the fake dynamic client")
	gr := opts.GVR.GroupResource()
	fc.PrependReactor("list", opts.GVR.Resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		if g.failing.Load() {
			g.failures.Add(1)
			// 404 with no details renders as "the server could not find the requested resource",
			// the error measured in #333 for an unserved CRD version.
			return true, nil, apierrors.NewGenericServerResponse(http.StatusNotFound, "list", gr, "", "", 0, true)
		}
		g.successes.Add(1)
		return false, nil, nil // fall through to the fake's object tracker: a real, successful list
	})
}

func newReadyzController(t *testing.T, gate *listGate) *Controller {
	t.Helper()
	opts := drainTestOptions([]runtime.Object{createTestUnstructured("readyz-obj", "test-namespace")}, durPtr(0))
	gate.install(t, opts)
	return newDrainController(t, opts, &mockExternalClient{
		observeFunc: func(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})
}

// TestReadyzCheck_NotReadyUntilFirstSuccessfulList_ThenReady is the transition #333 asks for. A
// check that always answers ready passes an after-only test, and one that never does passes a
// before-only test; only asserting BOTH sides of the first successful list, on one controller,
// pins the decision to the list.
func TestReadyzCheck_NotReadyUntilFirstSuccessfulList_ThenReady(t *testing.T) {
	gate := &listGate{}
	gate.failing.Store(true)
	c := newReadyzController(t, gate)

	require.Error(t, c.ReadyzCheck(nil), "a controller that has not been run cannot have listed anything")

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx, 1) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})

	// Before: wait for the informer to have TRIED and failed twice (the first attempt and one
	// backoff retry), so not-ready is a statement about failed lists, not about a slow start.
	require.Eventually(t, func() bool { return gate.failures.Load() >= 2 }, 15*time.Second, 10*time.Millisecond,
		"the informer never attempted the list, so the before-half would be vacuous")
	require.Zero(t, gate.successes.Load())
	err := c.ReadyzCheck(nil)
	require.Error(t, err, "ready while every list of the resource has failed: this is #333")
	require.Contains(t, err.Error(), c.gvr.String(), "the reason must name the resource that cannot be listed")

	// The resource becomes listable (the CRD version is served again).
	gate.failing.Store(false)

	// After: the reflector's next retry lists successfully, and the controller becomes ready.
	require.Eventually(t, func() bool { return c.ReadyzCheck(nil) == nil }, 15*time.Second, 10*time.Millisecond,
		"still not ready after the list succeeded")
	require.GreaterOrEqual(t, gate.successes.Load(), int32(1), "ready without any successful list")
}

// TestReadyzCheck_ListThatNeverSucceeds_StaysNotReady is the control for the case #333 measured: a
// controller whose list fails FOREVER. It must not drift to ready on a timer, on Run having started,
// or on the retry loop being alive. Run itself must still be running (blocked on the cache sync), so
// the not-ready answer is coming from a live controller and not from one that has already given up.
func TestReadyzCheck_ListThatNeverSucceeds_StaysNotReady(t *testing.T) {
	gate := &listGate{}
	gate.failing.Store(true)
	c := newReadyzController(t, gate)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx, 1) }()

	// Three failed attempts span two reflector backoff intervals.
	require.Eventually(t, func() bool { return gate.failures.Load() >= 3 }, 20*time.Second, 10*time.Millisecond,
		"the informer never retried the list")

	require.Error(t, c.ReadyzCheck(nil), "a controller whose list never succeeds reported ready")
	require.Zero(t, gate.successes.Load())
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) while the list was still failing; the check would be answering for a dead controller", err)
	default:
	}

	cancel()
	select {
	case err := <-runDone:
		// Cancelled during the cache sync: Run reports the sync it never completed.
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.Error(t, c.ReadyzCheck(nil), "a controller stopped before it ever listed cannot be ready")
}
