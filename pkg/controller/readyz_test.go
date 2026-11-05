package controller

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientfeatures "k8s.io/client-go/features"
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

// TestReadyzCheck_WatchListMode_NotReadyUntilInitialEventsEnd_ThenReady repeats the transition with
// the WatchListClient gate ON, which is client-go v0.35's DEFAULT and therefore what a deployed
// controller runs. This package's TestMain switches the gate off for every other test (the fake
// never sends the initial-events bookmark), so without this test the readiness decision would be
// proven only on the path production does not take first.
//
// In this mode the reflector first opens a streaming watch. For an unserved resource that watch
// fails, the reflector falls back to a LIST (client-go tools/cache/reflector.go, ListAndWatchWithContext),
// and the LIST fails too: that fallback is where the measured "failed to list" comes from. When the
// resource is served, the stream delivers a bookmark carrying k8s.io/initial-events-end and no LIST
// happens at all. Both halves are counted, so the test also proves which path it exercised.
func TestReadyzCheck_WatchListMode_NotReadyUntilInitialEventsEnd_ThenReady(t *testing.T) {
	type featureSetter interface {
		Set(clientfeatures.Feature, bool) error
	}
	fg, ok := clientfeatures.FeatureGates().(featureSetter)
	require.True(t, ok, "client-go feature gates are not settable; this test cannot select watch-list mode")
	require.NoError(t, fg.Set(clientfeatures.WatchListClient, true))
	t.Cleanup(func() { require.NoError(t, fg.Set(clientfeatures.WatchListClient, false)) })

	gate := &listGate{}
	gate.failing.Store(true)
	opts := drainTestOptions(nil, durPtr(0))
	gate.install(t, opts)
	var watchFailures, streams atomic.Int32
	fc := opts.Client.(*dynamicfake.FakeDynamicClient)
	gr := opts.GVR.GroupResource()
	fc.PrependWatchReactor(opts.GVR.Resource, func(clienttesting.Action) (bool, watch.Interface, error) {
		if gate.failing.Load() {
			watchFailures.Add(1)
			return true, nil, apierrors.NewGenericServerResponse(http.StatusNotFound, "watch", gr, "", "", 0, true)
		}
		streams.Add(1)
		w := watch.NewFakeWithChanSize(1, false)
		end := &unstructured.Unstructured{}
		end.SetResourceVersion("1")
		end.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
		w.Action(watch.Bookmark, end)
		return true, w, nil
	})
	c := newDrainController(t, opts, &mockExternalClient{})

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

	// Before: the stream was attempted and failed, and so was the fallback LIST, twice.
	require.Eventually(t, func() bool { return gate.failures.Load() >= 2 }, 15*time.Second, 10*time.Millisecond,
		"the reflector never fell back to a list")
	require.GreaterOrEqual(t, watchFailures.Load(), int32(2), "the LIST was not reached through a failed watch-list stream; wrong mode")
	require.Error(t, c.ReadyzCheck(nil), "ready while the stream and the list both fail")

	gate.failing.Store(false)

	require.Eventually(t, func() bool { return c.ReadyzCheck(nil) == nil }, 15*time.Second, 10*time.Millisecond,
		"still not ready after the stream delivered initial-events-end")
	require.GreaterOrEqual(t, streams.Load(), int32(1))
	require.Zero(t, gate.successes.Load(), "synced through a LIST, so this did not exercise the watch-list path")
}
