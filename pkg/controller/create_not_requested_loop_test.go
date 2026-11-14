package controller

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// rvTracker bumps metadata.resourceVersion on every write, as the apiserver does. The fake dynamic
// client's tracker leaves it unchanged on Update and Patch, and the informer's UpdateFunc reads an
// UNCHANGED resourceVersion as a periodic resync (queue an Observe) while a changed one with the same
// spec is ignored. Without this, every metadata write in a test reads as a resync, and a pass count
// measures the fake, not the runtime.
type rvTracker struct {
	clienttesting.ObjectTracker
	next atomic.Int64
}

func (r *rvTracker) bump(obj runtime.Object) {
	if a, err := apimeta.Accessor(obj); err == nil {
		a.SetResourceVersion(strconv.FormatInt(r.next.Add(1)+1000, 10))
	}
}

func (r *rvTracker) Update(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.UpdateOptions) error {
	r.bump(obj)
	return r.ObjectTracker.Update(gvr, obj, ns, opts...)
}

func (r *rvTracker) Patch(gvr schema.GroupVersionResource, obj runtime.Object, ns string, opts ...metav1.PatchOptions) error {
	r.bump(obj)
	return r.ObjectTracker.Patch(gvr, obj, ns, opts...)
}

// withRealResourceVersions routes the fake client through an rvTracker, for reads, writes and watches.
func withRealResourceVersions(opts Options) Options {
	fc := opts.Client.(*dynamicfake.FakeDynamicClient)
	rt := &rvTracker{ObjectTracker: fc.Tracker()}
	fc.PrependReactor("*", "*", clienttesting.ObjectReaction(rt))
	fc.PrependWatchReactor("*", func(action clienttesting.Action) (bool, watch.Interface, error) {
		w, err := rt.Watch(action.GetResource(), action.GetNamespace())
		return true, w, err
	})
	return opts
}

// notRequestedClient never creates: Observe says the resource does not exist, and CreateWithResult
// reports Requested=false (rest-dynamic-controller's no-create-verb kinds). It counts create passes.
type notRequestedClient struct {
	mockExternalClient
	creates atomic.Int64
}

func (c *notRequestedClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	return ExternalObservation{ResourceExists: false}, nil
}

func (c *notRequestedClient) CreateWithResult(ctx context.Context, mg *unstructured.Unstructured) (CreateResult, error) {
	c.creates.Add(1)
	return CreateResult{Requested: false, Reason: "NoCreateVerbDeclared"}, nil
}

// oasgen-provider#674, through the REAL loop (informer, queue, workers) with the builder's own
// annotation watch registered. v1.17.0's not-requested path DELETED external-create-pending, and the
// builder's OnDelete watch on that annotation enqueues a Create at once, so each pass scheduled the
// next with no rate limit (measured ~5 metadata writes a second per object on rest-dynamic-controller
// 0.66.0). The acceptance criterion is a pass COUNT per unit of time: a no-create object must be
// visited about once per resync, not continuously. Here the resync is 30s and the run 2s, so more
// than a couple of create passes is the loop.
func TestCreateNotRequested_DoesNotRetriggerItself(t *testing.T) {
	obj := createTestUnstructured("no-create", "test-namespace")
	opts := withRealResourceVersions(drainTestOptions([]runtime.Object{obj}, durPtr(-1)))
	watches := ctrlevent.NewAnnotationEvents()
	// The builder's registrations (builder/options.go defaultOptions), so the trigger is the real one.
	watches.Add(ctrlevent.Observe, meta.AnnotationKeyReconciliationPaused, ctrlevent.OnAny, false)
	watches.Add(ctrlevent.Create, meta.AnnotationKeyExternalCreatePending, ctrlevent.OnDelete, false)
	opts.WatchAnnotations = watches

	ext := &notRequestedClient{}
	c := newDrainController(t, opts, ext)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 2) }()

	time.Sleep(2 * time.Second)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	n := ext.creates.Load()
	require.GreaterOrEqual(t, n, int64(1), "precondition: the object was never visited, so the count proves nothing")
	require.LessOrEqual(t, n, int64(2), "%d create passes in 2s with a 30s resync: a not-requested pass re-triggers itself", n)
	t.Logf("%d create pass(es) in 2s", n)
}
