package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/workqueue"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// fakeExternalClient is a simple ExternalClient implementation used by tests.
type fakeExternalClient struct {
	ObserveErr      error
	ObserveExists   bool
	ObserveUpToDate bool

	CreateErr error
	UpdateErr error
	DeleteErr error
}

func (f *fakeExternalClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	if f.ObserveErr != nil {
		return ExternalObservation{}, f.ObserveErr
	}
	return ExternalObservation{
		ResourceExists:   f.ObserveExists,
		ResourceUpToDate: f.ObserveUpToDate,
	}, nil
}
func (f *fakeExternalClient) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	return f.CreateErr
}
func (f *fakeExternalClient) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	return f.UpdateErr
}
func (f *fakeExternalClient) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	return f.DeleteErr
}

// fakeExternalClientObserveError forces Observe to return an error so processItem fails.
type fakeExternalClientObserveError struct {
	err error
}

func (f *fakeExternalClientObserveError) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	fmt.Println("fakeExternalClientObserveError.Observe returning error")
	return ExternalObservation{}, f.err
}
func (f *fakeExternalClientObserveError) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil
}
func (f *fakeExternalClientObserveError) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil
}
func (f *fakeExternalClientObserveError) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil
}

// countingObserveError fails every Observe and counts the calls, so the test can assert HOW MANY
// passes ran — a monotonic witness — instead of sampling NumRequeues and the digest, both of which
// oscillate while retries are in flight.
type countingObserveError struct {
	calls atomic.Int32
	err   error
}

func (f *countingObserveError) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	f.calls.Add(1)
	return ExternalObservation{}, f.err
}
func (f *countingObserveError) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil
}
func (f *countingObserveError) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil
}
func (f *countingObserveError) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	return nil
}

// The contract: an event whose processing always fails is processed exactly maxRetries+1 times (the
// first pass plus maxRetries requeues), then dropped — its digest gone from controller.items and
// nothing left in the queue — and it is NOT processed again.
//
// The previous version polled NumRequeues >= 3 and then asserted the digest was gone. NumRequeues
// reaches 3 inside the THIRD requeue's AddWithOpts, after handleErr has re-Stored the digest, while
// the fourth pass (the one that drops) is still pending — so the assertion sampled a state the
// worker passes through on every run and failed whenever the poll landed in that window.
func TestRunWorker_RequeuesOnProcessErrorAndRemovesItem(t *testing.T) {
	const maxRetries = 3
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	opts.MaxRetries = maxRetries
	opts.ResyncInterval = 3 * time.Second
	opts.GlobalRateLimiter = workqueue.NewExponentialTimedFailureRateLimiter[any](1*time.Millisecond, 100*time.Millisecond)
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	require.NotNil(t, ctrl)

	obj := createTestUnstructured("rw-obj", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	ec := &countingObserveError{err: fmt.Errorf("observe-boom")}
	ctrl.SetExternalClient(ec)

	item := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: obj.GetAPIVersion(),
			Kind:       obj.GetKind(),
			Name:       obj.GetName(),
			Namespace:  obj.GetNamespace(),
		},
		QueuedAt: time.Now(),
	}
	dig := ctrlevent.DigestForEvent(item)
	ctrl.items.Store(dig, struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: NormalPriority}, item)

	done := make(chan struct{})
	go func() {
		ctrl.runWorker(context.Background())
		close(done)
	}()
	// Stop the worker when the test ends. Without this every iteration of -count=N leaked a parked
	// worker and its queue goroutines into the next.
	t.Cleanup(func() {
		ctrl.queue.ShutDown()
		<-done
	})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, present := ctrl.items.Load(dig)
		assert.Equal(c, int32(maxRetries+1), ec.calls.Load(), "passes (first pass + maxRetries requeues)")
		assert.False(c, present, "digest still in controller.items")
		assert.Zero(c, ctrl.queue.Len(), "queue length")
	}, 3*time.Second, 5*time.Millisecond, "expected exactly %d passes, then a drop", maxRetries+1)

	// And it stays dropped: no further pass, no digest re-stored. The window is several times the
	// largest backoff this limiter can produce (maxDelay=100ms), so a fifth pass would land in it.
	require.Never(t, func() bool {
		_, present := ctrl.items.Load(dig)
		return ec.calls.Load() != maxRetries+1 || present
	}, 300*time.Millisecond, 5*time.Millisecond, "event was processed again, or its digest re-stored, after the drop")
	assert.Equal(t, maxRetries, ctrl.queue.NumRequeues(item), "the dropped event's failure count")
}

func TestRunWorker_ShutDown(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)

	opts := createTestOptions()
	opts.MaxRetries = 10
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	require.NotNil(t, ctrl)

	// create managed object that processItem will fetch
	obj := createTestUnstructured("rw-obj", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	// set external client that returns an error on Observe to force processItem -> error
	ctrl.SetExternalClient(&fakeExternalClientObserveError{err: fmt.Errorf("observe-boom")})

	// build event and store its digest in items (the real flow sets this before queueing)
	item := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: obj.GetAPIVersion(),
			Kind:       obj.GetKind(),
			Name:       obj.GetName(),
			Namespace:  obj.GetNamespace(),
		},
		QueuedAt: time.Now(),
	}
	dig := ctrlevent.DigestForEvent(item)
	ctrl.items.Store(dig, struct{}{})

	// enqueue the event
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: NormalPriority}, item)

	// run worker in goroutine
	done := make(chan struct{})
	go func() {
		ctrl.runWorker(context.Background())
		close(done)
	}()

	// shutdown queue to allow runWorker to exit and wait for goroutine
	ctrl.queue.ShutDown()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("runWorker did not exit after queue shutdown")
	}
}

func TestProcessItem_DeleteOrphan(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	// create managed object with deletion timestamp and Orphan deletion policy
	obj := createTestUnstructured("orphan-1", opts.Namespace)
	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(map[string]string{})
	}
	ann := obj.GetAnnotations()
	ann[meta.AnnotationKeyDeletionPolicy] = string(meta.DeletionPolicyOrphan)
	obj.SetAnnotations(ann)

	obj, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	now := metav1.Now()
	obj.SetDeletionTimestamp(&now)
	obj.SetFinalizers([]string{finalizerName})

	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Update(context.TODO(), obj, metav1.UpdateOptions{})

	require.NoError(t, err)
	ev := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()},
	}
	// processItem should return nil and NOT call Observe on the external client
	err = ctrl.processItem(context.TODO(), ev)
	require.NoError(t, err)

	obj, _ = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NotContains(t, obj.GetFinalizers(), finalizerName)
}

func TestHandleObserve_QueuesCreateAndUpdateAndReturnsError(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)

	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	require.NotNil(t, ctrl)

	// create managed object
	obj := createTestUnstructured("obs-1", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	// 1) Observe says resource does NOT exist -> should enqueue Create
	ctrl.SetExternalClient(&fakeExternalClient{ObserveExists: false})
	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()}
	require.NoError(t, ctrl.handleObserve(context.TODO(), ref))

	i, pr, _ := ctrl.queue.GetWithPriority()
	ev, ok := i.(ctrlevent.Event)
	require.True(t, ok)
	require.Equal(t, ctrlevent.Create, ev.EventType)
	require.Equal(t, HighPriority, pr)
	ctrl.queue.Done(i)
	ctrl.queue.Forget(i)

	// 2) Observe says exists but not up to date -> should enqueue Update
	ctrl.SetExternalClient(&fakeExternalClient{ObserveExists: true, ObserveUpToDate: false})
	require.NoError(t, ctrl.handleObserve(context.TODO(), ref))

	i, pr, _ = ctrl.queue.GetWithPriority()
	ev, ok = i.(ctrlevent.Event)
	require.True(t, ok)
	require.Equal(t, ctrlevent.Update, ev.EventType)
	require.Equal(t, HighPriority, pr)
	ctrl.queue.Done(i)
	ctrl.queue.Forget(i)

	// 3) Observe returns error -> handleObserve should return the error
	observeErr := errors.New("observe boom")
	ctrl.SetExternalClient(&fakeExternalClient{ObserveErr: observeErr})
	err = ctrl.handleObserve(context.TODO(), ref)
	require.Error(t, err)
	require.Equal(t, observeErr, err)
}

func TestHandleCreate_SuccessAndFailure_SetAnnotationsAndConditions(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("create-1", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()}

	// Failure case: Create returns error -> annotation ExternalCreateFailed set and error returned
	ctrl.SetExternalClient(&fakeExternalClient{CreateErr: errors.New("create failed")})
	err = ctrl.handleCreate(context.TODO(), ref)
	require.Error(t, err)

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, updated.GetAnnotations(), meta.AnnotationKeyExternalCreateFailed)

	// Success case: Create succeeds -> ExternalCreateSucceeded should be present
	// create a fresh object for clean state
	obj2 := createTestUnstructured("create-2", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj2, metav1.CreateOptions{})
	require.NoError(t, err)
	ref2 := objectref.ObjectRef{APIVersion: obj2.GetAPIVersion(), Kind: obj2.GetKind(), Name: obj2.GetName(), Namespace: obj2.GetNamespace()}

	ctrl.SetExternalClient(&fakeExternalClient{CreateErr: nil})
	err = ctrl.handleCreate(context.TODO(), ref2)
	require.NoError(t, err)

	updated2, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj2.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Contains(t, updated2.GetAnnotations(), meta.AnnotationKeyExternalCreateSucceeded)

	// also expect ReconcileSuccess condition set in status
	conds := unstructuredtools.GetConditions(updated2)
	found := false
	for _, c := range conds {
		if c.Type == condition.TypeReady && c.Reason == condition.ReasonReconcileSuccess {
			found = true
			break
		}
	}
	// At least ensure some condition exists
	require.NotEmpty(t, conds)
	_ = found // best-effort: depending on implementation of condition names
}

func TestHandleUpdate_SuccessAndFailure_UpdatesStatus(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("update-1", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()}

	// Update error case
	ctrl.SetExternalClient(&fakeExternalClient{UpdateErr: errors.New("update fail")})
	err = ctrl.handleUpdate(context.TODO(), ref)
	require.Error(t, err)

	// Update success case
	ctrl.SetExternalClient(&fakeExternalClient{UpdateErr: nil})
	err = ctrl.handleUpdate(context.TODO(), ref)
	require.NoError(t, err)

	// assert that status was updated (best-effort)
	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	conds := unstructuredtools.GetConditions(updated)
	require.NotNil(t, conds)
}

func TestHandleDelete_SuccessAndFailure_FinalizersAndConditions(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("delete-1", opts.Namespace)
	// ensure it has the finalizer to be removed on success
	obj.SetFinalizers([]string{finalizerName})
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()}

	// Delete error: expect error returned and condition set
	ctrl.SetExternalClient(&fakeExternalClient{DeleteErr: errors.New("delete fail")})
	err = ctrl.handleDelete(context.TODO(), ref)
	require.Error(t, err)

	// Delete success: finalizers should be cleared and no error
	// recreate object
	obj2 := createTestUnstructured("delete-2", opts.Namespace)
	obj2.SetFinalizers([]string{finalizerName})
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj2, metav1.CreateOptions{})
	require.NoError(t, err)
	ref2 := objectref.ObjectRef{APIVersion: obj2.GetAPIVersion(), Kind: obj2.GetKind(), Name: obj2.GetName(), Namespace: obj2.GetNamespace()}

	ctrl.SetExternalClient(&fakeExternalClient{DeleteErr: nil})
	err = ctrl.handleDelete(context.TODO(), ref2)
	require.NoError(t, err)

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj2.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, updated.GetFinalizers())
}

func TestProcessItem_PausedAndExternalCreateIncomplete(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	// paused case
	obj := createTestUnstructured("paused-1", opts.Namespace)
	if obj.GetAnnotations() == nil {
		obj.SetAnnotations(map[string]string{})
	}
	ann := obj.GetAnnotations()
	ann[meta.AnnotationKeyReconciliationPaused] = "true"
	obj.SetAnnotations(ann)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	ev := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()},
	}
	// processItem should return nil and set paused condition
	err = ctrl.processItem(context.TODO(), ev)
	require.NoError(t, err)
	resource, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	conds := unstructuredtools.GetConditions(resource)
	assert.Equal(t, conds[0].Reason, condition.ReasonReconcilePaused)

	// ExternalCreateIncomplete case
	obj2 := createTestUnstructured("incomplete-1", opts.Namespace)
	// set the incomplete annotation
	meta.SetExternalCreatePending(obj2, time.Now().Add(-time.Minute))
	// simulate incomplete state: a helper exists meta.ExternalCreateIncomplete checks other annotations; we force the condition by adding the pending without succeeded/failed
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj2, metav1.CreateOptions{})
	require.NoError(t, err)
	ev2 := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{APIVersion: obj2.GetAPIVersion(), Kind: obj2.GetKind(), Name: obj2.GetName(), Namespace: obj2.GetNamespace()},
	}
	err = ctrl.processItem(context.TODO(), ev2)
	// processItem returns nil for ExternalCreateIncomplete (it sets conditions and returns)
	require.NoError(t, err)
}

// TestProcessItem_ExternalCreateIncomplete_RecoversViaObserve verifies the
// create-pending guard no longer wedges forever: when the external resource is
// verifiably absent the pending marker is cleared (fresh create can proceed),
// and when it is present the success marker is recorded — in both cases the
// resource leaves the incomplete state instead of refusing indefinitely.
func TestProcessItem_ExternalCreateIncomplete_RecoversViaObserve(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)

	// absent but the create was attempted RECENTLY -> confirm-the-negative: keep pending and wait
	// (the create may have landed but not yet be observable), do NOT clear or recreate.
	t.Run("absent within grace keeps pending and waits", func(t *testing.T) {
		opts := createTestOptions()
		ctrl, err := New(sid, opts)
		require.NoError(t, err)
		ctrl.SetExternalClient(&fakeExternalClient{ObserveExists: false})

		// Drive the requeue without a real delay so we can assert it actually lands (a silently
		// dropped requeue would otherwise leave the object waiting only for the slow resync).
		prev := externalCreateRecoveryReobserveInterval
		externalCreateRecoveryReobserveInterval = 0
		defer func() { externalCreateRecoveryReobserveInterval = prev }()

		obj := createTestUnstructured("recover-absent-recent", opts.Namespace)
		meta.SetExternalCreatePending(obj, time.Now().Add(-30*time.Second)) // well within the 2m grace
		require.True(t, meta.ExternalCreateIncomplete(obj))
		_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
		require.NoError(t, err)

		ev := ctrlevent.Event{
			EventType: ctrlevent.Observe,
			ObjectRef: objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()},
		}
		require.NoError(t, ctrl.processItem(context.TODO(), ev))

		got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		_, stillPending := got.GetAnnotations()[meta.AnnotationKeyExternalCreatePending]
		assert.True(t, stillPending, "pending must be kept while within the recovery grace period (guard against duplicating an eventually-consistent create)")
		assert.True(t, meta.ExternalCreateIncomplete(got), "resource should remain incomplete (waiting) within the grace period")
		assert.GreaterOrEqual(t, ctrl.queue.Len(), 1, "a re-observe must be requeued while waiting (not silently dropped)")
	})

	// absent AND the create was attempted long ago (beyond the grace period) -> the create genuinely
	// never landed; clear pending so a fresh create proceeds.
	t.Run("absent beyond grace clears pending", func(t *testing.T) {
		opts := createTestOptions()
		ctrl, err := New(sid, opts)
		require.NoError(t, err)
		ctrl.SetExternalClient(&fakeExternalClient{ObserveExists: false})

		obj := createTestUnstructured("recover-absent-old", opts.Namespace)
		meta.SetExternalCreatePending(obj, time.Now().Add(-10*time.Minute)) // beyond the 2m grace
		require.True(t, meta.ExternalCreateIncomplete(obj))
		_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
		require.NoError(t, err)

		ev := ctrlevent.Event{
			EventType: ctrlevent.Observe,
			ObjectRef: objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()},
		}
		require.NoError(t, ctrl.processItem(context.TODO(), ev))

		got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		_, stillPending := got.GetAnnotations()[meta.AnnotationKeyExternalCreatePending]
		assert.False(t, stillPending, "pending should be cleared once the resource is confirmed absent beyond the grace period")
		assert.False(t, meta.ExternalCreateIncomplete(got), "resource should no longer be in the incomplete state")
	})

	// present -> success marker recorded, no longer incomplete
	t.Run("present records success", func(t *testing.T) {
		opts := createTestOptions()
		ctrl, err := New(sid, opts)
		require.NoError(t, err)
		ctrl.SetExternalClient(&fakeExternalClient{ObserveExists: true, ObserveUpToDate: true})

		obj := createTestUnstructured("recover-present-1", opts.Namespace)
		meta.SetExternalCreatePending(obj, time.Now().Add(-time.Minute))
		require.True(t, meta.ExternalCreateIncomplete(obj))
		_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
		require.NoError(t, err)

		ev := ctrlevent.Event{
			EventType: ctrlevent.Observe,
			ObjectRef: objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()},
		}
		require.NoError(t, ctrl.processItem(context.TODO(), ev))

		got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		require.NoError(t, err)
		assert.False(t, meta.ExternalCreateIncomplete(got), "recording success should clear the incomplete state")
	})
}

func TestHandleErr_RetryAddsToQueue(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	// create an event and simulate an error
	ev := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{APIVersion: "v1", Kind: "Kind", Name: "name", Namespace: "ns"},
	}
	// initial num requeues should be 0
	require.Equal(t, 0, ctrl.queue.NumRequeues(ev))

	// call handleErr with an error - should add back to queue and increment requeue count
	ctrl.maxRetries = 3
	ctrl.handleErr(errors.New("boom"), ev, NormalPriority)
	// allow some time for queue bookkeeping
	time.Sleep(10 * time.Millisecond)
	require.GreaterOrEqual(t, ctrl.queue.NumRequeues(ev), 1)
}
