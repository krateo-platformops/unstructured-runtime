package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// recordingQueue records every event added, delayed or not: Len counts only ready items, so a key
// waiting out a delay would be invisible to it.
type recordingQueue struct {
	priorityqueue.PriorityQueue[any]
	mu   sync.Mutex
	adds []ctrlevent.Event
}

func (r *recordingQueue) AddWithOpts(o priorityqueue.AddOpts, items ...any) {
	r.mu.Lock()
	for _, it := range items {
		if ev, ok := it.(ctrlevent.Event); ok {
			r.adds = append(r.adds, ev)
		}
	}
	r.mu.Unlock()
	r.PriorityQueue.AddWithOpts(o, items...)
}

func (r *recordingQueue) added(typ ctrlevent.EventType) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, ev := range r.adds {
		if ev.EventType == typ {
			n++
		}
	}
	return n
}

// refusingClient observes every object as absent and refuses every create, classified as not
// committed (a vendor 4xx), as rest-dynamic-controller does for a 422.
type refusingClient struct{ countingClient }

func (r *refusingClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	r.calls.Add(1)
	return ExternalObservation{ResourceExists: false}, nil
}

func (r *refusingClient) Create(context.Context, *unstructured.Unstructured) error {
	r.calls.Add(1)
	return CreateNotCommitted(errors.New("vendor refused the create (422)"))
}

func recording(ctrl *Controller) *recordingQueue {
	rq := &recordingQueue{PriorityQueue: ctrl.queue}
	ctrl.queue = rq
	return rq
}

// #101 through the real lifecycle, not a hand-stored digest (oasgen-provider's question on #103): a
// Create pass that fails leaves its retry pending, and an Observe pass that then finds the resource
// absent must not add a second Create. Control: the same Observe pass with no failed Create before it
// adds exactly one.
func TestLifecycle_AFailedCreateKeepsTheOnlyCreateKey(t *testing.T) {
	for _, failedFirst := range []bool{false, true} {
		name := "control: no failed Create before, the Observe adds one"
		if failedFirst {
			name = "a failed Create pending: the Observe adds none"
		}
		t.Run(name, func(t *testing.T) {
			ctrl, observe, _ := gateController(t, nil, &refusingClient{})
			t.Cleanup(ctrl.queue.ShutDown)
			ctrl.maxRetries = 5
			rq := recording(ctrl)
			ctrl.runCtx = context.Background()

			if failedFirst {
				create := observe
				create.EventType = ctrlevent.Create
				ctrl.items.Store(ctrlevent.DigestForEvent(create), struct{}{})
				ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, create)
				require.True(t, ctrl.processNext(context.Background(), "test"))
				require.Equal(t, 2, rq.added(ctrlevent.Create), "setup: the failed Create was not re-added for its retry")
			}
			before := rq.added(ctrlevent.Create)

			ctrl.items.Store(ctrlevent.DigestForEvent(observe), struct{}{})
			ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, observe)
			require.True(t, ctrl.processNext(context.Background(), "test"))

			want := 1
			if failedFirst {
				want = 0
			}
			require.Equal(t, want, rq.added(ctrlevent.Create)-before, "Create keys the Observe pass added")
		})
	}
}

// #104: an incomplete create inside its recovery grace is re-observed after a delay, and the waiting
// key holds its digest, so an event arriving during the wait dedups against it. It used to be enqueued
// from inside the pass, where the pass end deleted that same digest. Control: the pass did take the
// recovery path (one delayed re-add, no create attempted).
func TestRecoveryReobserveHoldsItsDigestWhileItWaits(t *testing.T) {
	cc := &refusingClient{}
	ctrl, observe, opts := gateController(t, nil, cc)
	t.Cleanup(ctrl.queue.ShutDown)
	rq := recording(ctrl)
	ctrl.runCtx = context.Background()

	// An incomplete create, inside its grace: create-pending set just now, no outcome recorded.
	res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
	u, err := res.Get(context.TODO(), observe.ObjectRef.Name, metav1.GetOptions{})
	require.NoError(t, err)
	meta.SetExternalCreatePending(u, time.Now())
	_, err = res.Update(context.TODO(), u, metav1.UpdateOptions{})
	require.NoError(t, err)

	ctrl.items.Store(ctrlevent.DigestForEvent(observe), struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, observe)
	require.True(t, ctrl.processNext(context.Background(), "test"))

	require.Equal(t, 2, len(rq.adds), "want the original add and ONE re-observe, got %v", rq.adds)
	require.Equal(t, 0, rq.added(ctrlevent.Create), "a create was enqueued inside the recovery grace")
	_, held := ctrl.items.Load(ctrlevent.DigestForEvent(observe))
	require.True(t, held, "the waiting re-observe holds no digest, so a second key can join it")
	_, loaded := ctrl.items.LoadOrStore(ctrlevent.DigestForEvent(observe), struct{}{})
	require.True(t, loaded, "an event arriving during the wait was not deduped")
}
