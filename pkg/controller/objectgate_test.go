package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// #62. The queue key is the whole ctrlevent.Event, QueuedAt included, so two events for one object
// are two keys and two workers could reconcile one object at once. These tests run REAL workers
// against a handler that holds each Observe open and records how many passes overlap per object.

func refFor(name string) objectref.ObjectRef {
	return objectref.ObjectRef{APIVersion: "test.example.org/v1", Kind: "Test", Name: name, Namespace: "default"}
}

func TestObjectGate_OnePassPerObjectAndParkedEventsComeBack(t *testing.T) {
	g := newObjectGate()
	a := ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: refFor("a"), QueuedAt: time.Unix(1, 0)}

	require.True(t, g.begin(a, NormalPriority), "an idle object must be claimable")
	require.True(t, g.begin(ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: refFor("b")}, NormalPriority),
		"a DIFFERENT object must not wait behind a: the gate is per object, not global")

	obs2 := ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: refFor("a"), QueuedAt: time.Unix(2, 0)}
	obs3 := ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: refFor("a"), QueuedAt: time.Unix(3, 0)}
	del := ctrlevent.Event{EventType: ctrlevent.Delete, ObjectRef: refFor("a"), QueuedAt: time.Unix(4, 0)}
	require.False(t, g.begin(obs2, 10), "a second pass for a held object must be parked, not run")
	require.False(t, g.begin(obs3, NormalPriority))
	require.False(t, g.begin(del, NormalPriority))

	parked := g.end(a)
	byType := map[ctrlevent.EventType]parkedEvent{}
	for _, p := range parked {
		byType[p.event.EventType] = p
	}
	require.Len(t, parked, 2, "one parked event per TYPE: the two Observes collapse, the Delete is kept beside them")
	require.Contains(t, byType, ctrlevent.Delete, "a Delete that arrived during a pass must never be lost behind it")
	require.Equal(t, obs3.QueuedAt, byType[ctrlevent.Observe].event.QueuedAt, "the LATEST event of a type is kept")
	require.Equal(t, 10, byType[ctrlevent.Observe].priority, "the HIGHER priority of the collapsed events is kept")

	require.Empty(t, g.end(a), "end must clear what it returned, or a later end would requeue it twice")
	require.True(t, g.begin(obs3, NormalPriority), "after end the object is claimable again")
}

// overlapClient holds EVERY handler call open until release is closed, and records the peak number
// of concurrent passes per object. Every method, not just Observe: the runtime dispatches by event
// type, so an Update event calls Update and never Observe. A fixture that only watched Observe would
// not see the pass an Update event makes, which is exactly the overlap #62 is about.
type overlapClient struct {
	mu      sync.Mutex
	active  map[string]int
	peak    map[string]int
	calls   map[string]int
	release chan struct{}
}

func newOverlapClient() *overlapClient {
	return &overlapClient{active: map[string]int{}, peak: map[string]int{}, calls: map[string]int{}, release: make(chan struct{})}
}

func (o *overlapClient) pass(mg *unstructured.Unstructured) {
	n := mg.GetName()
	o.mu.Lock()
	o.active[n]++
	o.calls[n]++
	if o.active[n] > o.peak[n] {
		o.peak[n] = o.active[n]
	}
	o.mu.Unlock()
	<-o.release
	o.mu.Lock()
	o.active[n]--
	o.mu.Unlock()
}

func (o *overlapClient) Observe(_ context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	o.pass(mg)
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}
func (o *overlapClient) Create(_ context.Context, mg *unstructured.Unstructured) error {
	o.pass(mg)
	return nil
}
func (o *overlapClient) Update(_ context.Context, mg *unstructured.Unstructured) error {
	o.pass(mg)
	return nil
}
func (o *overlapClient) Delete(_ context.Context, mg *unstructured.Unstructured) error {
	o.pass(mg)
	return nil
}

func (o *overlapClient) totalActive() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	t := 0
	for _, v := range o.active {
		t += v
	}
	return t
}

func (o *overlapClient) get(m map[string]int, n string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return m[n]
}

// overlapController starts `workers` real workers over a controller whose handler is oc, with the
// named objects present in the fake cluster.
func overlapController(t *testing.T, workers int, oc *overlapClient, names ...string) (*Controller, string) {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	opts.MaxRetries = 5
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.SetExternalClient(oc)
	for _, n := range names {
		_, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
			Create(context.TODO(), createTestUnstructured(n, opts.Namespace), metav1.CreateOptions{})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); ctrl.runWorker(ctx) }()
	}
	t.Cleanup(func() {
		select {
		case <-oc.release:
		default:
			close(oc.release)
		}
		cancel()
		ctrl.queue.ShutDown()
		wg.Wait()
	})
	return ctrl, opts.Namespace
}

func enqueue(ctrl *Controller, ev ctrlevent.Event) {
	ctrl.items.Store(ctrlevent.DigestForEvent(ev), struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: NormalPriority}, ev)
}

func eventFor(ns string, typ ctrlevent.EventType, name string, at int64) ctrlevent.Event {
	return ctrlevent.Event{
		EventType: typ,
		ObjectRef: objectref.ObjectRef{APIVersion: "test.example.org/v1", Kind: "Test", Name: name, Namespace: ns},
		QueuedAt:  time.Unix(at, 0),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// THE CONTROL, and without it the next test proves nothing: two DIFFERENT objects on two workers
// must overlap. If they cannot, this fixture serialises everything on its own and a peak of 1 below
// would be the fixture's, not the gate's.
func TestWorkers_DifferentObjectsStillRunInParallel(t *testing.T) {
	oc := newOverlapClient()
	ctrl, ns := overlapController(t, 2, oc, "obj-a", "obj-b")
	enqueue(ctrl, eventFor(ns, ctrlevent.Observe, "obj-a", 1))
	enqueue(ctrl, eventFor(ns, ctrlevent.Observe, "obj-b", 2))

	waitFor(t, "both objects to be inside a handler at once", func() bool { return oc.totalActive() == 2 })
}

// THE DEFECT. Two events for ONE object, distinct keys (different QueuedAt), two free workers. The
// second must not start until the first has finished, and must still run afterwards.
func TestWorkers_OneObjectIsNeverReconciledConcurrently(t *testing.T) {
	oc := newOverlapClient()
	ctrl, ns := overlapController(t, 2, oc, "obj")
	enqueue(ctrl, eventFor(ns, ctrlevent.Observe, "obj", 1))
	enqueue(ctrl, eventFor(ns, ctrlevent.Update, "obj", 2))

	waitFor(t, "the first pass to be inside a handler", func() bool { return oc.get(oc.active, "obj") == 1 })
	// Give the second worker every chance to start a concurrent pass.
	waitFor(t, "the queue to be drained by the idle worker", func() bool { return ctrl.queue.Len() == 0 })
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, oc.get(oc.peak, "obj"), "two passes for one object overlapped (#62)")

	close(oc.release)
	waitFor(t, "the parked event to run after the first pass", func() bool { return oc.get(oc.calls, "obj") == 2 })
	require.Equal(t, 1, oc.get(oc.peak, "obj"), "the parked pass overlapped the first one")
}
