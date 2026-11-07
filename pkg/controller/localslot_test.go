package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// orderClient records the order objects reach the handler, and holds the first pass open until told.
type orderClient struct {
	countingClient
	mu      sync.Mutex
	order   []string
	gate    chan struct{}
	started atomic.Int64
}

func (o *orderClient) Observe(_ context.Context, u *unstructured.Unstructured) (ExternalObservation, error) {
	o.mu.Lock()
	o.order = append(o.order, u.GetName())
	o.mu.Unlock()
	if o.started.Add(1) == 1 {
		<-o.gate
	}
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}

func (o *orderClient) seen() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.order...)
}

// #72. With one slot and many workers, the queue's order must be the order work runs in. This used to
// fail: every worker popped, found no slot and slept out a deferral, so the high-priority item lost
// the free slot to whatever was undelayed.
func TestLocalSlot_HighPriorityRunsNextWhenTheSlotFrees(t *testing.T) {
	g := satGov(t, "healthy", false) // sampler unstarted: capacity stays at exactly 1
	oc := &orderClient{gate: make(chan struct{})}
	ctrl, _, opts := gateController(t, g, oc)
	for _, n := range []string{"first", "low-1", "low-2", "high"} {
		_, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
			Create(context.TODO(), createTestUnstructured(n, opts.Namespace), metav1.CreateOptions{})
		require.NoError(t, err)
	}

	add := func(name string, priority int, at int64) {
		ev := eventFor(opts.Namespace, ctrlevent.Observe, name, at)
		ev.ObjectRef.APIVersion, ev.ObjectRef.Kind = createdRef(t, opts, name)
		ctrl.items.Store(ctrlevent.DigestForEvent(ev), struct{}{})
		ctrl.queue.AddWithOpts(priorityqueue.AddOpts{Priority: priority}, ev)
	}
	add("first", LowPriority, 1)

	ctx, cancel := context.WithCancel(context.Background())
	ctrl.runCtx = ctx // workers run directly here, so nothing else would end a wait for a slot
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); ctrl.runWorker(ctx) }()
	}
	var open sync.Once
	letGo := func() { open.Do(func() { close(oc.gate) }) }
	t.Cleanup(func() {
		letGo() // a failed assertion must not leave the first pass, and so this cleanup, hanging
		cancel()
		ctrl.queue.ShutDown()
		wg.Wait()
	})

	waitFor(t, "the first pass to take the slot", func() bool { return oc.started.Load() == 1 })
	add("low-1", LowPriority, 2)
	add("low-2", LowPriority, 3)
	add("high", HighPriority, 4)
	time.Sleep(50 * time.Millisecond) // the other workers had every chance to pop, if they could
	require.Equal(t, []string{"first"}, oc.seen())

	letGo()
	waitFor(t, "all four to run", func() bool { return len(oc.seen()) == 4 })
	require.Equal(t, "high", oc.seen()[1], "the free slot went to a lower-priority item: %v", oc.seen())
}

func createdRef(t *testing.T, opts Options, name string) (string, string) {
	t.Helper()
	u, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return u.GetAPIVersion(), u.GetKind()
}

// A worker that popped must give the slot back on every way out, or capacity leaks until a restart.
func TestLocalSlot_IsReleasedOnEveryExit(t *testing.T) {
	for name, tc := range map[string]struct {
		item any
	}{
		"an item that is not an event": {item: "not-an-event"},
		"an ordinary pass":             {},
	} {
		t.Run(name, func(t *testing.T) {
			g := satGov(t, "healthy", false)
			cc := &countingClient{}
			ctrl, evt, _ := gateController(t, g, cc)
			if tc.item != nil {
				ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, tc.item)
			} else {
				ctrl.items.Store(ctrlevent.DigestForEvent(evt), struct{}{})
				ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, evt)
			}
			t.Cleanup(ctrl.queue.ShutDown)

			require.True(t, ctrl.processNext(context.Background(), "test"))
			release, ok := g.(interface {
				AcquireLocal(context.Context) (func(), bool)
			}).AcquireLocal(deadline(t))
			require.True(t, ok, "the slot was not returned (%s)", name)
			release()
		})
	}
}

// A panic in a pass unwinds through processNext; wait.Until restarts the worker, and the slot must not
// go with it.
func TestLocalSlot_IsReleasedWhenAPassPanics(t *testing.T) {
	g := satGov(t, "healthy", false)
	ctrl, evt, _ := gateController(t, g, &panicClient{})
	ctrl.items.Store(ctrlevent.DigestForEvent(evt), struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, evt)
	t.Cleanup(ctrl.queue.ShutDown)

	func() {
		defer func() { _ = recover() }()
		ctrl.processNext(context.Background(), "test")
	}()

	release, ok := g.(interface {
		AcquireLocal(context.Context) (func(), bool)
	}).AcquireLocal(deadline(t))
	require.True(t, ok, "a panicking pass kept the slot")
	release()
}

type panicClient struct{ countingClient }

func (*panicClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	panic("boom")
}

func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// A worker waiting for a slot must not hang a drain. reconcileCtx stays live through one, so waiting
// on it would never end.
func TestLocalSlot_AWaitingWorkerDoesNotHangADrain(t *testing.T) {
	g := satGov(t, "healthy", false)
	gate := g.(interface {
		AcquireLocal(context.Context) (func(), bool)
	})
	release, ok := gate.AcquireLocal(context.Background()) // the only slot, held for the whole test
	require.True(t, ok)
	t.Cleanup(release)

	ctrl, _, _ := gateController(t, g, &countingClient{})
	timeout := 5 * time.Second
	ctrl.gracefulShutdownTimeout = timeout

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = ctrl.Run(ctx, 4); close(done) }()
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(timeout / 2):
		t.Fatal("workers waiting for a slot held the drain open")
	}
}
