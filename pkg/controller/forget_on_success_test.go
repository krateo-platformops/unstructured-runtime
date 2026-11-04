package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientgowq "k8s.io/client-go/util/workqueue"
)

// #51. runWorker called queue.Forget UNCONDITIONALLY, immediately before handleErr reads
// NumRequeues — so the count was 0 on every pass, `0 >= maxRetries` was never true, and an
// unprocessable event was retried forever with maxRetries configured and silently ignored.
//
// ASSERTED AGAINST THE BUILDER'S DEFAULT LIMITER, deliberately, and this is the whole reason the
// defect survived. The existing TestRunWorker_RequeuesOnProcessErrorAndRemovesItem sets
// NewExponentialTimedFailureRateLimiter, whose Forget only deletes after 2*maxDelay of idleness —
// so an unconditional Forget right after processing is a no-op there and the bug is invisible. The
// suite was testing the one configuration that is immune.
//
// The upstream limiter below is the same shape as builder/options.go's default
// (NewTypedItemExponentialFailureRateLimiter), with short delays so the test is fast. Its Forget
// deletes outright, which is what makes the arm below meaningful.
func defaultLikeLimiter(base time.Duration) clientgowq.TypedRateLimiter[any] {
	return clientgowq.NewTypedItemExponentialFailureRateLimiter[any](base, 50*base)
}

type alwaysFailsObserve struct{ err error }

func (f *alwaysFailsObserve) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	return ExternalObservation{}, f.err
}
func (f *alwaysFailsObserve) Create(context.Context, *unstructured.Unstructured) error { return nil }
func (f *alwaysFailsObserve) Update(context.Context, *unstructured.Unstructured) error { return nil }
func (f *alwaysFailsObserve) Delete(context.Context, *unstructured.Unstructured) error { return nil }

// baseDelay is a parameter because the recovery control below needs a window in which the failure
// count is observably non-zero. With a 1ms base the whole fail-fail-succeed sequence finishes
// inside one poll interval, so the count rises and is cleared between two reads — and the control
// then fails its own vacuity guard rather than silently asserting nothing, which is how this was
// caught.
func failingController(t *testing.T, maxRetries int, baseDelay time.Duration) (*Controller, ctrlevent.Event) {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)

	opts := createTestOptions()
	opts.MaxRetries = maxRetries
	opts.ResyncInterval = time.Hour // keep the resync loop out of this measurement
	opts.GlobalRateLimiter = defaultLikeLimiter(baseDelay)

	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("forget-obj", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	ctrl.SetExternalClient(&alwaysFailsObserve{err: fmt.Errorf("observe-boom")})

	item := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
			Name: obj.GetName(), Namespace: obj.GetNamespace(),
		},
		QueuedAt: time.Now(),
	}
	return ctrl, item
}

// THE ARM THAT MATTERS: a repeatedly failing item must accumulate requeues. Before the fix this
// stayed at 0 forever on the default limiter.
func TestFailingItemAccumulatesRequeuesOnTheDefaultLimiter(t *testing.T) {
	ctrl, item := failingController(t, 100, time.Millisecond) // high retries: the drop must not interfere here

	dig := ctrlevent.DigestForEvent(item)
	ctrl.items.Store(dig, struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: NormalPriority}, item)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ctrl.runWorker(ctx); close(done) }()
	t.Cleanup(func() { cancel(); ctrl.queue.ShutDown(); <-done })

	var got int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got = ctrl.queue.NumRequeues(item); got >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.GreaterOrEqual(t, got, 3,
		"NumRequeues stayed at %d for a permanently failing item. runWorker Forgets the item "+
			"before handleErr reads this count, so it is 0 on every pass — which makes "+
			"`NumRequeues >= maxRetries` never true and the retry budget unenforceable (#51)", got)
}

// THE CONSEQUENCE, and the one the original report did not name: with the count always 0 the
// maxRetries drop never fires, so an unprocessable event is retried forever.
func TestFailingItemIsEventuallyDroppedOnceMaxRetriesIsReached(t *testing.T) {
	const maxRetries = 3
	ctrl, item := failingController(t, maxRetries, time.Millisecond)

	dig := ctrlevent.DigestForEvent(item)
	ctrl.items.Store(dig, struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: NormalPriority}, item)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ctrl.runWorker(ctx); close(done) }()
	t.Cleanup(func() { cancel(); ctrl.queue.ShutDown(); <-done })

	// Wait until the count reaches the budget, then assert it STOPS climbing — the drop fired.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && ctrl.queue.NumRequeues(item) < maxRetries {
		time.Sleep(10 * time.Millisecond)
	}
	reached := ctrl.queue.NumRequeues(item)
	require.GreaterOrEqual(t, reached, maxRetries,
		"the item never reached the retry budget (%d of %d), so this test cannot observe whether "+
			"the drop fires and asserts nothing", reached, maxRetries)

	// Give it room to climb past the budget if the drop is not working.
	time.Sleep(500 * time.Millisecond)
	after := ctrl.queue.NumRequeues(item)

	require.LessOrEqual(t, after, reached+1,
		"requeues went %d -> %d after the budget of %d was reached, so the maxRetries drop never "+
			"fired and this event will be retried forever. maxRetries is configured and ignored",
		reached, after, maxRetries)
}

// THE CONTROL, and without it a "fix" that simply never Forgets would pass both arms above. An item
// that SUCCEEDS must be forgotten, so a recovered item does not inherit an old backoff.
type failsThenSucceeds struct{ calls int }

func (f *failsThenSucceeds) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	f.calls++
	if f.calls <= 2 {
		return ExternalObservation{}, fmt.Errorf("transient-boom")
	}
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}
func (f *failsThenSucceeds) Create(context.Context, *unstructured.Unstructured) error { return nil }
func (f *failsThenSucceeds) Update(context.Context, *unstructured.Unstructured) error { return nil }
func (f *failsThenSucceeds) Delete(context.Context, *unstructured.Unstructured) error { return nil }

func TestRecoveredItemIsForgottenSoItDoesNotInheritABackoff(t *testing.T) {
	// 150ms base: after two failures the count must stay non-zero long enough to be read.
	ctrl, item := failingController(t, 100, 150*time.Millisecond)
	ctrl.SetExternalClient(&failsThenSucceeds{})

	dig := ctrlevent.DigestForEvent(item)
	ctrl.items.Store(dig, struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: NormalPriority}, item)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ctrl.runWorker(ctx); close(done) }()
	t.Cleanup(func() { cancel(); ctrl.queue.ShutDown(); <-done })

	// It fails twice, so the count must rise first — otherwise the reset below proves nothing,
	// because a count that was never non-zero cannot be shown to have been cleared.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && ctrl.queue.NumRequeues(item) < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	require.GreaterOrEqual(t, ctrl.queue.NumRequeues(item), 1,
		"the count never rose, so this test cannot observe a reset and asserts nothing")

	// Then it succeeds, and the success path must Forget.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ctrl.queue.NumRequeues(item) == 0 {
			return // forgotten — correct
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("NumRequeues is still %d after the item succeeded. Forget must run on the success "+
		"path, or a recovered item carries its old failure count and the next failure starts from "+
		"an inflated backoff", ctrl.queue.NumRequeues(item))
}
