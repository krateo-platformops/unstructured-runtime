package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// keyedClient is an ExternalClient that optionally names a rate-limit key and COUNTS its calls, so a
// test can assert the handler was NOT called — which is the whole point of a deferral.
type keyedClient struct {
	key      string
	keyErr   error
	observes atomic.Int32
}

func (k *keyedClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	k.observes.Add(1)
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}
func (k *keyedClient) Create(context.Context, *unstructured.Unstructured) error { return nil }
func (k *keyedClient) Update(context.Context, *unstructured.Unstructured) error { return nil }
func (k *keyedClient) Delete(context.Context, *unstructured.Unstructured) error { return nil }

// RateLimitKey makes keyedClient a RateLimitKeyer. A plain fakeExternalClient is not one, which is
// how the "handler never opted in" case is exercised.
func (k *keyedClient) RateLimitKey(*unstructured.Unstructured) (string, error) {
	return k.key, k.keyErr
}

func gateController(t *testing.T, gov ratelimit.Governor, ec ExternalClient) (*Controller, ctrlevent.Event, Options) {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	opts.RateLimitGovernor = gov
	// createTestOptions leaves MaxRetries at zero, and handleErr DROPS a failure when
	// NumRequeues >= maxRetries — so with zero every real failure is dropped before it reaches the
	// queue, and no test here could tell a deferral from a failure. The builder's default is 5;
	// use it, so this fixture retries the way a production controller does.
	opts.MaxRetries = 5

	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = ec

	obj := createTestUnstructured("gate-obj", opts.Namespace)
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	evt := ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	}
	return ctrl, evt, opts
}

func throttled(key string) ratelimit.Governor {
	g := ratelimit.New(ratelimit.Options{Ceiling: 4, RecoveryInterval: time.Hour})
	g.Observe(ratelimit.Signal{Key: key, Throttled: true, RetryAfter: time.Hour})
	return g
}

func gateSyncedOf(t *testing.T, opts Options, name string) (*metav1.Condition, []metav1.Condition) {
	t.Helper()
	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	all := unstructuredtools.GetConditions(got)
	for i := range all {
		if all[i].Type == condition.TypeSynced {
			return &all[i], all
		}
	}
	return nil, all
}

// THE ARM THE FEATURE EXISTS FOR. A throttled key must stop the handler being called at all, and
// must record why — on Synced, with the reason ReconcileDeferred, and without touching Ready.
func TestGate_ThrottledKeyDefersWithoutCallingTheHandler(t *testing.T) {
	ec := &keyedClient{key: "github.example.com"}
	ctrl, evt, opts := gateController(t, throttled("github.example.com"), ec)

	err := ctrl.processItem(context.TODO(), evt)

	var deferred *ratelimit.DeferredError
	require.True(t, errors.As(err, &deferred),
		"processItem returned %v, not a DeferredError. The worker must be able to tell a deferral "+
			"from a failure, or it requeues with backoff and counts it toward maxRetries", err)
	require.Equal(t, ratelimit.DeferThrottled, deferred.Deferral.Reason)
	require.Greater(t, deferred.After, 30*time.Minute,
		"the requeue delay must be the server's deadline (an hour here), not a short guess")

	require.Zero(t, ec.observes.Load(),
		"the handler was CALLED during a deferral. The whole point of the gate is that a throttled "+
			"endpoint receives no request at all")

	synced, all := gateSyncedOf(t, opts, evt.ObjectRef.Name)
	require.NotEmpty(t, all,
		"no conditions were written at all — the assertions below would pass over a deferral that "+
			"recorded nothing, which is the silence the design forbids")
	require.NotNil(t, synced, "no Synced condition was written")
	require.Equal(t, metav1.ConditionFalse, synced.Status, "nothing is converging this object")
	require.Equal(t, condition.ReasonReconcileDeferred, synced.Reason)

	for _, c := range all {
		require.NotEqual(t, condition.TypeReady, c.Type,
			"the deferral WROTE a Ready condition (%s/%s). Ready is the handler's finding and the "+
				"handler did not run — overwriting it repeats #45", c.Status, c.Reason)
	}
}

// THE CONTROL. With no governor the controller must behave exactly as before: the handler is
// called and nothing is deferred. Without this arm the test above passes against a gate that
// defers everything.
func TestGate_NoGovernorMeansNoGating(t *testing.T) {
	ec := &keyedClient{key: "github.example.com"}
	ctrl, evt, _ := gateController(t, nil, ec)

	err := ctrl.processItem(context.TODO(), evt)

	var deferred *ratelimit.DeferredError
	require.False(t, errors.As(err, &deferred), "a deferral occurred with NO governor configured")
	require.Equal(t, int32(1), ec.observes.Load(),
		"with no governor the handler must be called exactly as before the feature existed")
}

// A handler that never implements RateLimitKeyer has not opted in, and must not be gated even when
// a governor is configured and the endpoint it would talk to is throttled.
func TestGate_HandlerWithoutAKeyIsNeverGated(t *testing.T) {
	ec := &fakeExternalClient{ObserveExists: true}
	ctrl, evt, _ := gateController(t, throttled("github.example.com"), ec)

	err := ctrl.processItem(context.TODO(), evt)

	var deferred *ratelimit.DeferredError
	require.False(t, errors.As(err, &deferred),
		"a handler that is not a RateLimitKeyer was deferred. Opting in is the handler's choice")
}

// A key that cannot be determined FAILS OPEN — the opposite of dialguard, deliberately. A throttle
// that wedged every reconcile whenever its keyer errored would be worse than no throttle.
func TestGate_KeyErrorFailsOpen(t *testing.T) {
	ec := &keyedClient{keyErr: errors.New("cannot parse endpoint")}
	ctrl, evt, _ := gateController(t, throttled("github.example.com"), ec)

	err := ctrl.processItem(context.TODO(), evt)

	var deferred *ratelimit.DeferredError
	require.False(t, errors.As(err, &deferred), "a keyer error caused a deferral — that is a wedge")
	require.Equal(t, int32(1), ec.observes.Load(),
		"a keyer error must leave the pass ungated, so the handler is still called")
}

// A DEFERRAL MUST NOT CONSUME BACKOFF. This is the interaction with #52 the wiring was held for:
// handleErr must requeue a deferral without the rate-limited add, so it neither grows the item's
// backoff nor counts toward maxRetries — a global condition must not inflate a per-item one, and
// must never DROP an event that has nothing wrong with it.
func TestHandleErr_DeferralDoesNotConsumeBackoff(t *testing.T) {
	ctrl, evt, _ := gateController(t, nil, &keyedClient{})

	before := ctrl.queue.NumRequeues(evt)
	for i := 0; i < ctrl.maxRetries+3; i++ {
		ctrl.items.Delete(ctrlevent.DigestForEvent(evt))
		ctrl.handleErr(&ratelimit.DeferredError{
			Deferral: &ratelimit.Deferral{Key: "k", Reason: ratelimit.DeferAtCapacity},
			After:    time.Millisecond,
		}, evt, NormalPriority)
	}
	after := ctrl.queue.NumRequeues(evt)

	require.Equal(t, before, after,
		"NumRequeues went %d -> %d across deferrals. A deferral was counted as a failure, so enough "+
			"of them would reach maxRetries and DROP an event that had nothing wrong with it",
		before, after)

	// CONTROL: a genuine failure through the SAME path MUST count, or the assertion above passes
	// against a handleErr that never counts anything.
	ctrl.items.Delete(ctrlevent.DigestForEvent(evt))
	ctrl.handleErr(errors.New("a real failure"), evt, NormalPriority)
	require.Eventually(t, func() bool { return ctrl.queue.NumRequeues(evt) > after }, time.Second, 5*time.Millisecond,
		"a real failure did not increment NumRequeues, so this test cannot tell a deferral from a "+
			"failure and the assertion above proves nothing")
}

// THE METRIC A PASS INCREMENTS. Rows span the error SHAPES a pass can return — nil, a bare
// deferral, a deferral wrapped by a caller, and a plain failure — because errors.As versus a type
// assertion differ exactly on the wrapped row.
func TestClassifyOutcome_ADeferralIsNotAFailure(t *testing.T) {
	d := &ratelimit.DeferredError{Deferral: &ratelimit.Deferral{Key: "k", Reason: ratelimit.DeferThrottled}}
	cases := []struct {
		name string
		err  error
		want passOutcome
	}{
		{"nil — success", nil, outcomeSuccess},
		{"bare deferral — requeue-after, not failure", d, outcomeDeferred},
		{"wrapped deferral — still requeue-after", fmt.Errorf("observe: %w", d), outcomeDeferred},
		{"plain error — failure", errors.New("boom"), outcomeFailure},
	}
	for _, c := range cases {
		if got := classifyOutcome(c.err); got != c.want {
			t.Errorf("%s: got %d, want %d. A deferral counted as a failure shows a failure spike for "+
				"as long as a server rate-limits us", c.name, got, c.want)
		}
	}
	t.Logf("%d rows", len(cases))
}

// ORDER: THE DEFERRAL BRANCH PRECEDES THE maxRetries DROP. An item that has used up its retries and
// is then deferred must still be requeued: a deferral says nothing about the item, so the item's
// failure history must not decide its fate. Reversing the two branches passes every test above,
// because they all start from a zero count.
//
// The count is raised through the limiter the queue reads (NumRequeues delegates to it), with the
// queue EMPTY, so Len() reflects only the add under test and not a deduplicated earlier one.
func TestHandleErr_DeferralIsRequeuedEvenAtMaxRetries(t *testing.T) {
	setup := func(t *testing.T) (*Controller, ctrlevent.Event) {
		ctrl, evt, opts := gateController(t, nil, &keyedClient{})
		for i := 0; i < opts.MaxRetries; i++ {
			opts.GlobalRateLimiter.When(evt)
		}
		require.GreaterOrEqual(t, ctrl.queue.NumRequeues(evt), ctrl.maxRetries,
			"setup did not reach maxRetries (%d of %d), so this test exercises nothing",
			ctrl.queue.NumRequeues(evt), ctrl.maxRetries)
		require.Zero(t, ctrl.queue.Len(), "the queue must start empty")
		return ctrl, evt
	}

	t.Run("a deferral at maxRetries is requeued", func(t *testing.T) {
		ctrl, evt := setup(t)
		ctrl.handleErr(&ratelimit.DeferredError{
			Deferral: &ratelimit.Deferral{Key: "k", Reason: ratelimit.DeferAtCapacity},
			After:    time.Millisecond,
		}, evt, NormalPriority)
		require.Eventually(t, func() bool { return ctrl.queue.Len() == 1 }, time.Second, 5*time.Millisecond,
			"a deferral of an item at maxRetries was DROPPED. The endpoint being busy is not a reason "+
				"to give up on an item")
	})

	// CONTROL: at the same count a real failure IS dropped. Without it the arm above passes against
	// a handleErr that never drops anything.
	t.Run("control: a real failure at maxRetries is dropped", func(t *testing.T) {
		ctrl, evt := setup(t)
		ctrl.handleErr(errors.New("a real failure"), evt, NormalPriority)
		require.Never(t, func() bool { return ctrl.queue.Len() > 0 }, 200*time.Millisecond, 5*time.Millisecond,
			"a real failure at maxRetries was requeued, so the arm above cannot show the order matters")
	})
}

// reportingClient is a keyedClient whose Observe reports a signal through the context, the way a
// real handler reports a 429. It is the only test client that exercises the INPUT side of the
// governor: every other test seeds the governor directly, which passes against a gate that never
// gives the handler a sink.
type reportingClient struct {
	keyedClient
	report *ratelimit.Signal
}

func (r *reportingClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	if r.report != nil {
		ratelimit.Report(ctx, *r.report)
	}
	return r.keyedClient.Observe(ctx, mg)
}

// THE INPUT PATH. A signal the handler reports during a pass must reach the governor and throttle
// the NEXT pass. Without the sink on the handler's context the governor never learns anything, so
// the feature is inert in production while every test that seeds the governor directly still
// passes.
func TestGate_AHandlerReportedThrottleDefersTheNextPass(t *testing.T) {
	const key = "dev.azure.com"
	for _, c := range []struct {
		name       string
		report     *ratelimit.Signal
		wantSecond bool
	}{
		{"handler reports a throttle — the next pass is deferred",
			&ratelimit.Signal{Key: key, Throttled: true, RetryAfter: time.Hour}, true},
		// CONTROL: the same two passes with nothing reported must both run, or the arm above
		// passes against a gate that defers every second pass.
		{"control: handler reports nothing — the next pass runs", nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			gov := ratelimit.New(ratelimit.Options{Ceiling: 4, RecoveryInterval: time.Hour})
			ec := &reportingClient{keyedClient: keyedClient{key: key}, report: c.report}
			ctrl, evt, _ := gateController(t, gov, ec)

			var deferred *ratelimit.DeferredError
			first := ctrl.processItem(context.TODO(), evt)
			require.False(t, errors.As(first, &deferred), "the FIRST pass was deferred: %v", first)
			require.Equal(t, int32(1), ec.observes.Load(), "the first pass did not call the handler")

			second := ctrl.processItem(context.TODO(), evt)
			require.Equal(t, c.wantSecond, errors.As(second, &deferred),
				"second pass deferred=%v, want %v (err %v). A throttle the handler reported did not "+
					"reach the governor — the sink is missing from the handler's context",
				errors.As(second, &deferred), c.wantSecond, second)
		})
	}
}

// THE SLOT IS RELEASED. With a ceiling of one, sequential passes must all run: each returns its
// slot when it ends. A pass that kept its slot would leave the key AT CAPACITY forever after the
// first reconcile, deferring every object on that endpoint with no server involved.
func TestGate_APassReleasesItsSlot(t *testing.T) {
	const key = "github.example.com"
	gov := ratelimit.New(ratelimit.Options{Ceiling: 1, RecoveryInterval: time.Hour})
	ec := &keyedClient{key: key}
	ctrl, evt, _ := gateController(t, gov, ec)

	const passes = 3
	for i := 0; i < passes; i++ {
		var deferred *ratelimit.DeferredError
		err := ctrl.processItem(context.TODO(), evt)
		require.False(t, errors.As(err, &deferred),
			"pass %d of %d was deferred (%v) with nothing else holding the key: an earlier pass did "+
				"not release its slot", i+1, passes, err)
	}
	require.Equal(t, int32(passes), ec.observes.Load())

	// CONTROL: a slot that IS held makes the next pass defer at capacity. Without this the loop
	// above passes against a governor whose ceiling never binds.
	_, held := gov.TryAcquire(key)
	require.Nil(t, held, "control setup: could not take the one slot")
	var deferred *ratelimit.DeferredError
	err := ctrl.processItem(context.TODO(), evt)
	require.True(t, errors.As(err, &deferred), "a held slot did not defer the pass (%v): the ceiling "+
		"does not bind, so the loop above proves nothing", err)
	require.Equal(t, ratelimit.DeferAtCapacity, deferred.Deferral.Reason)
}

// statusWrites counts the status writes the fake dynamic client has seen.
func statusWrites(t *testing.T, opts Options) int {
	t.Helper()
	fc, ok := opts.Client.(*dynamicfake.FakeDynamicClient)
	require.True(t, ok, "the test fixture's client is not the fake dynamic client")
	n := 0
	for _, a := range fc.Actions() {
		if (a.GetVerb() == "patch" || a.GetVerb() == "update") && a.GetSubresource() == "status" {
			n++
		}
	}
	return n
}

// AN UNCHANGED DEFERRAL IS WRITTEN ONCE. A key at capacity requeues every second or two with the
// same message, and a status patch per pass would spend API writes saying nothing new.
func TestDeferPass_WritesAnUnchangedDeferralOnce(t *testing.T) {
	const key = "github.example.com"
	gov := ratelimit.New(ratelimit.Options{Ceiling: 4, RecoveryInterval: time.Hour})
	deadline := time.Now().Add(time.Hour)
	gov.Observe(ratelimit.Signal{Key: key, Throttled: true, ResetAt: deadline})
	ctrl, evt, opts := gateController(t, gov, &keyedClient{key: key})

	before := statusWrites(t, opts)
	_ = ctrl.processItem(context.TODO(), evt)
	first := statusWrites(t, opts)
	require.Equal(t, before+1, first, "the first deferral must be recorded on status")

	// The skipped write must still be a DEFERRAL. Returning nil here would read as success: runWorker
	// would Forget the item and not requeue it, though the handler never ran.
	var deferred *ratelimit.DeferredError
	second := ctrl.processItem(context.TODO(), evt)
	require.True(t, errors.As(second, &deferred),
		"the already-recorded pass returned %v, not a DeferredError, so it would not be requeued", second)
	require.Equal(t, first, statusWrites(t, opts),
		"an IDENTICAL deferral was written again; every requeue during a throttle would cost a write")

	// CONTROL: a NEW deadline changes the message and MUST be written, or the assertion above passes
	// against a deferPass that writes only once ever and leaves a stale deadline on status.
	gov.Observe(ratelimit.Signal{Key: key, Throttled: true, ResetAt: deadline.Add(time.Hour)})
	_ = ctrl.processItem(context.TODO(), evt)
	require.Equal(t, first+1, statusWrites(t, opts),
		"a later server deadline was NOT written, so status names a deadline the server has extended")
}
