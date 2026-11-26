package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Short timings so a handover takes about a second; the ratios are client-go's (lease > renew >
// retry * 1.2).
func testOwnership(cs *kubefake.Clientset, identity string) *Ownership {
	return &Ownership{
		Leases:        cs.CoordinationV1(),
		Namespace:     "krateo-system",
		Name:          "v1.tests.test.example.org",
		Identity:      identity,
		LeaseDuration: 3 * time.Second,
		RenewDeadline: 2 * time.Second,
		RetryPeriod:   250 * time.Millisecond,
	}
}

func ownedController(t *testing.T, o *Ownership, ext ExternalClient) *Controller {
	t.Helper()
	opts := drainTestOptions([]runtime.Object{createTestUnstructured("owned-obj", "test-namespace")}, durPtr(10*time.Second))
	opts.Ownership = o
	return newDrainController(t, opts, ext)
}

func waitForOwnership(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

// The guard itself (core-provider docs/design/cdc-pooling.md, step 2): a second controller for the
// same resource, started while the first holds the Lease, reconciles nothing and stays Ready. When
// the first stops, the second takes over, and only after the first's in-flight reconcile finished:
// the drain comes before the release, so the two never write at the same time.
func TestOwnership_SecondControllerWaitsUntilTheFirstHasDrained(t *testing.T) {
	cs := kubefake.NewClientset()

	firstInFlight := make(chan struct{})
	finishFirst := make(chan struct{})
	var firstReturned atomic.Int64 // unix nanos when the first's in-flight reconcile returned
	var firstObserves atomic.Int32
	first := ownedController(t, testOwnership(cs, "pod-a"), &mockExternalClient{
		observeFunc: func(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
			if firstObserves.Add(1) == 1 {
				close(firstInFlight)
				<-finishFirst
				firstReturned.Store(time.Now().UnixNano())
			}
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})

	var secondStarted atomic.Int64 // unix nanos of the second's first Observe
	var secondObserves atomic.Int32
	second := ownedController(t, testOwnership(cs, "pod-b"), &mockExternalClient{
		observeFunc: func(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
			if secondObserves.Add(1) == 1 {
				secondStarted.Store(time.Now().UnixNano())
			}
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})

	ctxA, stopA := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	go func() { doneA <- first.Run(ctxA, 1) }()
	select {
	case <-firstInFlight:
	case <-time.After(15 * time.Second):
		t.Fatal("the first controller never reconciled")
	}

	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	doneB := make(chan error, 1)
	go func() { doneB <- second.Run(ctxB, 1) }()

	// Refused: synced, Ready, and reconciling nothing, for longer than a full lease duration.
	waitForOwnership(t, "the second controller's first list", 10*time.Second, func() bool { return second.informer.HasSynced() })
	time.Sleep(4 * time.Second)
	require.Zero(t, secondObserves.Load(), "the second controller reconciled while the first held the lease")
	require.NoError(t, second.ReadyzCheck(nil), "a controller waiting for the lease must be Ready, or a rolling update never lets the old pod go")

	// SIGTERM the first while its reconcile is in flight: it drains, so the lease must stay held.
	stopA()
	time.Sleep(4 * time.Second)
	require.Zero(t, secondObserves.Load(), "the second controller started while the first was still draining")

	close(finishFirst)
	select {
	case err := <-doneA:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("the first controller did not return after its reconcile finished")
	}
	// Released on return, so the second takes over well inside one lease duration plus a retry.
	waitForOwnership(t, "the second controller to take over", 3*time.Second, func() bool { return secondObserves.Load() > 0 })
	require.Less(t, firstReturned.Load(), secondStarted.Load(), "the second controller's first reconcile began before the first's in-flight one returned")

	stopB()
	select {
	case err := <-doneB:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("the second controller did not stop")
	}
}

// A controller that cannot read the Lease can never reconcile. It must say so on /readyz instead of
// waiting forever while reporting Ready. The control is the same controller with a readable Lease.
func TestOwnership_AnUnreadableLeaseIsNotReady(t *testing.T) {
	for _, tc := range []struct {
		name   string
		forbid bool
	}{{"lease readable", false}, {"lease forbidden", true}} {
		t.Run(tc.name, func(t *testing.T) {
			cs := kubefake.NewClientset()
			if tc.forbid {
				cs.PrependReactor("get", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "v1.tests.test.example.org", errors.New("RBAC: no rule"))
				})
			}
			var observes atomic.Int32
			c := ownedController(t, testOwnership(cs, "pod-a"), &mockExternalClient{
				observeFunc: func(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
					observes.Add(1)
					return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
				},
			})
			ctx, stop := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx, 1) }()

			if !tc.forbid {
				waitForOwnership(t, "a reconcile", 10*time.Second, func() bool { return observes.Load() > 0 })
				require.NoError(t, c.ReadyzCheck(nil))
			} else {
				waitForOwnership(t, "readiness to report the lease", 10*time.Second, func() bool {
					err := c.ReadyzCheck(nil)
					return err != nil && strings.Contains(err.Error(), "ownership lease krateo-system/v1.tests.test.example.org") && strings.Contains(err.Error(), "forbidden")
				})
				require.Zero(t, observes.Load(), "reconciled without the lease")
			}
			stop()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("Run did not return")
			}
		})
	}
}

// Losing the Lease (it can no longer be renewed, so another process may own the resource within
// LeaseDuration) must stop writes at once: the in-flight reconcile's context is cancelled, no drain
// is awaited, and Run returns ErrOwnershipLost so the process exits and waits for the Lease again.
func TestOwnership_LosingTheLeaseCancelsInFlightReconcilesAtOnce(t *testing.T) {
	cs := kubefake.NewClientset()
	// Installed before Run (a fake's reactor chain is not safe to change while in use), and armed
	// once the controller holds the lease: from then on every renewal fails.
	var renewalsFail atomic.Bool
	cs.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !renewalsFail.Load() {
			return false, nil, nil
		}
		return true, nil, apierrors.NewServiceUnavailable("apiserver unreachable")
	})
	inFlight := make(chan struct{})
	var cancelled atomic.Bool
	var observes atomic.Int32
	c := ownedController(t, testOwnership(cs, "pod-a"), &mockExternalClient{
		observeFunc: func(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
			if observes.Add(1) == 1 {
				close(inFlight)
				select {
				case <-ctx.Done():
					cancelled.Store(true)
				case <-time.After(30 * time.Second):
				}
			}
			return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		},
	})
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 1) }()
	select {
	case <-inFlight:
	case <-time.After(15 * time.Second):
		t.Fatal("never reconciled")
	}

	renewalsFail.Store(true)
	start := time.Now()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrOwnershipLost)
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not return after the lease could no longer be renewed")
	}
	// RenewDeadline (2s) plus a retry, not the 10s grace period.
	require.Less(t, time.Since(start), 6*time.Second, "waited for a drain after losing the lease")
	waitForOwnership(t, "the in-flight reconcile's context to be cancelled", 2*time.Second, cancelled.Load)
}

func TestOwnership_Validate(t *testing.T) {
	cs := kubefake.NewClientset()
	ok := testOwnership(cs, "pod-a")
	require.NoError(t, ok.validate())
	var none *Ownership
	require.NoError(t, none.validate(), "nil Ownership is the default")
	for name, mut := range map[string]func(o *Ownership){
		"no client":    func(o *Ownership) { o.Leases = nil },
		"no namespace": func(o *Ownership) { o.Namespace = "" },
		"no name":      func(o *Ownership) { o.Name = "" },
		"no identity":  func(o *Ownership) { o.Identity = "" },
		// The timings are checked by New, not first by the elector after the informer has synced.
		"renewDeadline not below leaseDuration": func(o *Ownership) { o.LeaseDuration, o.RenewDeadline = 2*time.Second, 2*time.Second },
	} {
		o := *ok
		mut(&o)
		require.Error(t, o.validate(), name)
	}
}

// client-go releases a Lease by clearing its holder. That must read as a release, not as a process
// named "" holding it (the first version logged "held by another process" with holder="" on every
// handover), and our own release must not be announced as someone else's.
func TestOwnership_HolderChangeMessage(t *testing.T) {
	for _, tc := range []struct {
		name, holder, prev string
		changed            bool
		want               string
	}{
		{"unchanged", "pod-b", "pod-b", false, ""},
		{"we took it", "pod-a", "", true, ""},
		{"another holds it", "pod-b", "", true, msgLeaseHeldByAnother},
		{"another released it", "", "pod-b", true, msgLeaseReleased},
		{"we released it", "", "pod-a", true, ""},
	} {
		if got := holderChangeMessage("pod-a", tc.holder, tc.prev, tc.changed); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// ValidateOwnershipTimings accepts exactly the sets client-go's elector accepts. Each case is checked
// twice: against a literal verdict, and against leaderelection.NewLeaderElector itself, the independent
// source, with the defaults resolved the way Ownership resolves them.
func TestValidateOwnershipTimings(t *testing.T) {
	s := time.Second
	for _, c := range []struct {
		lease, renew, retry time.Duration
		ok                  bool
		says                string
	}{
		{0, 0, 0, true, ""},
		{15 * s, 10 * s, 2 * s, true, ""},
		{30 * s, 20 * s, 5 * s, true, ""},
		{60 * s, 40 * s, 15 * s, true, ""},
		{7 * s, 6100 * time.Millisecond, 5 * s, true, ""},
		{20 * s, 20 * s, 5 * s, false, "leaseDuration must be greater than renewDeadline"},
		{20 * s, 30 * s, 5 * s, false, "leaseDuration must be greater than renewDeadline"},
		{7 * s, 6 * s, 5 * s, false, "renewDeadline must be greater than retryPeriod x 1.2"},
		{30 * s, 20 * s, 20 * s, false, "renewDeadline must be greater than retryPeriod x 1.2"},
		{0, 20 * s, 0, false, "leaseDuration must be greater than renewDeadline"}, // 15s default lease
		{-30 * s, 20 * s, 5 * s, false, "greater than zero"},
		{30 * s, 20 * s, -5 * s, false, "greater than zero"},
	} {
		name := fmt.Sprintf("%s/%s/%s", c.lease, c.renew, c.retry)
		t.Run(name, func(t *testing.T) {
			err := ValidateOwnershipTimings(c.lease, c.renew, c.retry)
			if c.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, c.says)
			}
			lease, renew, retry := (&Ownership{LeaseDuration: c.lease, RenewDeadline: c.renew, RetryPeriod: c.retry}).timings()
			_, cgErr := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
				Lock:          &resourcelock.LeaseLock{LockConfig: resourcelock.ResourceLockConfig{Identity: "pod-a"}},
				LeaseDuration: lease, RenewDeadline: renew, RetryPeriod: retry,
				Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(context.Context) {}, OnStoppedLeading: func() {}},
			})
			require.Equal(t, cgErr == nil, err == nil, "client-go says %v, ValidateOwnershipTimings says %v", cgErr, err)
		})
	}
}

// The timings reach the elector: the Lease it writes carries this controller's leaseDuration, which
// is what a successor waits out. CONTROL: the default controller writes the default 15s.
func TestOwnership_TheLeaseCarriesItsDuration(t *testing.T) {
	for _, c := range []struct {
		name  string
		lease time.Duration
		want  int32
	}{{"set", 7 * time.Second, 7}, {"default", 0, 15}} {
		t.Run(c.name, func(t *testing.T) {
			cs := kubefake.NewClientset()
			o := testOwnership(cs, "pod-a")
			o.LeaseDuration = c.lease
			if c.lease == 0 {
				o.RenewDeadline, o.RetryPeriod = 0, 0
			}
			ctrl := ownedController(t, o, &mockExternalClient{
				observeFunc: func(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
					return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
				},
			})
			ctx, stop := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- ctrl.Run(ctx, 1) }()
			var got int32
			waitForOwnership(t, "the Lease to be taken", 10*time.Second, func() bool {
				l, err := cs.CoordinationV1().Leases(o.Namespace).Get(context.Background(), o.Name, metav1.GetOptions{})
				if err != nil || l.Spec.LeaseDurationSeconds == nil {
					return false
				}
				got = *l.Spec.LeaseDurationSeconds
				return true
			})
			require.Equal(t, c.want, got)
			stop()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(15 * time.Second):
				t.Fatal("Run did not return")
			}
		})
	}
}
