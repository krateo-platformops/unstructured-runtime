package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
)

// RecheckAfter (oasgen-provider#648). A handler outcome that settles on its own schedules nothing, so
// recovery waited a full resync. These tests drive the REAL loop (informer, queue, workers) with a
// 30s resync and assert the TIME of the second Observe, not merely that it happened: with the hint
// ignored the second Observe is a resync away and the wait below times out.

// recheckClient scripts one handler. It records when each hook ran.
type recheckClient struct {
	mockExternalClient
	mu       sync.Mutex
	observes []time.Time
	creates  []time.Time

	// observe returns the n-th (1-based) Observe's result.
	observe func(n int) (ExternalObservation, error)
	// create returns the n-th (1-based) create's result.
	create func(n int) (CreateResult, error)
}

func (c *recheckClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	c.mu.Lock()
	c.observes = append(c.observes, time.Now())
	n := len(c.observes)
	c.mu.Unlock()
	return c.observe(n)
}

func (c *recheckClient) CreateWithResult(ctx context.Context, mg *unstructured.Unstructured) (CreateResult, error) {
	c.mu.Lock()
	c.creates = append(c.creates, time.Now())
	n := len(c.creates)
	c.mu.Unlock()
	res, err := c.create(n)
	// The instant the pass hands its verdict back: the hint is measured from here.
	c.mu.Lock()
	c.creates[n-1] = time.Now()
	c.mu.Unlock()
	return res, err
}

func (c *recheckClient) observeTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.observes...)
}

func (c *recheckClient) createTimes() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.creates...)
}

// fixedDelayLimiter is a failure limiter whose backoff is a known constant, so a test can tell the
// error path's retry from a hint by time.
type fixedDelayLimiter struct{ d time.Duration }

func (f fixedDelayLimiter) When(any) time.Duration { return f.d }
func (f fixedDelayLimiter) Forget(any)             {}
func (f fixedDelayLimiter) NumRequeues(any) int    { return 0 }

var _ workqueue.TypedRateLimiter[any] = fixedDelayLimiter{}

// runRecheck starts the controller on one object and returns a stop func that fails if Run does not
// return.
func runRecheck(t *testing.T, ext *recheckClient, limiter workqueue.TypedRateLimiter[any]) func() {
	t.Helper()
	obj := createTestUnstructured("recheck", "test-namespace")
	opts := withRealResourceVersions(drainTestOptions([]runtime.Object{obj}, durPtr(-1)))
	opts.MaxRetries = 5
	if limiter != nil {
		opts.GlobalRateLimiter = limiter
	}
	c := newDrainController(t, opts, ext)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 2) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
		}
	}
}

func waitObserves(t *testing.T, ext *recheckClient, n int, within time.Duration) []time.Time {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ts := ext.observeTimes(); len(ts) >= n {
			return ts
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ext.observeTimes()
}

// settled is what the second Observe reports once the handler's outcome has settled.
var settled = ExternalObservation{ResourceExists: true, ResourceUpToDate: true}

const (
	// hint is the floor, so the hint is also a valid post-clamp value: the test asserts the hint, not
	// the clamp. slack covers scheduling, status writes and a loaded box.
	hint  = MinRecheckAfter
	slack = 1500 * time.Millisecond
)

// PUT-only adopt: Observe finds nothing, the create is really a PUT that adopts, and the adopted
// object converges on its own. Measured 0.9 / 1.7 / 2.0 / 0.8 resync periods after the parent.
func TestRecheckAfter_PutOnlyAdopt_SecondObserveWithinHint(t *testing.T) {
	ext := &recheckClient{
		observe: func(n int) (ExternalObservation, error) {
			if n == 1 {
				return ExternalObservation{ResourceExists: false}, nil
			}
			return settled, nil
		},
		create: func(int) (CreateResult, error) {
			return CreateResult{Requested: true, RecheckAfter: hint}, nil
		},
	}
	stop := runRecheck(t, ext, nil)
	defer stop()

	obs := waitObserves(t, ext, 2, hint+slack+time.Second)
	cr := ext.createTimes()
	require.Len(t, cr, 1, "precondition: the create pass ran")
	require.GreaterOrEqual(t, len(obs), 2, "no second Observe within %s: the hint was not honoured (resync is 30s)", hint+slack)
	gap := obs[1].Sub(cr[0])
	require.GreaterOrEqual(t, gap, hint-50*time.Millisecond, "second Observe came before the hint elapsed")
	require.Less(t, gap, hint+slack, "second Observe at %s after the create, hint %s", gap, hint)
}

// No get verb (RESTAction): Observe cannot read the object back, so it reports exists and up to date
// while Ready stays False; nothing else would look again. Measured 1.0 / 1.7 / 1.7 / 1.0.
func TestRecheckAfter_NoGetVerb_SecondObserveWithinHint(t *testing.T) {
	ext := &recheckClient{
		observe: func(n int) (ExternalObservation, error) {
			if n == 1 {
				return ExternalObservation{
					ResourceExists: true, ResourceUpToDate: true, RecheckAfter: hint,
					Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, Reason: "NotYetObservable"}},
				}, nil
			}
			return settled, nil
		},
	}
	stop := runRecheck(t, ext, nil)
	defer stop()

	obs := waitObserves(t, ext, 2, hint+slack+time.Second)
	require.GreaterOrEqual(t, len(obs), 2, "no second Observe within %s: the hint was not honoured (resync is 30s)", hint+slack)
	gap := obs[1].Sub(obs[0])
	require.GreaterOrEqual(t, gap, hint-50*time.Millisecond, "second Observe came before the hint elapsed")
	require.Less(t, gap, hint+slack, "second Observe at %s after the first, hint %s", gap, hint)
}

// Adopt-only, no create: the create is reported as not requested (the adopt happens in Observe), and
// the outcome settles on its own. Measured 0 / 2.0 / 3.2 / 0.8.
func TestRecheckAfter_AdoptOnlyNoCreate_SecondObserveWithinHint(t *testing.T) {
	ext := &recheckClient{
		observe: func(n int) (ExternalObservation, error) {
			if n == 1 {
				return ExternalObservation{ResourceExists: false}, nil
			}
			return settled, nil
		},
		create: func(int) (CreateResult, error) {
			return CreateResult{Requested: false, Reason: "NoCreateVerbDeclared", RecheckAfter: hint}, nil
		},
	}
	stop := runRecheck(t, ext, nil)
	defer stop()

	obs := waitObserves(t, ext, 2, hint+slack+time.Second)
	cr := ext.createTimes()
	require.Len(t, cr, 1, "precondition: the create pass ran")
	require.GreaterOrEqual(t, len(obs), 2, "no second Observe within %s: the hint was not honoured (resync is 30s)", hint+slack)
	gap := obs[1].Sub(cr[0])
	require.GreaterOrEqual(t, gap, hint-50*time.Millisecond, "second Observe came before the hint elapsed")
	require.Less(t, gap, hint+slack, "second Observe at %s after the create, hint %s", gap, hint)
}

// Zero is no hint: with the resync at 30s nothing observes again inside the window.
func TestRecheckAfter_Zero_IsTodaysBehaviour(t *testing.T) {
	ext := &recheckClient{observe: func(int) (ExternalObservation, error) { return settled, nil }}
	stop := runRecheck(t, ext, nil)
	defer stop()

	obs := waitObserves(t, ext, 1, 2*time.Second)
	require.Len(t, obs, 1, "precondition: the first Observe ran")
	time.Sleep(hint + slack)
	require.Len(t, ext.observeTimes(), 1, "a zero RecheckAfter must schedule nothing")
}

// The floor, end to end: a 10ms hint must not produce a pass 10ms later.
func TestRecheckAfter_BelowFloor_IsRaisedToTheFloor(t *testing.T) {
	ext := &recheckClient{
		observe: func(n int) (ExternalObservation, error) {
			if n == 1 {
				return ExternalObservation{ResourceExists: true, ResourceUpToDate: true, RecheckAfter: 10 * time.Millisecond}, nil
			}
			return settled, nil
		},
	}
	stop := runRecheck(t, ext, nil)
	defer stop()

	obs := waitObserves(t, ext, 2, MinRecheckAfter+slack+time.Second)
	require.GreaterOrEqual(t, len(obs), 2, "no second Observe: the clamped hint was not honoured")
	gap := obs[1].Sub(obs[0])
	require.GreaterOrEqual(t, gap, MinRecheckAfter-50*time.Millisecond, "a 10ms hint ran after %s: the floor was not applied", gap)
	require.Less(t, gap, MinRecheckAfter+slack)
}

// The clamp itself: zero is no hint, below the floor is raised, above the resync period is lowered.
func TestClampRecheck(t *testing.T) {
	const resync = 30 * time.Second
	cases := []struct {
		name   string
		hint   time.Duration
		resync time.Duration
		want   time.Duration
		ok     bool
	}{
		{"zero is no hint", 0, resync, 0, false},
		{"negative is no hint", -time.Second, resync, 0, false},
		{"below the floor", time.Millisecond, resync, MinRecheckAfter, true},
		{"just below the floor", MinRecheckAfter - 1, resync, MinRecheckAfter, true},
		{"at the floor", MinRecheckAfter, resync, MinRecheckAfter, true},
		{"inside the range", 5 * time.Second, resync, 5 * time.Second, true},
		{"at the resync period", resync, resync, resync, true},
		{"just above the resync period", resync + 1, resync, resync, true},
		{"far above the resync period", time.Hour, resync, resync, true},
		{"resync below the floor: the ceiling wins", 5 * time.Second, 500 * time.Millisecond, 500 * time.Millisecond, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := clampRecheck(tc.hint, tc.resync)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

// An error return IGNORES the hint: the failed pass keeps its existing backoff. The limiter's backoff
// is a fixed 2.5s, the hint 1s; a pass at the hint rather than at the backoff is the defect.
func TestRecheckAfter_ErrorReturnIgnoresTheHint(t *testing.T) {
	const backoff = 2500 * time.Millisecond

	t.Run("observe error", func(t *testing.T) {
		ext := &recheckClient{
			observe: func(n int) (ExternalObservation, error) {
				if n == 1 {
					return ExternalObservation{ResourceExists: true, ResourceUpToDate: true, RecheckAfter: hint}, errors.New("vendor said no")
				}
				return settled, nil
			},
		}
		stop := runRecheck(t, ext, fixedDelayLimiter{backoff})
		defer stop()

		obs := waitObserves(t, ext, 2, backoff+slack+time.Second)
		require.GreaterOrEqual(t, len(obs), 2, "precondition: the failed pass was retried at all")
		gap := obs[1].Sub(obs[0])
		require.GreaterOrEqual(t, gap, backoff-100*time.Millisecond, "the retry came after %s, at the hint (%s) and not at the backoff (%s)", gap, hint, backoff)
	})

	t.Run("create error", func(t *testing.T) {
		ext := &recheckClient{
			observe: func(n int) (ExternalObservation, error) {
				if n == 1 {
					return ExternalObservation{ResourceExists: false}, nil
				}
				return settled, nil
			},
			create: func(int) (CreateResult, error) {
				return CreateResult{Requested: true, RecheckAfter: hint}, errors.New("vendor said no")
			},
		}
		stop := runRecheck(t, ext, fixedDelayLimiter{backoff})
		defer stop()

		// Nothing may observe inside the backoff window after the failed create.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) && len(ext.createTimes()) == 0 {
			time.Sleep(20 * time.Millisecond)
		}
		cr := ext.createTimes()
		require.NotEmpty(t, cr, "precondition: the create pass ran")
		time.Sleep(hint + slack)
		for _, o := range ext.observeTimes()[1:] {
			require.GreaterOrEqual(t, o.Sub(cr[0]), backoff-100*time.Millisecond,
				"an Observe ran %s after the failed create: the hint was honoured on an error return", o.Sub(cr[0]))
		}
	})
}
