package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// #66, layer 3: the local capacity in the worker. A KEYLESS handler (the cdc's case) is gated by
// it, the startup ramp is quiet, and every other local deferral is written.

type countingClient struct {
	calls atomic.Int32
	err   error
}

func (c *countingClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	c.calls.Add(1)
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, c.err
}
func (c *countingClient) Create(context.Context, *unstructured.Unstructured) error {
	c.calls.Add(1)
	return c.err
}
func (c *countingClient) Update(context.Context, *unstructured.Unstructured) error {
	c.calls.Add(1)
	return c.err
}
func (c *countingClient) Delete(context.Context, *unstructured.Unstructured) error {
	c.calls.Add(1)
	return c.err
}

// satGov returns a saturating governor reading the given provider-runtime fixture. With start, its
// sampler runs (and has sampled once) for the test's lifetime.
func satGov(t *testing.T, fixtureDir string, start bool) ratelimit.Governor {
	t.Helper()
	g, err := ratelimit.NewWithSaturation(ratelimit.Options{Ceiling: 4},
		ratelimit.SaturationOptions{Enabled: true, CgroupDir: "../ratelimit/testdata/" + fixtureDir, SamplePeriod: time.Hour}, nil)
	require.NoError(t, err)
	if start {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func() { _ = g.(ratelimit.Starter).Start(ctx) }()
		require.Eventually(t, func() bool {
			return g.(interface {
				LocalInput() ratelimit.InputSnapshot
			}).LocalInput().Enabled
		}, 2*time.Second, 5*time.Millisecond, "the sampler never took its first sample")
	}
	return g
}

// holdLocalSlot takes the single local slot, as an in-flight pass would.
func holdLocalSlot(t *testing.T, g ratelimit.Governor) {
	t.Helper()
	release, d := g.TryAcquire("")
	require.Nil(t, d, "setup: could not take the local slot")
	t.Cleanup(release)
}

// THE QUIET RAMP. On the startup ramp a deferred pass calls no handler AND writes nothing: the gate
// precedes every handler call, so there is nothing learned to keep.
func TestSaturation_TheStartupRampIsQuiet(t *testing.T) {
	g := satGov(t, "healthy", true)
	holdLocalSlot(t, g)
	cc := &countingClient{}
	ctrl, evt, opts := gateController(t, g, cc)
	before := statusWrites(t, opts)

	err := ctrl.processItem(context.TODO(), evt)

	var de *ratelimit.DeferredError
	require.True(t, errors.As(err, &de), "want a deferral, got %v", err)
	require.Equal(t, ratelimit.DeferRamping, de.Deferral.Reason)
	require.Zero(t, cc.calls.Load(), "the handler was called on a deferred pass")
	require.Equal(t, before, statusWrites(t, opts), "the startup ramp wrote status; it must be quiet")
}

// ONLY THE RAMP IS QUIET. A saturated controller, and a governor whose sampler never started, both
// write Synced=False/ReconcileDeferred naming the cause, so neither hides as the ramp.
func TestSaturation_OnlyTheRampIsQuiet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		gov    func(*testing.T) ratelimit.Governor
		reason ratelimit.DeferReason
		inMsg  string
	}{
		{"saturated", func(t *testing.T) ratelimit.Governor { return satGov(t, "cpu-pressure", true) }, ratelimit.DeferSaturated, "CPU pressure 81%"},
		{"sampler never started", func(t *testing.T) ratelimit.Governor { return satGov(t, "healthy", false) }, ratelimit.DeferAtCapacity, "sampler is not running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.gov(t)
			holdLocalSlot(t, g)
			cc := &countingClient{}
			ctrl, evt, opts := gateController(t, g, cc)
			before := statusWrites(t, opts)

			err := ctrl.processItem(context.TODO(), evt)

			var de *ratelimit.DeferredError
			require.True(t, errors.As(err, &de), "want a deferral, got %v", err)
			require.Equal(t, tc.reason, de.Deferral.Reason)
			require.True(t, de.Deferral.Local)
			require.Zero(t, cc.calls.Load(), "the handler was called on a deferred pass")
			require.Greater(t, statusWrites(t, opts), before, "a %s deferral must be written", tc.name)
			synced, _ := gateSyncedOf(t, opts, evt.ObjectRef.Name)
			require.NotNil(t, synced)
			require.Equal(t, condition.ReasonReconcileDeferred, synced.Reason)
			require.True(t, strings.Contains(synced.Message, tc.inMsg), "message %q does not name %q", synced.Message, tc.inMsg)
			require.True(t, strings.Contains(synced.Message, "local capacity"), "message %q must say it is the controller's own capacity", synced.Message)
		})
	}
}

// The local slot is released on EVERY exit of the pass, success and failure alike, or one failing
// object would starve every other of local capacity.
func TestSaturation_TheLocalSlotIsReleasedOnEveryExit(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{"the handler succeeds", nil}, {"the handler fails", errors.New("boom")}} {
		t.Run(tc.name, func(t *testing.T) {
			g := satGov(t, "healthy", true)
			cc := &countingClient{err: tc.err}
			ctrl, evt, _ := gateController(t, g, cc)
			_ = ctrl.processItem(context.TODO(), evt)
			require.Positive(t, cc.calls.Load(), "setup: the pass never reached the handler")
			release, d := g.TryAcquire("")
			require.Nil(t, d, "the local slot was not returned after the pass (%s)", tc.name)
			release()
		})
	}
}

type startRecorder struct {
	ratelimit.Governor
	started chan struct{}
}

func (s *startRecorder) Start(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	return nil
}

// Run starts a governor that has a background loop; nothing else would.
func TestRun_StartsAStarterGovernor(t *testing.T) {
	sr := &startRecorder{Governor: ratelimit.New(ratelimit.Options{Ceiling: 2}), started: make(chan struct{})}
	ctrl, _, _ := gateController(t, sr, &countingClient{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(ctx, 1) }()
	select {
	case <-sr.started:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not start the governor's background loop")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
