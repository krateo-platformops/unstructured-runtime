package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
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
