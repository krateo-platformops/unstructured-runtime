package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	"github.com/krateo-platformops/unstructured-runtime/pkg/telemetry"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func meteredForTest(t *testing.T) (*telemetry.Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m, err := telemetry.NewWithMeter(provider.Meter("test"), logging.NewNopLogger(), "cdc-a")
	require.NoError(t, err)
	return m, reader
}

func deferredByReason(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, md := range scope.Metrics {
			if md.Name != "unstructured_runtime.ratelimit.deferred" {
				continue
			}
			for _, dp := range md.Data.(metricdata.Sum[int64]).DataPoints {
				reason, _ := dp.Attributes.Value(attribute.Key("reason"))
				got[reason.AsString()] = dp.Value
			}
		}
	}
	return got
}

// THE QUIET RAMP IS STILL COUNTED. It writes nothing on the object, so the counter is its only
// record; a count placed after the quiet return would lose exactly the deferrals a restart makes.
func TestSaturation_TheQuietRampIsCounted(t *testing.T) {
	g := satGov(t, "healthy", true)
	holdLocalSlot(t, g)
	ctrl, evt, _ := gateController(t, g, &countingClient{})
	m, reader := meteredForTest(t)
	ctrl.metrics = m

	err := ctrl.processItem(context.TODO(), evt)

	var de *ratelimit.DeferredError
	require.True(t, errors.As(err, &de), "want a deferral, got %v", err)
	require.Equal(t, ratelimit.DeferRamping, de.Deferral.Reason)
	require.Equal(t, map[string]int64{"Ramping": 1}, deferredByReason(t, reader))
}

// A saturating governor must reach the gauges through New, the way a real controller is built.
func TestSaturation_GovernorReachesTheGauges(t *testing.T) {
	g := satGov(t, "healthy", true)
	m, reader := meteredForTest(t)

	opts := drainTestOptions(nil, nil)
	opts.Metrics = m
	opts.RateLimitGovernor = g
	newDrainController(t, opts, &countingClient{})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	found := false
	for _, scope := range rm.ScopeMetrics {
		for _, md := range scope.Metrics {
			found = found || md.Name == "unstructured_runtime.ratelimit.local.capacity"
		}
	}
	require.True(t, found, "a saturating governor reported no capacity")
}
