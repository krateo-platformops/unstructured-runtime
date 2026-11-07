package controller

import (
	"context"
	"testing"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/telemetry"
	"github.com/stretchr/testify/require"
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
