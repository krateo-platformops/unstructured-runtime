package telemetry

import (
	"context"
	"testing"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	capacityMetric = "unstructured_runtime.ratelimit.local.capacity"
	inputMetric    = "unstructured_runtime.ratelimit.saturation.input"
	enabledMetric  = "unstructured_runtime.ratelimit.saturation.input.enabled"
	deferredMetric = "unstructured_runtime.ratelimit.deferred"
)

type fakeSource struct {
	capacity int
	input    ratelimit.InputSnapshot
}

func (f fakeSource) LocalCapacity() int                  { return f.capacity }
func (f fakeSource) LocalInput() ratelimit.InputSnapshot { return f.input }

func collect(t *testing.T, build func(*Metrics)) metricdata.ResourceMetrics {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	ctx := context.Background()
	t.Cleanup(func() { _ = provider.Shutdown(ctx) })

	m, err := NewWithMeter(provider.Meter("test"), logging.NewNopLogger(), "cdc-a")
	if err != nil {
		t.Fatalf("NewWithMeter() returned error: %v", err)
	}
	build(m)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect() returned error: %v", err)
	}
	return rm
}

func gaugeValue[N int64 | float64](t *testing.T, rm metricdata.ResourceMetrics, name string) N {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, md := range scope.Metrics {
			if md.Name != name {
				continue
			}
			g, ok := md.Data.(metricdata.Gauge[N])
			if !ok || len(g.DataPoints) != 1 {
				t.Fatalf("%s: want one gauge point, got %#v", name, md.Data)
			}
			return g.DataPoints[0].Value
		}
	}
	t.Fatalf("%s was not reported", name)
	return 0
}

func TestSaturation_ReportsCapacityAndInput(t *testing.T) {
	rm := collect(t, func(m *Metrics) {
		m.SetSaturationSource(fakeSource{capacity: 7, input: ratelimit.InputSnapshot{
			Input: ratelimit.InputCPUPressure, Enabled: true, Percent: 62.5,
		}})
	})

	if got := gaugeValue[int64](t, rm, capacityMetric); got != 7 {
		t.Fatalf("capacity = %d, want 7", got)
	}
	if got := gaugeValue[float64](t, rm, inputMetric); got != 62.5 {
		t.Fatalf("input = %v, want 62.5", got)
	}
	if got := gaugeValue[int64](t, rm, enabledMetric); got != 1 {
		t.Fatalf("enabled = %d, want 1", got)
	}
}

// OFF IS ABSENCE, NOT ZERO. Capacity 0 would say the controller may run nothing.
func TestSaturation_OffReportsNoSeries(t *testing.T) {
	for name, build := range map[string]func(*Metrics){
		"no source":         func(*Metrics) {},
		"governor rule off": func(m *Metrics) { m.SetSaturationSource(fakeSource{}) },
	} {
		t.Run(name, func(t *testing.T) {
			rm := collect(t, build)
			for _, n := range []string{capacityMetric, inputMetric, enabledMetric} {
				if hasMetric(rm, n) {
					t.Fatalf("%s reported with saturation off", n)
				}
			}
		})
	}
}

// A DISABLED INPUT HAS NO VALUE. 0% would read as a healthy controller; the state is on .enabled,
// and capacity is still reported because it still bounds passes.
func TestSaturation_DisabledInputReportsNoPercent(t *testing.T) {
	rm := collect(t, func(m *Metrics) {
		m.SetSaturationSource(fakeSource{capacity: 3, input: ratelimit.InputSnapshot{Input: ratelimit.InputCPUPressure}})
	})

	if hasMetric(rm, inputMetric) {
		t.Fatalf("%s reported for a disabled input", inputMetric)
	}
	if got := gaugeValue[int64](t, rm, enabledMetric); got != 0 {
		t.Fatalf("enabled = %d, want 0", got)
	}
	if got := gaugeValue[int64](t, rm, capacityMetric); got != 3 {
		t.Fatalf("capacity = %d, want 3", got)
	}
}

func TestRateLimitDeferred_CountsByReason(t *testing.T) {
	rm := collect(t, func(m *Metrics) {
		ctx := context.Background()
		m.IncRateLimitDeferred(ctx, string(ratelimit.DeferRamping))
		m.IncRateLimitDeferred(ctx, string(ratelimit.DeferRamping))
		m.IncRateLimitDeferred(ctx, string(ratelimit.DeferSaturated))
	})

	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, md := range scope.Metrics {
			if md.Name != deferredMetric {
				continue
			}
			for _, dp := range md.Data.(metricdata.Sum[int64]).DataPoints {
				reason, _ := dp.Attributes.Value(attribute.Key("reason"))
				got[reason.AsString()] = dp.Value
			}
		}
	}
	if got["Ramping"] != 2 || got["Saturated"] != 1 || len(got) != 2 {
		t.Fatalf("deferred by reason = %v, want Ramping:2 Saturated:1", got)
	}
}

func TestSaturationMetrics_NilReceiverIsSafe(t *testing.T) {
	var m *Metrics
	m.SetSaturationSource(fakeSource{capacity: 1})
	m.IncRateLimitDeferred(context.Background(), "Ramping")
}
