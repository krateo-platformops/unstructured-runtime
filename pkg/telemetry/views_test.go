package telemetry

import (
	"context"
	"testing"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const gvrKey = attribute.Key("krateo.composition.gvr")

func collectInt64(t *testing.T, reader *sdkmetric.ManualReader) map[string][]metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string][]metricdata.DataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				out[m.Name] = append(out[m.Name], d.DataPoints...)
			case metricdata.Gauge[int64]:
				out[m.Name] = append(out[m.Name], d.DataPoints...)
			}
		}
	}
	return out
}

// valueBy sums the points of a metric by the value of key ("" when absent).
func valueBy(points []metricdata.DataPoint[int64], key attribute.Key) map[string]int64 {
	out := map[string]int64{}
	for _, p := range points {
		v, _ := p.Attributes.Value(key)
		out[v.AsString()] += p.Value
	}
	return out
}

// core-provider docs/design/cdc-pooling.md, step 3, the proof it names: two controllers in ONE
// process produce two distinctly attributed metric streams. Each records through its own view
// (With); the root view, which a process hosting one controller uses, adds nothing, exactly as
// before.
func TestViews_TwoControllersInOneProcessAreDistinct(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	root, err := NewWithMeter(provider.Meter("test"), logging.NewNopLogger(), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a := root.With(gvrKey.String("composition.krateo.io/v1-0-0/fireworksapps"))
	b := root.With(gvrKey.String("composition.krateo.io/v0-3-0/portals"))

	a.IncReconcileSuccess(ctx)
	a.IncReconcileSuccess(ctx)
	b.IncReconcileSuccess(ctx)
	root.IncReconcileSuccess(ctx)
	a.AddReconcileInFlight(2)
	b.AddReconcileInFlight(1)
	a.IncReconcileErrorRequeue(ctx) // a point that carries its own attribute too

	got := collectInt64(t, reader)
	success := valueBy(got["unstructured_runtime.reconcile.success"], gvrKey)
	if success["composition.krateo.io/v1-0-0/fireworksapps"] != 2 || success["composition.krateo.io/v0-3-0/portals"] != 1 || success[""] != 1 {
		t.Fatalf("reconcile.success by %s = %v, want fireworksapps 2, portals 1, root (no attribute) 1", gvrKey, success)
	}
	inflight := valueBy(got["unstructured_runtime.reconcile.in_flight"], gvrKey)
	if inflight["composition.krateo.io/v1-0-0/fireworksapps"] != 2 || inflight["composition.krateo.io/v0-3-0/portals"] != 1 {
		t.Fatalf("in-flight by %s = %v, want each controller's own count", gvrKey, inflight)
	}
	requeue := got["unstructured_runtime.reconcile.queue.requeues"]
	found := false
	for _, p := range requeue {
		gvr, _ := p.Attributes.Value(gvrKey)
		reason, _ := p.Attributes.Value("reason")
		if gvr.AsString() == "composition.krateo.io/v1-0-0/fireworksapps" && reason.AsString() == "error" {
			found = true
		}
	}
	if !found {
		t.Fatalf("a point with its own attribute must keep the view's too; requeue points: %v", requeue)
	}
}

// An idle view adds no in-flight series; a nil Metrics stays a no-op.
func TestViews_IdleViewAddsNoSeriesAndNilIsSafe(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	root, err := NewWithMeter(provider.Meter("test"), logging.NewNopLogger(), "")
	if err != nil {
		t.Fatal(err)
	}
	_ = root.With(gvrKey.String("idle"))
	for _, p := range collectInt64(t, reader)["unstructured_runtime.reconcile.in_flight"] {
		if v, ok := p.Attributes.Value(gvrKey); ok && v.AsString() == "idle" {
			t.Fatal("a view that never counted a reconcile must not add an in-flight series")
		}
	}
	var nilM *Metrics
	if nilM.With(gvrKey.String("x")) != nil {
		t.Fatal("With on a nil Metrics must return nil")
	}
}
