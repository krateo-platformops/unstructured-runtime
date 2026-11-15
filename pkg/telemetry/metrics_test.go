package telemetry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestSetupDisabledReturnsNoopShutdown(t *testing.T) {
	metrics, shutdown, err := Setup(context.Background(), logging.NewNopLogger(), Config{})
	if err != nil {
		t.Fatalf("Setup() returned error: %v", err)
	}
	if metrics != nil {
		t.Fatalf("Setup() metrics = %#v, want nil", metrics)
	}
	if shutdown == nil {
		t.Fatal("Setup() shutdown = nil, want no-op shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown() returned error: %v", err)
	}
}

func TestNewMetricsRecordsData(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	ctx := context.Background()
	t.Cleanup(func() {
		if err := provider.Shutdown(ctx); err != nil {
			t.Fatalf("provider.Shutdown() returned error: %v", err)
		}
	})

	metrics, err := newMetrics(provider.Meter("github.com/krateo-platformops/unstructured-runtime/test"), logging.NewNopLogger(), "test-deployment")
	if err != nil {
		t.Fatalf("newMetrics() returned error: %v", err)
	}

	metrics.IncStartupSuccess(ctx)
	metrics.RecordGetDuration(ctx, 25*time.Millisecond)
	metrics.IncGetFailure(ctx)
	metrics.RecordReconcileDuration(ctx, 150*time.Millisecond)
	metrics.AddQueueDepth(ctx, 1)
	metrics.RecordQueueWaitDuration(ctx, 75*time.Millisecond)
	metrics.RecordQueueOldestItemAge(ctx, 5*time.Second)
	metrics.RecordQueueWorkDuration(ctx, 125*time.Millisecond)
	metrics.RecordConnectDuration(ctx, 20*time.Millisecond)
	metrics.IncConnectFailure(ctx)
	metrics.RecordObserveDuration(ctx, 10*time.Millisecond)
	metrics.IncObserveFailure(ctx)
	metrics.RecordCreateDuration(ctx, 11*time.Millisecond)
	metrics.IncCreateFailure(ctx)
	metrics.RecordUpdateDuration(ctx, 12*time.Millisecond)
	metrics.IncUpdateFailure(ctx)
	metrics.RecordDeleteDuration(ctx, 13*time.Millisecond)
	metrics.IncDeleteFailure(ctx)
	metrics.RecordFinalizerAddDuration(ctx, 14*time.Millisecond)
	metrics.IncFinalizerAddFailure(ctx)
	metrics.RecordFinalizerRemoveDuration(ctx, 15*time.Millisecond)
	metrics.IncFinalizerRemoveFailure(ctx)
	metrics.RecordStatusUpdateDuration(ctx, 16*time.Millisecond)
	metrics.IncStatusUpdateFailure(ctx)
	metrics.IncReconcileSuccess(ctx)
	metrics.IncReconcileFailure(ctx)
	metrics.IncReconcileRequeueAfter(ctx)
	metrics.IncReconcileRequeueImmediate(ctx)
	metrics.IncReconcileErrorRequeue(ctx)
	metrics.AddReconcileInFlight(1)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("reader.Collect() returned error: %v", err)
	}

	mustHaveMetric(t, rm, "unstructured_runtime.startup.success")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.get.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.get.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.queue.depth")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.queue.wait.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.queue.oldest_item_age_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.queue.work.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.queue.requeues")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.success")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.requeue.after")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.requeue.immediate")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.requeue.error")
	mustHaveMetric(t, rm, "unstructured_runtime.reconcile.in_flight")
	mustHaveMetric(t, rm, "unstructured_runtime.external.connect.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.external.connect.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.external.observe.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.external.observe.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.external.create.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.external.create.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.external.update.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.external.update.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.external.delete.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.external.delete.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.finalizer.add.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.finalizer.add.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.finalizer.remove.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.finalizer.remove.failure")
	mustHaveMetric(t, rm, "unstructured_runtime.status.update.duration_seconds")
	mustHaveMetric(t, rm, "unstructured_runtime.status.update.failure")
}

func mustHaveMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) {
	t.Helper()
	if !hasMetric(rm, name) {
		t.Fatalf("expected %s metric to be collected", name)
	}
}

func hasMetric(rm metricdata.ResourceMetrics, name string) bool {
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name == name {
				return true
			}
		}
	}

	return false
}

// Every *_seconds histogram carries secondsBoundaries, not the SDK's millisecond-style defaults, and
// a sub-second and a multi-second reconcile land in different buckets (under the defaults both fall
// in [0, 5]).
func TestSecondsHistogramsHaveSecondsBuckets(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	ctx := context.Background()
	defer func() { _ = provider.Shutdown(ctx) }()
	m, err := newMetrics(provider.Meter("test"), logging.NewNopLogger(), "test-deployment")
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range []func(context.Context, time.Duration){
		m.RecordGetDuration, m.RecordQueueWaitDuration, m.RecordQueueOldestItemAge, m.RecordQueueWorkDuration,
		m.RecordConnectDuration, m.RecordObserveDuration, m.RecordCreateDuration, m.RecordUpdateDuration,
		m.RecordDeleteDuration, m.RecordFinalizerAddDuration, m.RecordFinalizerRemoveDuration, m.RecordStatusUpdateDuration,
	} {
		rec(ctx, 10*time.Millisecond)
	}
	m.RecordReconcileDuration(ctx, 300*time.Millisecond)
	m.RecordReconcileDuration(ctx, 2800*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			h, ok := md.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			seen++
			for _, dp := range h.DataPoints {
				if fmt.Sprint(dp.Bounds) != fmt.Sprint(secondsBoundaries) {
					t.Errorf("%s: bounds %v, want %v", md.Name, dp.Bounds, secondsBoundaries)
				}
				if md.Name != "unstructured_runtime.reconcile.duration_seconds" {
					continue
				}
				nonEmpty := 0
				for _, c := range dp.BucketCounts {
					if c > 0 {
						nonEmpty++
					}
				}
				if nonEmpty != 2 {
					t.Errorf("reconcile 0.3 s and 2.8 s: %d non-empty buckets %v, want 2", nonEmpty, dp.BucketCounts)
				}
			}
		}
	}
	if seen != 13 {
		t.Fatalf("histograms collected: %d, want 13 (every *_seconds instrument)", seen)
	}
}
