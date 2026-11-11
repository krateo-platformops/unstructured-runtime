package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/tracecontext"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTracerProviderDefault(t *testing.T) {
	t.Cleanup(func() { SetTracerProvider(nil) })
	SetTracerProvider(nil)
	if TracerProvider() != nil || Tracer() != nil {
		t.Fatal("with no default set, tracing must be off (nil provider, nil tracer)")
	}
	if TracerFrom(nil) != nil {
		t.Fatal("TracerFrom(nil) must be nil")
	}
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	SetTracerProvider(tp)
	if TracerProvider() != trace.TracerProvider(tp) || Tracer() == nil {
		t.Fatal("SetTracerProvider must make tp the default and tracing on")
	}
	SetTracerProvider(nil)
	if TracerProvider() != nil {
		t.Fatal("SetTracerProvider(nil) must turn tracing off")
	}
}

// SetupTracing's shutdown clears the default it set, so passes after it start nothing and write no
// marker for a trace nobody exports; a default someone else set later is left alone.
func TestSetupTracingShutdownClearsOnlyItsOwnDefault(t *testing.T) {
	t.Cleanup(func() { SetTracerProvider(nil) })
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	shutdown, err := SetupTracing(context.Background(), nil, Config{TracingEnabled: true, ServiceName: "t"})
	if err != nil {
		t.Fatalf("SetupTracing(enabled): %v", err)
	}
	if TracerProvider() == nil {
		t.Fatal("control: SetupTracing(enabled) must set the default")
	}
	_ = shutdown(context.Background())
	if TracerProvider() != nil {
		t.Fatal("shutdown must clear the default it set")
	}

	shutdown2, err := SetupTracing(context.Background(), nil, Config{TracingEnabled: true, ServiceName: "t"})
	if err != nil {
		t.Fatalf("SetupTracing(enabled): %v", err)
	}
	other := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = other.Shutdown(context.Background()) })
	SetTracerProvider(other)
	_ = shutdown2(context.Background())
	if TracerProvider() != trace.TracerProvider(other) {
		t.Fatal("shutdown must not clear a default someone else set after it")
	}
}

func TestSetupTracingDisabledIsNoop(t *testing.T) {
	SetTracerProvider(nil)
	shutdown, err := SetupTracing(context.Background(), nil, Config{TracingEnabled: false})
	if err != nil {
		t.Fatalf("SetupTracing(disabled): %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("noop shutdown: %v", err)
	}
	// With tracing off the reconcile path gets a NIL tracer, which tracecontext treats as "start
	// nothing, allocate nothing" (#77). A non-nil no-op tracer would read as tracing on.
	if Tracer() != nil {
		t.Fatal("Tracer() must be nil with tracing disabled")
	}
}

// The injector writes the ACTIVE span's context onto a child, so the next controller can link to it.
func TestInjectTraceparentWritesTheActiveSpan(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	ann := map[string]string{}
	InjectTraceparent(ctx, ann)
	want := tracecontext.Canonical(span.SpanContext())
	if ann[meta.AnnotationKeyTraceparent] != want {
		t.Fatalf("InjectTraceparent wrote %q, want the active span %q", ann[meta.AnnotationKeyTraceparent], want)
	}
	if _, ok := ann[tracecontext.AnnotationKeyTraceparentContinued]; ok {
		t.Error("the injector must never write the continuation marker onto a child")
	}
}

// With tracing OFF there is no active span. The reconcile root keeps the object's inbound context
// (tracecontext.Inbound), and the injector falls back to it, so a trace that came in on a
// composition still reaches its children when this process does not export (#77). Before #77 the
// root itself was the extracted context, so this held by accident.
func TestInjectTraceparentFallsBackToInboundWithTracingOff(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	const inbound = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	ann := map[string]string{tracecontext.AnnotationKeyTraceparent: inbound}
	d := tracecontext.Decide(ann, "", time.Time{}, time.Now(), tracecontext.DefaultContinueWindow)
	ctx, _ := tracecontext.StartReconcile(context.Background(), nil, tracecontext.Object{Kind: "X", Name: "a", Namespace: "ns"}, d, time.Now())

	child := map[string]string{}
	InjectTraceparent(ctx, child)
	if child[meta.AnnotationKeyTraceparent] != inbound {
		t.Fatalf("with tracing off the child got %q, want the inbound %q", child[meta.AnnotationKeyTraceparent], inbound)
	}

	// Control: with no inbound context either, nothing is written.
	none := map[string]string{}
	InjectTraceparent(context.Background(), none)
	if len(none) != 0 {
		t.Errorf("with no span and no inbound context the injector wrote %v", none)
	}
}
