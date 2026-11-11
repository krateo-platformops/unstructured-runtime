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
)

func TestSetupTracingDisabledIsNoop(t *testing.T) {
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
