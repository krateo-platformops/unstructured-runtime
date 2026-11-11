package telemetry

import (
	"context"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/tracecontext"
	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/krateo-platformops/unstructured-runtime"

// SetupTracing configures an OTLP trace pipeline and installs the W3C propagator, sharing the
// metrics Resource (buildResource) so traces/metrics/logs all carry identical resource
// attributes. It is a no-op (returning a working no-op shutdown) when cfg.TracingEnabled is
// false, so callers can wire it unconditionally and otel.Tracer(...).Start stays safe.
//
// Uses WithBatcher (NEVER WithSyncer): a slow or dead collector must never block a reconcile.
func SetupTracing(ctx context.Context, log logging.Logger, cfg Config) (func(context.Context) error, error) {
	if log == nil {
		log = logging.NewNopLogger()
	}

	// Install the propagator even when not exporting, so an inbound traceparent is honored
	// and can be continued onto child resources regardless of this process's export config.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if !cfg.TracingEnabled {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := buildResource(cfg)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	SetTracerProvider(tp)
	log.Info("OpenTelemetry tracing initialized", "serviceName", cfg.ServiceName, "compositionID", cfg.CompositionID)
	return func(ctx context.Context) error {
		// Passes after shutdown start nothing and write no marker for a trace nobody exports.
		// Only this provider is cleared: a later SetTracerProvider is someone else's.
		if b := defaultProvider.Load(); b != nil && b.tp == trace.TracerProvider(tp) {
			defaultProvider.CompareAndSwap(b, nil)
		}
		return tp.Shutdown(ctx)
	}, nil
}

// providerBox holds the process default TracerProvider; atomic.Pointer needs a concrete type.
type providerBox struct{ tp trace.TracerProvider }

// defaultProvider is the process default TracerProvider, nil when tracing is off. It is NOT read
// from the OTel global: otel.GetTracerProvider() cannot tell off from on (its delegate allocates a
// span per Start either way), and a nil provider is what lets tracecontext start nothing and
// allocate nothing with tracing off. Same design, and names, as provider-runtime's pkg/tracing.
var defaultProvider atomic.Pointer[providerBox]

// SetTracerProvider sets the process default TracerProvider that every controller built without
// Options.TracerProvider reads, on every pass. nil turns tracing off. SetupTracing calls it when
// tracing is enabled; a controller built before that call still picks it up.
func SetTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		defaultProvider.Store(nil)
		return
	}
	defaultProvider.Store(&providerBox{tp: tp})
}

// TracerProvider is the process default TracerProvider, nil when tracing is off.
func TracerProvider() trace.TracerProvider {
	if b := defaultProvider.Load(); b != nil {
		return b.tp
	}
	return nil
}

// TracerFrom is this runtime's tracer from tp (its instrumentation scope), and nil for a nil tp.
// A nil tracer means tracing is off to plumbing's tracecontext; a non-nil one, even a no-op one,
// means on.
func TracerFrom(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		return nil
	}
	return tp.Tracer(tracerName)
}

// Tracer is the runtime's tracer from the process default, nil with tracing off. The reconcile
// root and the joined act step are started through plumbing's tracecontext with it (#77): every
// pass is a new root LINKED to the object's krateo.io/traceparent, and only the first act for a new
// value joins the upstream trace.
func Tracer() trace.Tracer { return TracerFrom(TracerProvider()) }

// RecordSpanError marks the span failed when err != nil; no-op otherwise.
func RecordSpanError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// InjectTraceparent stores the active W3C trace context on the annotations map under
// krateo.io/traceparent (+ krateo.io/tracestate), so a child resource carries it for the next
// controller to continue the distributed trace. With tracing OFF there is no active span, and it
// falls back to the inbound context the reconcile root kept (tracecontext.Inbound), so a trace that
// came in on the object still reaches its children; it never writes the
// krateo.io/traceparent-continued marker. No-op when neither is present.
func InjectTraceparent(ctx context.Context, annotations map[string]string) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		if in := tracecontext.Inbound(ctx); in.IsValid() {
			ctx = trace.ContextWithSpanContext(ctx, in)
		}
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp := carrier.Get("traceparent"); tp != "" {
		annotations[meta.AnnotationKeyTraceparent] = tp
		if ts := carrier.Get("tracestate"); ts != "" {
			annotations[meta.AnnotationKeyTracestate] = ts
		}
	}
}
