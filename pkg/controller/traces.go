package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/krateo-platformops/plumbing/tracecontext"
	"github.com/krateo-platformops/unstructured-runtime/pkg/telemetry"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// This file is unstructured-runtime's half of the krateo.io/traceparent rule (#77). The rule itself
// (Decide, StartReconcile, StartActStep) is plumbing's tracecontext, shared with provider-runtime;
// what lives here is the per-object state tracecontext's package doc says each runtime keeps, ported
// from provider-runtime v1.14.1 pkg/reconciler/traces.go.
//
// THE ACT IS DECLARED WHERE CHANGES ARE APPLIED. The runtime opens the act step around every
// Create, Update and Delete, as provider-runtime does. It does NOT open one around Observe, because
// whether an Observe applies anything is known only to the handler: rest-dynamic-controller's
// Observe is a REST read, and the composition-dynamic-controller's Observe applies the release's
// children (plumbing helm Reconcile) only when the release exists. Had the runtime treated every
// Observe as the act, the first pass on a NEW composition would have joined the upstream at an
// Observe that found no release and done nothing, written the marker, and left the install (the
// next pass, Create) linked instead of joined: the frontend's request would never show the install.
// So a handler whose Observe applies changes calls StartAct right before its mutating call.

// memoState is what this process knows about the value a memo entry holds.
type memoState int

const (
	// memoSeen: the value is new and not continued yet.
	memoSeen memoState = iota
	// memoMarked: this process wrote the marker; it holds until a read shows it.
	memoMarked
	// memoFailed: the marker write failed with something other than a conflict.
	memoFailed
)

// memoEntry is the memo's record for one object.
type memoEntry struct {
	uid           types.UID
	value         string    // the canonical traceparent last seen
	firstSeen     time.Time // when this process first saw value
	state         memoState
	failureLogged bool
}

// traceMemo is the controller's in-memory record, by object, of the traceparent values it has seen
// and marked. One mutex guards the map, held only to read or update an entry, never across an API
// call. The work queue never runs two passes of one object at once, so two passes never race on one
// entry. With tracing off nothing is ever added; NotFound drops an entry whether tracing is on or off.
type traceMemo struct {
	mu      sync.Mutex
	entries map[types.NamespacedName]memoEntry
}

// decide computes the pass's Decision from the object and the memo. With tracing off it keeps
// nothing and only parses the annotation, so StartReconcile can carry it for injectors.
func (m *traceMemo) decide(on bool, key types.NamespacedName, obj *unstructured.Unstructured, now time.Time, window time.Duration) tracecontext.Decision {
	ann := obj.GetAnnotations()
	if !on {
		return tracecontext.Decide(ann, "", time.Time{}, now, window)
	}
	plain := tracecontext.Decide(ann, "", time.Time{}, now, window)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = map[types.NamespacedName]memoEntry{}
	}
	e, ok := m.entries[key]
	if ok && e.uid != obj.GetUID() {
		delete(m.entries, key)
		ok = false
	}
	if !plain.Remote.IsValid() {
		delete(m.entries, key)
		return plain
	}
	cur := tracecontext.Canonical(plain.Remote)
	if ok && e.value != cur {
		delete(m.entries, key)
		ok = false
	}
	if plain.Mode == tracecontext.Link && plain.WriteMarker == tracecontext.Never {
		// The object shows the marker for this value: whatever this process remembers is confirmed.
		delete(m.entries, key)
		return plain
	}
	remembered, firstSeen := "", time.Time{}
	if ok {
		switch e.state {
		case memoMarked, memoFailed:
			remembered = cur
		case memoSeen:
			firstSeen = e.firstSeen
		}
	}
	d := tracecontext.Decide(ann, remembered, firstSeen, now, window)
	if !ok && d.WriteMarker != tracecontext.Never {
		m.entries[key] = memoEntry{uid: obj.GetUID(), value: cur, firstSeen: now, state: memoSeen}
	}
	return d
}

// set records the outcome of a marker write for value.
func (m *traceMemo) set(key types.NamespacedName, uid types.UID, value string, state memoState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = map[types.NamespacedName]memoEntry{}
	}
	e := m.entries[key]
	if e.uid != uid || e.value != value {
		e = memoEntry{uid: uid, value: value, firstSeen: time.Now()}
	}
	e.state = state
	m.entries[key] = e
}

// loggedFailure reports whether a marker write failure for key should be logged: once per entry.
func (m *traceMemo) loggedFailure(key types.NamespacedName) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok || e.failureLogged {
		return false
	}
	e.failureLogged = true
	m.entries[key] = e
	return true
}

// forget drops key's entry.
func (m *traceMemo) forget(key types.NamespacedName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
}

// len reports the number of entries, for tests.
func (m *traceMemo) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// reconcileTracer is the tracer a pass starts its root with: nil when tracing is off.
func (c *Controller) reconcileTracer() trace.Tracer {
	if c.tracerProv != nil {
		return telemetry.TracerFrom(c.tracerProv)
	}
	return telemetry.Tracer()
}

// tracePass is one reconcile's tracing state. With tracing off tracer is nil and every method does
// nothing, so one shared offPass serves every pass.
type tracePass struct {
	tracer trace.Tracer
	span   trace.Span
	d      tracecontext.Decision
	obj    tracecontext.Object
	key    types.NamespacedName
	uid    types.UID
	memo   *traceMemo
	marked bool // the marker was written, or the write failed, this pass
	// c and target are where the marker is written: target is the object the runtime's handler
	// for this pass holds and writes status from (handleObserve re-reads it), set before the
	// handler's external call.
	c      *Controller
	target *unstructured.Unstructured
	// actJoined: this pass's act step for a new value has been parented on the upstream (D8).
	actJoined bool
}

var offPass = &tracePass{}

type tracePassKey struct{}

// withTracePass carries p to the act site (handleObserve), which runs in a nested call.
func withTracePass(ctx context.Context, p *tracePass) context.Context {
	return context.WithValue(ctx, tracePassKey{}, p)
}

func tracePassFrom(ctx context.Context) *tracePass {
	if p, ok := ctx.Value(tracePassKey{}).(*tracePass); ok && p != nil {
		return p
	}
	return offPass
}

// event adds a fixed-name event to the root.
func (p *tracePass) event(name string, attrs ...attribute.KeyValue) {
	if p.tracer == nil {
		return
	}
	p.span.AddEvent(name, trace.WithAttributes(attrs...))
}

// joinAct marks the root as having acted (plumbing tracecontext.AttrActed) for EVERY act, and
// opens the JOINED act step, and only that one: the pass's first act (Create, Update,
// Delete, or a handler's StartAct inside Observe) for a value in Parent mode. Every other act opens
// NO step span: provider-runtime has a span per step, but a child span per pass would double the
// span volume to say nothing the root does not. d.Remote is valid in Link mode too, so the mode
// check is what keeps a value from being re-joined on every pass.
func (p *tracePass) joinAct(ctx context.Context, name string) (context.Context, trace.Span) {
	if p.tracer == nil {
		return ctx, trace.SpanFromContext(context.Background())
	}
	if p.d.Mode != tracecontext.Parent || p.actJoined {
		// No step span for this act, but the pass DID act: mark the root (AttrActed), so a tail
		// sampler keeps the record of the change. A joined act is marked by StartActStep itself.
		tracecontext.MarkActed(ctx)
		return ctx, trace.SpanFromContext(context.Background())
	}
	p.actJoined = true
	return tracecontext.StartActStep(ctx, p.tracer, p.obj, name, p.d.Remote)
}

// forget drops the object's memo entry (its finalizer was removed).
func (p *tracePass) forget() {
	if p.tracer == nil {
		return
	}
	p.memo.forget(p.key)
}

// errTraceMarkerConflict is a marker write that lost an optimistic-concurrency race. It wraps
// tools.ErrStatusConflict so the worker requeues it QUIETLY (no retry budget, no backoff growth):
// nothing is wrong, the object just changed under the pass, and the next pass reads it afresh.
var errTraceMarkerConflict = fmt.Errorf("the object changed since this pass read it; requeueing before acting: %w", tools.ErrStatusConflict)

// stepMarker is the AttrStep value for a conflicting marker write, provider-runtime's string.
const stepMarker = "marker"

// writeMarkerIf writes the marker when the pass's decision says to write it at when, and it hasn't
// been written this pass. On a conflict it returns errTraceMarkerConflict and the pass must end
// before it acts. Any other failure is recorded as trace.marker_failed, the memo remembers the value
// as failed so later passes link, and the pass goes on.
func (c *Controller) writeMarkerIf(ctx context.Context, el *unstructured.Unstructured, p *tracePass, when tracecontext.When) error {
	if p.tracer == nil || p.marked || p.d.WriteMarker != when || p.d.Marker == "" {
		return nil
	}
	err := c.writeMarker(ctx, el, p.d.Marker)
	switch {
	case err == nil || apierrors.IsNotFound(err):
		p.marked = true
		p.memo.set(p.key, p.uid, p.d.Marker, memoMarked)
		return nil
	case apierrors.IsConflict(err):
		p.event(tracecontext.EventConflict, tracecontext.AttrStep.String(stepMarker))
		return errTraceMarkerConflict
	default:
		p.marked = true
		p.memo.set(p.key, p.uid, p.d.Marker, memoFailed)
		p.event(tracecontext.EventMarkerFailed, attribute.String("exception.message", truncate(err.Error(), 4096)))
		if p.memo.loggedFailure(p.key) && c.logger != nil {
			c.logger.Info("Cannot write the trace marker; later passes in this process link to the trace",
				"annotation", tracecontext.AnnotationKeyTraceparentContinued, "error", err.Error())
		}
		return nil
	}
}

// writeMarker writes value as krateo.io/traceparent-continued with a JSON merge patch that carries
// the resourceVersion the pass read, so the apiserver rejects it with 409 if the object changed
// (optimistic lock). On success it copies only the new resourceVersion and the marker onto el, so
// the pass's later writes stay version-checked against the object it read.
func (c *Controller) writeMarker(ctx context.Context, el *unstructured.Unstructured, value string) error {
	gvr, err := c.pluralizer.GVKtoGVR(el.GroupVersionKind())
	if err != nil {
		return fmt.Errorf("resolving %s for the trace marker: %w", el.GroupVersionKind(), err)
	}
	if el.GetResourceVersion() == "" {
		return errors.New("the object has no resourceVersion, so the trace marker cannot be written under an optimistic lock")
	}
	patch := fmt.Appendf(nil, `{"metadata":{"resourceVersion":%q,"annotations":{%q:%q}}}`,
		el.GetResourceVersion(), tracecontext.AnnotationKeyTraceparentContinued, value)
	updated, err := c.dynamicClient.Resource(gvr).Namespace(el.GetNamespace()).
		Patch(ctx, el.GetName(), types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	el.SetResourceVersion(updated.GetResourceVersion())
	ann := el.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[tracecontext.AnnotationKeyTraceparentContinued] = value
	el.SetAnnotations(ann)
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// beginAct writes the marker due before the pass's first act (Decision.WriteMarker
// BeforeFirstAction) and opens the joined act step for the external call that follows. With tracing
// off it does nothing and allocates nothing. A marker conflict is returned so the handler ends the
// pass BEFORE the act, and the worker requeues it quietly.
func (c *Controller) beginAct(ctx context.Context, el *unstructured.Unstructured, name string) (context.Context, trace.Span, error) {
	p := tracePassFrom(ctx)
	p.observing(el)
	if err := c.writeMarkerIf(ctx, el, p, tracecontext.BeforeFirstAction); err != nil {
		return ctx, nil, err
	}
	actCtx, span := p.joinAct(ctx, name)
	return actCtx, span, nil
}

// observing records el as the object this pass's marker is written on, before the handler's
// external call.
func (p *tracePass) observing(el *unstructured.Unstructured) {
	if p.tracer == nil {
		return
	}
	p.target = el
}

// StartAct declares the handler's act inside Observe: call it right before the call that APPLIES
// changes (an apply, an install, a write to the external system), with ctx as Observe received it
// and mg the object Observe was given (or the handler's copy of it). Create, Update and Delete need
// not call it, because the runtime opens their act step itself; a call there does nothing.
//
// For the first act on a new krateo.io/traceparent value it writes the marker
// krateo.io/traceparent-continued and opens the act step joined to the upstream trace, so the
// change shows in the trace that asked for it (#77). The marker is written under mg's
// resourceVersion, and on mg only the resourceVersion and the marker change, so the handler's later
// writes stay version-checked. Every other call, and every call
// with tracing off, opens nothing and returns ctx.
//
// A non-nil error means the marker write lost an optimistic-lock race: the handler must NOT act,
// and should return the error (wrapped with %w if at all). It wraps tools.ErrStatusConflict, so the
// runtime requeues the pass quietly and the next one reads the object afresh.
//
// end records the act's error on the act step and ends it; call it once, after the act.
func StartAct(ctx context.Context, mg *unstructured.Unstructured, name string) (actCtx context.Context, end func(error), err error) {
	p := tracePassFrom(ctx)
	if p.tracer == nil || p.c == nil || p.target == nil {
		return ctx, func(error) {}, nil
	}
	// The marker is written under the HANDLER's resourceVersion (mg), its freshest view: a handler
	// that wrote the object earlier in Observe holds a newer version than the runtime's copy, and a
	// patch under the older one would conflict on every pass and never act. The runtime's copy is
	// advanced only when it held that same version, so a runtime copy that was already stale stays
	// stale, exactly as it would have without the marker.
	obj := p.target
	if mg != nil {
		obj = mg
	}
	before, wasMarked := obj.GetResourceVersion(), p.marked
	if err := p.c.writeMarkerIf(ctx, obj, p, tracecontext.BeforeFirstAction); err != nil {
		return ctx, func(error) {}, err
	}
	if v, wrote := obj.GetAnnotations()[tracecontext.AnnotationKeyTraceparentContinued]; wrote && v == p.d.Marker && !wasMarked && p.marked && obj != p.target {
		if p.target.GetResourceVersion() == before {
			p.target.SetResourceVersion(obj.GetResourceVersion())
		}
		ann := p.target.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		ann[tracecontext.AnnotationKeyTraceparentContinued] = v
		p.target.SetAnnotations(ann)
	}
	actCtx, span := p.joinAct(ctx, name)
	return actCtx, func(err error) {
		telemetry.RecordSpanError(span, err)
		span.End()
	}, nil
}
