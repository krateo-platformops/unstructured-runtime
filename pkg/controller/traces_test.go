package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/plumbing/tracecontext"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/krateo-platformops/unstructured-runtime/pkg/telemetry"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
)

// #77/#78: the krateo.io/traceparent rule (plumbing tracecontext, provider-runtime's D8) as this
// runtime applies it. Every pass is a new root linked to the object's traceparent; only the FIRST
// act for a new value joins the upstream trace, and the marker krateo.io/traceparent-continued is
// what keeps later passes from joining it again.

// upstreamTP is a sampled upstream traceparent, as the frontend or a parent composition stamps it.
const upstreamTP = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

func upstreamSC(t *testing.T) trace.SpanContext {
	t.Helper()
	d := tracecontext.Decide(map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP}, "", time.Time{}, time.Now(), tracecontext.DefaultContinueWindow)
	require.True(t, d.Remote.IsValid(), "control: the fixture traceparent must parse")
	return d.Remote
}

// actingClient is a handler. Its Observe applies changes only when the "release" exists, and then
// declares the act with StartAct first, as the composition-dynamic-controller's does; with exists
// false it applies nothing and returns ResourceExists false, so the runtime dispatches Create.
type actingClient struct {
	fakeExternalClient
	exists   bool
	bumpRV   string // when set, Observe "writes" the object first: the handler's copy is newer
	observes atomic.Int32
	applies  atomic.Int32
	creates  atomic.Int32
	actErrs  []error
}

func (c *actingClient) Observe(ctx context.Context, el *unstructured.Unstructured) (ExternalObservation, error) {
	c.observes.Add(1)
	if !c.exists {
		return ExternalObservation{ResourceExists: false}, nil
	}
	mg := el.DeepCopy() // handlers deep-copy, so StartAct must reach the runtime's copy too
	if c.bumpRV != "" {
		mg.SetResourceVersion(c.bumpRV)
	}
	actCtx, end, err := StartAct(ctx, mg, "apply")
	c.actErrs = append(c.actErrs, err)
	if err != nil {
		return ExternalObservation{}, err
	}
	c.applies.Add(1)
	_ = actCtx // a real handler passes it to the call that applies
	end(nil)
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}

func (c *actingClient) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	c.creates.Add(1)
	return nil
}

type traceHarness struct {
	t    *testing.T
	opts Options
	ctrl *Controller
	rec  *tracetest.SpanRecorder
	ext  *actingClient
	name string
}

func newTraceHarness(t *testing.T, tracerOn bool, annotations map[string]string) *traceHarness {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	var tp trace.TracerProvider
	if tracerOn {
		sdk := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		t.Cleanup(func() { _ = sdk.Shutdown(context.Background()) })
		tp = sdk
	}
	return newTraceHarnessWith(t, tp, rec, annotations)
}

// newTraceHarnessWith builds the controller with Options.TracerProvider = tp (nil: the process
// default), the way a consumer does.
func newTraceHarnessWith(t *testing.T, tp trace.TracerProvider, rec *tracetest.SpanRecorder, annotations map[string]string) *traceHarness {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	opts.TracerProvider = tp
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	h := &traceHarness{t: t, opts: opts, ctrl: ctrl, rec: rec, ext: &actingClient{exists: true}, name: "traced"}
	ctrl.externalClient = h.ext

	obj := createTestUnstructured(h.name, opts.Namespace)
	obj.SetUID("uid-1")
	obj.SetAnnotations(annotations)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	return h
}

func (h *traceHarness) pass(et ctrlevent.EventType) error {
	return h.ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: et,
		ObjectRef: objectref.ObjectRef{APIVersion: "test.example.org/v1", Kind: "Test", Name: h.name, Namespace: h.opts.Namespace},
	})
}

func (h *traceHarness) live() *unstructured.Unstructured {
	got, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.opts.Namespace).Get(context.TODO(), h.name, metav1.GetOptions{})
	require.NoError(h.t, err)
	return got
}

func (h *traceHarness) marker() (string, bool) {
	v, ok := h.live().GetAnnotations()[tracecontext.AnnotationKeyTraceparentContinued]
	return v, ok
}

func (h *traceHarness) memoState() (memoState, bool) {
	h.ctrl.traces.mu.Lock()
	defer h.ctrl.traces.mu.Unlock()
	e, ok := h.ctrl.traces.entries[types.NamespacedName{Namespace: h.opts.Namespace, Name: h.name}]
	return e.state, ok
}

// markerPatches intercepts every patch that writes the marker: it records the patch and, when err
// is set, fails it; other patches pass through.
func (h *traceHarness) markerPatches(err error) *[]string {
	var seen []string
	h.opts.Client.(*dynamicfake.FakeDynamicClient).PrependReactor("patch", "tests", func(a clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := a.(clienttesting.PatchAction)
		if !ok || !strings.Contains(string(pa.GetPatch()), tracecontext.AnnotationKeyTraceparentContinued) {
			return false, nil, nil
		}
		seen = append(seen, string(pa.GetPatch()))
		if err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	return &seen
}

// split separates the recorded spans into reconcile roots and act steps joined to upstream.
func split(spans []sdktrace.ReadOnlySpan, upstream trace.SpanContext) (roots, joined, other []sdktrace.ReadOnlySpan) {
	for _, s := range spans {
		switch {
		case s.Name() == tracecontext.SpanReconcile:
			roots = append(roots, s)
		case s.Parent().TraceID() == upstream.TraceID() && s.Parent().SpanID() == upstream.SpanID():
			joined = append(joined, s)
		default:
			other = append(other, s)
		}
	}
	return
}

func linksTo(s sdktrace.ReadOnlySpan, sc trace.SpanContext) bool {
	for _, l := range s.Links() {
		if l.SpanContext.TraceID() == sc.TraceID() && l.SpanContext.SpanID() == sc.SpanID() {
			return true
		}
	}
	return false
}

func hasEvent(s sdktrace.ReadOnlySpan, name string) bool {
	for _, e := range s.Events() {
		if e.Name == name {
			return true
		}
	}
	return false
}

var withUpstream = map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP}

// An Observe that applies changes declares its act: the first pass for a new value is a NEW root
// linked to the upstream, with the act joined to it and the marker on the object; the second pass
// reads the marker, links, and joins nothing. Before #77 every pass's root was the upstream's
// CHILD, so a composition's trace grew by a reconcile per resync for as long as the annotation
// stayed (62,123 spans in one trace on krateo-057).
func TestTraceObserveThatAppliesJoinsOnceThenLinks(t *testing.T) {
	up := upstreamSC(t)
	h := newTraceHarness(t, true, withUpstream)

	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Equal(t, int32(1), h.ext.applies.Load(), "control: the first pass must reach the apply")
	roots, joined, other := split(h.rec.Ended(), up)
	require.Len(t, roots, 1, "one reconcile root per pass")
	require.Len(t, joined, 1, "the first act for a new value joins the upstream")
	require.Empty(t, other, "no step span besides the joined act: %v", other)

	root, act := roots[0], joined[0]
	require.False(t, root.Parent().IsValid(), "the root is a new root, never the upstream's child")
	require.NotEqual(t, up.TraceID(), root.SpanContext().TraceID())
	require.True(t, linksTo(root, up), "the root links to the upstream")
	require.Equal(t, "apply", act.Name(), "the act is the step the handler declared")
	require.True(t, linksTo(act, root.SpanContext()), "the act links back to its reconcile root")
	require.True(t, linksTo(root, act.SpanContext()), "the root links to the sampled act")

	v, ok := h.marker()
	require.True(t, ok, "the marker is written before the act")
	require.Equal(t, tracecontext.Canonical(up), v)

	h.rec.Reset()
	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Equal(t, int32(2), h.ext.applies.Load(), "control: the second pass must reach the apply too")
	roots, joined, other = split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Empty(t, joined, "a continued value is linked, never joined again")
	require.Empty(t, other)
	require.True(t, linksTo(roots[0], up))
	require.Zero(t, h.ctrl.traces.len(), "a read that shows the marker confirms and drops the memo entry")
}

// THE CASE THE TRACEPARENT EXISTS FOR: a new composition. Its first Observe finds no release and
// applies nothing, so it must not join and must not mark; the Create pass that installs is the one
// that joins the upstream. Had Observe been the act, the marker would have been written at the no-op
// Observe and the install would have only linked.
func TestTraceNewObjectJoinsAtCreateNotAtObserve(t *testing.T) {
	up := upstreamSC(t)
	h := newTraceHarness(t, true, withUpstream)
	h.ext.exists = false

	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Equal(t, int32(1), h.ext.observes.Load(), "control: the pass reached Observe")
	roots, joined, other := split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Empty(t, joined, "an Observe that applied nothing joins nothing")
	require.Empty(t, other)
	_, marked := h.marker()
	require.False(t, marked, "a pass that does not act writes no marker")
	st, ok := h.memoState()
	require.True(t, ok)
	require.Equal(t, memoSeen, st, "the value is remembered as seen, so Create still joins")

	h.rec.Reset()
	require.NoError(t, h.pass(ctrlevent.Create))
	require.Equal(t, int32(1), h.ext.creates.Load(), "control: the Create pass reached Create")
	roots, joined, _ = split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Len(t, joined, 1, "the install joins the upstream")
	require.Equal(t, "create", joined[0].Name())
	v, _ := h.marker()
	require.Equal(t, tracecontext.Canonical(up), v)
}

// A marker write that loses an optimistic-lock race ends the pass BEFORE it acts, quietly (it wraps
// tools.ErrStatusConflict), with the conflict on the root and the memo left as it was: at the
// handler's StartAct and at the runtime's Create alike.
func TestTraceMarkerConflictRequeuesBeforeActing(t *testing.T) {
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "test.example.org", Resource: "tests"}, "traced", errors.New("changed"))
	for _, tc := range []struct {
		name   string
		exists bool
		event  ctrlevent.EventType
		acted  func(*actingClient) int32
	}{
		{"observe", true, ctrlevent.Observe, func(c *actingClient) int32 { return c.applies.Load() }},
		{"create", false, ctrlevent.Create, func(c *actingClient) int32 { return c.creates.Load() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTraceHarness(t, true, withUpstream)
			h.ext.exists = tc.exists
			writes := h.markerPatches(conflict)

			err := h.pass(tc.event)
			require.ErrorIs(t, err, errTraceMarkerConflict)
			require.Len(t, *writes, 1, "control: the marker write was attempted")
			require.Zero(t, tc.acted(h.ext), "a marker conflict must end the pass before it acts")
			roots, joined, _ := split(h.rec.Ended(), upstreamSC(t))
			require.Len(t, roots, 1)
			require.Empty(t, joined, "nothing acted, so nothing joined")
			require.True(t, hasEvent(roots[0], tracecontext.EventConflict))
			st, _ := h.memoState()
			require.Equal(t, memoSeen, st, "a conflict leaves the memo as it was: the next pass reads afresh")
		})
	}
}

// A handler that wrote the object earlier in Observe holds a newer resourceVersion than the
// runtime's copy. The marker must be written under the HANDLER's version: under the runtime's older
// one it would conflict on every pass, and the handler would never act.
func TestTraceMarkerUsesTheHandlersResourceVersion(t *testing.T) {
	h := newTraceHarness(t, true, withUpstream)
	h.ext.bumpRV = "7"
	writes := h.markerPatches(nil)

	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Len(t, *writes, 1)
	require.Contains(t, (*writes)[0], `"resourceVersion":"7"`, "the patch carries the handler's version")
	require.Equal(t, int32(1), h.ext.applies.Load())
}

// Any other marker failure (RBAC, a webhook) lets the pass act, records trace.marker_failed, and
// remembers the value as failed, so the next pass LINKS although the object never got the marker.
func TestTraceMarkerFailureActsAndThenLinks(t *testing.T) {
	up := upstreamSC(t)
	h := newTraceHarness(t, true, withUpstream)
	writes := h.markerPatches(apierrors.NewForbidden(schema.GroupResource{Group: "test.example.org", Resource: "tests"}, h.name, errors.New("rbac")))

	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Len(t, *writes, 1)
	require.Equal(t, int32(1), h.ext.applies.Load(), "a non-conflict marker failure must not stop the act")
	roots, joined, _ := split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Len(t, joined, 1, "the pass still joins the upstream at its act")
	require.True(t, hasEvent(roots[0], tracecontext.EventMarkerFailed))
	_, marked := h.marker()
	require.False(t, marked, "control: the object did not get the marker")

	h.rec.Reset()
	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Len(t, *writes, 1, "the marker is not retried by a pass that remembers it failed")
	require.Equal(t, int32(2), h.ext.applies.Load(), "control: the second pass reached the apply")
	roots, joined, _ = split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Empty(t, joined, "a value remembered as failed is linked, not re-joined")
}

// With tracing off nothing starts, nothing is remembered, and no marker is written: the object is
// not touched on behalf of traces this process does not export. StartAct is a no-op.
func TestTraceOffWritesNothing(t *testing.T) {
	h := newTraceHarness(t, false, withUpstream)
	writes := h.markerPatches(nil)
	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Equal(t, int32(1), h.ext.applies.Load(), "control: the pass reached the apply")
	require.Equal(t, []error{nil}, h.ext.actErrs)
	require.Empty(t, h.rec.Ended())
	require.Empty(t, *writes)
	require.Zero(t, h.ctrl.traces.len())
}

// #78: a paused pass still emits its root (the controller is alive) and marks it paused.
func TestTracePausedPassIsMarked(t *testing.T) {
	h := newTraceHarness(t, true, map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
	require.NoError(t, h.pass(ctrlevent.Observe))
	require.Zero(t, h.ext.observes.Load(), "control: a paused pass does not observe")
	roots, _, _ := split(h.rec.Ended(), upstreamSC(t))
	require.Len(t, roots, 1)
	require.True(t, hasEvent(roots[0], tracecontext.EventReconcilePaused))

	// Control: an unpaused pass carries no such event.
	h2 := newTraceHarness(t, true, nil)
	require.NoError(t, h2.pass(ctrlevent.Observe))
	roots, _, _ = split(h2.rec.Ended(), upstreamSC(t))
	require.Len(t, roots, 1)
	require.False(t, hasEvent(roots[0], tracecontext.EventReconcilePaused))
}

// The provider a pass uses: Options.TracerProvider when set, else the process default
// (telemetry.TracerProvider), read on EVERY pass, so a default set after New still takes effect.
// With neither, tracing is off.
func TestTraceProviderOptionAndProcessDefault(t *testing.T) {
	t.Cleanup(func() { telemetry.SetTracerProvider(nil) })
	up := upstreamSC(t)

	// Neither set: off.
	telemetry.SetTracerProvider(nil)
	off := newTraceHarnessWith(t, nil, tracetest.NewSpanRecorder(), withUpstream)
	require.NoError(t, off.pass(ctrlevent.Observe))
	require.Equal(t, int32(1), off.ext.applies.Load(), "control: the pass ran")
	require.Zero(t, off.ctrl.traces.len(), "no provider anywhere: tracing is off")

	// The default set AFTER New is picked up by the next pass.
	defRec := tracetest.NewSpanRecorder()
	def := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(defRec))
	t.Cleanup(func() { _ = def.Shutdown(context.Background()) })
	h := newTraceHarnessWith(t, nil, defRec, withUpstream)
	telemetry.SetTracerProvider(def)
	require.NoError(t, h.pass(ctrlevent.Observe))
	roots, joined, _ := split(defRec.Ended(), up)
	require.Len(t, roots, 1, "the process default records the pass")
	require.Len(t, joined, 1)

	// A default already set at New and REPLACED afterwards: the next pass uses the new one. This is
	// the arm that tells "read on every pass" from "read once at New"; the arm above cannot, because
	// a nil captured at New falls back to the default either way.
	nextRec := tracetest.NewSpanRecorder()
	next := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(nextRec))
	t.Cleanup(func() { _ = next.Shutdown(context.Background()) })
	r := newTraceHarnessWith(t, nil, nextRec, withUpstream) // built while def is the default
	atNew := len(defRec.Ended())
	telemetry.SetTracerProvider(next)
	require.NoError(t, r.pass(ctrlevent.Observe))
	roots, _, _ = split(nextRec.Ended(), up)
	require.Len(t, roots, 1, "the replaced default records the pass")
	require.Len(t, defRec.Ended(), atNew, "the default at New records nothing of it")

	// The option wins over the default.
	optRec := tracetest.NewSpanRecorder()
	opt := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(optRec))
	t.Cleanup(func() { _ = opt.Shutdown(context.Background()) })
	before := len(nextRec.Ended())
	o := newTraceHarnessWith(t, opt, optRec, withUpstream)
	require.NoError(t, o.pass(ctrlevent.Observe))
	roots, _, _ = split(optRec.Ended(), up)
	require.Len(t, roots, 1, "Options.TracerProvider records the pass")
	require.Len(t, nextRec.Ended(), before, "and the process default records nothing of it")
}

func TestTraceMemoDecide(t *testing.T) {
	key := types.NamespacedName{Namespace: "ns", Name: "a"}
	now := time.Now()
	obj := func(uid string, ann map[string]string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetUID(types.UID(uid))
		u.SetAnnotations(ann)
		return u
	}
	with := map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP}
	var m traceMemo

	d := m.decide(false, key, obj("u1", with), now, tracecontext.DefaultContinueWindow)
	require.Equal(t, tracecontext.Parent, d.Mode, "tracing off still parses, for the injectors")
	require.Zero(t, m.len(), "tracing off keeps nothing")

	d = m.decide(true, key, obj("u1", nil), now, tracecontext.DefaultContinueWindow)
	require.Equal(t, tracecontext.NoParent, d.Mode)
	require.Zero(t, m.len(), "no annotation, no entry")

	d = m.decide(true, key, obj("u1", with), now, tracecontext.DefaultContinueWindow)
	require.Equal(t, tracecontext.Parent, d.Mode)
	require.Equal(t, tracecontext.BeforeFirstAction, d.WriteMarker)
	require.Equal(t, 1, m.len())

	// Past the window from first sight, a pass that never acted links and marks now.
	d = m.decide(true, key, obj("u1", with), now.Add(tracecontext.DefaultContinueWindow+time.Second), tracecontext.DefaultContinueWindow)
	require.Equal(t, tracecontext.Link, d.Mode)
	require.Equal(t, tracecontext.Now, d.WriteMarker)

	// A recreated object (new uid) starts over.
	d = m.decide(true, key, obj("u2", with), now.Add(tracecontext.DefaultContinueWindow+time.Second), tracecontext.DefaultContinueWindow)
	require.Equal(t, tracecontext.Parent, d.Mode, "a new uid must not inherit the old object's firstSeen")

	m.set(key, "u2", tracecontext.Canonical(d.Remote), memoMarked)
	d = m.decide(true, key, obj("u2", with), now, tracecontext.DefaultContinueWindow)
	require.Equal(t, tracecontext.Link, d.Mode, "a marked value links before the object shows the marker")
	require.Equal(t, tracecontext.Never, d.WriteMarker)

	// The annotation going away drops the entry.
	m.decide(true, key, obj("u2", nil), now, tracecontext.DefaultContinueWindow)
	require.Zero(t, m.len())
}
