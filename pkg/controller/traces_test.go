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

// countingObserver counts Observe calls, so a test can tell "the pass acted" from "it did not".
type countingObserver struct {
	fakeExternalClient
	observes atomic.Int32
}

func (c *countingObserver) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	c.observes.Add(1)
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}

type traceHarness struct {
	t        *testing.T
	opts     Options
	ctrl     *Controller
	rec      *tracetest.SpanRecorder
	ext      *countingObserver
	name     string
	tracerOn bool
}

func newTraceHarness(t *testing.T, tracerOn bool, annotations map[string]string) *traceHarness {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	h := &traceHarness{t: t, opts: opts, ctrl: ctrl, rec: tracetest.NewSpanRecorder(), ext: &countingObserver{}, name: "traced", tracerOn: tracerOn}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(h.rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctrl.tracerFn = func() trace.Tracer {
		if !tracerOn {
			return nil
		}
		return tp.Tracer("test")
	}
	ctrl.externalClient = h.ext

	obj := createTestUnstructured(h.name, opts.Namespace)
	obj.SetUID("uid-1")
	obj.SetAnnotations(annotations)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	return h
}

func (h *traceHarness) pass() error {
	return h.ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{APIVersion: "test.example.org/v1", Kind: "Test", Name: h.name, Namespace: h.opts.Namespace},
	})
}

func (h *traceHarness) live() *unstructured.Unstructured {
	got, err := h.opts.Client.Resource(h.opts.GVR).Namespace(h.opts.Namespace).Get(context.TODO(), h.name, metav1.GetOptions{})
	require.NoError(h.t, err)
	return got
}

// failMarkerPatches makes every patch that writes the marker fail with err; other patches pass.
func (h *traceHarness) failMarkerPatches(err error) *atomic.Int32 {
	var n atomic.Int32
	h.opts.Client.(*dynamicfake.FakeDynamicClient).PrependReactor("patch", "tests", func(a clienttesting.Action) (bool, runtime.Object, error) {
		pa, ok := a.(clienttesting.PatchAction)
		if !ok || !strings.Contains(string(pa.GetPatch()), tracecontext.AnnotationKeyTraceparentContinued) {
			return false, nil, nil
		}
		n.Add(1)
		return true, nil, err
	})
	return &n
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

// The first pass for a new value: a NEW root linked to the upstream, the observe act joined to it,
// and the marker on the object. The second pass reads the marker: a root linked to the upstream,
// and nothing joined. Before #77 every pass's root was the upstream's CHILD, so a composition's
// trace grew by a reconcile per resync for as long as the annotation stayed (62,123 spans on 057).
func TestTraceFirstActJoinsUpstreamThenLinks(t *testing.T) {
	up := upstreamSC(t)
	h := newTraceHarness(t, true, map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP})

	require.NoError(t, h.pass())
	require.Equal(t, int32(1), h.ext.observes.Load(), "control: the first pass must reach Observe")
	roots, joined, other := split(h.rec.Ended(), up)
	require.Len(t, roots, 1, "one reconcile root per pass")
	require.Len(t, joined, 1, "the first act for a new value joins the upstream")
	require.Empty(t, other, "no step span besides the joined act: %v", other)

	root, act := roots[0], joined[0]
	require.False(t, root.Parent().IsValid(), "the root is a new root, never the upstream's child")
	require.NotEqual(t, up.TraceID(), root.SpanContext().TraceID())
	require.True(t, linksTo(root, up), "the root links to the upstream")
	require.Equal(t, "observe", act.Name())
	require.Equal(t, up.TraceID(), act.SpanContext().TraceID(), "the act is in the upstream trace")
	require.True(t, linksTo(act, root.SpanContext()), "the act links back to its reconcile root")
	require.True(t, linksTo(root, act.SpanContext()), "the root links to the sampled act")

	require.Equal(t, tracecontext.Canonical(up), h.live().GetAnnotations()[tracecontext.AnnotationKeyTraceparentContinued],
		"the marker records the continued value")

	h.rec.Reset()
	require.NoError(t, h.pass())
	require.Equal(t, int32(2), h.ext.observes.Load(), "control: the second pass must reach Observe too")
	roots, joined, other = split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Empty(t, joined, "a continued value is linked, never joined again")
	require.Empty(t, other)
	require.True(t, linksTo(roots[0], up))
	require.Zero(t, h.ctrl.traces.len(), "a read that shows the marker confirms and drops the memo entry")
}

// A marker write that loses an optimistic-lock race ends the pass BEFORE it acts, quietly (it wraps
// tools.ErrStatusConflict), with the conflict on the root and the memo left as it was.
func TestTraceMarkerConflictRequeuesBeforeActing(t *testing.T) {
	h := newTraceHarness(t, true, map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP})
	writes := h.failMarkerPatches(apierrors.NewConflict(schema.GroupResource{Group: "test.example.org", Resource: "tests"}, h.name, errors.New("changed")))

	err := h.pass()
	require.ErrorIs(t, err, errTraceMarkerConflict)
	require.Equal(t, int32(1), writes.Load(), "control: the marker write was attempted")
	require.Zero(t, h.ext.observes.Load(), "a marker conflict must end the pass before Observe")
	roots, joined, _ := split(h.rec.Ended(), upstreamSC(t))
	require.Len(t, roots, 1)
	require.Empty(t, joined, "nothing acted, so nothing joined")
	require.True(t, hasEvent(roots[0], tracecontext.EventConflict))

	h.ctrl.traces.mu.Lock()
	e := h.ctrl.traces.entries[types.NamespacedName{Namespace: h.opts.Namespace, Name: h.name}]
	h.ctrl.traces.mu.Unlock()
	require.Equal(t, memoSeen, e.state, "a conflict leaves the memo as it was: the next pass reads afresh")
}

// Any other marker failure (RBAC, a webhook) lets the pass act, records trace.marker_failed, and
// remembers the value as failed, so the next pass LINKS although the object never got the marker.
func TestTraceMarkerFailureActsAndThenLinks(t *testing.T) {
	up := upstreamSC(t)
	h := newTraceHarness(t, true, map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP})
	writes := h.failMarkerPatches(apierrors.NewForbidden(schema.GroupResource{Group: "test.example.org", Resource: "tests"}, h.name, errors.New("rbac")))

	require.NoError(t, h.pass())
	require.Equal(t, int32(1), writes.Load())
	require.Equal(t, int32(1), h.ext.observes.Load(), "a non-conflict marker failure must not stop the act")
	roots, joined, _ := split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Len(t, joined, 1, "the pass still joins the upstream at its act")
	require.True(t, hasEvent(roots[0], tracecontext.EventMarkerFailed))
	_, marked := h.live().GetAnnotations()[tracecontext.AnnotationKeyTraceparentContinued]
	require.False(t, marked, "control: the object did not get the marker")

	h.rec.Reset()
	require.NoError(t, h.pass())
	require.Equal(t, int32(1), writes.Load(), "the marker is not retried by a pass that remembers it failed")
	require.Equal(t, int32(2), h.ext.observes.Load(), "control: the second pass reached Observe")
	roots, joined, _ = split(h.rec.Ended(), up)
	require.Len(t, roots, 1)
	require.Empty(t, joined, "a value remembered as failed is linked, not re-joined")
}

// With tracing off nothing starts, nothing is remembered, and no marker is written: the object is
// not touched on behalf of traces this process does not export.
func TestTraceOffWritesNothing(t *testing.T) {
	h := newTraceHarness(t, false, map[string]string{tracecontext.AnnotationKeyTraceparent: upstreamTP})
	require.NoError(t, h.pass())
	require.Equal(t, int32(1), h.ext.observes.Load(), "control: the pass reached Observe")
	require.Empty(t, h.rec.Ended())
	require.Zero(t, h.ctrl.traces.len())
	_, marked := h.live().GetAnnotations()[tracecontext.AnnotationKeyTraceparentContinued]
	require.False(t, marked)
}

// #78: a paused pass still emits its root (the controller is alive) and marks it paused.
func TestTracePausedPassIsMarked(t *testing.T) {
	h := newTraceHarness(t, true, map[string]string{meta.AnnotationKeyReconciliationPaused: "true"})
	require.NoError(t, h.pass())
	require.Zero(t, h.ext.observes.Load(), "control: a paused pass does not observe")
	roots, _, _ := split(h.rec.Ended(), upstreamSC(t))
	require.Len(t, roots, 1)
	require.True(t, hasEvent(roots[0], tracecontext.EventReconcilePaused))

	// Control: an unpaused pass carries no such event.
	h2 := newTraceHarness(t, true, nil)
	require.NoError(t, h2.pass())
	roots, _, _ = split(h2.rec.Ended(), upstreamSC(t))
	require.Len(t, roots, 1)
	require.False(t, hasEvent(roots[0], tracecontext.EventReconcilePaused))
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
