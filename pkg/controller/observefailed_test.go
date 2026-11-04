package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// An Observe failure used to write Synced=ReconcileError and NOTHING to Ready, so Ready kept
// whatever it last held — including True/Available. A CR then reported ready for a resource nobody
// had successfully observed, for as long as the observation kept failing.
//
// Brought by the oasgen-provider session as the gap left by the Ready-message fix, and they were
// right that it is worse than the thing that fix addressed: an empty message is confusing, a stale
// Ready=True is silently wrong.

func TestHandleObserve_FailureReplacesAStaleReadyWithUnknown(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("observe-fail-1", opts.Namespace)
	// THE STALE VALUE. Seeded deliberately: the defect is not "Ready is absent", it is "Ready is
	// a correct value that stopped being refreshed". A test on an object with no Ready would pass
	// against the unfixed code.
	require.NoError(t, unstructuredtools.SetConditions(obj, condition.Available()))
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	// PREMISE: the stale True is really there before we start.
	var seeded metav1.Condition
	for _, c := range unstructuredtools.GetConditions(created) {
		if c.Type == condition.TypeReady {
			seeded = c
		}
	}
	require.Equal(t, metav1.ConditionTrue, seeded.Status,
		"premise: the object must start at Ready=True, or this measures nothing")

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}

	const boom = "cannot observe: Get \"https://vendor.example:8443/v1/things/abc\": dial tcp: i/o timeout"
	ctrl.SetExternalClient(&fakeExternalClient{ObserveErr: errors.New(boom)})

	require.Error(t, ctrl.handleObserve(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	var ready, synced metav1.Condition
	for _, c := range unstructuredtools.GetConditions(updated) {
		switch c.Type {
		case condition.TypeReady:
			ready = c
		case condition.TypeSynced:
			synced = c
		}
	}

	// THE CONTRACT THAT MUST NOT BREAK, asserted FIRST. Consumers read Synced's reason to decide
	// whether the last reconcile errored — oasgen's lastReconcileErrored feeds their not-exists
	// branch, and it fires for exactly this condition. Writing Ready INSTEAD of Synced would
	// remove a guard that works today, so Synced is checked before the subject.
	require.Equal(t, condition.ReasonReconcileError, synced.Reason,
		"Synced=ReconcileError must SURVIVE beside the new Ready write. A consumer's "+
			"lastReconcileErrored guard depends on it, and the two conditions are not redundant")
	require.Contains(t, synced.Message, "i/o timeout",
		"CONTROL: Synced must still carry the error")

	// THE SUBJECT: the stale True is gone, replaced by honest ignorance.
	require.Equal(t, metav1.ConditionUnknown, ready.Status,
		"Ready must become UNKNOWN on an Observe failure. It was seeded True and the observation "+
			"failed, so leaving it True reports ready for a resource nobody observed")
	require.Equal(t, condition.ReasonObserveFailed, ready.Reason,
		"the reason must name the FAILURE to observe, so a consumer can tell it from a resource "+
			"that was observed and found unhealthy")
	require.Contains(t, ready.Message, "i/o timeout",
		"Ready must carry the diagnostic too, not merely a reason")
}

// UNKNOWN, NOT FALSE, and this is the arm that pins it. We did not observe the resource becoming
// unhealthy — we failed to observe it at all. False would report an outage of the RESOURCE when the
// outage is in our ability to look, and would move every readiness-gating consumer on what may be a
// transient blip.
func TestObserveFailed_IsUnknownAndNotFalse(t *testing.T) {
	c := condition.ObserveFailed("boom")
	require.Equal(t, condition.TypeReady, c.Type)
	require.Equal(t, metav1.ConditionUnknown, c.Status,
		"False would make a failure to LOOK indistinguishable from a resource that IS unhealthy")
	require.Equal(t, "ObserveFailed", c.Reason,
		"the reason string is consumer-visible and case-sensitive; core-provider's cdc rollup "+
			"matches it by equality")
	require.Equal(t, "boom", c.Message)

	// It must be distinguishable from the sibling that means "we looked and found nothing to read".
	require.NotEqual(t, condition.ReasonZeroEvidence, c.Reason,
		"ObserveFailed (could not look) and ZeroEvidence (looked, nothing to read) are different "+
			"claims and must not collapse into one reason")
}

// The UPDATE-failure path deliberately does NOT get a Ready write. There, Observe succeeded and
// said not-up-to-date: the resource's health is unchanged and only the new spec failed to land.
// An observation failure invalidates our knowledge of health; a failed update does not.
//
// Asserted rather than left as a comment, because the obvious "fix" is to apply the same treatment
// to every failure path, and that would be wrong.
func TestHandleUpdate_FailureLeavesReadyAlone(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("update-fail-1", opts.Namespace)
	require.NoError(t, unstructuredtools.SetConditions(obj, condition.Available()))
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}
	ctrl.SetExternalClient(&fakeExternalClient{
		ObserveExists: true, ObserveUpToDate: false,
		UpdateErr: errors.New("update rejected: field is immutable"),
	})

	require.Error(t, ctrl.handleUpdate(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	var ready, synced metav1.Condition
	for _, c := range unstructuredtools.GetConditions(updated) {
		switch c.Type {
		case condition.TypeReady:
			ready = c
		case condition.TypeSynced:
			synced = c
		}
	}
	// CONTROL: the update really did fail and was recorded.
	require.Equal(t, condition.ReasonReconcileError, synced.Reason,
		"CONTROL: Synced must record the update failure, or this test is about the wrong path")

	require.Equal(t, metav1.ConditionTrue, ready.Status,
		"an UPDATE failure must leave Ready alone: the resource was observed and is healthy, only "+
			"the new spec did not land. Marking it Unknown would report ignorance we do not have")
	require.NotEqual(t, condition.ReasonObserveFailed, ready.Reason,
		"ObserveFailed must not leak onto the update path")
}
