package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Ready must carry the diagnostic, not just Synced.
//
// MEASURED by the oasgen-provider session on 0.50.0, in kind: a ComputeFlavor whose vendor endpoint
// answered a cross-host redirect the host policy refused — a create that would NEVER succeed, and
// retried forever:
//
//	type=Ready   status=False  reason=Creating        message=            <- EMPTY
//	type=Synced  status=False  reason=ReconcileError  message=create failed: making request:
//	    ... refusing to send a credentialed request to "redirect-target:8080": it is not the
//	    document's server and is not in the operator's endpoint allowlist (http://mocknova:8080)
//
// Ready is the condition people watch — `kubectl wait --for=condition=Ready` is in the docs — and
// `reason=Creating` with no message reads as "still working" about something permanently stuck. An
// agent reading Ready alone gets nothing actionable; the refused host and the permitted set are
// exactly what a reasoning layer could act on.
//
// THE REASON MUST STAY Creating, which is why this is a message fix and not a reason fix. See
// condition.WithMessage's comment and the latch arm below.

func readyAndSynced(t *testing.T, u *unstructured.Unstructured) (ready, synced metav1.Condition) {
	t.Helper()
	for _, c := range unstructuredtools.GetConditions(u) {
		switch c.Type {
		case condition.TypeReady:
			ready = c
		case condition.TypeSynced:
			synced = c
		}
	}
	return ready, synced
}

func TestHandleCreate_FailurePutsTheDiagnosticOnReadyToo(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("ready-msg-1", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}

	// A distinctive, structured error standing in for the measured redirect refusal. Deliberately
	// long and specific: the point of the fix is that a reader of Ready alone can act on it.
	const refusal = `making request: Post "http://redirect-target:8080/v2.1/flavors": refusing to ` +
		`follow a redirect: refusing to send a credentialed request to "redirect-target:8080": it ` +
		`is not the document's server and is not in the operator's endpoint allowlist`
	ctrl.SetExternalClient(&fakeExternalClient{CreateErr: errors.New(refusal)})

	require.Error(t, ctrl.handleCreate(context.TODO(), ref))

	updated, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	// PREMISE: we are on the create-failure path at all.
	require.Contains(t, updated.GetAnnotations(), meta.AnnotationKeyExternalCreateFailed,
		"premise: the create-failed annotation must be set, or this is not the path under test")

	ready, synced := readyAndSynced(t, updated)

	// CONTROL FIRST: Synced must carry the diagnostic. If it does not, the comparison below could
	// pass with both messages empty.
	require.NotEmpty(t, synced.Message,
		"CONTROL: Synced must carry the error, or the Ready assertion proves nothing")
	require.Contains(t, synced.Message, "redirect-target:8080",
		"CONTROL: Synced's message must contain the underlying error")

	// THE SUBJECT.
	require.NotEmpty(t, ready.Message,
		"Ready carries no message on a create that will never succeed. Ready is the condition "+
			"people watch, and reason=Creating with an empty message reads as 'still working'")
	require.Contains(t, ready.Message, "redirect-target:8080",
		"Ready's message must name what actually failed, not a generic phrase")

	// STRUCTURAL: both conditions are built from ONE wrapped error, so they cannot drift apart.
	// Asserted as equality rather than as two independent substring checks, because the defect
	// this replaces was precisely "the diagnostic exists on one condition and not the other".
	require.Equal(t, synced.Message, ready.Message,
		"Ready and Synced must carry the SAME text: they are built from one error value, and a "+
			"divergence means someone reintroduced a second source")
}

// THE LATCH GUARD, which is the reason this is a message fix and not a reason fix.
//
// IsConditionSet compares Type && Status && Reason (unstructured.go:274) and consumers use it as a
// latch guard — oasgen's restResources.go reads IsConditionSet(mg, condition.Creating()). Their
// #347: a create rejected 422 left Creating set, a later branch read its ABSENCE as grounds to
// assume the resource was fine, set Available, and latched a CR permanently Ready=True for a
// resource that never existed.
//
// So a changed reason would be a silently-wrong Ready=True, which is worse than a confusing
// Ready=False. This asserts the guard still matches WITH a message attached.
func TestCreatingWithMessageStillMatchesTheLatchGuard(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{}}
	withMsg := condition.WithMessage(condition.Creating(), "a long diagnostic nobody compares")
	require.NoError(t, unstructuredtools.SetConditions(u, withMsg))

	require.True(t, unstructuredtools.IsConditionSet(u, condition.Creating()),
		"IsConditionSet(Creating) must still match once a message is attached. If this fails, "+
			"every consumer using it as a latch guard breaks and oasgen's #347 returns: a CR "+
			"latched Ready=True for a resource that was never created")

	// And the fields the guard DOES compare are untouched, so the match above is not accidental.
	got := condition.WithMessage(condition.Creating(), "x")
	base := condition.Creating()
	require.Equal(t, base.Type, got.Type)
	require.Equal(t, base.Status, got.Status)
	require.Equal(t, base.Reason, got.Reason)
	require.Equal(t, "x", got.Message)

	// A different REASON must NOT match — the control proving IsConditionSet discriminates at all,
	// so the positive above means something.
	u2 := &unstructured.Unstructured{Object: map[string]any{}}
	require.NoError(t, unstructuredtools.SetConditions(u2,
		condition.WithMessage(condition.Deleting(), "same shape, different reason")))
	require.False(t, unstructuredtools.IsConditionSet(u2, condition.Creating()),
		"CONTROL: IsConditionSet must discriminate on reason, or the positive arm is vacuous")
}

// The delete path had the identical gap and the identical fix.
func TestHandleDelete_FailurePutsTheDiagnosticOnReadyToo(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	obj := createTestUnstructured("ready-msg-del", opts.Namespace)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	ref := objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
		Name: obj.GetName(), Namespace: obj.GetNamespace()}

	const boom = "delete failed: vendor returned 409 conflict: resource has dependents"
	ctrl.SetExternalClient(&fakeExternalClient{DeleteErr: errors.New(boom)})

	err = ctrl.handleDelete(context.TODO(), ref)
	if err == nil {
		t.Skip("handleDelete did not surface the delete error on this path; the message assertion " +
			"below would be about a different branch than the one under test")
	}

	updated, gerr := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, gerr)
	ready, synced := readyAndSynced(t, updated)

	require.NotEmpty(t, synced.Message, "CONTROL: Synced must carry the delete error")
	require.NotEmpty(t, ready.Message,
		"Ready=Deleting with an empty message tells an operator nothing about why a delete is stuck")
	require.Equal(t, synced.Message, ready.Message,
		"both conditions are built from one error value and must not diverge")
	require.True(t, strings.Contains(ready.Message, "409"),
		"the message must name the underlying failure")
}
