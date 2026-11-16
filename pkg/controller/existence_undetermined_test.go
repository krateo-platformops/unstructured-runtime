package controller

// The marked half of the incomplete-create branch, and its boundary with the unmarked half.
//
// THE EXISTING TEST IS THE SIGNPOST FOR THIS CHANGE. create_incomplete_reason_test.go records, in its
// own words: "Turning this into a returned error would requeue with backoff, which is probably right,
// but it changes retry behaviour for every caller and belongs in its own change with its own argument.
// This test pins the CURRENT contract so that change is visible when someone makes it."
//
// This is that change, and it is deliberately NOT for every caller: the error is returned only for a
// refusal the handler has MARKED transient. That test's `require.NoError` therefore still holds
// unmodified, which is the strongest available evidence that the default did not move.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestExistenceUndetermined_WrapIsTransparent: the marker must not hide the cause from anyone.
func TestExistenceUndetermined_WrapIsTransparent(t *testing.T) {
	sentinel := errors.New("an async operation is still in flight")
	marked := ExistenceUndetermined(sentinel)

	require.True(t, IsExistenceUndetermined(marked), "the marker must be detectable")
	require.True(t, errors.Is(marked, sentinel), "errors.Is must still reach the cause")
	require.Equal(t, sentinel.Error(), marked.Error(),
		"Error() must be unchanged: a handler that marks its refusal must not thereby change what "+
			"every log line and condition message says about it")

	// Through another layer of wrapping, which is how it will actually arrive -- the handler wraps
	// with %w on the way out and the branch wraps again with errCreateIncomplete.
	nested := ExistenceUndetermined(errors.Join(sentinel, errors.New("and a second cause")))
	require.True(t, IsExistenceUndetermined(nested))
	require.True(t, errors.Is(nested, sentinel), "errors.Is must reach through a joined cause")

	// nil in, nil out: a handler returning no error must not accidentally mark success.
	require.NoError(t, ExistenceUndetermined(nil))
	require.False(t, IsExistenceUndetermined(nil))
	// And an UNMARKED error must not be detected, or the default silently becomes transient.
	require.False(t, IsExistenceUndetermined(sentinel),
		"an unmarked error must NOT read as transient -- the default has to stay structural, or a "+
			"kind that genuinely cannot be observed requeues forever with nothing explaining why")
}

// TestExistenceUndetermined_MarkedRefusalIsCreatingAndRequeues is the behavioural core: Creating, not
// CreateOutcomeUnknown; ReconcileError naming the cause; an error RETURNED; and the create-pending
// annotation untouched.
func TestExistenceUndetermined_MarkedRefusalIsCreatingAndRequeues(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	const cause = "async create operation 9f3c is still in flight"
	ctrl.externalClient = &fakeExternalClient{ObserveErr: ExistenceUndetermined(errors.New(cause))}

	obj := createTestUnstructured("undetermined", opts.Namespace)
	pendingAt := time.Now().Add(-24 * time.Hour) // past the grace period, as the sibling test does
	meta.SetExternalCreatePending(obj, pendingAt)
	created, cerr := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, cerr)

	// PREMISE, or the whole test asserts nothing: the recovery block must be entered.
	require.True(t, meta.ExternalCreateIncomplete(created),
		"premise: the object must look like an incomplete external create")

	perr := ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	})

	// THE RETRY CONTRACT, and the one thing that differs from the unmarked path.
	require.Error(t, perr,
		"a marked refusal must RETURN the error so the queue backs off and re-checks; the unmarked "+
			"path returns nil and waits for the resync, and create_incomplete_reason_test.go pins that")
	require.True(t, errors.Is(perr, ctrl.externalClient.(*fakeExternalClient).ObserveErr) ||
		strings.Contains(perr.Error(), cause),
		"the returned error must still carry the cause, got %q", perr.Error())

	got, gerr := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, gerr)

	conds := unstructuredtools.GetConditions(got)
	require.NotEmpty(t, conds, "no conditions written at all -- the assertions below would pass over a "+
		"branch that never ran, so this is a calibration and not a nicety")

	var ready, synced *metav1.Condition
	for i := range conds {
		switch conds[i].Type {
		case condition.TypeReady:
			ready = &conds[i]
		case condition.TypeSynced:
			synced = &conds[i]
		}
	}

	require.NotNil(t, ready, "Ready must be written")
	require.Equal(t, string(condition.ReasonCreating), ready.Reason,
		"Ready must use the EXISTING non-sticky Creating reason. Not CreateOutcomeUnknown, which names "+
			"a state a human must act on and which the cdc rolls up as FAILED. Not ObserveFailed, which "+
			"is Ready=Unknown and which the cdc reads as unevaluated and rolls up HEALTHY -- a parent "+
			"could go Ready over a create nobody has confirmed.")
	require.Equal(t, metav1.ConditionFalse, ready.Status,
		"Creating must be False here: a create is pending and unconfirmed")

	require.NotNil(t, synced, "Synced must be written: the operator has to see the backend refusal, "+
		"which is not an internal race like statusConflict")
	require.Contains(t, synced.Message, cause,
		"Synced's message must NAME the cause. A marked refusal that never clears requeues forever "+
			"with backoff, and this message is the only thing telling an operator why.")

	// THE ANNOTATION IS UNTOUCHED. Clearing it would hand the resource to a reconciler that
	// re-creates -- the duplicate this branch exists to prevent.
	require.True(t, meta.ExternalCreateIncomplete(got),
		"the create-pending marker must survive: the create is still unresolved")
	require.Equal(t, pendingAt.Unix(), meta.GetExternalCreatePending(got).Unix(),
		"the pending TIMESTAMP must be unchanged too -- refreshing it would reset the grace period on "+
			"every pass and a resource could never age out of it")
}

// TestExistenceUndetermined_UnmarkedIsUnchanged pins the boundary from this side as well. The sibling
// test already covers the unmarked path; this one asserts the two paths DIVERGE, so a change that
// collapsed them would fail here even if someone edited the sibling.
func TestExistenceUndetermined_UnmarkedIsUnchanged(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	const cause = "the kind declares no findby verb, so existence cannot be read"
	ctrl.externalClient = &fakeExternalClient{ObserveErr: errors.New(cause)} // NOT marked

	obj := createTestUnstructured("unmarked", opts.Namespace)
	meta.SetExternalCreatePending(obj, time.Now().Add(-24*time.Hour))
	created, cerr := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, cerr)
	require.True(t, meta.ExternalCreateIncomplete(created), "premise")

	perr := ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	})
	require.NoError(t, perr,
		"an UNMARKED refusal must keep returning nil. If this starts returning an error the default "+
			"moved, and every handler's retry behaviour changed with it.")

	got, gerr := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, gerr)
	for _, c := range unstructuredtools.GetConditions(got) {
		if c.Type == condition.TypeReady {
			require.Equal(t, string(condition.ReasonCreateOutcomeUnknown), c.Reason,
				"an unmarked refusal must still get CreateOutcomeUnknown")
		}
	}
}
