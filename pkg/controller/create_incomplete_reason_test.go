package controller

// When we cannot determine the result of an interrupted create, the CONDITION must say why.
//
// Found by the oasgen-provider session, read in source: obsErr was assigned at worker.go and then
// referenced exactly once, in the `!= nil` test. A grep returned two lines — the assignment and the
// test — so it reached no log line, no event and no condition. Every reason an existence check can
// fail collapsed into one fixed sentence.
//
// WHY THAT IS WORSE THAN A MISSING %w, in their words and worth preserving: rest-dynamic-controller's
// Exists distinguishes EIGHT reasons it cannot answer — observation delegated to a RESTAction, an
// async operation in flight, a Pending condition, a kind declaring no read verb, unresolved path
// parameters, a synthesised 404 (a list that succeeded and matched nothing), a transport/5xx
// failure, and a client-build failure. Each implies a different operator action. All eight arrived
// as errCreateIncomplete, whose own remediation is "remove the annotation if it is safe to proceed"
// — while the reason that would let anyone judge "if it is safe" was the thing being discarded.
//
// It got concrete: RDC 0.49.0 correctly changed a findby no-match from "absent" to "cannot
// determine" (a successful list matching nothing is not proof of absence, and treating it as
// absence duplicated a live resource). For a kind addressed ONLY by findby, an interrupted create
// now sits in this branch indefinitely and the operator is told a wedge exists and nothing about
// why.
//
// This test is their falsifier inverted. They wrote: "if its condition message carries anything
// about why, I have read the wrong version." It did not. Now it must.

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
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreateIncomplete_ConditionCarriesTheReason(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()

	ctrl, err := New(sid, opts)
	require.NoError(t, err)

	// A reason worded like one of RDC's eight, so the assertion is about a REASON reaching the
	// condition rather than about any error text at all. fakeExternalClient has no Exists method,
	// so there is no ExistenceChecker fallback and externalResourceExists returns this verbatim.
	const reason = "findby matched nothing and the kind declares no get verb"
	ctrl.externalClient = &fakeExternalClient{ObserveErr: errors.New(reason)}

	obj := createTestUnstructured("incomplete-reason", opts.Namespace)
	meta.SetExternalCreatePending(obj, time.Now().Add(-24*time.Hour)) // past the grace period
	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	// PREMISE: the recovery block must actually be entered, or this test asserts nothing. This is
	// the arm that matters most here — a processItem that returned before reaching the branch would
	// leave no conditions at all, and "no condition mentions the reason" would then be trivially
	// true for the wrong cause.
	require.True(t, meta.ExternalCreateIncomplete(created),
		"premise: the object must look like an incomplete external create")

	err = ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Observe,
		ObjectRef: objectref.ObjectRef{
			APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
			Name: created.GetName(), Namespace: created.GetNamespace(),
		},
	})
	// IT RETURNS nil, AND I EXPECTED AN ERROR. Worth recording rather than quietly matching.
	//
	// The branch writes condition.ReconcileError(...) and then `return nil` — so the CR says
	// "reconcile error" while the QUEUE is told the item succeeded. Nothing requeues it; recovery
	// waits for the next resync. That is a second reason the discarded cause mattered: for a kind
	// addressed only by findby, the object sits in this branch with no prompt retry AND, until this
	// change, no stated reason.
	//
	// Not changing it here. Turning this into a returned error would requeue with backoff, which is
	// probably right, but it changes retry behaviour for every caller and belongs in its own change
	// with its own argument. This test pins the CURRENT contract so that change is visible when
	// someone makes it.
	require.NoError(t, err,
		"the branch records the refusal on the condition and returns nil; if this now returns an "+
			"error, the retry contract changed and that is a deliberate decision to document")

	got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), created.GetName(), metav1.GetOptions{})
	require.NoError(t, err)

	conds := unstructuredtools.GetConditions(got)
	require.NotEmpty(t, conds,
		"no conditions were written at all — the assertion below would pass over a branch that "+
			"never ran, so this is a calibration and not a nicety")

	var messages []string
	for _, c := range conds {
		messages = append(messages, c.Message)
	}
	joined := strings.Join(messages, " | ")

	// The condition is the channel that matters: charts and gates read a CR condition, not our logs.
	require.Contains(t, joined, reason,
		"no condition message carries the underlying reason. Eight distinct causes collapse into "+
			"one sentence whose own remediation is \"remove the annotation if it is safe\" — and "+
			"the operator cannot judge \"if it is safe\" without the reason. Conditions were: "+joined)

	// AND the existing remediation advice must survive, not be replaced by the cause. The wrapping
	// is "%s: %w", so both are present; asserting only the cause would let a change that DROPPED
	// the advice pass.
	require.Contains(t, joined, "remove the",
		"the remediation advice was lost: the message must carry BOTH what to do and why, "+
			"conditions were: "+joined)
}
