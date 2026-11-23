package controller

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// existenceClient counts every handler call and answers Observe with a fixed result.
type existenceClient struct {
	recordingExternalClient
	err    error
	exists bool
}

func (e *existenceClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	e.observed++
	if e.err != nil {
		return ExternalObservation{}, e.err
	}
	return ExternalObservation{ResourceExists: e.exists, ResourceUpToDate: true}, nil
}

const unanswerableCause = "the read needs status.id, which was never recorded, and the kind declares no findby"

func TestExistenceUnanswerable_WrapIsTransparent(t *testing.T) {
	cause := errors.New(unanswerableCause)
	marked := ExistenceUnanswerable(cause)
	require.True(t, IsExistenceUnanswerable(marked))
	require.True(t, errors.Is(marked, ErrExistenceUnanswerable))
	require.True(t, errors.Is(marked, cause), "the handler's own error must stay reachable")
	require.Equal(t, cause.Error(), marked.Error(), "declaring it permanent must not change the message")
	require.False(t, IsExistenceUndetermined(marked), "permanent is not transient")
	require.NoError(t, ExistenceUnanswerable(nil))
	// A handler may call the predicate unconditionally, including on the nil error of a definite
	// answer (oasgen-provider wires it that way): nil is never unanswerable.
	require.False(t, IsExistenceUnanswerable(nil))
	// CONTROLS: an unmarked error and a transient one are NOT unanswerable.
	require.False(t, IsExistenceUnanswerable(cause))
	require.False(t, IsExistenceUnanswerable(ExistenceUndetermined(cause)))
}

// pass runs one processItem for an object with an incomplete external create, optionally being deleted.
func unanswerablePass(t *testing.T, name string, deleting bool, client *existenceClient) (perr error, got *unstructured.Unstructured, rec *mockEventRecorder) {
	t.Helper()
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	rec = &mockEventRecorder{}
	opts.Recorder, opts.ThrottledRecorder = rec, rec
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = client

	obj := createTestUnstructured(name, opts.Namespace)
	meta.SetExternalCreatePending(obj, time.Now().Add(-24*time.Hour)) // past the recovery grace
	// A second finalizer keeps a deleting object readable after ours is removed: the fake client does
	// not reap, which is what lets "our finalizer was released" be asserted at all.
	obj.SetFinalizers([]string{finalizerName, "test.example.org/hold"})
	res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
	created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	evType := ctrlevent.Observe
	if deleting {
		now := metav1.Now()
		created.SetDeletionTimestamp(&now)
		created, err = res.Update(context.TODO(), created, metav1.UpdateOptions{})
		require.NoError(t, err)
		evType = ctrlevent.Delete
	}
	// PREMISES, or each arm could pass by never reaching the gate.
	require.True(t, meta.ExternalCreateIncomplete(created), "premise: an incomplete external create")
	require.Equal(t, deleting, meta.WasDeleted(created), "premise: deleting matches the arm")
	require.True(t, meta.ManagementDecision(created).May(managementpolicy.Delete),
		"premise: delete is allowed, so the orphan release above the gate does not handle this object")

	perr = ctrl.processItem(context.TODO(), ctrlevent.Event{EventType: evType, ObjectRef: objectref.ObjectRef{
		APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace(),
	}})
	got, err = res.Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	return perr, got, rec
}

func unansReady(t *testing.T, u *unstructured.Unstructured) metav1.Condition {
	t.Helper()
	for _, c := range unstructuredtools.GetConditions(u) {
		if c.Type == condition.TypeReady {
			return c
		}
	}
	t.Fatal("no Ready condition written")
	return metav1.Condition{}
}

func unansHasEvent(rec *mockEventRecorder, reason string) bool {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, e := range rec.events {
		if string(e.Reason) == reason {
			return true
		}
	}
	return false
}

// #119, not deleting: stay stopped (no create, marker kept: no duplicate possible), and Ready says the
// outcome can never be determined and names the two ways out.
func TestExistenceUnanswerable_NotDeleting_StaysStoppedAndSaysHow(t *testing.T) {
	client := &existenceClient{err: ExistenceUnanswerable(errors.New(unanswerableCause))}
	perr, got, _ := unanswerablePass(t, "unans-live", false, client)
	require.NoError(t, perr, "it waits for the resync, as an unanswered check does")
	require.Zero(t, client.created, "no create may be retried: that is the duplicate this keeps from happening")
	require.False(t, meta.GetExternalCreatePending(got).IsZero(), "the create-pending marker must be kept")
	ready := unansReady(t, got)
	require.Equal(t, string(condition.ReasonCreateOutcomeUnknown), ready.Reason)
	require.Contains(t, ready.Message, "never", "it must say the check can never answer")
	require.Contains(t, ready.Message, unanswerableCause, "it must carry the handler's cause")
	require.Contains(t, ready.Message, "remove the "+meta.AnnotationKeyExternalCreatePending, "way out 1")
	require.Contains(t, ready.Message, "delete this object", "way out 2")
	require.NotContains(t, ready.Message, "clearing by itself", "that would be false for a check that can never answer")
}

// CONTROLS for the not-deleting arm: an unmarked error keeps errCreateIncomplete; a transient one keeps
// Creating and is returned. Neither creates.
func TestExistenceUnanswerable_NotDeleting_ControlsUnchanged(t *testing.T) {
	unmarked := &existenceClient{err: errors.New("a 503 from the vendor")}
	perr, got, _ := unanswerablePass(t, "unans-ctl-unmarked", false, unmarked)
	require.NoError(t, perr)
	require.Zero(t, unmarked.created)
	r := unansReady(t, got)
	require.Equal(t, string(condition.ReasonCreateOutcomeUnknown), r.Reason)
	require.Contains(t, r.Message, "clearing by itself", "the unmarked path keeps errCreateIncomplete")
	// The remediation must warn that removing the annotation enqueues a create at once: the runtime itself
	// CLOSES the marker rather than deleting it for that reason (createNotRequested, oasgen-provider#674),
	// so advising the deletion without the warning tells an operator to do what the runtime refuses to.
	require.Contains(t, r.Message, "enqueues a create immediately", "the remediation names the side effect of removing the annotation")
	require.Contains(t, r.Message, "does NOT exist", "the remediation says to remove it only once the resource is verified absent")

	transient := &existenceClient{err: ExistenceUndetermined(errors.New("async operation still in flight"))}
	perr, got, _ = unanswerablePass(t, "unans-ctl-transient", false, transient)
	require.Error(t, perr, "a transient refusal is returned so the queue backs off")
	require.Zero(t, transient.created)
	require.Equal(t, string(condition.ReasonCreating), unansReady(t, got).Reason)
}

// #119, deleting: our finalizer is released WITHOUT the handler's Delete, and a Warning event says the
// external resource is kept.
func TestExistenceUnanswerable_Deleting_ReleasesWithoutDelete(t *testing.T) {
	client := &existenceClient{err: ExistenceUnanswerable(errors.New(unanswerableCause))}
	perr, got, rec := unanswerablePass(t, "unans-del", true, client)
	require.NoError(t, perr)
	require.False(t, slices.Contains(got.GetFinalizers(), finalizerName), "our finalizer must be released")
	require.Contains(t, got.GetFinalizers(), "test.example.org/hold", "only ours")
	require.Zero(t, client.deleted, "the handler's Delete must NOT be called: the resource cannot be addressed and is kept")
	require.Zero(t, client.created)
	require.True(t, unansHasEvent(rec, string(reasonKeptExistenceUnanswerable)), "a Warning event must say the resource was kept")
}

// CONTROLS for the deleting arm. A transient or unmarked refusal still WAITS (finalizer kept, no
// Delete). An answered check proceeds to the handler's Delete, so the zero above is not an instrument
// that can never see a Delete.
func TestExistenceUnanswerable_Deleting_Controls(t *testing.T) {
	for name, c := range map[string]*existenceClient{
		"unmarked":  {err: errors.New("a 503 from the vendor")},
		"transient": {err: ExistenceUndetermined(errors.New("async operation still in flight"))},
	} {
		t.Run("waits: "+name, func(t *testing.T) {
			_, got, rec := unanswerablePass(t, "unans-delctl-"+name, true, c)
			require.Contains(t, got.GetFinalizers(), finalizerName, "a check that may answer later must keep the finalizer")
			require.Zero(t, c.deleted)
			require.False(t, unansHasEvent(rec, string(reasonKeptExistenceUnanswerable)))
		})
	}
	for name, c := range map[string]*existenceClient{
		"absent":  {exists: false},
		"present": {exists: true},
	} {
		t.Run("answered, Delete runs: "+name, func(t *testing.T) {
			_, _, rec := unanswerablePass(t, "unans-delans-"+name, true, c)
			require.Equal(t, 1, c.deleted, "an answered check reaches the handler's Delete, as before")
			require.False(t, unansHasEvent(rec, string(reasonKeptExistenceUnanswerable)))
		})
	}
}
