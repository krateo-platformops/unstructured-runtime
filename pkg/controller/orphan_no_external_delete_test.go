package controller

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// #23. The orphan branch removed the finalizer and fell through to the dispatch, which switches on
// evt.EventType — and a PENDING delete (deletionTimestamp set, object still present) is enqueued as
// EventType Delete by controller.go's UpdateFunc. So the orphan pass reached handleDelete, whose
// fetch is a live apiserver Get, and then called externalClient.Delete: for a composition, a
// `helm uninstall` of exactly what the annotation was set to preserve.
//
// TestProcessItem_DeleteOrphan already existed and did not catch this, because it drives the event
// as EventType **Observe** and asserts only that the finalizer was removed. The fall-through from an
// Observe event lands in handleObserve, never handleDelete, so the branch carrying the bug was
// never executed and the external client was never checked. The test's name described the scenario;
// its event type described a different one.
//
// These two therefore assert what the old test could not: that NO external delete is dispatched.

// recordingExternalClient counts dispatches. The shared fakeExternalClient only returns canned
// errors, so it cannot answer "was Delete called?" — which is the whole question here.
type recordingExternalClient struct {
	observed int
	created  int
	updated  int
	deleted  int
}

func (r *recordingExternalClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	r.observed++
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}
func (r *recordingExternalClient) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	r.created++
	return nil
}
func (r *recordingExternalClient) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	r.updated++
	return nil
}
func (r *recordingExternalClient) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	r.deleted++
	return nil
}

// orphanedObject seeds an object that is mid-deletion, orphan-policied, and still present —
// the state the worker actually sees when a pending delete is dequeued.
func orphanedObject(t *testing.T, opts Options, name string, finalizers []string) *unstructured.Unstructured {
	t.Helper()
	obj := createTestUnstructured(name, opts.Namespace)
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[meta.AnnotationKeyDeletionPolicy] = string(meta.DeletionPolicyOrphan)
	obj.SetAnnotations(ann)

	created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Create(context.TODO(), obj, metav1.CreateOptions{})
	require.NoError(t, err)

	now := metav1.Now()
	created.SetDeletionTimestamp(&now)
	created.SetFinalizers(finalizers)
	_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Update(context.TODO(), created, metav1.UpdateOptions{})
	require.NoError(t, err)

	// PREMISE CHECK. The branch under test is `WasDeleted && !ShouldDelete`; assert the second
	// half with the runtime's own predicate rather than trusting that the annotation key and value
	// written above are spelled the way meta reads them. Otherwise a typo turns this into a test
	// that passes because the branch was never entered.
	cur, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), name, metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, meta.WasDeleted(cur), "premise: object must carry a deletionTimestamp")
	require.False(t, meta.ShouldDelete(cur),
		"premise: ShouldDelete must be false, or the orphan branch is never entered and this test "+
			"asserts nothing")
	return cur
}

// The case that fails without the early return. A second finalizer keeps the object alive after
// ours is removed, so handleDelete's fetch succeeds and the external resource is destroyed.
//
// The fake dynamic client does not implement finalizer-based reaping — it keeps the object after
// our removal — which makes it a faithful model of precisely this arm, and the reason this test
// reproduces the defect deterministically rather than racily.
func TestProcessItem_OrphanWithSecondFinalizer_DoesNotDeleteExternalResource(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	rec := &recordingExternalClient{}
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = rec

	obj := orphanedObject(t, opts, "orphan-two-finalizers", []string{
		finalizerName,
		"other.example.com/holds-the-object",
	})

	// EventType Delete: what controller.go's UpdateFunc enqueues for a pending delete.
	err = ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Delete,
		ObjectRef: objectref.ObjectRef{
			APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
			Name: obj.GetName(), Namespace: obj.GetNamespace(),
		},
	})
	require.NoError(t, err)

	require.Zero(t, rec.deleted,
		"orphan policy was honoured and the external resource was deleted anyway: the orphan "+
			"branch fell through to handleDelete, which found the object still held by the second "+
			"finalizer and dispatched the delete (#23)")

	cur, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
		Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, cur.GetFinalizers(), finalizerName,
		"our finalizer must still be released — the fix must not make orphan stop working")
	require.Contains(t, cur.GetFinalizers(), "other.example.com/holds-the-object",
		"the other owner's finalizer must be left alone")
}

// The sole-finalizer case. Kept deliberately, but note what it does and does not prove.
//
// On a REAL apiserver this arm passes even with the bug present: removing the last finalizer
// completes the deletion inside that same update request, so the fall-through fetch 404s and no
// delete is dispatched. That is why the defect was invisible for all 83 compositions in the fleet,
// and why a regression suite built only on this shape would have stayed green.
//
// Under the FAKE dynamic client it fails without the fix, because the fake does not implement
// finalizer-based reaping and keeps the object after our removal — the same reason the arm above
// reproduces the defect deterministically. So in THIS harness both arms are protective, while on a
// live cluster only the second-finalizer arm is. Verified both ways rather than assumed.
func TestProcessItem_OrphanWithSoleFinalizer_DoesNotDeleteExternalResource(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	rec := &recordingExternalClient{}
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	ctrl.externalClient = rec

	obj := orphanedObject(t, opts, "orphan-one-finalizer", []string{finalizerName})

	err = ctrl.processItem(context.TODO(), ctrlevent.Event{
		EventType: ctrlevent.Delete,
		ObjectRef: objectref.ObjectRef{
			APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(),
			Name: obj.GetName(), Namespace: obj.GetNamespace(),
		},
	})
	require.NoError(t, err)

	require.Zero(t, rec.deleted, "an orphaned resource must never be deleted externally")
	require.Zero(t, rec.observed,
		"the orphan branch must return before any external call: observing a resource the policy "+
			"says to leave alone is wasted work and, for helm, not free")
}
