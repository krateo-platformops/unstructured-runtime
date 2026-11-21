package controller

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// #122. A successful delete released the object by clearing EVERY finalizer (SetFinalizers([]string{})),
// so another owner's finalizer, or Kubernetes' foregroundDeletion, was stripped and the object deleted
// while that owner still held it (measured live on GKE, 2026-11-21). Only OUR finalizer may go.
// The fake dynamic client does not reap, so the object stays readable after ours is removed.
func TestHandleDelete_RemovesOnlyItsOwnFinalizer(t *testing.T) {
	for _, foreign := range []string{"test.krateo.io/hold", "foregroundDeletion"} {
		t.Run(foreign, func(t *testing.T) {
			sid, err := shortid.New(1, shortid.DefaultABC, 2342)
			require.NoError(t, err)
			opts := createTestOptions()
			opts.Recorder, opts.ThrottledRecorder = &mockEventRecorder{}, &mockEventRecorder{}
			cli := &observingClient{obs: ExternalObservation{ResourceExists: false}}
			ctrl, err := New(sid, opts)
			require.NoError(t, err)
			ctrl.externalClient = cli

			obj := createTestUnstructured("fin-"+map[string]string{"test.krateo.io/hold": "hold", "foregroundDeletion": "fg"}[foreign], opts.Namespace)
			obj.SetFinalizers([]string{finalizerName, foreign})
			res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
			created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
			require.NoError(t, err)
			now := metav1.Now()
			created.SetDeletionTimestamp(&now)
			created, err = res.Update(context.TODO(), created, metav1.UpdateOptions{})
			require.NoError(t, err)
			ref := objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}

			require.NoError(t, ctrl.processItem(context.TODO(), ctrlevent.Event{EventType: ctrlevent.Delete, ObjectRef: ref}))

			cur, err := res.Get(context.TODO(), created.GetName(), metav1.GetOptions{})
			require.NoError(t, err)
			// PREMISE: the release path really ran (Delete called, our finalizer gone); without it the
			// assertion below would pass on an object nothing touched.
			require.Equal(t, 1, cli.deleted, "premise: the handler's Delete must have run")
			require.NotContains(t, cur.GetFinalizers(), finalizerName, "premise: our finalizer must be released")
			require.Equal(t, []string{foreign}, cur.GetFinalizers(), "another owner's finalizer must survive our release")
		})
	}
}
