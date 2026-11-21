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
)

// #110 D1 made "update but not create" reachable (a verb set such as "observe,update"). An Update
// event from the informer (CRUpdated maps to Update) used to reach the handler even when the external
// resource was absent, where an upsert PUT creates it: the one write that policy forbids.
// provider-runtime found the same shape (provider-runtime#115). Each case is one informer Update
// event plus every follow-up it queues, as the worker would run them.
func TestManagementPolicy_UpdateEventUnderCreateWithheld(t *testing.T) {
	for _, tc := range []struct {
		name         string
		policy       string
		obs          ExternalObservation
		wantObserved int
		wantCreated  int
		wantUpdated  int
	}{
		{"observe,update + absent: observed, nothing written", "observe,update", ExternalObservation{ResourceExists: false}, 1, 0, 0},
		{"observe,update,delete + absent: observed, nothing written", "observe,update,delete", ExternalObservation{ResourceExists: false}, 1, 0, 0},
		{"observe,update + drifted: observed first, then updated (no loop)", "observe,update", ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, 1, 0, 1},
		{"observe,update + up to date: observed, nothing written", "observe,update", ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, 1, 0, 0},
		// CONTROLS: create allowed, so the informer Update still goes straight to the handler, as before.
		{"control: default + absent: Update dispatched directly", "", ExternalObservation{ResourceExists: false}, 0, 0, 1},
		{"control: observe-create-update + drifted: Update dispatched directly", "observe-create-update", ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sid, err := shortid.New(1, shortid.DefaultABC, 2342)
			require.NoError(t, err)
			opts := createTestOptions()
			opts.Recorder, opts.ThrottledRecorder = &mockEventRecorder{}, &mockEventRecorder{}
			cli := &observingClient{obs: tc.obs}
			ctrl, err := New(sid, opts)
			require.NoError(t, err)
			ctrl.externalClient = cli

			obj := createTestUnstructured("upd-"+shortName(tc.name), opts.Namespace)
			if tc.policy != "" {
				obj.SetAnnotations(map[string]string{meta.AnnotationKeyManagementPolicy: tc.policy})
			}
			obj.SetFinalizers([]string{finalizerName})
			created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
			require.NoError(t, err)
			ref := objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}

			pending := []ctrlevent.Event{{EventType: ctrlevent.Update, ObjectRef: ref}} // the informer's: no ObservedExisting
			for hop := 0; len(pending) > 0 && hop < 4; hop++ {
				ev := pending[0]
				pending = pending[1:]
				_ = ctrl.processItem(context.TODO(), ev)
				// The WORKER's own queued follow-ups, taken from its queue as they are, never rebuilt
				// here: a rebuilt event would carry whatever ObservedExisting this test chose, and the
				// no-loop arm would pass whether or not handleObserve marks its Update.
				for ctrl.queue.Len() > 0 {
					item, shutdown := ctrl.queue.Get()
					require.False(t, shutdown)
					ctrl.queue.Done(item)
					next, ok := item.(ctrlevent.Event)
					require.True(t, ok, "queued item %T is not an event", item)
					ctrl.items.Delete(ctrlevent.DigestForEvent(next))
					pending = append(pending, next)
				}
			}
			require.Empty(t, pending, "follow-ups still queued after 4 hops: a loop")
			require.Equal(t, tc.wantObserved, cli.observed, "observed")
			require.Equal(t, tc.wantCreated, cli.created, "created")
			require.Equal(t, tc.wantUpdated, cli.updated, "updated")
		})
	}
}

func shortName(s string) string {
	out := make([]byte, 0, 20)
	for i := 0; i < len(s) && len(out) < 20; i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			out = append(out, c)
		}
	}
	return string(out)
}
