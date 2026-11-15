package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// selectorProbeClient records every handler call the runtime makes.
type selectorProbeClient struct {
	fakeExternalClient
	calls []string
}

func (c *selectorProbeClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	c.calls = append(c.calls, "Observe")
	return ExternalObservation{ResourceExists: true, ResourceUpToDate: false}, nil
}

func (c *selectorProbeClient) Update(ctx context.Context, mg *unstructured.Unstructured) error {
	c.calls = append(c.calls, "Update")
	return nil
}

func (c *selectorProbeClient) Delete(ctx context.Context, mg *unstructured.Unstructured) error {
	c.calls = append(c.calls, "Delete")
	return nil
}

// core-provider#533. On a composition version bump, the retiring controller (selector
// krateo.io/composition-version=v0-3-64) rendered its OLD chart with the composition's NEW values:
// a write at the new apiVersion had already re-labelled the object v0-3-65, so the informer had
// stopped delivering it, but an event queued earlier reached it through the worker's LIVE Get. The
// live read must honour the informer's selector, for every event type.
func TestAnObjectThatLeftTheSelectorIsNotActedOn(t *testing.T) {
	const key = "krateo.io/composition-version"
	for _, et := range []ctrlevent.EventType{ctrlevent.Observe, ctrlevent.Update, ctrlevent.Delete} {
		for _, tc := range []struct {
			label    string
			wantActs bool
		}{
			{"v0-3-65", false}, // left: another controller's object now
			{"v0-3-64", true},  // control: still this controller's, so the instrument does fire
		} {
			t.Run(fmt.Sprintf("%s label=%s", et, tc.label), func(t *testing.T) {
				sid, err := shortid.New(1, shortid.DefaultABC, 2342)
				require.NoError(t, err)
				opts := createTestOptions()
				sel := key + "=v0-3-64"
				opts.ListWatcher = ListWatcherConfiguration{LabelSelector: &sel}
				ctrl, err := New(sid, opts)
				require.NoError(t, err)
				client := &selectorProbeClient{}
				ctrl.externalClient = client

				obj := createTestUnstructured("left-"+tc.label, opts.Namespace)
				obj.SetLabels(map[string]string{key: tc.label})
				if et == ctrlevent.Delete {
					now := metav1.Now()
					obj.SetDeletionTimestamp(&now)
				}
				created, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).
					Create(context.TODO(), obj, metav1.CreateOptions{})
				require.NoError(t, err)

				err = ctrl.processItem(context.TODO(), ctrlevent.Event{
					EventType: et,
					ObjectRef: objectref.ObjectRef{
						APIVersion: created.GetAPIVersion(), Kind: created.GetKind(),
						Name: created.GetName(), Namespace: created.GetNamespace(),
					},
				})
				if !tc.wantActs {
					require.NoError(t, err, "an object that left the selector is dropped, not failed")
					require.Empty(t, client.calls, "the handler was called on an object outside this controller's selector")
					return
				}
				require.NotEmpty(t, client.calls, "control: the handler must be called for an object still inside the selector")
			})
		}
	}
}

// A re-read in the middle of a pass can find the object gone from the selector too. handleErr drops
// that error instead of requeueing it: a retry could only fetch the object again and drop it then.
func TestLeftSelectorErrorIsNotRequeued(t *testing.T) {
	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	require.NoError(t, err)
	opts := createTestOptions()
	// A retry budget, or handleErr drops EVERY error at its maxRetries check and the "not requeued"
	// assertion passes for any error at all (it did, before the control below caught it).
	opts.MaxRetries = 5
	ctrl, err := New(sid, opts)
	require.NoError(t, err)
	evt := func(name string) ctrlevent.Event {
		return ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: objectref.ObjectRef{
			APIVersion: "test.example.org/v1", Kind: "Test", Name: name, Namespace: "test-namespace"}}
	}
	left, other := evt("left"), evt("other")
	ctrl.handleErr(fmt.Errorf("observe: %w", errLeftSelector), left, 0)
	ctrl.handleErr(fmt.Errorf("observe: transient"), other, 0)
	_, leftQueued := ctrl.items.Load(ctrlevent.DigestForEvent(left))
	_, otherQueued := ctrl.items.Load(ctrlevent.DigestForEvent(other))
	require.False(t, leftQueued, "a left-selector error was requeued")
	require.True(t, otherQueued, "control: an ordinary error must be requeued, or this test proves nothing")
}
