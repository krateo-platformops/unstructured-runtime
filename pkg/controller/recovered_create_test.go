package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// countingCreates is fakeExternalClient with a monotonic count of Create and Observe calls.
type countingCreates struct {
	fakeExternalClient
	creates, observes atomic.Int32
}

func (c *countingCreates) Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error) {
	c.observes.Add(1)
	return c.fakeExternalClient.Observe(ctx, mg)
}

func (c *countingCreates) Create(ctx context.Context, mg *unstructured.Unstructured) error {
	c.creates.Add(1)
	return c.fakeExternalClient.Create(ctx, mg)
}

// oasgen-provider#676, the runtime half. A create whose status write lost a race leaves
// external-create-pending open, and the quiet requeue re-adds the CREATE event unchanged. The next
// pass recovers: the existence check finds the resource and records it as created. That pass must
// then not call Create again. The control is the same Create event after a recovery that found the
// resource absent beyond the grace period: there the create is needed and must run once.
func TestRecoveredCreate_IsNotCreatedAgainOnceTheResourceExists(t *testing.T) {
	for _, tc := range []struct {
		name        string
		exists      bool
		pendingAge  time.Duration
		wantCreates int32
	}{
		{"recovery found the resource", true, time.Minute, 0},
		{"recovery found it absent beyond the grace (control)", false, 10 * time.Minute, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sid, err := shortid.New(1, shortid.DefaultABC, 2342)
			require.NoError(t, err)
			opts := createTestOptions()
			ctrl, err := New(sid, opts)
			require.NoError(t, err)
			ext := &countingCreates{fakeExternalClient: fakeExternalClient{ObserveExists: tc.exists, ObserveUpToDate: true}}
			ctrl.SetExternalClient(ext)

			obj := createTestUnstructured("recovered-create", opts.Namespace)
			meta.SetExternalCreatePending(obj, time.Now().Add(-tc.pendingAge))
			require.True(t, meta.ExternalCreateIncomplete(obj))
			_, err = opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
			require.NoError(t, err)

			ev := ctrlevent.Event{
				EventType: ctrlevent.Create,
				ObjectRef: objectref.ObjectRef{APIVersion: obj.GetAPIVersion(), Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()},
			}
			require.NoError(t, ctrl.processItem(context.TODO(), ev))

			got, err := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
			require.NoError(t, err)
			require.False(t, meta.ExternalCreateIncomplete(got), "the recovery must leave the incomplete state")
			require.Equal(t, tc.wantCreates, ext.creates.Load(), "Create calls in the pass after the recovery (observes: %d)", ext.observes.Load())
			if tc.exists {
				// Not vacuous: the pass reached the dispatch and observed, after the existence check.
				require.GreaterOrEqual(t, ext.observes.Load(), int32(2), "the recovered pass must still observe the resource")
			}
		})
	}
}
