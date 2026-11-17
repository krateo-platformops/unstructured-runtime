package controller

import (
	"context"
	"testing"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// absentClient observes every object as absent, so an Observe pass decides on a Create.
type absentClient struct{ countingClient }

func (a *absentClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	a.calls.Add(1)
	return ExternalObservation{ResourceExists: false}, nil
}

// #101 (oasgen-provider#769): an Observe that finds the resource absent must not add a Create key when a
// Create for the object is already pending, or the object gets a second retry budget. Control: with
// nothing pending, the same pass does enqueue the Create, so a pass here is not just the follow-up
// being dropped altogether.
func TestObserveAbsentDoesNotAddASecondCreate(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "control: nothing pending, the Create is queued"
		if pending {
			name = "a Create already pending: no second key"
		}
		t.Run(name, func(t *testing.T) {
			ctrl, observe, _ := gateController(t, nil, &absentClient{})
			t.Cleanup(ctrl.queue.ShutDown)
			create := observe
			create.EventType = ctrlevent.Create
			if pending {
				// What handleErr leaves for a Create waiting out its backoff: its digest is stored.
				ctrl.items.Store(ctrlevent.DigestForEvent(create), struct{}{})
			}
			ctrl.items.Store(ctrlevent.DigestForEvent(observe), struct{}{})
			ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, observe)

			require.True(t, ctrl.processNext(context.Background(), "test"))

			want := 1
			if pending {
				want = 0
			}
			require.Equal(t, want, ctrl.queue.Len(), "Create keys queued by the Observe pass")
			if pending {
				_, still := ctrl.items.Load(ctrlevent.DigestForEvent(create))
				require.True(t, still, "the pending Create's digest was cleared by another key's pass")
			}
		})
	}
}
