package controller

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// answeringClient's existence check fails until answer is set, then reports exists. It counts every
// call into the handler that acts (Delete) or observes (Observe).
type answeringClient struct {
	answered atomic.Bool
	exists   bool
	deletes  atomic.Int32
	observes atomic.Int32
}

func (c *answeringClient) Exists(context.Context, *unstructured.Unstructured) (bool, error) {
	if !c.answered.Load() {
		return false, errors.New("this kind declares no findby verb, so existence cannot be read")
	}
	return c.exists, nil
}
func (c *answeringClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	c.observes.Add(1)
	return ExternalObservation{ResourceExists: c.exists, ResourceUpToDate: true}, nil
}
func (c *answeringClient) Create(context.Context, *unstructured.Unstructured) error { return nil }
func (c *answeringClient) Update(context.Context, *unstructured.Unstructured) error { return nil }
func (c *answeringClient) Delete(context.Context, *unstructured.Unstructured) error {
	c.deletes.Add(1)
	return nil
}

// #107 (Diego, 2026-11-17): the gate keeps blocking an object whose create outcome is unknown,
// deletion included; once its existence check can answer, the next pass (a resync) clears it BY
// ITSELF, with nobody touching krateo.io/external-create-pending. The case no test had: an object being
// deleted, create-pending past its grace, an existence check that fails and then answers. Every
// assertion on the annotation reads the stored object; the test itself never edits it after setup.
func TestCreatePendingClearsByItselfOnceExistenceAnswers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deleting bool
		exists   bool
		answers  bool
	}{
		{name: "deleting, then the check answers absent: Delete runs, the finalizer is released", deleting: true, exists: false, answers: true},
		{name: "deleting, then the check answers exists: the create is recorded, Delete runs", deleting: true, exists: true, answers: true},
		{name: "not deleting, then the check answers exists: reconciliation resumes", deleting: false, exists: true, answers: true},
		{name: "control: the check never answers, so it stays blocked, deletion included", deleting: true, answers: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ec := &answeringClient{exists: tc.exists}
			ctrl, evt, opts := gateController(t, nil, ec)
			t.Cleanup(ctrl.queue.ShutDown)
			ctrl.runCtx = context.Background()
			res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
			ctx := context.Background()

			u, err := res.Get(ctx, evt.ObjectRef.Name, metav1.GetOptions{})
			require.NoError(t, err)
			u.SetFinalizers([]string{finalizerName})
			meta.SetExternalCreatePending(u, time.Now().Add(-externalCreateRecoveryGracePeriod-time.Minute))
			if tc.deleting {
				now := metav1.Now()
				u.SetDeletionTimestamp(&now)
				evt.EventType = ctrlevent.Delete
			}
			_, err = res.Update(ctx, u, metav1.UpdateOptions{})
			require.NoError(t, err)

			pass := func() {
				t.Helper()
				ctrl.items.Store(ctrlevent.DigestForEvent(evt), struct{}{})
				ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, evt)
				require.True(t, ctrl.processNext(ctx, "test"))
			}
			stored := func() *unstructured.Unstructured {
				t.Helper()
				got, err := res.Get(ctx, evt.ObjectRef.Name, metav1.GetOptions{})
				require.NoError(t, err)
				return got
			}

			// Pass 1: the check cannot answer. Blocked: nothing acted, the finalizer and the mark
			// stay, Ready says CreateOutcomeUnknown and the message says deletion waits too and it
			// clears by itself.
			pass()
			got := stored()
			require.Zero(t, ec.deletes.Load(), "Delete ran while the create outcome was unknown")
			require.Zero(t, ec.observes.Load(), "Observe ran while the create outcome was unknown")
			require.True(t, meta.ExternalCreateIncomplete(got), "the create-pending mark was cleared without an answer")
			require.True(t, slices.Contains(got.GetFinalizers(), finalizerName), "the finalizer was released without an answer")
			ready := unstructuredtools.GetCondition(got, condition.TypeReady, condition.ReasonCreateOutcomeUnknown)
			require.NotNil(t, ready, "Ready must be False/CreateOutcomeUnknown")
			require.Contains(t, ready.Message, "deletion both wait")
			require.Contains(t, ready.Message, "clearing by itself")

			// Pass 2, the next resync. The test edits nothing: only the check's answer changed.
			ec.answered.Store(tc.answers)
			pass()
			got = stored()

			if !tc.answers {
				require.Zero(t, ec.deletes.Load(), "control: Delete ran on a check that never answered")
				require.True(t, meta.ExternalCreateIncomplete(got), "control: the mark cleared on a check that never answered")
				require.True(t, slices.Contains(got.GetFinalizers(), finalizerName), "control: the finalizer was released on a check that never answered")
				return
			}
			require.False(t, meta.ExternalCreateIncomplete(got), "the runtime did not clear the create-pending state once the check answered")
			if tc.deleting {
				require.EqualValues(t, 1, ec.deletes.Load(), "Delete did not run once the check answered")
				require.False(t, slices.Contains(got.GetFinalizers(), finalizerName), "the finalizer was not released once the check answered")
			} else {
				require.Zero(t, ec.deletes.Load())
				require.EqualValues(t, 1, ec.observes.Load(), "reconciliation did not resume once the check answered")
			}
		})
	}
}
