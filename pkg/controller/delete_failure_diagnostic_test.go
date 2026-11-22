package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A FAILED delete persists the runtime's own Ready=False/Deleting carrying the wrapped vendor error, and
// keeps the finalizer. rest-dynamic-controller stands on this (oasgen-provider#878): its handler writes
// only a plain Deleting marker on its delete paths and never persists it, so this write is the ONLY
// place a delete failure's diagnosis reaches the cluster. If this branch ever stopped writing Deleting
// with the message, REST resources would lose it silently. CONTROL: a successful delete releases.
func TestHandleDelete_FailureCarriesTheVendorErrorOnReady(t *testing.T) {
	run := func(t *testing.T, name string, deleteErr error) (readyReason, readyMsg, syncedReason string, finalizers []string, procErr error) {
		t.Helper()
		sid, err := shortid.New(1, shortid.DefaultABC, 2342)
		require.NoError(t, err)
		opts := createTestOptions()
		opts.Recorder, opts.ThrottledRecorder = &mockEventRecorder{}, &mockEventRecorder{}
		ctrl, err := New(sid, opts)
		require.NoError(t, err)
		ctrl.SetExternalClient(&fakeExternalClient{ObserveExists: true, ObserveUpToDate: true, DeleteErr: deleteErr})

		obj := createTestUnstructured(name, opts.Namespace)
		obj.SetFinalizers([]string{finalizerName, "test.krateo.io/hold"}) // the second keeps it readable after release
		res := opts.Client.Resource(opts.GVR).Namespace(opts.Namespace)
		created, err := res.Create(context.TODO(), obj, metav1.CreateOptions{})
		require.NoError(t, err)
		now := metav1.Now()
		created.SetDeletionTimestamp(&now)
		created, err = res.Update(context.TODO(), created, metav1.UpdateOptions{})
		require.NoError(t, err)
		ref := objectref.ObjectRef{APIVersion: created.GetAPIVersion(), Kind: created.GetKind(), Name: created.GetName(), Namespace: created.GetNamespace()}

		procErr = ctrl.processItem(context.TODO(), ctrlevent.Event{EventType: ctrlevent.Delete, ObjectRef: ref})
		cur, err := res.Get(context.TODO(), name, metav1.GetOptions{})
		require.NoError(t, err)
		r, s := readyOf(t, cur), syncedOf(t, cur)
		require.NotNil(t, r)
		require.NotNil(t, s)
		return r.Reason, r.Message, s.Reason, cur.GetFinalizers(), procErr
	}

	reason, msg, synced, fins, perr := run(t, "del-fails", errors.New("vendor answered 500: dependency still attached"))
	require.Error(t, perr, "premise: the delete failed")
	require.Equal(t, condition.ReasonDeleting, reason, "a failed delete must say Deleting on Ready")
	require.True(t, strings.Contains(msg, "vendor answered 500: dependency still attached"),
		"Ready's message must carry the vendor's error; it is the only place REST resources' delete failure reaches the cluster, got %q", msg)
	require.Equal(t, condition.ReasonReconcileError, synced)
	require.Contains(t, fins, finalizerName, "a failed delete keeps our finalizer")

	_, _, _, fins, perr = run(t, "del-ok", nil)
	require.NoError(t, perr, "control: a successful delete")
	require.NotContains(t, fins, finalizerName, "control: a successful delete releases our finalizer")
}
