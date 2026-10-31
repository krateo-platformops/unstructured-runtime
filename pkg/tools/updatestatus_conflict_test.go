package tools

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
)

var cmGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

func cmWithStatus(rv string, status map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":            "test-cm",
			"namespace":       "default",
			"resourceVersion": rv,
		},
	}
	if status != nil {
		obj["status"] = status
	}
	return &unstructured.Unstructured{Object: obj}
}

// conflictOnce rejects the first UpdateStatus with a 409 and allows the rest, which is the shape of
// a real race: another writer landed between the fetch and the write, and the object is fine on the
// next read.
func conflictOnce(c *fake.FakeDynamicClient) *int {
	calls := 0
	c.PrependReactor("update", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ua, ok := a.(k8stesting.UpdateAction)
		if !ok || ua.GetSubresource() != "status" {
			return false, nil, nil
		}
		calls++
		if calls == 1 {
			return true, nil, apierrors.NewConflict(
				a.GetResource().GroupResource(), "test-cm",
				context.Canceled)
		}
		return false, nil, nil
	})
	return &calls
}

// THE CASE THE FIX EXISTS FOR. A conflict on the first attempt must be retried and the intended
// status must land -- NOT be returned to the caller, which records it on the object as a
// ReconcileError and so turns a transient race into a permanent error condition (#30).
func TestUpdateStatus_RetriesOnConflictAndLandsTheStatus(t *testing.T) {
	client := fake.NewSimpleDynamicClient(scheme.Scheme, cmWithStatus("1", nil))
	calls := conflictOnce(client)

	in := cmWithStatus("1", map[string]any{"phase": "Ready"})
	out, err := UpdateStatus(context.Background(), in, UpdateOptions{
		Pluralizer:    &mockPluralizer{gvr: cmGVR},
		DynamicClient: client,
	})
	if err != nil {
		t.Fatalf("a conflict must be retried, not returned: %v", err)
	}

	// CALIBRATION. If the reactor never fired, this test proves nothing about conflicts -- it would
	// pass against the unfixed code too. Printing the denominator next to the result is the point.
	if *calls < 2 {
		t.Fatalf("the fake never produced a conflict (UpdateStatus called %d times), so this test "+
			"could not have distinguished the fix from its absence", *calls)
	}

	phase, found, _ := unstructured.NestedString(out.Object, "status", "phase")
	if !found || phase != "Ready" {
		t.Errorf("the intended status must survive the retry, got phase=%q found=%v", phase, found)
	}
}

// THE CONTROL. A non-conflict error must still be returned immediately and must NOT be retried --
// otherwise the fix would paper over real failures, and the test above would pass for a function
// that swallowed every error.
func TestUpdateStatus_NonConflictErrorIsReturnedNotRetried(t *testing.T) {
	client := fake.NewSimpleDynamicClient(scheme.Scheme, cmWithStatus("1", nil))
	calls := 0
	client.PrependReactor("update", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ua, ok := a.(k8stesting.UpdateAction)
		if !ok || ua.GetSubresource() != "status" {
			return false, nil, nil
		}
		calls++
		return true, nil, apierrors.NewForbidden(a.GetResource().GroupResource(), "test-cm", context.Canceled)
	})

	_, err := UpdateStatus(context.Background(), cmWithStatus("1", map[string]any{"phase": "Ready"}),
		UpdateOptions{Pluralizer: &mockPluralizer{gvr: cmGVR}, DynamicClient: client})
	if err == nil {
		t.Fatal("a Forbidden must be returned, not retried away")
	}
	if !apierrors.IsForbidden(err) {
		t.Errorf("the original error must survive, got %v", err)
	}
	if calls != 1 {
		t.Errorf("a non-conflict error must not be retried, got %d attempts", calls)
	}
}

// A status write must not revert metadata somebody else changed in the meantime. The retry re-reads
// the live object and puts only the status onto it, so a label added between the fetch and the
// retry survives -- the alternative, re-sending the caller's whole stale object, would silently
// undo another writer's metadata.
// The foreign label is PRE-SEEDED on the live object rather than written from inside the reactor.
// Writing to the fake client from within its own reactor re-enters the tracker's lock and
// deadlocks -- this test hung for 600s before being restructured, which is a defect in the test and
// not in the code under test. Pre-seeding is also the state that matters: the caller's `el` lacks
// the label, the live object has it, and the retried write must not revert it.
func TestUpdateStatus_RetryDoesNotRevertForeignMetadata(t *testing.T) {
	live := cmWithStatus("1", nil)
	live.SetLabels(map[string]string{"written-by": "someone-else"})
	client := fake.NewSimpleDynamicClient(scheme.Scheme, live)
	calls := conflictOnce(client)

	// The caller's object carries NO label -- it was fetched before the other writer landed.
	out, err := UpdateStatus(context.Background(), cmWithStatus("1", map[string]any{"phase": "Ready"}),
		UpdateOptions{Pluralizer: &mockPluralizer{gvr: cmGVR}, DynamicClient: client})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *calls < 2 {
		t.Fatalf("the fake never produced a conflict (%d attempts), so nothing was exercised", *calls)
	}
	if got := out.GetLabels()["written-by"]; got != "someone-else" {
		t.Errorf("a status write must not revert another writer's metadata, label = %q", got)
	}
	if phase, _, _ := unstructured.NestedString(out.Object, "status", "phase"); phase != "Ready" {
		t.Errorf("the intended status must still land, phase = %q", phase)
	}
}

// Removal must stay possible. A caller passing an object with NO status is asking for the status to
// be cleared, and a field-by-field merge on retry would make that unexpressible -- which is why the
// retry replaces the stanza rather than merging it.
func TestUpdateStatus_RetryPreservesStatusRemoval(t *testing.T) {
	client := fake.NewSimpleDynamicClient(scheme.Scheme,
		cmWithStatus("1", map[string]any{"phase": "Ready"}))
	calls := conflictOnce(client)

	out, err := UpdateStatus(context.Background(), cmWithStatus("1", nil),
		UpdateOptions{Pluralizer: &mockPluralizer{gvr: cmGVR}, DynamicClient: client})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *calls < 2 {
		t.Fatalf("the fake never produced a conflict (%d attempts), so nothing was exercised", *calls)
	}
	if _, found, _ := unstructured.NestedMap(out.Object, "status"); found {
		t.Error("a caller passing no status is clearing it; the retry must not resurrect the live one")
	}
}
