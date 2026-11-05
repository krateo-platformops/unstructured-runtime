package tools

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// #359. A conflict means the caller's status was computed from a stale read. It must come back as
// ErrStatusConflict (and still satisfy apierrors.IsConflict), and must NOT be re-submitted: the
// newer write on the server, including a field the stale caller never saw, has to survive. The
// test this replaces asserted the opposite ("the intended status must land"), which is the lost
// update that wiped status.managed on a snowplow bench cluster.
func TestUpdateStatus_ConflictIsReturnedTypedAndNotResubmitted(t *testing.T) {
	live := cmWithStatus("2", map[string]any{"marker": "newer", "managed": []any{"child"}})
	client := fake.NewSimpleDynamicClient(scheme.Scheme, live)
	calls := conflictOnce(client)

	stale := cmWithStatus("1", map[string]any{"marker": "stale"})
	_, err := UpdateStatus(context.Background(), stale,
		UpdateOptions{Pluralizer: &mockPluralizer{gvr: cmGVR}, DynamicClient: client})
	if !errors.Is(err, ErrStatusConflict) {
		t.Fatalf("want ErrStatusConflict, got %v", err)
	}
	if !apierrors.IsConflict(err) {
		t.Errorf("the apiserver's conflict must stay matchable alongside the sentinel: %v", err)
	}
	if *calls != 1 {
		t.Fatalf("the stale status was re-submitted after the conflict (%d status writes): the lost update", *calls)
	}
	got, gErr := client.Resource(cmGVR).Namespace("default").Get(context.Background(), "test-cm", metav1.GetOptions{})
	if gErr != nil {
		t.Fatal(gErr)
	}
	if m, _, _ := unstructured.NestedString(got.Object, "status", "marker"); m != "newer" {
		t.Errorf("the newer status was replaced: marker=%q", m)
	}
	if managed, found, _ := unstructured.NestedSlice(got.Object, "status", "managed"); !found || len(managed) != 1 {
		t.Errorf("a field written by the newer pass was removed (found=%v, %v)", found, managed)
	}
}

// CONTROL: without a conflict the status lands in one write, so the test above is about the 409 and
// not about a write that never happens.
func TestUpdateStatus_NoConflictLandsInOneWrite(t *testing.T) {
	client := fake.NewSimpleDynamicClient(scheme.Scheme, cmWithStatus("1", nil))
	calls := 0
	client.PrependReactor("update", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if ua, ok := a.(k8stesting.UpdateAction); ok && ua.GetSubresource() == "status" {
			calls++
		}
		return false, nil, nil
	})
	out, err := UpdateStatus(context.Background(), cmWithStatus("1", map[string]any{"marker": "fresh"}),
		UpdateOptions{Pluralizer: &mockPluralizer{gvr: cmGVR}, DynamicClient: client})
	if err != nil || calls != 1 {
		t.Fatalf("want one successful write, got err=%v writes=%d", err, calls)
	}
	if m, _, _ := unstructured.NestedString(out.Object, "status", "marker"); m != "fresh" {
		t.Errorf("marker = %q, want fresh", m)
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
