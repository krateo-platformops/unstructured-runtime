package tools

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8stesting "k8s.io/client-go/testing"
)

// KNOWN LIMIT OF EVERY TEST IN THIS FILE, written down so a future reader does not over-trust them.
// The fake dynamic client does NOT enforce status-subresource semantics: `testing.ObjectReaction`
// applies the merge patch to the WHOLE tracked object and an UpdateStatus likewise replaces the
// whole tracked object, regardless of the "status" subresource on the action. So these tests CAN
// assert "a sibling status field survived the write", which is the defect in #33, and they CANNOT
// assert that the patch is scoped to the status subresource — that the subresource is passed at all
// is only visible as the action's GetSubresource(), which is what the reactors below check. Nothing
// here would catch a patch that also carried a `spec` key; only a real apiserver would.

// cmWithConditions builds the object the RUNTIME holds: a status stanza carrying conditions and
// nothing else, because the runtime's only status mutation is SetConditions.
func cmWithConditions(rv string, extraStatus map[string]any) *unstructured.Unstructured {
	status := map[string]any{
		"conditions": []any{
			map[string]any{
				"type":               "Synced",
				"status":             "True",
				"reason":             "Available",
				"message":            "",
				"lastTransitionTime": "2026-11-01T10:00:00Z",
			},
		},
	}
	for k, v := range extraStatus {
		status[k] = v
	}
	return cmWithStatus(rv, status)
}

// liveWithHandlerInventory is the state the HANDLER left on the apiserver after the runtime fetched:
// the conditions the runtime last wrote, plus the inventory the handler owns. `status.managed` is
// the sibling field core-provider's composition-dynamic-controller writes and the one whose loss
// motivated #33.
func liveWithHandlerInventory() *unstructured.Unstructured {
	return cmWithStatus("1", map[string]any{
		"conditions": []any{
			map[string]any{
				"type":               "Synced",
				"status":             "False",
				"reason":             "ReconcileError",
				"message":            "stale",
				"lastTransitionTime": "2026-11-01T09:00:00Z",
			},
		},
		"managed": []any{
			map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "child", "namespace": "default"},
		},
		"managedDigest": "abc123",
	})
}

// statusWriter is the shape both writers share, which is what makes the differential below possible
// as a drop-in substitution at all 12 worker.go call sites.
type statusWriter func(context.Context, *unstructured.Unstructured, UpdateOptions) (*unstructured.Unstructured, error)

// runInterleaving plays the exact race from #33 and returns the object as the server holds it
// afterwards. The read-back is deliberate: asserting on the writer's return value would let a
// writer that never contacted the server pass.
//
// Interleaving: the runtime fetched (so it has no `managed`), the handler then wrote `managed`, and
// the runtime now writes its conditions. withConflict additionally makes the first full-object
// status write fail with a 409, which is the case the #30 retry handles — and the case where that
// retry re-applies the stanza captured BEFORE the handler's write.
func runInterleaving(t *testing.T, write statusWriter, withConflict bool) (out *unstructured.Unstructured, updatesN, patchesN, conflictsN int) {
	t.Helper()

	client := fake.NewSimpleDynamicClient(scheme.Scheme, liveWithHandlerInventory())

	updates, patches := 0, 0
	client.PrependReactor("patch", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if pa, ok := a.(k8stesting.PatchAction); ok && pa.GetSubresource() == "status" {
			patches++
		}
		return false, nil, nil
	})
	client.PrependReactor("update", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if ua, ok := a.(k8stesting.UpdateAction); ok && ua.GetSubresource() == "status" {
			updates++
		}
		return false, nil, nil
	})
	conflicts := new(int)
	if withConflict {
		// Registered last so it runs FIRST (PrependReactor), ahead of the counter above; the counter
		// therefore sees only the attempts that reach the tracker. The count it returns is the
		// calibration for the retry arm -- it must be >= 2 for a conflict to have been produced and
		// retried.
		conflicts = conflictOnce(client)
	}

	// The runtime's object: conditions only, fetched before the handler's write, so it does not
	// carry `managed` and a full-stanza write expresses its ABSENCE.
	el := cmWithConditions("1", nil)
	if _, found, _ := unstructured.NestedSlice(el.Object, "status", "managed"); found {
		t.Fatal("the runtime's object must NOT carry the handler's inventory, or this proves nothing")
	}

	if _, err := write(context.Background(), el, UpdateOptions{
		Pluralizer:    &mockPluralizer{gvr: cmGVR},
		DynamicClient: client,
	}); err != nil {
		t.Fatalf("the write failed: %v", err)
	}

	got, err := client.Resource(cmGVR).Namespace("default").Get(context.Background(), "test-cm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the object back: %v", err)
	}
	return got, updates, patches, *conflicts
}

// THE DIFFERENTIAL. The same interleaving is played through both writers. It must destroy
// `status.managed` through UpdateStatus and preserve it through PatchStatusConditions -- the second
// assertion is the fix, and the first is the calibration that proves the scenario can tell them
// apart at all. If UpdateStatus ever stopped destroying the field, this test would say so rather
// than quietly becoming a test of nothing.
//
// The assertion is on the PRESENCE of the sibling field, not on an exact conditions array: merge
// patch replaces arrays wholesale, so conditions remain last-writer-wins by design and an exact
// array assertion would be asserting the thing that did not change.
func TestStatusWrite_SiblingFieldSurvival_Differential(t *testing.T) {
	for _, tc := range []struct {
		name           string
		write          statusWriter
		withConflict   bool
		wantManaged    bool
		wantUpdates    int
		wantPatches    int
		becauseItMusnt string
	}{
		{
			name:           "UpdateStatus destroys the handler's inventory",
			write:          UpdateStatus,
			wantManaged:    false,
			wantUpdates:    1,
			wantPatches:    0,
			becauseItMusnt: "a full-stanza write expresses the ABSENCE of status.managed (#33)",
		},
		{
			name:           "UpdateStatus destroys it through the conflict retry too",
			write:          UpdateStatus,
			withConflict:   true,
			wantManaged:    false,
			wantUpdates:    1, // the counter sits behind the conflict reactor; only the landing write is counted
			wantPatches:    0,
			becauseItMusnt: "on 409 the retry re-applies the stanza captured BEFORE the handler's write (#30 widened #33)",
		},
		{
			name:           "PatchStatusConditions preserves it",
			write:          PatchStatusConditions,
			wantManaged:    true,
			wantUpdates:    0,
			wantPatches:    1,
			becauseItMusnt: "a body containing only conditions cannot carry the absence of a sibling",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, updates, patches, conflicts := runInterleaving(t, tc.write, tc.withConflict)

			// CALIBRATION for the retry arm: if the fake never produced a 409, that arm says nothing
			// about the retry path and would pass against code with no retry at all.
			if tc.withConflict && conflicts < 2 {
				t.Fatalf("the fake never produced a conflict (%d status writes seen by its reactor), "+
					"so the retry path was not exercised", conflicts)
			}

			_, gotManaged, err := unstructured.NestedSlice(out.Object, "status", "managed")
			if err != nil {
				t.Fatalf("reading status.managed back: %v", err)
			}
			if gotManaged != tc.wantManaged {
				t.Errorf("status.managed present = %v, want %v: %s", gotManaged, tc.wantManaged, tc.becauseItMusnt)
			}

			// CALIBRATION. Without this, an arm in which no write reached the server at all would
			// look like whichever outcome it was asserting.
			if updates != tc.wantUpdates || patches != tc.wantPatches {
				t.Errorf("status writes reaching the tracker: %d updates / %d patches, want %d / %d -- "+
					"the write did not take the path this arm is about", updates, patches, tc.wantUpdates, tc.wantPatches)
			}

			// The runtime's own field must still land, whichever writer did it; otherwise "the
			// sibling survived" would be satisfied by a writer that did nothing.
			conds, found, _ := unstructured.NestedSlice(out.Object, "status", "conditions")
			if !found || len(conds) == 0 {
				t.Fatal("the conditions the runtime wrote must land")
			}
			first, _ := conds[0].(map[string]any)
			if first["reason"] != "Available" {
				t.Errorf("the runtime's conditions must replace the stale ones, reason = %v", first["reason"])
			}
		})
	}
}

// THE GUARD (#33, "the one detail that could destroy data"). GetConditions returns nil for ALL
// conditions when any single one has a missing or non-RFC3339 lastTransitionTime, so a caller can
// reach this function with an empty slice while the server still holds a full conditions array.
// Emitting `{"status":{"conditions":null}}` would then DELETE that array. No patch may be issued.
//
// The counter is the whole assertion: "the conditions on the server are unchanged" would also hold
// for a patch that happened to rewrite them identically, and the point is that nothing is sent.
func TestPatchStatusConditions_EmptyConditionsIssuesNoPatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		el   *unstructured.Unstructured
	}{
		{name: "no status at all", el: cmWithStatus("1", nil)},
		{name: "status without conditions", el: cmWithStatus("1", map[string]any{"managed": []any{}})},
		{name: "conditions present but empty", el: cmWithStatus("1", map[string]any{"conditions": []any{}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleDynamicClient(scheme.Scheme, liveWithHandlerInventory())
			patches := 0
			client.PrependReactor("patch", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
				patches++
				return false, nil, nil
			})

			out, err := PatchStatusConditions(context.Background(), tc.el, UpdateOptions{
				Pluralizer:    &mockPluralizer{gvr: cmGVR},
				DynamicClient: client,
			})
			if err != nil {
				t.Fatalf("skipping the patch is not an error: %v", err)
			}
			if patches != 0 {
				t.Errorf("an empty conditions slice must issue NO patch, got %d", patches)
			}
			if out == nil {
				t.Fatal("the caller's object must be returned, not nil: handleDelete keeps using it")
			}

			// And the server's conditions are untouched -- the data this guard exists to protect.
			live, gerr := client.Resource(cmGVR).Namespace("default").Get(context.Background(), "test-cm", metav1.GetOptions{})
			if gerr != nil {
				t.Fatalf("reading the object back: %v", gerr)
			}
			conds, found, _ := unstructured.NestedSlice(live.Object, "status", "conditions")
			if !found || len(conds) != 1 {
				t.Errorf("the conditions on the server must survive, found=%v len=%d", found, len(conds))
			}
		})
	}
}

// PatchStatusConditions keeps the argument guards UpdateStatus has: a nil pluralizer or client is a
// programming error and must surface as one rather than as a nil dereference.
func TestPatchStatusConditions_ArgumentGuards(t *testing.T) {
	el := cmWithConditions("1", nil)
	client := fake.NewSimpleDynamicClient(scheme.Scheme, liveWithHandlerInventory())

	if _, err := PatchStatusConditions(context.Background(), el, UpdateOptions{DynamicClient: client}); err == nil {
		t.Error("a nil pluralizer must be an error")
	}
	if _, err := PatchStatusConditions(context.Background(), el, UpdateOptions{Pluralizer: &mockPluralizer{gvr: cmGVR}}); err == nil {
		t.Error("a nil dynamic client must be an error")
	}
}
