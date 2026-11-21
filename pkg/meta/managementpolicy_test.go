package meta

import (
	"testing"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The deprecated Should* wrappers answer exactly what ManagementDecision answers (#110, D9), over
// presets, sets (D1/D3), an invalid value (D2) and every deletion policy, including an unrecognised
// one. The pinned rows are the ones that CHANGED from the pre-#110 parser, so a wrapper that kept
// its own copy of the old rules fails here, not only one that drifts from the package.
func TestShouldWrappers(t *testing.T) {
	obj := func(mp, dp string) metav1.Object {
		u := &unstructured.Unstructured{}
		ann := map[string]string{}
		if mp != "" {
			ann[AnnotationKeyManagementPolicy] = mp
		}
		if dp != "" {
			ann[AnnotationKeyDeletionPolicy] = dp
		}
		u.SetAnnotations(ann)
		return u
	}
	rows := 0
	for _, mp := range []string{"", "default", "observe-create-update", "observe-delete", "observe", "observe-create", "observe,create", "create", "update,delete"} {
		for _, dp := range []string{"", "delete", "orphan", "orphaned"} {
			o := obj(mp, dp)
			d := ManagementDecision(o)
			p := d.Policy
			if ShouldCreate(o) != d.May(managementpolicy.Create) || ShouldUpdate(o) != d.May(managementpolicy.Update) || ShouldDelete(o) != d.May(managementpolicy.Delete) {
				t.Errorf("mp=%q dp=%q: a Should* wrapper disagrees with ManagementDecision", mp, dp)
			}
			if want := !p.Allows(managementpolicy.Create) && !p.Allows(managementpolicy.Update) && !p.Allows(managementpolicy.Delete); ShouldOnlyObserve(o) != want {
				t.Errorf("mp=%q dp=%q: ShouldOnlyObserve = %v, want %v", mp, dp, ShouldOnlyObserve(o), want)
			}
			rows++
		}
	}
	if rows != 9*4 {
		t.Fatalf("checked %d rows, want %d", rows, 9*4)
	}

	for _, tc := range []struct {
		name                      string
		mp, dp                    string
		create, update, del, only bool
	}{
		{"D5: observe-delete + orphan keeps (it deleted before)", "observe-delete", "orphan", false, false, false, false},
		{"unknown deletion-policy keeps under default", "", "orphaned", true, true, false, false},
		{"unknown deletion-policy keeps under observe-delete (it deleted before)", "observe-delete", "orphaned", false, false, false, false},
		{"D1: a set", "observe,create", "", true, false, false, false},
		{"D3: observe implied", "create", "", true, false, false, false},
		{"D2: invalid acts on nothing, as observe", "observe-create", "", false, false, false, true},
		{"control: default deletes", "", "", true, true, true, false},
		{"control: observe-delete without orphan deletes", "observe-delete", "", false, false, true, false},
	} {
		o := obj(tc.mp, tc.dp)
		if ShouldCreate(o) != tc.create || ShouldUpdate(o) != tc.update || ShouldDelete(o) != tc.del || ShouldOnlyObserve(o) != tc.only {
			t.Errorf("%s: create=%v update=%v delete=%v onlyObserve=%v; want %v %v %v %v", tc.name,
				ShouldCreate(o), ShouldUpdate(o), ShouldDelete(o), ShouldOnlyObserve(o), tc.create, tc.update, tc.del, tc.only)
		}
	}
}

// IsActionAllowed keeps its documented quirk: delete ignores the deletion policy.
func TestIsActionAllowed_IgnoresDeletionPolicy(t *testing.T) {
	u := &unstructured.Unstructured{}
	u.SetAnnotations(map[string]string{AnnotationKeyDeletionPolicy: "orphan"})
	if !IsActionAllowed(u, ActionDelete) {
		t.Error("IsActionAllowed(delete) under default + orphan: want true (it reads the management policy only)")
	}
	u.SetAnnotations(map[string]string{AnnotationKeyManagementPolicy: "observe,create"})
	if !IsActionAllowed(u, ActionCreate) || IsActionAllowed(u, ActionUpdate) || IsActionAllowed(u, ActionDelete) {
		t.Error("IsActionAllowed must read a verb set")
	}
}
