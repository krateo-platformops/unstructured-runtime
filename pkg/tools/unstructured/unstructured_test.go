package unstructured

import (
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestGetConditions(t *testing.T) {
	un := createFakeObject()

	all := GetConditions(un)
	assert.Equal(t, 0, len(all))

	err := SetConditions(un, condition.Unavailable())
	assert.Nil(t, err)

	all = GetConditions(un)
	assert.Equal(t, 1, len(all))
}

func TestIsAvailable(t *testing.T) {
	un := createFakeObject()

	ok, err := IsAvailable(un)
	assert.Nil(t, err)
	assert.True(t, ok)

	err = SetConditions(un, condition.Unavailable())
	assert.Nil(t, err)

	ok, err = IsAvailable(un)
	assert.NotNil(t, err)
	assert.False(t, ok)
	if assert.IsType(t, &NotAvailableError{}, err) {
		ex, _ := err.(*NotAvailableError)
		assert.NotNil(t, ex.FailedObjectRef)
	}
}

func TestFailedObjectRef(t *testing.T) {
	un := createFakeObject()

	ref, err := ExtractFailedObjectRef(un)
	assert.Nil(t, err)
	assert.Nil(t, ref)

	want := &objectref.ObjectRef{
		APIVersion: "test.example.org",
		Kind:       "Test",
		Name:       "test",
		Namespace:  "test-system",
	}

	err = SetFailedObjectRef(un, want)
	assert.Nil(t, err)

	ref, err = ExtractFailedObjectRef(un)
	assert.Nil(t, err)
	assert.NotNil(t, ref)
	assert.Equal(t, want, ref)

	UnsetFailedObjectRef(un)
	ref, err = ExtractFailedObjectRef(un)
	assert.Nil(t, err)
	assert.Nil(t, ref)
}

func TestSetConditions(t *testing.T) {
	un := createFakeObject()
	cond := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Tested",
		LastTransitionTime: metav1.Time{Time: time.Now()},
	}
	err := SetConditions(un, cond)
	assert.Nil(t, err)

	conds := GetConditions(un)
	assert.Equal(t, 1, len(conds))
	assert.Equal(t, cond.Type, conds[0].Type)
	assert.Equal(t, cond.Status, conds[0].Status)
	assert.Equal(t, cond.Reason, conds[0].Reason)
}

func TestGetCondition(t *testing.T) {
	un := createFakeObject()
	cond := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Tested",
		LastTransitionTime: metav1.Time{Time: time.Now()},
	}
	err := SetConditions(un, cond)
	assert.Nil(t, err)

	retrievedCond := GetCondition(un, "Ready", "Tested")
	assert.NotNil(t, retrievedCond)
	assert.Equal(t, cond.Type, retrievedCond.Type)
	assert.Equal(t, cond.Status, retrievedCond.Status)
	assert.Equal(t, cond.Reason, retrievedCond.Reason)
}

func TestIsConditionSet(t *testing.T) {
	un := createFakeObject()
	cond := metav1.Condition{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
		Reason: "Tested",
		LastTransitionTime: metav1.Time{
			Time: time.Now(),
		},
	}
	err := SetConditions(un, cond)
	assert.Nil(t, err)

	isSet := IsConditionSet(un, cond)
	assert.True(t, isSet)
}

func TestGetFieldsFromUnstructured(t *testing.T) {
	un := createFakeObject()
	fields, err := GetFieldsFromUnstructured(un, "metadata")
	assert.Nil(t, err)
	assert.NotNil(t, fields)
	assert.Equal(t, "test", fields["name"])
	assert.Equal(t, "test-system", fields["namespace"])
}

func createFakeObject() *unstructured.Unstructured {
	un := &unstructured.Unstructured{}
	un.SetGroupVersionKind(schema.FromAPIVersionAndKind("tests.example.org/v1", "Test"))
	un.SetName("test")
	un.SetNamespace("test-system")
	return un
}

// #115. Each malformed shape is read entry by entry: no panic, and no sibling lost.
func TestGetConditions_MalformedEntriesDoNotHideSiblings(t *testing.T) {
	good := map[string]interface{}{"type": "Ready", "status": "True", "reason": "Available", "lastTransitionTime": "2026-11-21T10:00:00Z"}
	for name, bad := range map[string]interface{}{
		"null lastTransitionTime (a zero metav1.Time)": map[string]interface{}{"type": "Synced", "status": "True", "lastTransitionTime": nil},
		"absent lastTransitionTime":                    map[string]interface{}{"type": "Synced", "status": "True"},
		"lastTransitionTime not RFC3339":               map[string]interface{}{"type": "Synced", "status": "True", "lastTransitionTime": "not-a-timestamp"},
		"lastTransitionTime not a string":              map[string]interface{}{"type": "Synced", "status": "True", "lastTransitionTime": int64(5)},
		"reason and message not strings":               map[string]interface{}{"type": "Synced", "status": "True", "reason": int64(7), "message": true, "lastTransitionTime": "2026-11-21T10:00:00Z"},
	} {
		t.Run(name, func(t *testing.T) {
			un := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{good, bad}}}}
			var got []metav1.Condition
			require.NotPanics(t, func() { got = GetConditions(un) })
			require.Len(t, got, 2, "both conditions must be returned; a malformed field is not a reason to drop the entry")
			require.Equal(t, "Ready", got[0].Type)
			require.Equal(t, "Available", got[0].Reason)
			require.Equal(t, "Synced", got[1].Type)
			require.Equal(t, metav1.ConditionTrue, got[1].Status)
		})
	}
	for name, bad := range map[string]interface{}{
		"not a map":         "junk",
		"no type":           map[string]interface{}{"status": "True"},
		"type not a string": map[string]interface{}{"type": int64(3), "status": "True"},
		"no status":         map[string]interface{}{"type": "Synced"},
	} {
		t.Run("unidentifiable entry, siblings kept: "+name, func(t *testing.T) {
			un := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{good, bad}}}}
			got := GetConditions(un)
			require.Len(t, got, 1, "only the unidentifiable entry is skipped")
			require.Equal(t, "Ready", got[0].Type)
		})
	}
}

// #115, the user-visible symptom: SetConditions reads through GetConditions and writes the whole list
// back, so one bad stamp on Synced used to ERASE Ready on the next write.
func TestSetConditions_ABadSiblingIsNotErased(t *testing.T) {
	un := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
		map[string]interface{}{"type": "Ready", "status": "True", "reason": "Available", "lastTransitionTime": "2026-11-21T10:00:00Z"},
		map[string]interface{}{"type": "Synced", "status": "True", "reason": "ReconcileSuccess", "lastTransitionTime": nil},
	}}}}
	require.NoError(t, SetConditions(un, metav1.Condition{Type: "Other", Status: metav1.ConditionTrue, Reason: "X", LastTransitionTime: metav1.Now()}))
	types := map[string]bool{}
	for _, c := range GetConditions(un) {
		types[c.Type] = true
	}
	require.True(t, types["Ready"] && types["Synced"] && types["Other"], "all three must survive the write, got %v", types)
}

// #115: a zero LastTransitionTime is stamped, so SetConditions never writes `lastTransitionTime: null`.
// CONTROL: a condition whose status does not change keeps its existing stamp (#36).
func TestSetConditions_ZeroTimeIsStamped(t *testing.T) {
	un := &unstructured.Unstructured{Object: map[string]interface{}{}}
	require.NoError(t, SetConditions(un, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Available"}))
	items, _, _ := unstructured.NestedSlice(un.Object, "status", "conditions")
	require.Len(t, items, 1)
	ts, ok := items[0].(map[string]interface{})["lastTransitionTime"].(string)
	require.True(t, ok && ts != "", "lastTransitionTime must be a stamp, got %#v", items[0].(map[string]interface{})["lastTransitionTime"])
	first := GetConditions(un)[0].LastTransitionTime

	require.NoError(t, SetConditions(un, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Available"}))
	require.True(t, GetConditions(un)[0].LastTransitionTime.Equal(&first), "control: an unchanged status keeps its stamp")
}
