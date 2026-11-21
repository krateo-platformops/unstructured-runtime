package unstructured

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type NotAvailableError struct {
	FailedObjectRef *objectref.ObjectRef
	Err             error
}

func (r *NotAvailableError) Error() string {
	if r.FailedObjectRef == nil {
		return fmt.Sprintf("err %v", r.Err)
	}
	return fmt.Sprintf("failedObjectRef %v k8s.io/api/core/v1: err %v", r.FailedObjectRef, r.Err)
}

func IsAvailable(un *unstructured.Unstructured) (bool, error) {
	positives := []string{
		"ready", "complete", "healthy", "active", "able",
	}

	conds := GetConditions(un)
	if len(conds) == 0 {
		return true, nil
	}

	for _, co := range conds {
		if has(positives, string(co.Type)) {
			if string(co.Status) != "True" {
				return false, &NotAvailableError{
					FailedObjectRef: &objectref.ObjectRef{
						APIVersion: un.GetAPIVersion(),
						Kind:       un.GetKind(),
						Name:       un.GetName(),
						Namespace:  un.GetNamespace(),
					},
					Err: fmt.Errorf("%s", co.Reason),
				}
			}
		}
	}

	return true, nil
}

func SetConditions(un *unstructured.Unstructured, conds ...metav1.Condition) error {
	conditions := GetConditions(un)
	for _, cond := range conds {
		// #115: a zero LastTransitionTime marshals to `lastTransitionTime: null`, a shape the
		// apiserver's condition schema refuses and that every earlier reader crashed on. Stamp it
		// here; Upsert still keeps the existing stamp when the status did not change (#36).
		if cond.LastTransitionTime.IsZero() {
			cond.LastTransitionTime = metav1.Now()
		}
		condition.Upsert(&conditions, cond)
	}

	res, err := encodeStruct(conditions)
	if err != nil {
		return err
	}

	return unstructured.SetNestedField(un.Object, res, "status", "conditions")
}

// GetConditions returns the conditions on un, ENTRY BY ENTRY (#115).
//
// It used to be all-or-nothing: one entry that was not a map, lacked a string type or status, or
// whose lastTransitionTime was absent, null or not RFC3339 made it return nil for ALL of them, and a
// present-but-null lastTransitionTime (what a zero metav1.Time marshals to) panicked on an unchecked
// type assertion. Since SetConditions reads through here and writes the whole list back, one bad
// stamp then ERASED every other condition on the next write: Ready and Synced vanished from a
// resource that was being reconciled. Now:
//   - an entry that cannot be identified (not a map, or no string type or status) is skipped, and
//     only that entry: there is no condition to return for it;
//   - a reason or message that is not a string reads as "";
//   - a lastTransitionTime that is absent, null, not a string or not RFC3339 reads as the zero time,
//     and the condition is KEPT: its status and reason are what callers decide on.
//
// The message IS included (the loop below populates it). core-provider's setReadyCondition
// short-circuits on `cur.Message == message`, so a stale comment once claiming otherwise nearly became
// a root cause.
func GetConditions(un *unstructured.Unstructured) []metav1.Condition {
	if un == nil {
		return nil
	}
	items, _, _ := unstructured.NestedSlice(un.Object, "status", "conditions")
	x := []metav1.Condition{}
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		typ, ok := m["type"].(string)
		if !ok || typ == "" {
			continue
		}
		status, ok := m["status"].(string)
		if !ok {
			continue
		}
		reason, _ := m["reason"].(string)
		message, _ := m["message"].(string)
		var tm time.Time
		if ts, ok := m["lastTransitionTime"].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
				tm = parsed
			}
		}
		x = append(x, metav1.Condition{
			Type:               typ,
			Status:             metav1.ConditionStatus(status),
			LastTransitionTime: metav1.Time{Time: tm},
			Reason:             reason,
			Message:            message,
		})
	}
	return x
}

func GetCondition(un *unstructured.Unstructured, condType string, reason string) *metav1.Condition {
	conds := GetConditions(un)
	for _, co := range conds {
		if co.Type == condType && co.Reason == reason {
			return &co
		}
	}
	return nil
}

func SetFailedObjectRef(un *unstructured.Unstructured, ref *objectref.ObjectRef) error {
	if ref == nil {
		return fmt.Errorf("ref cannot be nil")
	}
	if un == nil {
		return fmt.Errorf("un cannot be nil")
	}
	return setNestedFieldNoCopy(un, map[string]interface{}{
		"apiVersion": ref.APIVersion,
		"kind":       ref.Kind,
		"name":       ref.Name,
		"namespace":  ref.Namespace,
	}, "status", "failedObjectRef")
}

func ExtractFailedObjectRef(un *unstructured.Unstructured) (*objectref.ObjectRef, error) {
	if un == nil {
		return nil, fmt.Errorf("un cannot be nil")
	}
	obj, ok, err := unstructured.NestedFieldNoCopy(un.Object, "status", "failedObjectRef")
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	dat, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	ref := &objectref.ObjectRef{}
	err = json.Unmarshal(dat, ref)
	return ref, err
}

func UnsetFailedObjectRef(un *unstructured.Unstructured) {
	removeNestedField(un, "status", "failedObjectRef")
}
func setNestedFieldNoCopy(uns *unstructured.Unstructured, value interface{}, fields ...string) error {
	if uns == nil {
		return fmt.Errorf("uns cannot be nil")
	}

	m := uns.Object

	for i, field := range fields[:len(fields)-1] {
		if val, ok := m[field]; ok {
			if valMap, ok := val.(map[string]interface{}); ok {
				m = valMap
			} else {
				return fmt.Errorf("value cannot be set because %v is not a map[string]interface{}", fields[:i+1])
			}
		} else {
			newVal := make(map[string]interface{})
			m[field] = newVal
			m = newVal
		}
	}
	m[fields[len(fields)-1]] = value
	return nil
}

// removeNestedField removes the nested field from the obj.
func removeNestedField(uns *unstructured.Unstructured, fields ...string) {
	if uns == nil {
		return
	}
	m := uns.Object
	for _, field := range fields[:len(fields)-1] {
		if x, ok := m[field].(map[string]interface{}); ok {
			m = x
		} else {
			return
		}
	}
	delete(m, fields[len(fields)-1])
}

// has checks if a string is present in a slice
func has(s []string, str string) bool {
	for _, v := range s {
		if strings.Contains(strings.ToLower(str), v) {
			return true
		}
	}

	return false
}

// Converts a struct to a map while maintaining the json alias as keys
func encodeStruct(obj interface{}) (res interface{}, err error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, &res)
	return
}

func GetFieldsFromUnstructured(u *unstructured.Unstructured, field string) (map[string]interface{}, error) {
	if u == nil {
		return nil, fmt.Errorf("unstructured cannot be nil")
	}
	spec, ok, err := unstructured.NestedFieldNoCopy(u.Object, field)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s not found", field)
	}
	fields := make(map[string]interface{})
	if reflect.ValueOf(spec).CanInterface() {
		iter := reflect.ValueOf(spec).MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value()
			fields[k.String()] = v.Interface()
		}
	}
	return fields, nil
}

func IsConditionSet(un *unstructured.Unstructured, cond metav1.Condition) bool {
	if un == nil {
		return false
	}
	conds := GetConditions(un)
	for _, co := range conds {
		if co.Type == cond.Type && co.Status == cond.Status && co.Reason == cond.Reason {
			return true
		}
	}
	return false
}
