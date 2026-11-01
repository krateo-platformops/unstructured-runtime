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
		condition.Upsert(&conditions, cond)
	}

	res, err := encodeStruct(conditions)
	if err != nil {
		return err
	}

	return unstructured.SetNestedField(un.Object, res, "status", "conditions")
}

// GetConditions returns the conditions on un.
//
// The message IS included, despite what this comment said until now — the loop below populates
// `Message: message`. The stale "excluding the `message` field" wording nearly became a root cause:
// core-provider's setReadyCondition short-circuits on `cur.Message == message`, so "the message is
// stripped here" would have explained a condition being rewritten on every reconcile exactly, and I
// believed it from the comment before reading the code three lines down.
//
// ALL-OR-NOTHING ON PARSE FAILURE, which is the real hazard in here. Any condition whose
// `lastTransitionTime` is absent or not RFC3339 makes this return nil for ALL conditions — so a
// caller asking about Ready gets nothing because Synced was malformed, and a short-circuit keyed on
// the result then fails open. Worth fixing separately; noted here because the signature gives no
// hint of it.
func GetConditions(un *unstructured.Unstructured) []metav1.Condition {
	if un == nil {
		return nil
	}
	items, _, _ := unstructured.NestedSlice(un.Object, "status", "conditions")
	x := []metav1.Condition{}
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			return nil
		}
		_, ok = m["type"].(string)
		if !ok {
			return nil
		}
		_, ok = m["status"].(string)
		if !ok {
			return nil
		}
		var reason, message, t string
		_, ok = m["reason"]
		if ok {
			reason = m["reason"].(string)
		}
		_, ok = m["message"]
		if ok {
			message = m["message"].(string)
		}
		_, ok = m["lastTransitionTime"]
		if ok {
			t = m["lastTransitionTime"].(string)
		}

		tm, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return nil
		}

		x = append(x, metav1.Condition{
			Type:               m["type"].(string),
			Status:             metav1.ConditionStatus(m["status"].(string)),
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
