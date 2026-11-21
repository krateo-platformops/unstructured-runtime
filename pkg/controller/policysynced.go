package controller

import (
	"fmt"

	"github.com/krateo-platformops/plumbing/managementpolicy"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// setConditions is the ONE way the worker writes conditions (#110, D2 reporting). Every
// unstructuredtools.SetConditions call in this package goes through here, so the policy report below
// cannot be bypassed by a path that forgot it (TestNoConditionWriteBypassesTheFunnel).
func setConditions(el *unstructured.Unstructured, conds ...metav1.Condition) error {
	for i := range conds {
		conds[i] = policySynced(el, conds[i])
	}
	return unstructuredtools.SetConditions(el, conds...)
}

// policySynced replaces a pass's Synced outcome with the policy report when an annotation does not
// parse (Diego, 2026-11-21, unstructured-runtime#110):
//
//   - krateo.io/management-policy invalid: Synced=False/ManagementPolicyInvalid, WHATEVER the pass
//     concluded (ReconcileSuccess or ReconcileError). Fixing the annotation is what changes the
//     outcome, so it is the reason a reader must see. The pass's own error is still returned to the
//     queue (backoff unchanged) and its events still fire; only the condition's reason changes.
//   - else krateo.io/deletion-policy unrecognised: Synced=False/DeletionPolicyInvalid on EVERY pass
//     while it is set, not only while deleting, so the typo is visible before the delete it decides.
//
// ReconcilePaused and ReconcileDeferred are left alone: no pass ran, and those reasons say why. Only
// a Synced condition is touched; every other type passes through unchanged.
func policySynced(el *unstructured.Unstructured, c metav1.Condition) metav1.Condition {
	if c.Type != condition.TypeSynced {
		return c
	}
	if c.Reason != condition.ReasonReconcileSuccess && c.Reason != condition.ReasonReconcileError {
		return c
	}
	d := meta.ManagementDecision(el)
	switch {
	case d.Policy.Invalid != "":
		msg := fmt.Sprintf("%s %s; acting on nothing (observe only)", managementpolicy.AnnotationKey, d.Policy.Invalid)
		if d.DeletionInvalid != "" {
			msg += "; also, " + d.DeletionInvalid
		}
		return condition.ManagementPolicyInvalid(msg)
	case d.DeletionInvalid != "":
		return condition.DeletionPolicyInvalid(d.DeletionInvalid)
	}
	return c
}
