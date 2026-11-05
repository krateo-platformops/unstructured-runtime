package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// observeErrClient returns a fixed observation AND a fixed error from Observe.
type observeErrClient struct {
	obs ExternalObservation
	err error
}

func (o *observeErrClient) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	return o.obs, o.err
}
func (o *observeErrClient) Create(context.Context, *unstructured.Unstructured) error { return nil }
func (o *observeErrClient) Update(context.Context, *unstructured.Unstructured) error { return nil }
func (o *observeErrClient) Delete(context.Context, *unstructured.Unstructured) error { return nil }

func syncedReason(t *testing.T, opts Options, name string) string {
	t.Helper()
	synced, _ := gateSyncedOf(t, opts, name)
	if synced == nil {
		return ""
	}
	return synced.Reason
}

// #359. A handler whose status write lost a race returns tools.ErrStatusConflict, possibly ALONGSIDE
// an observation (rest-dynamic-controller's restResources.go returns one with
// ResourceExists=true, ResourceUpToDate=true). The runtime must treat it as "recompute": record no
// ReconcileError, write no Ready, ignore the observation, and hand the conflict to the worker.
func TestProcessItem_StatusConflictRecordsNothingAndIgnoresTheObservation(t *testing.T) {
	conflict := fmt.Errorf("handler: persisting status: %w", tools.ErrStatusConflict)
	ec := &observeErrClient{obs: ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, err: conflict}
	ctrl, evt, opts := gateController(t, nil, ec)

	err := ctrl.processItem(context.TODO(), evt)

	require.True(t, statusConflict(err), "processItem returned %v, not a status conflict", err)
	_, all := gateSyncedOf(t, opts, evt.ObjectRef.Name)
	for _, c := range all {
		require.NotEqual(t, condition.ReasonReconcileError, c.Reason,
			"a lost status race was recorded as %s/%s: that is #258's noise, the reason #30 added a retry", c.Type, c.Reason)
		require.NotEqual(t, condition.TypeReady, c.Type,
			"Ready was written from an observation that came with a status conflict (%s/%s); it must be ignored", c.Status, c.Reason)
	}
}

// CONTROL: an ordinary Observe error still records ReconcileError. Without it, the test above passes
// against a runtime that records nothing for any error.
func TestProcessItem_OrdinaryErrorStillRecordsReconcileError(t *testing.T) {
	ec := &observeErrClient{err: errors.New("the endpoint refused")}
	ctrl, evt, opts := gateController(t, nil, ec)

	err := ctrl.processItem(context.TODO(), evt)

	require.Error(t, err)
	require.False(t, statusConflict(err))
	require.Equal(t, condition.ReasonReconcileError, syncedReason(t, opts, evt.ObjectRef.Name),
		"an ordinary failure must still be recorded")
}

// A lost status race must not spend the item's backoff or retry budget, even when the item has
// already used it all: it says nothing about the item.
func TestHandleErr_StatusConflictIsRequeuedWithoutSpendingBackoff(t *testing.T) {
	ctrl, evt, opts := gateController(t, nil, &keyedClient{})
	for i := 0; i < opts.MaxRetries; i++ {
		opts.GlobalRateLimiter.When(evt)
	}
	before := ctrl.queue.NumRequeues(evt)
	require.GreaterOrEqual(t, before, ctrl.maxRetries, "setup: the item must be at maxRetries")
	require.Zero(t, ctrl.queue.Len(), "the queue must start empty")

	ctrl.handleErr(fmt.Errorf("wrapped: %w", tools.ErrStatusConflict), evt, NormalPriority)

	require.Equal(t, before, ctrl.queue.NumRequeues(evt), "a status conflict grew the item's backoff")
	require.Eventually(t, func() bool { return ctrl.queue.Len() == 1 }, 3*statusConflictRequeue, 20*time.Millisecond,
		"a status conflict at maxRetries was DROPPED instead of requeued")

	// CONTROL: at the same count an ordinary failure IS dropped.
	ctrl2, evt2, opts2 := gateController(t, nil, &keyedClient{})
	for i := 0; i < opts2.MaxRetries; i++ {
		opts2.GlobalRateLimiter.When(evt2)
	}
	ctrl2.handleErr(errors.New("a real failure"), evt2, NormalPriority)
	require.Never(t, func() bool { return ctrl2.queue.Len() > 0 }, 300*time.Millisecond, 20*time.Millisecond,
		"control: an ordinary failure at maxRetries was requeued, so the arm above shows nothing")
}

func TestClassifyOutcome_StatusConflictIsARequeueNotAFailure(t *testing.T) {
	require.Equal(t, outcomeStatusConflict, classifyOutcome(fmt.Errorf("x: %w", tools.ErrStatusConflict)))
	require.Equal(t, outcomeFailure, classifyOutcome(errors.New("boom")), "control")
}

// conflictEverywhere returns a status conflict from every action.
type conflictEverywhere struct{ err error }

func (c *conflictEverywhere) Observe(context.Context, *unstructured.Unstructured) (ExternalObservation, error) {
	return ExternalObservation{}, c.err
}
func (c *conflictEverywhere) Create(context.Context, *unstructured.Unstructured) error { return c.err }
func (c *conflictEverywhere) Update(context.Context, *unstructured.Unstructured) error { return c.err }
func (c *conflictEverywhere) Delete(context.Context, *unstructured.Unstructured) error { return c.err }

// The guard sits in EVERY handler's error block, not just Observe's: a handler that persists status
// inside Create, Update or Delete can lose the same race. Rows span the three, and each has a control
// with an ordinary error that must still record ReconcileError.
func TestHandlers_StatusConflictRecordsNothingInCreateUpdateDelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Controller, context.Context, objectref.ObjectRef) error
	}{
		{"Create", func(c *Controller, ctx context.Context, r objectref.ObjectRef) error { return c.handleCreate(ctx, r) }},
		{"Update", func(c *Controller, ctx context.Context, r objectref.ObjectRef) error { return c.handleUpdate(ctx, r) }},
		{"Delete", func(c *Controller, ctx context.Context, r objectref.ObjectRef) error { return c.handleDelete(ctx, r) }},
	} {
		t.Run(tc.name+": conflict records nothing", func(t *testing.T) {
			ctrl, evt, opts := gateController(t, nil, &conflictEverywhere{err: fmt.Errorf("x: %w", tools.ErrStatusConflict)})
			err := tc.run(ctrl, context.TODO(), evt.ObjectRef)
			require.True(t, statusConflict(err), "%s returned %v, not a status conflict", tc.name, err)
			require.NotEqual(t, condition.ReasonReconcileError, syncedReason(t, opts, evt.ObjectRef.Name),
				"%s recorded a lost status race as ReconcileError", tc.name)
		})
		t.Run(tc.name+": control, an ordinary error is recorded", func(t *testing.T) {
			ctrl, evt, opts := gateController(t, nil, &conflictEverywhere{err: errors.New("refused")})
			_ = tc.run(ctrl, context.TODO(), evt.ObjectRef)
			require.Equal(t, condition.ReasonReconcileError, syncedReason(t, opts, evt.ObjectRef.Name),
				"%s did not record an ordinary failure, so the arm above proves nothing", tc.name)
		})
	}
}
