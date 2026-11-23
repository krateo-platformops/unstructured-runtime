package controller

import (
	"context"
	"slices"
	"time"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"github.com/krateo-platformops/plumbing/managementpolicy"
	"github.com/krateo-platformops/plumbing/tracecontext"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/errors"
	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// CredentialUse lets the process that runs the handler protect what its handlers read (Secrets, and
// the objects that lead to them) for as long as an object uses it (core-provider#402). The runtime
// calls it at fixed points of every pass and knows nothing of how the protection works. The contract
// is provider-runtime's pkg/credentialuse, whose Tracker an adapter wraps; this package does not
// import it.
//
// "The object" (the holder) is the custom resource this controller reconciles. "The record" is what
// the implementation writes on it to say what it uses. "Established" means the object carries this
// controller's own finalizer.
//
// The runtime's side of the contract:
//
//   - Acquire is called ONCE per pass, before the pass's first call into the external client: the
//     create recovery's existence check, the rate-limit key, and the Observe/Create/Update/Delete
//     dispatch. Deletion passes included. Not called on a pass that makes no such call (paused, or
//     released under a policy that withholds Delete), nor for an object already finalized: being
//     deleted, without this controller's finalizer.
//   - BeforeEstablish is called just before the runtime adds its own finalizer, only when it is
//     absent, with that pass's Acquire result.
//   - Forget is called on the object whose write removes the finalizer, so both go in one write, and
//     alone on a deletion pass that finds the finalizer already gone and a record still present.
//   - Acquire and BeforeEstablish each get a context that ends after CredentialUseCallTimeout. The
//     pass itself has no deadline: a handler may run for many minutes.
//
// Every method may be called concurrently for different objects.
type CredentialUse interface {
	// Acquire holds what obj's handlers will read this pass. It may write its record on obj on the
	// server, and then updates obj's metadata in place to match.
	//
	// The outcome of a failure is the implementation's to choose, as a flag on the result and a nil
	// error: MustNotConnect (fail closed, an object that is not established) or Degraded (fail open,
	// one that is). The error is for two things: ErrCredentialUseConflict, and a failure the
	// implementation could not turn into an outcome. The runtime reads the second as "could not
	// determine" and stops the pass as MustNotConnect does, for every object: nothing is dispatched
	// and nothing is released.
	Acquire(ctx context.Context, obj *unstructured.Unstructured) (CredentialUseAcquisition, error)

	// BeforeEstablish checks again what Acquire found missing, just before the runtime adds its own
	// finalizer. On an error the finalizer is not added this pass, and the pass stops.
	BeforeEstablish(ctx context.Context, obj *unstructured.Unstructured, acq CredentialUseAcquisition) error

	// Forget removes the record from obj in memory and reports whether it removed anything. The
	// runtime writes obj.
	Forget(obj *unstructured.Unstructured) bool

	// IsOwnRecordWrite reports whether the only change between two versions of one object is the
	// record this process wrote. The runtime drops such an update event.
	IsOwnRecordWrite(oldObj, newObj *unstructured.Unstructured) bool

	// IsEstablished reports whether the implementation counts obj as established. The runtime asks
	// before it treats a deleting object without its finalizer as finalized (no Acquire, and its
	// record dropped): both must agree.
	IsEstablished(obj *unstructured.Unstructured) bool

	// WithResolved returns ctx carrying what Acquire read on the way to the Secrets, for the handler
	// to read instead of its caches. Called with the result of a granted Acquire, before the first
	// external call; the handlers get the returned context.
	WithResolved(ctx context.Context, acq CredentialUseAcquisition) context.Context
}

// CredentialUseAcquisition is what one Acquire decided for one pass of one object.
type CredentialUseAcquisition struct {
	// MustNotConnect: no external call this pass. Recorded as Synced=False/ReconcileError with Err;
	// Ready is not touched. Rate-limited requeue.
	MustNotConnect bool
	// Degraded: what the handlers read could not be held, and the pass proceeds. One Warning event.
	Degraded bool
	// Pending: no external call this pass, and no condition or status write; the pass runs again
	// after RetryAfter, without failure backoff. Wins over MustNotConnect.
	Pending bool
	// Reread: the record's write changed more of the object than the record (a webhook, defaulting),
	// so the runtime reads it again before continuing.
	Reread bool
	// RetryAfter: without Pending, the pass proceeds and, if it succeeds, runs again no later than
	// this. A failed or deferred pass keeps its own requeue.
	RetryAfter time.Duration
	// Err says why, with MustNotConnect or Degraded.
	Err error
	// Opaque is the implementation's own result, handed back to BeforeEstablish and WithResolved.
	Opaque any
}

// ErrCredentialUseConflict is what Acquire and BeforeEstablish return, wrapped or not, when their own
// write of the object lost a race to a newer one. Nothing is wrong with the object: the pass stops
// with nothing recorded and is requeued, rate-limited, and never dropped at MaxRetries.
var ErrCredentialUseConflict = errors.New("credential-use: the object changed while its record was written")

// CredentialUseCallTimeout bounds each Acquire and each BeforeEstablish, not the pass. An
// implementation that must bound how late a write of its can land counts from the START of the call,
// not from anything the call reads on the way: provider-runtime's Disown switch guards only the
// Secret's finalizer, and the write of the object's record comes before that read.
const CredentialUseCallTimeout = 30 * time.Second

// credentialUseMinRetry is the shortest Pending requeue, so a missing RetryAfter is not a hot loop.
const credentialUseMinRetry = time.Second

const (
	errCredentialUseAcquire   = "credential-use: cannot determine whether the object's credentials are held; not connecting" // #nosec G101 -- an error message
	errCredentialUseUnheld    = "credential-use: the object's credentials can't be held; not connecting"                     // #nosec G101 -- an error message
	errCredentialUseEstablish = "credential-use: cannot hold the object's credentials before adding the finalizer"           // #nosec G101 -- an error message

	// reasonCredentialUseDegraded matches the event reason provider-runtime uses for the same outcome.
	reasonCredentialUseDegraded event.Reason = "CredentialUseDegraded"

	// stepCredentialUse is the tracecontext.AttrStep value of a conflicting record write.
	stepCredentialUse = "credential_use"
)

// credentialUseHold is what a pass's Acquire left for the rest of the pass.
type credentialUseHold struct {
	// stop: the pass ends here, with the error acquireCredentialUse returned beside it.
	stop       bool
	acq        CredentialUseAcquisition
	retryAfter time.Duration
}

// finalizedDeleting reports whether el is being deleted and has already been finalized, so no handler
// call it can make is protected and no record it carries counts.
func (c *Controller) finalizedDeleting(el *unstructured.Unstructured) bool {
	return meta.WasDeleted(el) && !slices.Contains(el.GetFinalizers(), finalizerName) && !c.credentialUse.IsEstablished(el)
}

// acquireCredentialUse is the pass's one Acquire. It returns the context and the object the pass goes
// on with; with hold.stop, the pass returns err and makes NO external call. That includes the create
// recovery's existence check: a check that was not made must never be read as "does not exist", which
// would clear external-create-pending and create the resource a second time.
func (c *Controller) acquireCredentialUse(ctx context.Context, el *unstructured.Unstructured, ref objectref.ObjectRef, lg logging.Logger) (context.Context, *unstructured.Unstructured, credentialUseHold, error) {
	if c.credentialUse == nil || c.finalizedDeleting(el) {
		return ctx, el, credentialUseHold{}, nil
	}
	stopped := credentialUseHold{stop: true}

	acqCtx, cancel := context.WithTimeout(ctx, c.credentialUseTimeout)
	acq, err := c.credentialUse.Acquire(acqCtx, el)
	cancel()

	switch {
	case errors.Is(err, ErrCredentialUseConflict):
		return ctx, el, stopped, c.credentialUseConflict(ctx, err, lg)
	case err != nil:
		return ctx, el, stopped, c.refuseCredentialUse(ctx, el, errors.Wrap(err, errCredentialUseAcquire), lg)
	case acq.Pending:
		after := max(acq.RetryAfter, credentialUseMinRetry)
		lg.Debug("Not connecting yet: credential-use is waiting for a grant; requeueing", "requeueAfter", after.String())
		return ctx, el, stopped, &requeueAfter{after: after, keepPriority: true}
	case acq.MustNotConnect:
		cause := acq.Err
		if cause == nil {
			cause = errors.New(errCredentialUseUnheld)
		}
		return ctx, el, stopped, c.refuseCredentialUse(ctx, el, cause, lg)
	}

	if acq.Degraded {
		cause := acq.Err
		if cause == nil {
			cause = errors.New("credential-use: the object's credentials could not be held")
		}
		lg.Warn("Credential-use is degraded for this object; proceeding", "error", cause.Error())
		c.recordEvent(el, event.Warning(reasonCredentialUseDegraded, actionProcessEvent, cause))
	}
	if acq.Reread {
		fresh, ferr := c.fetch(ctx, ref, false)
		if apierrors.IsNotFound(ferr) || errors.Is(ferr, errLeftSelector) {
			lg.Debug("The object is gone or left this controller's selector while its credentials were acquired", "reason", ferr.Error())
			return ctx, el, stopped, nil
		}
		if ferr != nil {
			lg.Error(ferr, "Cannot fetch managed resource again after the credential-use record write")
			return ctx, el, stopped, ferr
		}
		// The pause, the orphan release and the finalized check were decided on the object read before
		// the Acquire. If the new read would have taken one of them, this pass must not act on it.
		if meta.IsPaused(fresh) || (meta.WasDeleted(fresh) && (!meta.WasDeleted(el) || !meta.ManagementDecision(fresh).May(managementpolicy.Delete))) {
			return ctx, el, stopped, c.credentialUseConflict(ctx, errors.Wrap(ErrCredentialUseConflict, "the object was paused or deleted meanwhile"), lg)
		}
		el = fresh
		tracePassFrom(ctx).observing(el)
	}
	return c.credentialUse.WithResolved(ctx, acq), el, credentialUseHold{acq: acq, retryAfter: acq.RetryAfter}, nil
}

// credentialUseConflict ends a pass whose record write lost a race: quietly, with nothing written.
func (c *Controller) credentialUseConflict(ctx context.Context, err error, lg logging.Logger) error {
	lg.Debug("The credential-use record write lost a race to a newer write; requeueing", "error", err.Error())
	tracePassFrom(ctx).event(tracecontext.EventConflict, tracecontext.AttrStep.String(stepCredentialUse))
	return err
}

// refuseCredentialUse records cause as Synced=False/ReconcileError and returns it, so the queue backs
// off. Ready is not written: nothing was learned about the external resource.
func (c *Controller) refuseCredentialUse(ctx context.Context, el *unstructured.Unstructured, cause error, lg logging.Logger) error {
	lg.Debug("Not connecting: the object's credentials are not held", "error", cause.Error())
	if err := setConditions(el, condition.ReconcileError(cause)); err != nil {
		lg.Error(err, "Cannot set condition")
		c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
		return err
	}
	if _, err := tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	}); err != nil {
		lg.Error(err, "Cannot update status")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
		return err
	}
	return cause
}

// beforeEstablish runs CredentialUse.BeforeEstablish. A non-nil error means the finalizer must not be
// added and the pass stops; it has been recorded.
func (c *Controller) beforeEstablish(ctx context.Context, el *unstructured.Unstructured, hold credentialUseHold, lg logging.Logger) error {
	if c.credentialUse == nil {
		return nil
	}
	callCtx, cancel := context.WithTimeout(ctx, c.credentialUseTimeout)
	err := c.credentialUse.BeforeEstablish(callCtx, el, hold.acq)
	cancel()
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrCredentialUseConflict):
		return c.credentialUseConflict(ctx, err, lg)
	default:
		return c.refuseCredentialUse(ctx, el, errors.Wrap(err, errCredentialUseEstablish), lg)
	}
}

// releaseOwnFinalizer removes this controller's finalizer from el in memory, and the credential-use
// record with it, so the caller's one write carries both. Every removal of finalizerName goes through
// here (TestCredentialUse_FinalizerRemovalSites): a finalizer removed without the record would leave a
// record nobody ever removes, and a record removed first would end the protection of an object whose
// external resource is not deleted yet.
func (c *Controller) releaseOwnFinalizer(el *unstructured.Unstructured) {
	if c.credentialUse != nil {
		c.credentialUse.Forget(el)
	}
	meta.RemoveFinalizer(el, finalizerName)
}

// dropStaleRecord removes the record of an object that is being deleted without this controller's
// finalizer, in a write of its own: the write that removed the finalizer did not (an older process, a
// person). It returns the object the pass goes on with, and whether the pass stops there.
func (c *Controller) dropStaleRecord(ctx context.Context, el *unstructured.Unstructured, lg logging.Logger) (*unstructured.Unstructured, bool, error) {
	if c.credentialUse == nil || !c.finalizedDeleting(el) || !c.credentialUse.Forget(el) {
		return el, false, nil
	}
	updated, err := tools.Update(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	})
	switch {
	case err == nil:
		return updated, false, nil
	case apierrors.IsNotFound(err):
		return el, true, nil // the object is gone, and its record with it
	case apierrors.IsConflict(err):
		return el, true, c.credentialUseConflict(ctx, errors.Wrap(ErrCredentialUseConflict, err.Error()), lg)
	default:
		lg.Error(err, "Cannot remove the credential-use record")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
		return el, true, err
	}
}

// requeue applies RetryAfter to a pass's result. A pass that succeeded runs again after it, as an
// OBSERVE whatever event it ran: re-running a Create that succeeded would create again. One that
// already asked to run again takes the sooner of the two. A failure, a deferral and a conflict keep
// their own requeue, and a Delete that succeeded released the finalizer, so it has no hold left.
func (h credentialUseHold) requeue(err error, evt ctrlevent.Event) error {
	if h.retryAfter <= 0 {
		return err
	}
	var again *requeueAfter
	switch {
	case err == nil && evt.EventType != ctrlevent.Delete:
		return &requeueAfter{
			after:    h.retryAfter,
			priority: NormalPriority,
			next:     &ctrlevent.Event{EventType: ctrlevent.Observe, ObjectRef: evt.ObjectRef, QueuedAt: time.Now()},
		}
	case errors.As(err, &again) && again.after > h.retryAfter:
		sooner := *again
		sooner.after = h.retryAfter
		return &sooner
	}
	return err
}

// Enqueue queues a pass for the object key names and reports whether this controller serves it: its
// informer delivered the object (namespace, label selector and API version included). It is how a
// CredentialUse implementation asks for a pass, and it never blocks. An object the informer has not
// delivered yet is queued when it does, so false loses nothing; a process that hosts several
// controllers of one kind can offer the key to each.
//
// The event is the one a periodic resync queues for the object, and dedups against it.
func (c *Controller) Enqueue(key types.NamespacedName) bool {
	k := key.Name
	if key.Namespace != "" {
		k = key.Namespace + "/" + key.Name
	}
	obj, ok, err := c.store.GetByKey(k)
	if err != nil || !ok {
		return false
	}
	el, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	action := ctrlevent.CRObserved
	if meta.WasDeleted(el) {
		action = ctrlevent.CRDeleted
	}
	item := ctrlevent.Event{
		EventType: c.actionsEvent.GetEventType(action),
		ObjectRef: objectref.ObjectRef{
			APIVersion: el.GetAPIVersion(),
			Kind:       el.GetKind(),
			Name:       el.GetName(),
			Namespace:  el.GetNamespace(),
		},
		QueuedAt: time.Now(),
	}
	if _, loaded := c.items.LoadOrStore(ctrlevent.DigestForEvent(item), struct{}{}); !loaded {
		c.queue.AddWithOpts(priorityqueue.AddOpts{Priority: NormalPriority}, item)
	}
	return true
}
