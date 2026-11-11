package controller

import (
	"context"
	"fmt"
	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	"k8s.io/client-go/util/retry"
	"time"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/plumbing/tracecontext"
	contextutils "github.com/krateo-platformops/unstructured-runtime/pkg/context"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/errors"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/krateo-platformops/unstructured-runtime/pkg/telemetry"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools"
	unstructuredtools "github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured"
	"github.com/krateo-platformops/unstructured-runtime/pkg/tools/unstructured/condition"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"slices"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	runtimeUtil "k8s.io/apimachinery/pkg/util/runtime"
)

const (
	finalizerName = "composition.krateo.io/finalizer"
)

// Event reasons.
const (
	reasonCannotInitialize  event.Reason = "CannotInitializeManagedResource"
	reasonCannotResolveRefs event.Reason = "CannotResolveResourceReferences"
	reasonCannotObserve     event.Reason = "CannotObserveExternalResource"
	reasonCannotCreate      event.Reason = "CannotCreateExternalResource"
	reasonCannotDelete      event.Reason = "CannotDeleteExternalResource"
	// reasonReconcileDeferred: the rate-limit governor declined this pass. Normal, not Warning —
	// nothing is wrong with the resource, and a Warning would page someone over a server's quota.
	reasonReconcileDeferred   event.Reason = "ReconcileDeferred"
	reasonCannotPublish       event.Reason = "CannotPublishConnectionDetails"
	reasonCannotUnpublish     event.Reason = "CannotUnpublishConnectionDetails"
	reasonCannotUpdate        event.Reason = "CannotUpdateExternalResource"
	reasonCannotUpdateManaged event.Reason = "CannotUpdateManagedResource"
	reasonCannotSetConditions event.Reason = "CannotSetConditions"
	reasonCannotFetchManaged  event.Reason = "CannotFetchManagedResource"

	reasonDeleted event.Reason = "DeletedExternalResource"
	reasonCreated event.Reason = "CreatedExternalResource"
	reasonUpdated event.Reason = "UpdatedExternalResource"
	reasonPending event.Reason = "PendingExternalResource"
)

// Event actions.
const (
	actionProcessEvent            event.Action = "ProcessEvent"
	actionCreateEvent             event.Action = "CreateEvent"
	actionUpdateEvent             event.Action = "UpdateEvent"
	actionDeleteEvent             event.Action = "DeleteEvent"
	actionObserveEvent            event.Action = "ObserveEvent"
	actionCreateExternalResource  event.Action = "CreateExternalResource"
	actionUpdateExternalResource  event.Action = "UpdateExternalResource"
	actionDeleteExternalResource  event.Action = "DeleteExternalResource"
	actionObserveExternalResource event.Action = "ObserveExternalResource"

	actionFetchManagedResource  event.Action = "FetchManagedResource"
	actionUpdateManagedResource event.Action = "UpdateManagedResource"
)

// Error strings.
const (
	errGetManaged                = "cannot get managed resource"
	errUpdateManagedAnnotations  = "cannot update managed resource annotations"
	errCreateIncomplete          = "cannot determine creation result - remove the " + meta.AnnotationKeyExternalCreatePending + " annotation if it is safe to proceed"
	errReconcileConnect          = "connect failed"
	errReconcileObserve          = "observe failed"
	errReconcileCreate           = "create failed"
	errReconcileUpdate           = "update failed"
	errReconcileDelete           = "delete failed"
	errExternalResourceNotExist  = "external resource does not exist"
	errCreateOrUpdateSecret      = "cannot create or update connection secret"
	errUpdateManaged             = "cannot update managed resource"
	errUpdateManagedStatus       = "cannot update managed resource status"
	errResolveReferences         = "cannot resolve references"
	errUpdateCriticalAnnotations = "cannot update critical annotations"
)

// externalCreateRecoveryGracePeriod bounds how long after an external Create attempt the
// incomplete-create recovery keeps waiting for the resource to become observable before it
// concludes the create never landed and recreates. It guards against duplicating a resource that
// DID get created but is not yet visible in an eventually-consistent external API.
const externalCreateRecoveryGracePeriod = 2 * time.Minute

// externalCreateRecoveryReobserveInterval is how soon to re-observe while still within the grace
// period. A var (not const) only so tests can drive the requeue without a real delay.
var externalCreateRecoveryReobserveInterval = 15 * time.Second

// externalResourceExists answers whether the external resource exists, for incomplete-create
// recovery only.
//
// It prefers an ExistenceChecker when the client implements one, because that is the narrow
// question recovery needs. Observe is the fallback, and it is a poor proxy: it also reports
// convergence, so a client whose Observe does repair work (a helm reconcile, say) returns an error
// whenever convergence fails — for reasons that say nothing about whether the create landed.
// Recovery then treats that as "cannot determine the result" and refuses forever, which wedges the
// resource and stops the reconcile that would have fixed the convergence failure in the first
// place. See ExistenceChecker for the case that motivated this.
func (c *Controller) externalResourceExists(ctx context.Context, el *unstructured.Unstructured) (bool, error) {
	if c.externalClient == nil {
		return false, errors.New("no external client registered")
	}

	if checker, ok := c.externalClient.(ExistenceChecker); ok {
		return checker.Exists(ctx, el)
	}

	obs, err := c.externalClient.Observe(ctx, el)
	if err != nil {
		return false, err
	}
	return obs.ResourceExists, nil
}

func (c *Controller) recordMetric(evt ctrlevent.Event, operation string, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}

	reconcileTotal.WithLabelValues(
		evt.ObjectRef.Kind,
		evt.ObjectRef.Namespace,
		result,
	).Inc()
}

// statusConflict reports a pass that ended because its status write lost a race to a newer write
// (tools.ErrStatusConflict, core-provider#359). It is NOT a failure. The pass computed its status
// from a read the conflict proved stale, so the right response is to recompute, not to record an
// error. It is handled like a deferral: no ReconcileError, no event, no failure metric, no
// maxRetries budget spent, and a prompt requeue so the next pass reads the newer write.
func statusConflict(err error) bool { return errors.Is(err, tools.ErrStatusConflict) }

// statusConflictRequeue is how soon a pass that lost a status race runs again. Short, because nothing
// is wrong: the object just changed under it.
const statusConflictRequeue = time.Second

// passOutcome is how a pass is COUNTED, which is a separate question from how it is requeued.
type passOutcome int

const (
	outcomeSuccess passOutcome = iota
	outcomeDeferred
	outcomeStatusConflict
	outcomeFailure
)

// classifyOutcome decides which metric a pass increments. A DEFERRAL IS A REQUEUE-AFTER, NOT A
// FAILURE: counting it as one would show a failure spike on every dashboard for as long as a server
// is rate-limiting us, telling an operator things are breaking when nothing is. Extracted from
// runWorker so the decision is testable without a running worker.
func classifyOutcome(err error) passOutcome {
	var deferred *ratelimit.DeferredError
	switch {
	case err == nil:
		return outcomeSuccess
	case errors.As(err, &deferred):
		return outcomeDeferred
	case statusConflict(err):
		return outcomeStatusConflict
	default:
		return outcomeFailure
	}
}

func (c *Controller) runWorker(ctx context.Context) {
	// Generate unique worker ID for tracking
	workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)

	for c.processNext(ctx, workerID) {
	}
}

// acquireLocal takes a local capacity slot, waiting for one. It waits on the context Run was given,
// not on ctx: reconcileCtx stays live through a drain, so a worker with no slot would hang the drain.
func (c *Controller) acquireLocal() (func(), bool) {
	gate, ok := c.governor.(ratelimit.LocalGate)
	if !ok {
		return func() {}, true
	}
	runCtx := c.runCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	return gate.AcquireLocal(runCtx)
}

// processNext runs one item, holding a local slot from before the pop until it is done, on every
// path out. It reports whether the worker should carry on.
func (c *Controller) processNext(ctx context.Context, workerID string) bool {
	release, ok := c.acquireLocal()
	if !ok {
		c.logger.Info("Shutting down worker", "workerID", workerID)
		return false
	}
	defer release()

	obj, priority, shutdown := c.queue.GetWithPriority()
	if shutdown {
		c.logger.Info("Shutting down worker", "workerID", workerID)
		return false
	}

	ev, ok := obj.(ctrlevent.Event)
	if !ok {
		c.queue.Forget(obj)
		c.queue.Done(obj)
		runtimeUtil.HandleError(fmt.Errorf("unexpected object in queue: %v", obj))
		if c.metrics != nil {
			c.metrics.AddReconcileInFlight(-1)
		}
		return true
	}

	if c.metrics != nil {
		c.metrics.AddReconcileInFlight(1)
	}

	dig := ctrlevent.DigestForEvent(ev)

	// Start timing for processing
	processingStartTime := time.Now()
	queueLength := c.queue.Len()

	// Calculate queue wait time
	var queueWaitDuration time.Duration
	if !ev.QueuedAt.IsZero() {
		queueWaitDuration = processingStartTime.Sub(ev.QueuedAt)
		if c.metrics != nil {
			c.metrics.RecordQueueWaitDuration(ctx, queueWaitDuration)
		}
	}

	traceId, err := shortid.Generate()
	if err != nil {
		runtimeUtil.HandleError(fmt.Errorf("cannot generate trace ID: %v", err))
		if c.metrics != nil {
			c.metrics.AddReconcileInFlight(-1)
		}
		return true
	}
	ctx = contextutils.BuildContext(ctx, contextutils.WithLogger(c.logger), contextutils.WithTraceId(traceId))

	// ONE PASS PER OBJECT (#62). Another worker already holds this object, so park the event; it is
	// requeued when that pass ends. Its `items` digest stays stored while parked, so further events
	// of the same type keep deduplicating against it. Claimed here, after every early `continue`
	// above, so no path leaves an object held.
	if !c.objects.begin(ev, priority) {
		c.queue.Done(ev)
		if c.metrics != nil {
			c.metrics.AddReconcileInFlight(-1)
		}
		return true
	}
	err = c.processItem(ctx, ev)

	// End timing and calculate durations
	processingEndTime := time.Now()
	processingDuration := processingEndTime.Sub(processingStartTime)

	var totalDuration time.Duration
	if !ev.QueuedAt.IsZero() {
		totalDuration = processingEndTime.Sub(ev.QueuedAt)
	} else {
		totalDuration = processingDuration
	}
	if c.metrics != nil {
		c.metrics.RecordQueueWorkDuration(ctx, processingDuration)
		c.metrics.RecordReconcileDuration(ctx, totalDuration)
		switch classifyOutcome(err) {
		case outcomeSuccess:
			c.metrics.IncReconcileSuccess(ctx)
		case outcomeDeferred, outcomeStatusConflict:
			c.metrics.IncReconcileRequeueAfter(ctx)
		default:
			c.metrics.IncReconcileFailure(ctx)
			c.metrics.IncReconcileErrorRequeue(ctx)
		}
	}

	// Enhanced debug logging
	c.logger.WithValues(
		"kind", ev.ObjectRef.Kind,
		"apiVersion", ev.ObjectRef.APIVersion,
		"name", ev.ObjectRef.Name,
		"namespace", ev.ObjectRef.Namespace,
		"queuedAt", ev.QueuedAt,
		"traceId", contextutils.TraceId(ctx, false),
	).Debug("Event processed",
		"priority", priority,
		"type", ev.EventType,
		"queueWaitTime", queueWaitDuration.String(),
		"processingTime", processingDuration.String(),
		"totalTime", totalDuration.String(),
		"success", err == nil,
		"workerID", workerID,
		"queueLength", queueLength,
	)

	c.items.Delete(dig)
	// FORGET ON SUCCESS ONLY (#51). Forget is how a limiter is told "this item recovered"; it
	// deletes the item's failure record. Calling it unconditionally told the limiter that EVERY
	// item was healthy, including the one that had just failed, and handleErr reads
	// NumRequeues immediately afterwards:
	//
	//	NumRequeues(obj) >= maxRetries   was 0 >= maxRetries on every pass, never true,
	//	                                 so the drop NEVER fired and an unprocessable event
	//	                                 was retried forever with maxRetries configured and
	//	                                 silently ignored
	//	When(obj)                        also saw a zero count, so the builder default's
	//	                                 exponential never climbed past its 3s base
	//
	// Both of this repo's in-house consumers were immune by accident rather than design: the
	// cdc and rest-dynamic-controller override with NewExponentialTimedFailureRateLimiter,
	// whose Forget only deletes after 2*maxDelay of idleness, so an unconditional Forget right
	// after processing is a no-op for an actively-retrying item. Anything on the builder
	// default was affected.
	//
	// NOT MOVED AFTER handleErr, which is the fix that looks complete and is not: the
	// NumRequeues check would then be correct while the count was still wiped afterwards, so
	// the NEXT pass would start from zero and the backoff half would stay broken.
	if err == nil {
		c.queue.Forget(ev)
	}
	c.queue.Done(ev)
	c.handleErr(err, ev, priority)
	for _, p := range c.objects.end(ev) {
		c.queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: false, Priority: p.priority}, p.event)
	}
	if c.metrics != nil {
		c.metrics.AddReconcileInFlight(-1)
	}
	return true
}

func (c *Controller) handleErr(err error, obj ctrlevent.Event, priority int) {
	if err == nil {
		return
	}

	// A DEFERRAL IS HANDLED FIRST, AHEAD OF THE maxRetries CHECK, and differently from a failure in
	// every respect that matters:
	//
	//	delay      the governor's — the server's own deadline when throttled, a short jitter when we
	//	           are merely at capacity — never the failure limiter's exponential
	//	limiter    NOT rate-limited. AddRateLimited would count the deferral toward NumRequeues and
	//	           grow the item's backoff, so a global condition ("the endpoint is busy") would
	//	           inflate a per-item one ("this item is failing") and could even hit maxRetries
	//	           and DROP an event that had nothing wrong with it
	//	priority   kept, so a deferred high-priority event is not demoted by being deferred
	//	Forget     not called by runWorker, because err is non-nil (#52), so a deferral leaves the
	//	           item's genuine failure history exactly as it found it
	var deferred *ratelimit.DeferredError
	if errors.As(err, &deferred) {
		retryEvent := obj
		if retryEvent.QueuedAt.IsZero() {
			retryEvent.QueuedAt = time.Now()
		}
		dig := ctrlevent.DigestForEvent(retryEvent)
		if _, loaded := c.items.Load(dig); !loaded {
			c.items.Store(dig, struct{}{})
			c.queue.AddWithOpts(priorityqueue.AddOpts{
				After:       deferred.After,
				RateLimited: false,
				Priority:    priority,
			}, obj)
		}
		return
	}

	// A LOST STATUS RACE IS REQUEUED QUIETLY, also ahead of the maxRetries check, for the same reasons
	// as a deferral: it says nothing about the item, so it must not grow the item's backoff or spend
	// its retry budget (core-provider#359).
	if statusConflict(err) {
		retryEvent := obj
		if retryEvent.QueuedAt.IsZero() {
			retryEvent.QueuedAt = time.Now()
		}
		dig := ctrlevent.DigestForEvent(retryEvent)
		if _, loaded := c.items.Load(dig); !loaded {
			c.items.Store(dig, struct{}{})
			c.queue.AddWithOpts(priorityqueue.AddOpts{
				After:       statusConflictRequeue,
				RateLimited: false,
				Priority:    priority,
			}, obj)
		}
		return
	}

	if c.queue.NumRequeues(obj) >= c.maxRetries {
		c.logger.WithValues("retries", c.queue.NumRequeues(obj), "queueLenght", c.queue.Len()).Debug("dropping event out of the queue, max retries reached", "error", err.Error())
		return
	}

	// Preserve original queue timestamp for retry
	retryEvent := obj
	if retryEvent.QueuedAt.IsZero() {
		retryEvent.QueuedAt = time.Now() // Set timestamp if missing
	}

	dig := ctrlevent.DigestForEvent(retryEvent)
	if _, loaded := c.items.Load(dig); !loaded {
		c.logger.WithValues(
			"retries", c.queue.NumRequeues(obj),
			"kind", obj.ObjectRef.Kind,
			"apiVersion", obj.ObjectRef.APIVersion,
			"name", obj.ObjectRef.Name,
			"namespace", obj.ObjectRef.Namespace).
			Debug("handling event, requequeing", "error", err)
		c.items.Store(dig, struct{}{})
		c.queue.AddWithOpts(priorityqueue.AddOpts{
			RateLimited: true, // Always rate limit retries to avoid overwhelming the controller
			Priority:    priority,
		}, obj)
	}
}

func (c *Controller) processItem(ctx context.Context, obj interface{}) (err error) {
	lg := contextutils.Logger(ctx).WithValues("traceId", contextutils.TraceId(ctx, false))

	evt, ok := obj.(ctrlevent.Event)
	if !ok {
		lg.Warn("Unexpected event in queue, skipping", "object", obj)
		return nil
	}

	lg = lg.WithValues(
		"kind", evt.ObjectRef.Kind,
		"apiVersion", evt.ObjectRef.APIVersion,
		"name", evt.ObjectRef.Name,
		"namespace", evt.ObjectRef.Namespace,
		"queuedAt", evt.QueuedAt,
	)

	ctx = contextutils.BuildContext(ctx, contextutils.WithLogger(lg))
	traceKey := types.NamespacedName{Namespace: evt.ObjectRef.Namespace, Name: evt.ObjectRef.Name}
	el, err := c.fetch(ctx, evt.ObjectRef, false)
	if err != nil {
		lg.Debug("Cannot fetch managed resource", "err", err) // This is returned as debug as debug info. This can occur normally if the resource was deleted.
		if apierrors.IsNotFound(err) {
			// The object is gone: its trace memo entry goes with it, tracing on or off (#77).
			c.traces.forget(traceKey)
		}
		// if the object is not found, we will not retry to process it and we not throw an error
		return nil
	}

	// The reconcile root (#77, plumbing tracecontext, provider-runtime's D8). Every pass is a NEW
	// root, linked to el's krateo.io/traceparent when it carries one; only the first act for a new
	// value joins the upstream trace (beginAct around Create/Update/Delete, or the handler's
	// StartAct inside Observe). With tracing off the
	// tracer is nil: nothing starts, and the inbound context is kept for InjectTraceparent only. The
	// deferred RecordSpanError reads the named return value.
	tracer := c.reconcileTracer()
	decision := c.traces.decide(tracer != nil, traceKey, el, time.Now(), tracecontext.DefaultContinueWindow)
	traceObj := tracecontext.Object{Kind: el.GetKind(), Name: el.GetName(), Namespace: el.GetNamespace()}
	ctx, span := tracecontext.StartReconcile(ctx, tracer, traceObj, decision, time.Now())
	pass := offPass
	if tracer != nil {
		pass = &tracePass{tracer: tracer, span: span, d: decision, obj: traceObj, key: traceKey, uid: el.GetUID(), memo: &c.traces, c: c, target: el}
	}
	ctx = withTracePass(ctx, pass)
	defer func() {
		telemetry.RecordSpanError(span, err)
		span.End()
	}()

	if meta.IsPaused(el) {
		// #78. The root is still emitted, because the controller IS alive; the shared event is what
		// tells a paused object from a freshly reconciled one in the traces (the same bytes
		// provider-runtime records).
		pass.event(tracecontext.EventReconcilePaused)
		log := lg.WithValues("annotation", meta.AnnotationKeyReconciliationPaused)

		log.Debug("Reconciliation is paused via the pause annotation")
		c.recordEvent(el, event.Normal(reasonReconciliationPaused, actionProcessEvent, "Reconciliation is paused via the pause annotation"))
		// BOTH conditions (core-provider#241). ReconcilePaused sets only Synced, so Ready kept
		// whatever the handler wrote before the pause — measured on krateo-057 as
		// Ready=True/Available sitting beside Synced=False/ReconcilePaused on a composition carrying
		// krateo.io/paused. A consumer reading Ready could not tell a paused resource from a
		// converging one, and 66 of 185 compositions on one production estate are in that state.
		//
		// This is the one place the runtime speaks for Ready. Elsewhere a handler owns the types it
		// returns and the runtime owns only Synced — but here the runtime short-circuits BEFORE
		// calling the handler, so there is no verdict to defer to and nothing else can refresh Ready.
		//
		// ReadyPaused keeps Status=True deliberately: a paused resource is still available, it is
		// merely not converging. Flipping it to False would make a deliberate pause look like an
		// outage and could stall every dependent — that is the verdict change #241 leaves open, and
		// it is not this.
		err = unstructuredtools.SetConditions(el, condition.ReconcilePaused(), condition.ReadyPaused())
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err := tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, err))
			return err
		}
		// if the pause annotation is removed, we will have a chance to reconcile again and resume
		// and if status update fails, we will reconcile again to retry to update the status
		return nil
	}

	// If managed resource has a deletion timestamp and a deletion policy of
	// Orphan, we do not need to observe the external resource before attempting
	// to remove finalizer.
	if meta.WasDeleted(el) && !meta.ShouldDelete(el) {
		log := lg.WithValues("deletion-timestamp", el.GetDeletionTimestamp())

		if slices.Contains(el.GetFinalizers(), finalizerName) {
			meta.RemoveFinalizer(el, finalizerName)
			el, err = tools.Update(ctx, el, tools.UpdateOptions{
				Pluralizer:    c.pluralizer,
				DynamicClient: c.dynamicClient,
			})
			if err != nil {
				log.Error(err, "Cannot remove finalizer")
				c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
				return err
			}
			tracePassFrom(ctx).forget() // the finalizer is gone, and the memo entry with it (#77)
		}
		// The policy is "do not touch the external resource", so this pass is DONE. Without this
		// return, execution falls through to the dispatch below with WasDeleted(el) still true —
		// and a pending delete is enqueued as EventType Delete (controller.go, UpdateFunc: "the
		// resource has a deletion timestamp but was not effectively deleted yet"). That lands in
		// handleDelete, whose fetch is a LIVE apiserver Get, and then calls externalClient.Delete:
		// the external resource is destroyed on the very pass that honoured the orphan policy.
		//
		// Whether it was destroyed depended on whether the object had already been reaped, which
		// is decided by finalizer MULTIPLICITY rather than by timing. Measured over 400 trials on
		// two apiserver versions (kind v1.35.0 and GKE v1.37.1, identical results):
		//
		//   orphan, this finalizer alone   0/160  — removing the last finalizer completes the
		//                                           deletion inside that same update request, so
		//                                           the fetch 404s and Delete is never reached
		//   orphan + any other finalizer   40/40  — the object survives our removal, the fetch
		//                                           succeeds, and the external resource is deleted
		//
		// No gradient and no window to widen: completely safe, or completely unsafe, decided by a
		// precondition nothing enforces. See krateo-platformops/unstructured-runtime#23.
		return nil
	}

	// If we started but never completed creation of an external resource we may
	// have lost critical information (e.g. an unpersisted external name). The
	// historically "safe" action was to refuse to proceed, but that refuses
	// forever: it permanently wedges the resource — and everything that depends
	// on it — whenever a create is interrupted before the success marker is
	// persisted (a process restart, an API timeout, a transient crash). Instead,
	// reconcile the create-tracking annotations against reality by observing the
	// external resource:
	//   - observe errors     => we genuinely cannot determine the result; keep the
	//                           conservative refuse so we never risk leaking.
	//   - resource is absent  => the create never landed; clear the pending marker
	//                           and proceed with a fresh create.
	//   - resource is present => the create actually succeeded but we lost the
	//                           success marker; record success and proceed.
	// RATE-LIMIT GATE (docs/design/rate-limit-governor.md). Here and only here, deliberately:
	//
	//   AFTER every path that makes no external call. The pause short-circuit and the Orphan
	//   finalizer release above talk only to the apiserver; gating them would delay a deletion
	//   with nothing to throttle.
	//   BEFORE the first call into the handler — externalResourceExists immediately below, then
	//   the handleObserve/Create/Update/Delete dispatch. Measured: all five of those entry points,
	//   plus the ExistenceChecker path, are reached ONLY from processItem, so this one gate covers
	//   every way into the handler. A gate at each call site would be five places to keep in step,
	//   and the plumbing dialguard enumeration showed what that costs: a check placed where a
	//   mechanism already existed covered two paths of five and looked complete.
	//
	// The AST guard in ratelimit_wiring_test.go fails if a handler call ever appears ahead of it.
	ctx, release, deferral := c.acquireRateLimit(ctx, el)
	if deferral != nil {
		return c.deferPass(ctx, el, deferral)
	}
	defer release()

	if meta.ExternalCreateIncomplete(el) {
		resourceExists, obsErr := c.externalResourceExists(ctx, el)

		if obsErr != nil {
			// obsErr REACHES ALL THREE CHANNELS, and it did not used to.
			//
			// It was assigned and then referenced exactly once, in the test above — a grep for it
			// returned two lines, the assignment and the `!= nil`. So every reason an existence
			// check can fail collapsed into one fixed sentence, in the log, the event AND the
			// condition. Found by the oasgen-provider session, read in source and confirmed here.
			//
			// WHY THAT IS WORSE THAN A MISSING %w. rest-dynamic-controller's Exists distinguishes
			// EIGHT reasons it cannot answer, each implying a different operator action:
			// observation delegated to a RESTAction, an async operation still in flight, a Pending
			// condition, a kind declaring no read verb, unresolved path parameters, a synthesised
			// 404 (a list that succeeded and matched nothing), a transport/5xx failure, and a
			// client-build failure. All eight arrived as errCreateIncomplete.
			//
			// And errCreateIncomplete's own remediation is "remove the annotation if it is safe to
			// proceed" — while the reason that would let anyone judge "if it is safe" was the thing
			// being discarded. This branch deliberately refuses to proceed so it can never leak a
			// resource; the refusal is right, and it was silent about why.
			//
			// The condition matters most of the three: charts and gates read a CR condition, not
			// our logs. It keeps the existing remediation advice and adds the cause.
			if statusConflict(obsErr) {
				lg.Debug("Status write lost a race to a newer write; requeueing to recompute", "error", obsErr.Error())
				return obsErr
			}
			wrapped := fmt.Errorf("%s: %w", errCreateIncomplete, obsErr)
			lg.Warn(errCreateIncomplete, "error", obsErr.Error())
			c.recordEvent(el, event.Warning(reasonCannotInitialize, actionProcessEvent, wrapped))

			err = unstructuredtools.SetConditions(el,
				condition.Creating(),
				condition.ReconcileError(wrapped),
			)
			if err != nil {
				lg.Error(err, "Cannot set condition")
				c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
				return err
			}

			el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
				Pluralizer:    c.pluralizer,
				DynamicClient: c.dynamicClient,
			})
			if err != nil {
				lg.Error(err, "Cannot update status")
				c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
				return err
			}

			return nil
		}

		// The existence question was answered: reconcile the create-tracking annotations with the
		// observed reality, then fall through to normal processing.
		// Which annotation mutation to apply, captured as a closure rather than applied here.
		// #28: the write below used to be a bare tools.Update on `el` — the object fetched BEFORE
		// externalResourceExists ran. That call falls through to the client's Observe when the
		// client does not implement ExistenceChecker, and a client whose Observe writes (the status
		// subresource is enough) bumps resourceVersion server-side. `el` is then stale, the write
		// conflicts, and because the error returns BEFORE the dispatch below, no Create/Update ever
		// runs: the resource stays external-create-pending forever and its spec is never pushed.
		// Measured on rest-dynamic-controller: 51 consecutive conflicts, 0 recoveries, in 20 minutes.
		//
		// The mutation must be re-applied to a FRESHLY FETCHED object on each attempt. Retrying the
		// stale `el` would succeed and clobber whatever the Observe just wrote, trading a wedge for
		// data loss — so a retry that reuses the object is worse than no retry at all.
		var mutate func(*unstructured.Unstructured)

		if resourceExists {
			mutate = func(o *unstructured.Unstructured) { meta.SetExternalCreateSucceeded(o, time.Now()) }
		} else if meta.ExternalCreatePendingDuring(el, externalCreateRecoveryGracePeriod) {
			// Confirm-the-negative before recreating: an external Create that DID land may not yet be
			// visible in an eventually-consistent external API. While the create attempt is still
			// within the recovery grace period, DO NOT clear the pending marker or recreate (that
			// would leak a duplicate external resource). Requeue to re-observe; once the resource
			// becomes visible we record success, and only once the grace period has fully elapsed is a
			// still-absent resource treated as a genuine miss to be recreated.
			lg.Info("incomplete external create: resource not yet observed, waiting out recovery grace period before recreating",
				"pendingAge", time.Since(meta.GetExternalCreatePending(el)).String(),
				"gracePeriod", externalCreateRecoveryGracePeriod.String())
			// LowPriority: this is a patient background re-observe, not a user-urgent event — it must
			// not crowd out genuine user Update/Delete work during a mass restart-mid-create.
			c.queue.AddWithOpts(priorityqueue.AddOpts{
				After:    externalCreateRecoveryReobserveInterval,
				Priority: LowPriority,
			}, ctrlevent.Event{
				EventType: evt.EventType,
				ObjectRef: evt.ObjectRef,
				QueuedAt:  time.Now(),
			})
			return nil
		} else {
			mutate = func(o *unstructured.Unstructured) {
				meta.RemoveAnnotations(o, meta.AnnotationKeyExternalCreatePending)
			}
		}

		if rerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh, ferr := c.fetch(ctx, evt.ObjectRef, false)
			if ferr != nil {
				return ferr
			}
			mutate(fresh)
			updated, uerr := tools.Update(ctx, fresh, tools.UpdateOptions{
				Pluralizer:    c.pluralizer,
				DynamicClient: c.dynamicClient,
			})
			if uerr != nil {
				return uerr
			}
			el = updated
			return nil
		}); rerr != nil {
			lg.Error(rerr, "Cannot reconcile external-create annotations after incomplete create")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionProcessEvent, rerr))
			return rerr
		}
		lg.Info("Recovered from incomplete external create", "resourceExists", resourceExists)
	}

	if !meta.WasDeleted(el) {
		finalizers := el.GetFinalizers()
		exist := slices.Contains(finalizers, finalizerName)

		if !exist {
			el.SetFinalizers(append(finalizers, finalizerName))
			el, err = tools.Update(ctx, el, tools.UpdateOptions{
				Pluralizer:    c.pluralizer,
				DynamicClient: c.dynamicClient,
			})
			if err != nil {
				lg.Error(err, "Cannot add finalizer")
				c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
				return err
			}
		}
	}

	// #77: a value whose continue window has closed is marked NOW: past the pause check and every
	// path that releases the object without acting, and before anything acts. A conflict ends the
	// pass before it acts; the worker requeues it quietly.
	if merr := c.writeMarkerIf(ctx, el, tracePassFrom(ctx), tracecontext.Now); merr != nil {
		return merr
	}

	lg.Debug("Start processing")
	switch evt.EventType {
	case ctrlevent.Create:
		// WasDeleted gates the finalizer-ADD above; it never gated this dispatch. So a Create
		// queued before the object acquired a deletionTimestamp — or requeued after a restart —
		// was still dispatched, and handleCreate called externalClient.Create on an object that is
		// being deleted. Observed on a live cluster: THREE creates after the deletionTimestamp,
		// interleaved with the delete attempts, because it re-dispatches for as long as the delete
		// is in flight rather than once.
		//
		// Harmless only while the external API refuses the create. When it SUCCEEDS the sequence is
		// create succeeds -> delete completes -> finalizer released -> CR reaped -> the external
		// resource remains with nothing referencing it, and its identifier may never have been
		// persisted because the create's status write races the deletion. On billable resources
		// that is an unbounded leak, one orphan per reconcile, that no later reconcile can notice
		// because the object that would have pointed at it is gone.
		//
		// Dropped rather than requeued: the object is going away, so there is no later moment at
		// which creating it becomes correct. Deliberately narrow — Update and Observe are left
		// dispatching, because an update during deletion is wasteful rather than destructive and
		// the create-recovery path above intentionally reconciles objects in odd states.
		if meta.WasDeleted(el) {
			lg.Debug("Dropping a Create event for an object that is being deleted; creating an "+
				"external resource now would orphan it once the object is reaped",
				"deletion-timestamp", el.GetDeletionTimestamp())
			return nil
		}
		err = c.handleCreate(ctx, evt.ObjectRef)
		c.recordMetric(evt, "create", err)
		return err
	case ctrlevent.Update:
		err = c.handleUpdate(ctx, evt.ObjectRef)
		c.recordMetric(evt, "update", err)
		return err
	case ctrlevent.Delete:
		err = c.handleDelete(ctx, evt.ObjectRef)
		c.recordMetric(evt, "delete", err)
		return err
	default:
		err = c.handleObserve(ctx, evt.ObjectRef)
		c.recordMetric(evt, "observe", err)
		return err
	}
}

// acquireRateLimit decides whether this pass may call the handler, and returns a context carrying
// the governor as the signal sink when it may.
//
// WITHOUT A GOVERNOR NOTHING IS GATED. With one, the pass always asks it, with the handler's key or
// with "" when there is none:
//
//	handler does not name a key        it never opted in (no ratelimit.RateLimitKeyer)
//	the key cannot be determined       an error or an empty key from RateLimitKey
//
// A keyless pass is not gated by any key's state, but IS gated by the governor's local capacity when
// saturation is on (#66): the controller's own CPU bounds every pass, keyed or not. Without
// saturation a keyless pass is not gated at all, exactly as before.
//
// AN UNDETERMINABLE KEY FAILS OPEN to keyless, and that is the opposite of plumbing's dialguard on purpose. dialguard is a
// security control, where "could not determine" must refuse: the cost of guessing wrong is a
// credential sent somewhere it should not go. This is a throttle. Failing closed on a key it cannot
// compute would stop every reconcile of the object for as long as the keyer is wrong — a wedge —
// while failing open costs at most the 429 the server will enforce anyway. Same question, opposite
// answer, because the two failures are not the same size.
//
// The sink is installed only when the pass is gated: a governor accumulating state for keys it can
// never gate would be memory with no purpose.
func (c *Controller) acquireRateLimit(ctx context.Context, el *unstructured.Unstructured) (context.Context, func(), *ratelimit.Deferral) {
	noop := func() {}
	if c.governor == nil {
		return ctx, noop, nil
	}
	key := ""
	if keyer, ok := c.externalClient.(ratelimit.RateLimitKeyer); ok {
		k, err := keyer.RateLimitKey(el)
		if err != nil {
			contextutils.Logger(ctx).Warn("Cannot determine the rate-limit key; proceeding without one (the local capacity still applies)", "err", err)
		} else {
			key = k
		}
	}
	release, d := c.governor.TryAcquire(key)
	if d != nil {
		return ctx, noop, d
	}
	if key == "" {
		// Gated by the local capacity at most: no key's state to feed, so no sink.
		return ctx, release, nil
	}
	return ratelimit.WithSink(ctx, c.governor), release, nil
}

// deferPass records that this pass was deferred and returns a DeferredError for the worker to
// requeue.
//
// SYNCED ONLY, READY UNTOUCHED. The handler did not run, so nothing was learned about the resource;
// Ready is the handler's finding, and overwriting it would repeat #45, where ObserveFailed
// overwrote a handler's own verdict. Synced=False is correct: nothing is converging this object
// right now, which is the semantic the pause conditions already carry.
//
// NOT SILENCE. A deferral that records nothing is indistinguishable from a pass that ran and found
// everything fine, so a long backoff would leave conditions stale while reporting healthy. The
// condition carries the deadline in its message, because "deferred" with no deadline becomes a lie
// through elapsed time alone.
//
// A FAILED STATUS WRITE IS RETURNED AS A REAL ERROR, not as a DeferredError. That keeps an invariant
// the tests can assert: every DeferredError the worker sees has already written its condition. The
// deferral is simply re-evaluated on the retry.
func (c *Controller) deferPass(ctx context.Context, el *unstructured.Unstructured, d *ratelimit.Deferral) error {
	log := contextutils.Logger(ctx).WithValues("rateLimitKey", d.Key, "reason", string(d.Reason))

	c.metrics.IncRateLimitDeferral(ctx, d.Key, d.Reason)

	after := c.governor.RequeueAfter(d)
	cond := condition.ReconcileDeferred(d.Key, d.Until)

	// AN UNCHANGED DEFERRAL IS NOT WRITTEN AGAIN. At capacity a key requeues every second or two
	// with the same message, and during a long throttle every requeue carries the same deadline; a
	// status patch per pass would spend API writes saying nothing new, against the apiserver of a
	// controller that is already being asked to slow down.
	if deferralAlreadyRecorded(el, cond) {
		log.Debug("Reconciliation deferred by the rate-limit governor (already recorded)", "requeueAfter", after.String())
		return &ratelimit.DeferredError{Deferral: d, After: after}
	}

	if err := unstructuredtools.SetConditions(el, cond); err != nil {
		log.Error(err, "Cannot set the deferral condition")
		c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
		return err
	}
	el, err := tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	})
	if err != nil {
		log.Error(err, "Cannot record the deferral on status")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateExternalResource, err))
		return err
	}

	log.Debug("Reconciliation deferred by the rate-limit governor", "requeueAfter", after.String())
	c.recordEvent(el, event.Normal(reasonReconcileDeferred, actionProcessEvent, cond.Message))
	return &ratelimit.DeferredError{Deferral: d, After: after}
}

// deferralAlreadyRecorded reports whether el's Synced condition already says exactly what cond says.
// Status, Reason AND Message, because a new server deadline changes only the message and must be
// written.
func deferralAlreadyRecorded(el *unstructured.Unstructured, cond metav1.Condition) bool {
	for _, have := range unstructuredtools.GetConditions(el) {
		if have.Type == cond.Type {
			return have.Status == cond.Status && have.Reason == cond.Reason && have.Message == cond.Message
		}
	}
	return false
}

func (c *Controller) handleObserve(ctx context.Context, ref objectref.ObjectRef) error {
	log := contextutils.Logger(ctx).WithValues("event", "observe").WithValues("traceId", contextutils.TraceId(ctx, false))

	if c.externalClient == nil {
		log.Warn("No event handler registered.")
		return nil
	}

	el, err := c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	// ONE DELETION DECISION PER PASS (#47), captured from this function's own and only read of the
	// object before anything can change underneath it.
	//
	// The error path below takes a SECOND fetch — deliberately, and it stays: it is the write
	// target for the conditions, and `el` has been handed to a handler by then, so writing onto it
	// risks a stale resourceVersion. The defect was never "two reads". It was that a DECISION and
	// a WRITE TARGET are different uses of an object: re-fetching to write is right, and
	// re-fetching and then DECIDING from the new object — when the dispatch above was chosen from
	// the old one — is the bug. A deletion timestamp landing between the two made the pass
	// internally inconsistent, with nothing to reconcile the halves.
	//
	// The invariant is provider-runtime's, which does not have this shape: it reads once
	// (pkg/reconciler/reconciler.go:775) and takes every deletion decision from that copy, so a
	// timestamp arriving mid-reconcile surfaces as a resourceVersion conflict on the status write,
	// requeues, and the next pass sees it consistently. "Read once" does not transfer here because
	// the write needs a fresh object; "every deletion decision in one pass comes from one object"
	// does.
	//
	// STILL OPEN, and narrower than this fix: processItem takes its own earlier read and dispatches
	// to this function on it, so a deletion landing between THAT read and this one leaves the same
	// kind of disagreement one level up. Closing it means passing the fact down rather than
	// re-deriving it, which changes this function's signature and seven call sites — recorded on
	// #47 rather than folded in here.
	deletedAtEntry := meta.WasDeleted(el)

	start := time.Now()
	// #77: Observe is NOT an act for the runtime. A handler whose Observe applies changes calls
	// StartAct before it does, which writes the marker on el and joins the upstream trace; a
	// marker conflict comes back as its error and requeues quietly below (statusConflict).
	tracePassFrom(ctx).observing(el)
	observation, actionErr := c.externalClient.Observe(ctx, el)
	if c.metrics != nil {
		c.metrics.RecordObserveDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncObserveFailure(ctx)
		}
	}
	if actionErr != nil {
		// A lost status race is not an error: requeue quietly, record nothing (#359).
		if statusConflict(actionErr) {
			log.Debug("Status write lost a race to a newer write; requeueing to recompute", "error", actionErr.Error())
			return actionErr
		}
		c.recordEvent(el, event.Warning(reasonCannotObserve, actionObserveExternalResource, actionErr))
		log.Error(actionErr, "Cannot observe external resource")

		e, err := c.fetch(ctx, ref, false)
		if err != nil {
			log.Error(err, "Cannot fetch managed resource")
			return err
		}

		// BOTH conditions, and Ready is UNKNOWN rather than left alone.
		//
		// This path used to write Synced only, so Ready kept whatever it last held — including
		// True/Available. A CR then reported ready for a resource nobody had successfully
		// observed, for as long as the observation kept failing. A stale True is silently wrong,
		// which is worse than an honest "nobody knows".
		//
		// Synced=ReconcileError STAYS, and that is a contract rather than redundancy: consumers
		// read Synced's reason to decide whether the last reconcile errored — oasgen's
		// lastReconcileErrored feeds their not-exists branch, and it fires for exactly this
		// condition. Writing Ready INSTEAD of Synced would remove a guard that works today.
		//
		// The UPDATE-failure path deliberately does NOT get this. There, Observe succeeded and
		// said not-up-to-date; the resource's health is unchanged and only the new spec failed to
		// land. Ready legitimately keeps its value, and Synced=ReconcileError is the whole story.
		// An observation failure invalidates our knowledge of health; a failed update does not.
		wrapped := errors.Wrap(actionErr, errReconcileObserve)

		// WHICH Ready to write, and the two cases where ObserveFailed is the WRONG answer.
		//
		// 1. THE HANDLER ALREADY ASSESSED IT. A handler may write a Ready FINDING and then return
		//    its error — core-provider's cdc does exactly that on two paths, with the diagnostic
		//    in the message. Overwriting that with "could not observe" replaces a finding with an
		//    absence of finding, which is worse information in the one direction an operator
		//    cannot recover from. ReadyAssessed is how a handler says so; a before/after snapshot
		//    cannot tell, because the handler's write is often the SAME value as what was there.
		//
		// 2. THE OBJECT IS BEING DELETED. handleObserve has no deletion guard — the Create branch
		//    drops deleted objects explicitly, this path does not — so a composition with a
		//    deletion timestamp reaches here. "Nobody knows" is false about an object whose fate is
		//    certain, and Deleting is what every other deletion path on this object reports.
		//
		// Synced=ReconcileError is written in ALL cases: consumers read its reason to decide
		// whether the last reconcile errored, and that guard must not depend on which Ready we
		// chose.
		readyCond := condition.ObserveFailed(wrapped.Error())
		switch {
		case IsReadyAssessed(actionErr):
			// The handler owns Ready for this call. Write only Synced.
			err = unstructuredtools.SetConditions(e, condition.ReconcileError(wrapped))
		default:
			// ONE DELETION DECISION PER PASS (#47/#48), made STICKY (#46). Two fixes to the same
			// defect, combined rather than chosen between:
			//
			//   deletedAtEntry   the decision taken ONCE, from the queue entry, at the top of this
			//                    pass. This replaces meta.WasDeleted(e): `e` is a fresh RE-FETCH of a
			//                    different object than the one the pass began with, and deciding
			//                    twice from two copies is how the two decisions came to disagree.
			//   Ready=Deleting   kept from #46. A timestamp that has not yet landed can be missing
			//                    from EITHER copy, so neither read alone is safe. A previous pass that
			//                    already concluded Deleting wrote it to Ready, and that is a durable
			//                    record independent of which copy this pass happened to read.
			//                    provider-runtime tests the same pair (pkg/reconciler/ready.go:64).
			//
			// The two are consistent rather than a second decision in disguise: a deletion timestamp
			// is permanent once set, so Ready=Deleting with deletedAtEntry false can only mean the
			// ENTRY is stale — exactly the case the sticky disjunct exists for.
			//
			// Keyed on the REASON and not on a non-True Ready: a converging object also reports a
			// non-True Ready and must not be mistaken for a deleting one.
			deleting := deletedAtEntry ||
				unstructuredtools.GetCondition(e, condition.TypeReady, condition.ReasonDeleting) != nil
			if deleting {
				readyCond = condition.Deleting()
			}
			err = unstructuredtools.SetConditions(e, readyCond, condition.ReconcileError(wrapped))
		}
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}

		e, err = tools.PatchStatusConditions(ctx, e, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(e, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}
		return actionErr
	}

	el, err = c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	// Apply whatever the handler asked to be recorded. AFTER this re-fetch, deliberately: the
	// fetch replaces `el`, so conditions set before it are silently discarded — which is the first
	// thing I got wrong implementing core-provider#258.
	//
	// A handler cannot do this itself. Observe receives a deep copy (handlers copy to keep Observe
	// a read phase), so anything written there never leaves the function. Returning them instead
	// means they ride the single status write below rather than needing one of their own.
	//
	// A failure here is logged and does NOT abort the observation: a condition that cannot be
	// recorded must not turn a successful observe into an error, which is the shape that wedges a
	// resource.
	if len(observation.Conditions) > 0 {
		if cerr := unstructuredtools.SetConditions(el, observation.Conditions...); cerr != nil {
			log.Error(cerr, "Cannot set conditions returned by the external client")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, cerr))
		}
	}

	// THE HANDLER'S CONDITIONS MUST SURVIVE BOTH EARLY RETURNS (core-provider#241).
	//
	// They were applied to `el` above and then thrown away on either branch below, because the only
	// status write in this function lives in the final `else`. A handler that returned a condition
	// alongside ResourceExists=false or ResourceUpToDate=false had it set on an in-memory object
	// that was never persisted — so the API we offer ("return conditions and we will record them")
	// held on exactly one of three paths.
	//
	// Written only when the handler actually returned something, so the paths that previously did
	// no status write still do none. The write is PatchStatusConditions, which carries only
	// status.conditions, so it cannot express the absence of a handler-owned sibling field (#33).
	persistHandlerConditions := func() {
		if len(observation.Conditions) == 0 {
			return
		}
		updated, perr := tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if perr != nil {
			// Logged, not returned: failing the observe here would turn an unrecordable condition
			// into a wedged resource, which is the shape the comment above this block warns about.
			log.Error(perr, "Cannot persist the conditions returned by the external client")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, perr))
			return
		}
		el = updated
	}

	if !observation.ResourceExists {
		persistHandlerConditions()
		item := ctrlevent.Event{
			EventType: ctrlevent.Create,
			ObjectRef: objectref.ObjectRef{
				APIVersion: el.GetAPIVersion(),
				Kind:       el.GetKind(),
				Name:       el.GetName(),
				Namespace:  el.GetNamespace(),
			},
			QueuedAt: time.Now(),
		}
		c.queue.AddWithOpts(priorityqueue.AddOpts{
			RateLimited: false,
			Priority:    HighPriority,
		}, item)
		return nil
	} else if !observation.ResourceUpToDate {
		persistHandlerConditions()
		item := ctrlevent.Event{
			EventType: ctrlevent.Update,
			ObjectRef: objectref.ObjectRef{
				APIVersion: el.GetAPIVersion(),
				Kind:       el.GetKind(),
				Name:       el.GetName(),
				Namespace:  el.GetNamespace(),
			},
			QueuedAt: time.Now(),
		}
		c.queue.AddWithOpts(priorityqueue.AddOpts{
			RateLimited: false,
			Priority:    HighPriority,
		}, item)
		return nil
	} else {
		// DO NOT OVERRIDE A CONDITION THE HANDLER EXPLICITLY RETURNED (core-provider#241).
		//
		// ReconcileSuccess() is Synced=True. Stamping it unconditionally overwrote whatever the
		// handler had just returned for Synced, which made one documented state unreachable: the
		// cdc's GRACEFUL pause returns ResourceExists=true and ResourceUpToDate=true, so it lands
		// here, and any Synced it set was replaced on the same reconcile. The annotation pause
		// escapes only by returning earlier (ReconcilePaused at the top of processItem) — so two
		// pause mechanisms had inconsistent condition shapes for no reason anyone chose.
		//
		// "Up to date" is the runtime's conclusion from ResourceUpToDate, and it is right by
		// default. But a handler that returns a Synced condition is making a MORE specific
		// statement about its own convergence, and the specific statement wins. The runtime's
		// default is a default, not an override.
		if !hasConditionType(observation.Conditions, condition.TypeSynced) {
			err = unstructuredtools.SetConditions(el, condition.ReconcileSuccess())
			if err != nil {
				log.Error(err, "Cannot set condition")
				c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
				return err
			}
		}
		el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}
		log.Info("External resource is up to date")
	}

	return nil
}

func (c *Controller) handleCreate(ctx context.Context, ref objectref.ObjectRef) error {
	log := contextutils.Logger(ctx).WithValues("event", "create").WithValues("traceId", contextutils.TraceId(ctx, false))

	if c.externalClient == nil {
		log.Warn("No event handler registered.")
		return nil
	}
	el, err := c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	// We write this annotation for two reasons. Firstly, it helps
	// us to detect the case in which we fail to persist critical
	// information (like the external name) that may be set by the
	// subsequent external.Create call. Secondly, it guarantees that
	// we're operating on the latest version of our resource. We
	// don't use the CriticalAnnotationUpdater because we _want_ the
	// update to fail if we get a 409 due to a stale version.
	meta.SetExternalCreatePending(el, time.Now())
	if el, uerr := tools.Update(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	}); uerr != nil {
		log.Error(uerr, errUpdateManaged)
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionCreateEvent, errors.Wrap(uerr, errUpdateManaged)))
		// If we cannot update the managed resource, we set the conditions
		el, err := c.fetch(ctx, ref, false)
		if err != nil {
			log.Error(err, "Cannot fetch managed resource")
			return err
		}
		// ONE error value for BOTH conditions, so Ready's message and Synced's cannot diverge.
		wrapped := errors.Wrap(uerr, errUpdateManaged)
		err = unstructuredtools.SetConditions(el,
			condition.WithMessage(condition.Creating(), wrapped.Error()),
			condition.ReconcileError(wrapped))
		if err != nil {
			log.Error(err, "Cannot set conditions")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}
		return errors.Wrap(uerr, errUpdateManaged)
	}

	el, err = c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	start := time.Now()
	// #77: the marker due before the first act, then the joined act step (only for a new
	// traceparent value, once). A marker conflict ends the pass before it acts.
	actCtx, act, merr := c.beginAct(ctx, el, "create")
	if merr != nil {
		return merr
	}
	actionErr := c.externalClient.Create(actCtx, el)
	telemetry.RecordSpanError(act, actionErr)
	act.End()
	if c.metrics != nil {
		c.metrics.RecordCreateDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncCreateFailure(ctx)
		}
	}
	if actionErr != nil {
		// A lost status race is not an error: requeue quietly, record nothing (#359).
		if statusConflict(actionErr) {
			log.Debug("Status write lost a race to a newer write; requeueing to recompute", "error", actionErr.Error())
			return actionErr
		}
		c.recordEvent(el, event.Warning(reasonCannotCreate, actionCreateExternalResource, actionErr))
		log.Error(actionErr, "Cannot create external resource")
		el, err := c.fetch(ctx, ref, false)
		if err != nil {
			log.Error(err, "Cannot fetch managed resource")
			return err
		}

		meta.SetExternalCreateFailed(el, time.Now())
		el, err = tools.Update(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update managed resource")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}

		// ONE error value for BOTH conditions. This is the site that produced the measured
		// Ready=False/Creating with an EMPTY message beside a complete Synced message, on a
		// create that retried forever.
		wrapped := errors.Wrap(actionErr, errReconcileCreate)
		err = unstructuredtools.SetConditions(el,
			condition.WithMessage(condition.Creating(), wrapped.Error()),
			condition.ReconcileError(wrapped))
		if err != nil {
			log.Error(err, "Cannot set conditions")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update managed resource")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}

		return actionErr
	}

	el, err = c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	meta.SetExternalCreateSucceeded(el, time.Now())
	if el, uerr := tools.Update(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	}); uerr != nil {
		log.Error(uerr, errUpdateManagedAnnotations)
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, errors.Wrap(uerr, errUpdateManagedAnnotations)))
		// If we cannot update the managed resource, we set the conditions
		el, err := c.fetch(ctx, ref, false)
		if err != nil {
			log.Error(err, "Cannot fetch managed resource")
			return err
		}
		wrapped := errors.Wrap(uerr, errUpdateManagedAnnotations)
		err = unstructuredtools.SetConditions(el,
			condition.WithMessage(condition.Creating(), wrapped.Error()),
			condition.ReconcileError(wrapped))
		if err != nil {
			log.Error(err, "Cannot set conditions")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}
		return errors.Wrap(uerr, errUpdateManagedAnnotations)
	}

	log.Info("Successfully requested creation of external resource")

	el, err = c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}
	err = unstructuredtools.SetConditions(el, condition.ReconcileSuccess())
	if err != nil {
		log.Error(err, "Cannot set conditions")
		c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
		return err
	}
	el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	})
	if err != nil {
		log.Error(err, "Cannot update status")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
		return err
	}
	c.recordEvent(el, event.Normal(reasonCreated, actionCreateEvent, "Successfully requested creation of external resource"))
	return nil
}

func (c *Controller) handleUpdate(ctx context.Context, ref objectref.ObjectRef) error {
	log := contextutils.Logger(ctx).WithValues("event", "update").WithValues("traceId", contextutils.TraceId(ctx, false))
	if c.externalClient == nil {
		log.Warn("No event handler registered.")
		return nil
	}

	el, err := c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	start := time.Now()
	// #77: the marker due before the first act, then the joined act step (only for a new
	// traceparent value, once). A marker conflict ends the pass before it acts.
	actCtx, act, merr := c.beginAct(ctx, el, "update")
	if merr != nil {
		return merr
	}
	actionErr := c.externalClient.Update(actCtx, el)
	telemetry.RecordSpanError(act, actionErr)
	act.End()
	if c.metrics != nil {
		c.metrics.RecordUpdateDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncUpdateFailure(ctx)
		}
	}
	if actionErr != nil {
		// A lost status race is not an error: requeue quietly, record nothing (#359).
		if statusConflict(actionErr) {
			log.Debug("Status write lost a race to a newer write; requeueing to recompute", "error", actionErr.Error())
			return actionErr
		}
		c.recordEvent(el, event.Warning(reasonCannotUpdate, actionUpdateEvent, actionErr))
		log.Error(actionErr, "Cannot update external resource")
		e, err := c.fetch(ctx, ref, false)
		if err != nil {
			log.Error(err, "Cannot fetch managed resource")
			return err
		}

		err = unstructuredtools.SetConditions(e, condition.ReconcileError(errors.Wrap(actionErr, errReconcileUpdate)))
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}

		el, err = tools.PatchStatusConditions(ctx, e, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}

		return actionErr
	}

	el, err = c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	err = unstructuredtools.SetConditions(el, condition.ReconcileSuccess())
	if err != nil {
		log.Error(err, "Cannot set conditions")
		c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
		return err
	}
	el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	})
	if err != nil {
		log.Error(err, "Cannot update status")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
		return err
	}
	log.Info("Successfully requested update of external resource")
	c.recordEvent(el, event.Normal(reasonUpdated, actionUpdateEvent, "Successfully requested update of external resource"))
	return nil
}

func (c *Controller) handleDelete(ctx context.Context, ref objectref.ObjectRef) error {
	log := contextutils.Logger(ctx).WithValues("event", "delete").WithValues("traceId", contextutils.TraceId(ctx, false))
	if c.externalClient == nil {
		log.Warn("No event handler registered.")
		return nil
	}

	el, err := c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}
	start := time.Now()
	// #77: the marker due before the first act, then the joined act step (only for a new
	// traceparent value, once). A marker conflict ends the pass before it acts.
	actCtx, act, merr := c.beginAct(ctx, el, "delete")
	if merr != nil {
		return merr
	}
	actionErr := c.externalClient.Delete(actCtx, el)
	telemetry.RecordSpanError(act, actionErr)
	act.End()
	if c.metrics != nil {
		c.metrics.RecordDeleteDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncDeleteFailure(ctx)
		}
	}
	if actionErr != nil {
		// A lost status race is not an error: requeue quietly, record nothing (#359).
		if statusConflict(actionErr) {
			log.Debug("Status write lost a race to a newer write; requeueing to recompute", "error", actionErr.Error())
			return actionErr
		}
		log.Error(actionErr, "Cannot delete external resource")
		c.recordEvent(el, event.Warning(reasonCannotDelete, actionDeleteExternalResource, actionErr))

		wrapped := errors.Wrap(actionErr, errReconcileDelete)
		err := unstructuredtools.SetConditions(el,
			condition.WithMessage(condition.Deleting(), wrapped.Error()),
			condition.ReconcileError(wrapped))
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			log.Error(err, "Cannot update status")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
			return err
		}
		return actionErr
	}

	el, err = c.fetch(ctx, ref, false)
	if err != nil {
		log.Error(err, "Cannot fetch managed resource")
		return err
	}

	err = unstructuredtools.SetConditions(el, condition.Deleting(), condition.ReconcileSuccess())
	if err != nil {
		log.Error(err, "Cannot set conditions")
		c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
		return err
	}
	el, err = tools.PatchStatusConditions(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	})
	if err != nil {
		log.Error(err, "Cannot update managed resource")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
		return err
	}

	el.SetFinalizers([]string{})
	el, err = tools.Update(ctx, el, tools.UpdateOptions{
		Pluralizer:    c.pluralizer,
		DynamicClient: c.dynamicClient,
	})
	if err != nil {
		log.Error(err, "Cannot remove finalizer")
		c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionUpdateManagedResource, err))
		return err
	}
	tracePassFrom(ctx).forget() // the finalizer is gone, and the memo entry with it (#77)

	// "External resource is gone", NOT "deletion requested" — because a nil from Delete establishes
	// only the first (#34).
	//
	// externalClient.Delete is called unconditionally above: there is no Observe-reported-present
	// precondition, so nil means EITHER the client deleted it OR it was already absent, and at this
	// point the runtime cannot tell which. Claiming a request was made is an assertion about our own
	// action that the return value does not support.
	//
	// Measured by the aruba-provider-kog session on a run where ZERO DELETE requests reached the
	// vendor API for the whole lifetime of the CR (148 GETs, 1 POST, 0 DELETEs). An operator removed
	// the resource by hand; the next verification found it absent, returned nil, and the runtime
	// logged that it had requested the deletion. Everything it DID was correct — only the sentence
	// was wrong, and an operator reading it concludes the controller removed the resource.
	//
	// DO NOT "fix" the identical sentence in provider-runtime's reconciler to match. It is correct
	// there, and for a reason that is not the wording: that path fires only after Observe reported
	// the resource PRESENT, then requeues to verify while holding the finalizer. Already-gone never
	// reaches it, because Observe reports absent and Delete is never called. The precondition is
	// what makes the claim true, and this path has none.
	log.Info("External resource is gone; releasing finalizer")
	c.recordEvent(el, event.Normal(reasonDeleted, actionDeleteEvent, "External resource is gone; releasing finalizer"))
	return nil
}

func (c *Controller) fetch(ctx context.Context, ref objectref.ObjectRef, clean bool) (res *unstructured.Unstructured, err error) {
	if c.dynamicClient == nil {
		return nil, fmt.Errorf("dynamic client is not configured")
	}
	start := time.Now()
	defer func() {
		if c.metrics == nil {
			return
		}
		c.metrics.RecordGetDuration(ctx, time.Since(start))
		if err != nil {
			c.metrics.IncGetFailure(ctx)
		}
	}()
	res, err = c.dynamicClient.Resource(c.gvr).
		Namespace(ref.Namespace).
		Get(ctx, ref.Name, metav1.GetOptions{})
	if err == nil {
		if clean && res != nil {
			unstructured.RemoveNestedField(res.Object,
				"metadata", "annotations", "kubectl.kubernetes.io/last-applied-configuration")
			unstructured.RemoveNestedField(res.Object, "metadata", "creationTimestamp")
			unstructured.RemoveNestedField(res.Object, "metadata", "generation")
			unstructured.RemoveNestedField(res.Object, "metadata", "uid")
		}
	}
	return res, err
}

// Safe record event - do not panic if obj is nil
func (c *Controller) recordEvent(obj runtime.Object, e event.Event) {
	if obj == nil {
		c.logger.Debug("Cannot record event, object is nil")
		return
	}

	if e.Reason == event.Reason(event.TypeNormal) {
		c.recorder.Event(obj, e)
		return
	}

	c.throttledRecorder.Event(obj, e)
}

// hasConditionType reports whether conds contains a condition of type t.
//
// Used to decide whether the runtime's default Synced stamp would OVERRIDE a statement the handler
// already made (core-provider#241). Keyed on the TYPE, not on reason or status: the question is
// "has the handler spoken about this condition", and any answer it gave is more specific than the
// runtime's default.
func hasConditionType(conds []metav1.Condition, t string) bool {
	for _, c := range conds {
		if c.Type == t {
			return true
		}
	}
	return false
}
