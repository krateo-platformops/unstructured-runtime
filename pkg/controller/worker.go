package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"github.com/krateo-platformops/plumbing/shortid"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"slices"

	"k8s.io/apimachinery/pkg/runtime"
	runtimeUtil "k8s.io/apimachinery/pkg/util/runtime"
)

const (
	finalizerName = "composition.krateo.io/finalizer"
)

// Event reasons.
const (
	reasonCannotInitialize    event.Reason = "CannotInitializeManagedResource"
	reasonCannotResolveRefs   event.Reason = "CannotResolveResourceReferences"
	reasonCannotObserve       event.Reason = "CannotObserveExternalResource"
	reasonCannotCreate        event.Reason = "CannotCreateExternalResource"
	reasonCannotDelete        event.Reason = "CannotDeleteExternalResource"
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

func (c *Controller) runWorker(ctx context.Context) {
	// Generate unique worker ID for tracking
	workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano()%10000)

	for {
		obj, priority, shutdown := c.queue.GetWithPriority()
		if shutdown {
			c.logger.Info("Shutting down worker", "workerID", workerID)
			break
		}

		ev, ok := obj.(ctrlevent.Event)
		if !ok {
			c.queue.Forget(obj)
			c.queue.Done(obj)
			runtimeUtil.HandleError(fmt.Errorf("unexpected object in queue: %v", obj))
			if c.metrics != nil {
				c.metrics.AddReconcileInFlight(-1)
			}
			continue
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
			continue
		}
		ctx = contextutils.BuildContext(ctx, contextutils.WithLogger(c.logger), contextutils.WithTraceId(traceId))
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
			if err == nil {
				c.metrics.IncReconcileSuccess(ctx)
			} else {
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
		c.queue.Forget(ev)
		c.queue.Done(ev)
		c.handleErr(err, ev, priority)
		if c.metrics != nil {
			c.metrics.AddReconcileInFlight(-1)
		}
	}
}

func (c *Controller) handleErr(err error, obj ctrlevent.Event, priority int) {
	if err == nil {
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
	el, err := c.fetch(ctx, evt.ObjectRef, false)
	if err != nil {
		lg.Debug("Cannot fetch managed resource", "err", err) // This is returned as debug as debug info. This can occur normally if the resource was deleted.
		// if the object is not found, we will not retry to process it and we not throw an error
		return nil
	}

	// Per-reconcile root span, continuing any cross-composition trace carried on el's
	// krateo.io/traceparent annotation. No-op tracer when tracing is disabled; the deferred
	// RecordSpanError reads the named return value.
	ctx, span := telemetry.StartReconcileSpan(ctx, el)
	defer func() {
		telemetry.RecordSpanError(span, err)
		span.End()
	}()

	if meta.IsPaused(el) {
		log := lg.WithValues("annotation", meta.AnnotationKeyReconciliationPaused)

		log.Debug("Reconciliation is paused via the pause annotation")
		c.recordEvent(el, event.Normal(reasonReconciliationPaused, actionProcessEvent, "Reconciliation is paused via the pause annotation"))
		err = unstructuredtools.SetConditions(el, condition.ReconcilePaused())
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err := tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
		}
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
	if meta.ExternalCreateIncomplete(el) {
		resourceExists, obsErr := c.externalResourceExists(ctx, el)

		if obsErr != nil {
			lg.Warn(errCreateIncomplete)
			c.recordEvent(el, event.Warning(reasonCannotInitialize, actionProcessEvent, errors.New(errCreateIncomplete)))

			err = unstructuredtools.SetConditions(el,
				condition.Creating(),
				condition.ReconcileError(errors.New(errCreateIncomplete)),
			)
			if err != nil {
				lg.Error(err, "Cannot set condition")
				c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
				return err
			}

			el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
		if resourceExists {
			meta.SetExternalCreateSucceeded(el, time.Now())
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
			meta.RemoveAnnotations(el, meta.AnnotationKeyExternalCreatePending)
		}
		el, err = tools.Update(ctx, el, tools.UpdateOptions{
			Pluralizer:    c.pluralizer,
			DynamicClient: c.dynamicClient,
		})
		if err != nil {
			lg.Error(err, "Cannot reconcile external-create annotations after incomplete create")
			c.recordEvent(el, event.Warning(reasonCannotUpdateManaged, actionProcessEvent, err))
			return err
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

	lg.Debug("Start processing")
	switch evt.EventType {
	case ctrlevent.Create:
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

	start := time.Now()
	observation, actionErr := c.externalClient.Observe(ctx, el)
	if c.metrics != nil {
		c.metrics.RecordObserveDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncObserveFailure(ctx)
		}
	}
	if actionErr != nil {
		c.recordEvent(el, event.Warning(reasonCannotObserve, actionObserveExternalResource, actionErr))
		log.Error(actionErr, "Cannot observe external resource")

		e, err := c.fetch(ctx, ref, false)
		if err != nil {
			log.Error(err, "Cannot fetch managed resource")
			return err
		}

		err = unstructuredtools.SetConditions(e, condition.ReconcileError(errors.Wrap(actionErr, errReconcileObserve)))
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}

		e, err = tools.UpdateStatus(ctx, e, tools.UpdateOptions{
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

	if !observation.ResourceExists {
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
		err = unstructuredtools.SetConditions(el, condition.ReconcileSuccess())
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
		err = unstructuredtools.SetConditions(el, condition.Creating(), condition.ReconcileError(errors.Wrap(uerr, errUpdateManaged)))
		if err != nil {
			log.Error(err, "Cannot set conditions")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
	actionErr := c.externalClient.Create(ctx, el)
	if c.metrics != nil {
		c.metrics.RecordCreateDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncCreateFailure(ctx)
		}
	}
	if actionErr != nil {
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

		err = unstructuredtools.SetConditions(el, condition.Creating(), condition.ReconcileError(errors.Wrap(actionErr, errReconcileCreate)))
		if err != nil {
			log.Error(err, "Cannot set conditions")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
		err = unstructuredtools.SetConditions(el, condition.Creating(), condition.ReconcileError(errors.Wrap(uerr, errUpdateManagedAnnotations)))
		if err != nil {
			log.Error(err, "Cannot set conditions")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
	el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
	actionErr := c.externalClient.Update(ctx, el)
	if c.metrics != nil {
		c.metrics.RecordUpdateDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncUpdateFailure(ctx)
		}
	}
	if actionErr != nil {
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

		el, err = tools.UpdateStatus(ctx, e, tools.UpdateOptions{
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
	el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
	actionErr := c.externalClient.Delete(ctx, el)
	if c.metrics != nil {
		c.metrics.RecordDeleteDuration(ctx, time.Since(start))
		if actionErr != nil {
			c.metrics.IncDeleteFailure(ctx)
		}
	}
	if actionErr != nil {
		log.Error(actionErr, "Cannot delete external resource")
		c.recordEvent(el, event.Warning(reasonCannotDelete, actionDeleteExternalResource, actionErr))

		err := unstructuredtools.SetConditions(el, condition.Deleting(), condition.ReconcileError(errors.Wrap(actionErr, errReconcileDelete)))
		if err != nil {
			log.Error(err, "Cannot set condition")
			c.recordEvent(el, event.Warning(reasonCannotSetConditions, actionUpdateManagedResource, err))
			return err
		}
		el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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
	el, err = tools.UpdateStatus(ctx, el, tools.UpdateOptions{
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

	log.Info("Successfully requested deletion of external resource")
	c.recordEvent(el, event.Normal(reasonDeleted, actionDeleteEvent, "Successfully requested deletion of external resource"))
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
