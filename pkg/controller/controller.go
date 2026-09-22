package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	metricsserver "github.com/krateo-platformops/unstructured-runtime/pkg/metrics/server"

	"github.com/google/go-cmp/cmp"
	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"github.com/krateo-platformops/plumbing/shortid"
	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/listwatcher"
	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/meta"
	"github.com/krateo-platformops/unstructured-runtime/pkg/pluralizer"
	"github.com/krateo-platformops/unstructured-runtime/pkg/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	reasonReconciliationPaused event.Reason = "ReconciliationPaused"
)

const LowPriority = -100 //Low priority for the priorityqueue
const NormalPriority = 0 //Normal priority for the priorityqueue
const HighPriority = 100 //High priority for the priorityqueue

// defaultGracefulShutdownPeriod is the default drain window after context
// cancellation (SIGTERM), matching controller-runtime's manager default.
const defaultGracefulShutdownPeriod = 30 * time.Second

// An ExternalClient manages the lifecycle of an external resource.
// None of the calls here should be blocking. All of the calls should be
// idempotent. For example, Create call should not return AlreadyExists error
// if it's called again with the same parameters or Delete call should not
// return error if there is an ongoing deletion or resource does not exist.
type ExternalClient interface {
	Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error)
	Create(ctx context.Context, mg *unstructured.Unstructured) error
	Update(ctx context.Context, mg *unstructured.Unstructured) error
	Delete(ctx context.Context, mg *unstructured.Unstructured) error
}

// An ExistenceChecker answers the one question incomplete-create recovery actually asks: did the
// external resource get created?
//
// Recovery otherwise falls back to Observe, which answers a broader question — does it exist AND is
// it converged. For some clients that second half does real work that can fail for reasons having
// nothing to do with existence, and then recovery misreads a convergence failure as "we cannot
// determine whether the create landed" and refuses. Permanently: the resource stays wedged, so the
// reconcile that would repair the underlying problem never runs again, and the controller can never
// recover from the very failure that wedged it.
//
// The concrete case: composition-dynamic-controller's Observe runs a full helm reconcile including
// a self-heal apply. A composition missing one permission fails that apply on every attempt, so
// Observe never succeeds and recovery never resolves — even though "does the helm release exist?"
// is cheap, reliable, and already answered earlier in that same function
// (krateo-platformops/core-provider#110, #130).
//
// Implement this on an ExternalClient that can answer cheaply without doing the convergence work.
// Recovery prefers it and uses Observe only when it is absent, so this is purely additive: an
// ExternalClient that does not implement it behaves exactly as before.
type ExistenceChecker interface {
	// Exists reports whether the external resource for mg exists. It must not attempt to converge,
	// repair or otherwise mutate anything — a failure to converge is not an answer to this question.
	Exists(ctx context.Context, mg *unstructured.Unstructured) (bool, error)
}

// An ExternalObservation is the result of an observation of an external resource.
type ExternalObservation struct {
	// ResourceExists must be true if a corresponding external resource exists
	// for the managed resource.
	ResourceExists bool

	// ResourceUpToDate should be true if the corresponding external resource
	// appears to be up-to-date - i.e. updating the external resource to match
	// the desired state of the managed resource would be a no-op.
	ResourceUpToDate bool
}

type ListWatcherConfiguration struct {
	LabelSelector *string
	FieldSelector *string
}

type Options struct {
	Client            dynamic.Interface
	GVR               schema.GroupVersionResource
	Namespace         string
	ResyncInterval    time.Duration
	Recorder          event.Recorder
	ThrottledRecorder event.Recorder
	Logger            logging.Logger
	Metrics           *telemetry.Metrics
	ListWatcher       ListWatcherConfiguration
	Pluralizer        pluralizer.PluralizerInterface
	GlobalRateLimiter workqueue.TypedRateLimiter[any]
	MetricsServer     metricsserver.Server
	WatchAnnotations  ctrlevent.AnnotationEvents
	MaxRetries        int
	ActionsEvent      ctrlevent.ActionsEvent

	// GracefulShutdownTimeout bounds how long Run keeps the process alive after its
	// context is cancelled (SIGTERM), letting in-flight reconciles finish before exit.
	// Pointer semantics mirror controller-runtime's manager.Options.GracefulShutdownTimeout:
	//   nil       => default (defaultGracefulShutdownPeriod, 30s)
	//   0         => graceful shutdown disabled (abrupt exit, the pre-drain behavior)
	//   negative  => wait forever for in-flight reconciles
	// The value MUST be set below the pod's terminationGracePeriodSeconds or the kubelet
	// SIGKILLs mid-drain.
	GracefulShutdownTimeout *time.Duration
}

func (o Options) validate() error {
	if o.Client == nil {
		return fmt.Errorf("client is required")
	}
	if o.GVR.Empty() {
		return fmt.Errorf("GVR is required")
	}
	if o.Recorder == nil {
		return fmt.Errorf("recorder is required")
	}
	if o.Logger == nil {
		return fmt.Errorf("logger is required")
	}
	if o.Pluralizer == nil {
		return fmt.Errorf("pluralizer is required")
	}
	if o.GlobalRateLimiter == nil {
		return fmt.Errorf("global rate limiter is required")
	}
	if o.ResyncInterval <= 0 {
		return fmt.Errorf("resync interval must be greater than 0")
	}
	if o.MaxRetries < 0 {
		return fmt.Errorf("max retries must be greater than or equal to 0")
	}
	return nil
}

type Controller struct {
	metricsServer     metricsserver.Server
	pluralizer        pluralizer.PluralizerInterface
	dynamicClient     dynamic.Interface
	gvr               schema.GroupVersionResource
	queue             priorityqueue.PriorityQueue[any]
	items             *sync.Map
	informer          cache.Controller
	recorder          event.Recorder
	throttledRecorder event.Recorder
	logger            logging.Logger
	metrics           *telemetry.Metrics
	externalClient    ExternalClient
	maxRetries        int

	// gracefulShutdownTimeout is the resolved drain window used by Run on shutdown.
	// Guarded by gracefulShutdownMu so a future leader-election runnable can zero it via
	// SetGracefulShutdownTimeout (release-on-lease-loss) without racing Run's read.
	gracefulShutdownMu      sync.RWMutex
	gracefulShutdownTimeout time.Duration
}

var (
	reconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "controller_reconcile_total",
			Help: "Total number of reconciliations",
		},
		[]string{"kind", "namespace", "result"},
	)

	reconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "controller_reconcile_duration_seconds",
			Help: "Time spent reconciling",
		},
		[]string{"kind", "namespace"},
	)
)

func init() {
	prometheus.MustRegister(reconcileTotal)
	prometheus.MustRegister(reconcileDuration)
}

func New(sid *shortid.Shortid, opts Options) (*Controller, error) {
	if err := opts.validate(); err != nil {
		opts.Logger.Error(err, "Invalid controller options")
		return nil, fmt.Errorf("invalid controller options: %w", err)
	}
	queue := priorityqueue.New("controller", func(o *priorityqueue.Opts[any]) {
		o.RateLimiter = opts.GlobalRateLimiter
	})

	// Wrap the queue with metrics instrumentation if metrics are available
	var finalQueue priorityqueue.PriorityQueue[any] = queue
	if opts.Metrics != nil {
		finalQueue = NewInstrumentedQueue(queue, opts.Metrics)
	}
	items := &sync.Map{}

	lw, err := listwatcher.Create(listwatcher.CreateOption{
		Client:        opts.Client,
		GVR:           opts.GVR,
		LabelSelector: opts.ListWatcher.LabelSelector,
		FieldSelector: opts.ListWatcher.FieldSelector,
		Namespace:     opts.Namespace,
	})
	if err != nil {
		opts.Logger.Error(err, "Failed to create listwatcher.")
		return nil, fmt.Errorf("failed to create listwatcher: %w", err)
	}

	_, informer := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &unstructured.Unstructured{},
		ResyncPeriod:  opts.ResyncInterval,
		Indexers:      cache.Indexers{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				log := opts.Logger
				el, ok := obj.(*unstructured.Unstructured)
				if !ok {
					log.Warn("Object is not an unstructured.")
					return
				}

				item := ctrlevent.Event{
					EventType: opts.ActionsEvent.GetEventType(ctrlevent.CRCreated),
					ObjectRef: objectref.ObjectRef{
						APIVersion: el.GetAPIVersion(),
						Kind:       el.GetKind(),
						Name:       el.GetName(),
						Namespace:  el.GetNamespace(),
					},
					QueuedAt: time.Now(),
				}
				dig := ctrlevent.DigestForEvent(item)

				// Checking if the object is already being processed
				priority := NormalPriority
				annotations := el.GetAnnotations()
				if annotations == nil {
					priority = NormalPriority
				}
				_, ok = annotations[meta.AnnotationKeyExternalCreateFailed]
				_, ok_pending := annotations[meta.AnnotationKeyExternalCreatePending]
				_, ok_succedeed := annotations[meta.AnnotationKeyExternalCreateSucceeded]
				if ok || ok_pending || ok_succedeed {
					priority = LowPriority //These are events that are already being processed, so we can lower the priority
				}

				if _, loaded := items.LoadOrStore(dig, struct{}{}); !loaded {
					log.WithValues(
						"kind", item.ObjectRef.Kind,
						"apiVersion", item.ObjectRef.APIVersion,
						"name", item.ObjectRef.Name,
						"namespace", item.ObjectRef.Namespace,
						"queuedAt", item.QueuedAt,
					).Debug("Adding Observe event to queue", "priority", priority)
					// Generally a user event, occurs when a user creates a resource or the resource is first seen
					// by the controller. We want to process these events as soon as possible, so we add them to the front of the queue.
					// However, if the resource has an annotation that indicates it is being processed, we lower the priority.
					queue.AddWithOpts(priorityqueue.AddOpts{
						RateLimited: false,
						Priority:    priority,
					}, item)
				}
			},
			UpdateFunc: func(old, new interface{}) {
				log := opts.Logger
				oldUns, ok := old.(*unstructured.Unstructured)
				if !ok {
					log.Warn("Object is not an unstructured.")
					return
				}

				newUns, ok := new.(*unstructured.Unstructured)
				if !ok {
					log.Warn("Object is not an unstructured.")
					return
				}

				if meta.WasDeleted(newUns) {
					log.Debug(fmt.Sprintf("Object %s/%s is being deleted", newUns.GetNamespace(), newUns.GetName()))

					item := ctrlevent.Event{
						EventType: opts.ActionsEvent.GetEventType(ctrlevent.CRDeleted),
						ObjectRef: objectref.ObjectRef{
							APIVersion: newUns.GetAPIVersion(),
							Kind:       newUns.GetKind(),
							Name:       newUns.GetName(),
							Namespace:  newUns.GetNamespace(),
						},
						QueuedAt: time.Now(),
					}

					dig := ctrlevent.DigestForEvent(item)

					if _, loaded := items.LoadOrStore(dig, struct{}{}); !loaded {
						log.WithValues(
							"kind", item.ObjectRef.Kind,
							"apiVersion", item.ObjectRef.APIVersion,
							"name", item.ObjectRef.Name,
							"namespace", item.ObjectRef.Namespace,
							"queuedAt", item.QueuedAt,
						).Debug("Adding Delete event to queue", "priority", HighPriority)
						// Generally this is a pending delete event, where the resource has a deletion timestamp but was not effectively deleted yet.
						// We want to process these events as soon as possible, so we add the event with high priority.
						// This is a user event, so we want to process it quickly.
						queue.AddWithOpts(priorityqueue.AddOpts{
							RateLimited: false,
							Priority:    HighPriority,
						}, item)
					}
					return
				}

				if len(opts.WatchAnnotations) > 0 {
					// Check if any of the annotations we are watching have changed
					for _, event := range opts.WatchAnnotations {
						oldValue, oldExists := oldUns.GetAnnotations()[event.Annotation]
						newValue, newExists := newUns.GetAnnotations()[event.Annotation]

						deletedCondition := !newExists && oldExists
						createdCondition := newExists && !oldExists
						changedCondition := oldExists && newExists && !cmp.Equal(oldValue, newValue)
						anyConditions := deletedCondition || createdCondition || changedCondition

						trigger := false
						action := event.OnAction
						if action == ctrlevent.OnDelete && deletedCondition ||
							action == ctrlevent.OnCreate && createdCondition ||
							action == ctrlevent.OnChange && changedCondition ||
							action == ctrlevent.OnAny && anyConditions {
							trigger = true
						}
						if trigger {
							item := ctrlevent.Event{
								EventType: event.EventType,
								ObjectRef: objectref.ObjectRef{
									APIVersion: newUns.GetAPIVersion(),
									Kind:       newUns.GetKind(),
									Name:       newUns.GetName(),
									Namespace:  newUns.GetNamespace(),
								},
								QueuedAt: time.Now(),
							}

							log.WithValues(
								"kind", item.ObjectRef.Kind,
								"apiVersion", item.ObjectRef.APIVersion,
								"name", item.ObjectRef.Name,
								"namespace", item.ObjectRef.Namespace,
								"queuedAt", item.QueuedAt,
							).Debug("Adding event to queue for annotation change", "priority", NormalPriority, "eventType", event.EventType, "annotation", event.Annotation)
							// Generally a user event, occurs when a user changes an annotation we are watching. We want to process these events as soon as possible, so we add them to the front of the queue.
							// We use normal priority because these are user events that should be processed quickly, but they are not as urgent as create or delete events.
							// This is a user event, so we want to process it quickly.
							queue.AddWithOpts(priorityqueue.AddOpts{
								RateLimited: false,
								Priority:    NormalPriority,
							}, item)

							return
						}
					}
				}

				// Check if ResourceVersion changed to distinguish between:
				// 1. Periodic resync (same ResourceVersion) - should queue Observe
				// 2. Status-only update (different ResourceVersion, same spec) - should IGNORE
				// 3. Spec change (different ResourceVersion, different spec) - should queue Update
				oldResourceVersion := oldUns.GetResourceVersion()
				newResourceVersion := newUns.GetResourceVersion()

				newSpec, _, err := unstructured.NestedMap(newUns.Object, "spec")
				if err != nil {
					log.Error(err, "getting new object spec")
					return
				}

				oldSpec, _, err := unstructured.NestedMap(oldUns.Object, "spec")
				if err != nil {
					log.Error(err, "getting old object spec")
					return
				}

				diff := cmp.Diff(newSpec, oldSpec)

				if len(diff) > 0 {
					// Spec changed - user-initiated update
					item := ctrlevent.Event{
						EventType: opts.ActionsEvent.GetEventType(ctrlevent.CRUpdated),
						ObjectRef: objectref.ObjectRef{
							APIVersion: newUns.GetAPIVersion(),
							Kind:       newUns.GetKind(),
							Name:       newUns.GetName(),
							Namespace:  newUns.GetNamespace(),
						},
						QueuedAt: time.Now(),
					}

					dig := ctrlevent.DigestForEvent(item)

					if _, loaded := items.LoadOrStore(dig, struct{}{}); !loaded {
						log.WithValues(
							"kind", item.ObjectRef.Kind,
							"apiVersion", item.ObjectRef.APIVersion,
							"name", item.ObjectRef.Name,
							"namespace", item.ObjectRef.Namespace,
							"queuedAt", item.QueuedAt,
						).Debug("Adding Update event to queue (spec changed)", "priority", HighPriority)
						queue.AddWithOpts(priorityqueue.AddOpts{
							RateLimited: false,
							Priority:    HighPriority,
						}, item)
					}
				} else if oldResourceVersion == newResourceVersion {
					// Periodic resync from informer - ResourceVersion unchanged
					item := ctrlevent.Event{
						EventType: opts.ActionsEvent.GetEventType(ctrlevent.CRObserved),
						ObjectRef: objectref.ObjectRef{
							APIVersion: newUns.GetAPIVersion(),
							Kind:       newUns.GetKind(),
							Name:       newUns.GetName(),
							Namespace:  newUns.GetNamespace(),
						},
						QueuedAt: time.Now(),
					}

					dig := ctrlevent.DigestForEvent(item)

					if _, loaded := items.LoadOrStore(dig, struct{}{}); !loaded {
						log.WithValues(
							"kind", item.ObjectRef.Kind,
							"apiVersion", item.ObjectRef.APIVersion,
							"name", item.ObjectRef.Name,
							"namespace", item.ObjectRef.Namespace,
							"queuedAt", item.QueuedAt,
						).Debug("Adding Observe event to queue (periodic resync)", "priority", LowPriority)
						queue.AddWithOpts(priorityqueue.AddOpts{
							RateLimited: false,
							Priority:    LowPriority,
						}, item)
					}
				} else {
					// ResourceVersion changed but spec didn't - this is a status-only update from the controller itself
					// IGNORE to prevent self-triggering loop
					log.WithValues(
						"kind", newUns.GetKind(),
						"name", newUns.GetName(),
						"namespace", newUns.GetNamespace(),
						"oldResourceVersion", oldResourceVersion,
						"newResourceVersion", newResourceVersion,
					).Debug("Ignoring status-only update")
				}

			},
			DeleteFunc: func(obj interface{}) {
				log := opts.Logger

				// Attempt to cast the object to *unstructured.Unstructured
				el, ok := obj.(*unstructured.Unstructured)
				if !ok {
					// If the cast fails, check if it's a Tombstone (DeletedFinalStateUnknown)
					tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
					if !ok {
						log.Warn("Failed to recover object from DeleteFunc: unknown type")
						return
					}
					// Recover the last known state of the object from the tombstone
					el, ok = tombstone.Obj.(*unstructured.Unstructured)
					if !ok {
						log.Warn("Tombstone does not contain an unstructured object")
						return
					}
				}

				if el.GetDeletionTimestamp() == nil {
					log.WithValues(
						"name", el.GetName(),
						"apiVersion", el.GetAPIVersion(),
						"kind", el.GetKind(),
						"namespace", el.GetNamespace(),
					).Info("Object exited controller control without deletion request. Skipping external resource cleanup.")
					return
				}

				log.Debug(fmt.Sprintf("Deleting object %s/%s", el.GetNamespace(), el.GetName()))

				item := ctrlevent.Event{
					EventType: opts.ActionsEvent.GetEventType(ctrlevent.CRDeleted),
					ObjectRef: objectref.ObjectRef{
						APIVersion: el.GetAPIVersion(),
						Kind:       el.GetKind(),
						Name:       el.GetName(),
						Namespace:  el.GetNamespace(),
					},
					QueuedAt: time.Now(),
				}

				log.WithValues(
					"kind", item.ObjectRef.Kind,
					"apiVersion", item.ObjectRef.APIVersion,
					"name", item.ObjectRef.Name,
					"namespace", item.ObjectRef.Namespace,
					"queuedAt", item.QueuedAt,
				).Debug("Adding Delete event to queue")
				// Generally this is a delete event where the resource is already gone. We want to process these events as soon as possible, so we add the event with high priority.
				// This is a user event, so we want to process it quickly.
				queue.AddWithOpts(priorityqueue.AddOpts{
					RateLimited: false,
					Priority:    HighPriority,
				}, item)
			},
		},
	})
	gracefulShutdownTimeout := defaultGracefulShutdownPeriod
	if opts.GracefulShutdownTimeout != nil {
		gracefulShutdownTimeout = *opts.GracefulShutdownTimeout
	}

	return &Controller{
		dynamicClient:           opts.Client,
		gvr:                     opts.GVR,
		items:                   items,
		recorder:                opts.Recorder,
		throttledRecorder:       opts.ThrottledRecorder,
		logger:                  opts.Logger,
		metrics:                 opts.Metrics,
		informer:                informer,
		queue:                   finalQueue,
		pluralizer:              opts.Pluralizer,
		metricsServer:           opts.MetricsServer,
		maxRetries:              opts.MaxRetries,
		gracefulShutdownTimeout: gracefulShutdownTimeout,
	}, nil
}

func (c *Controller) SetExternalClient(ec ExternalClient) {
	c.externalClient = ec
}

// Run begins watching and syncing.
// Run starts the informer and worker pool and blocks until ctx is cancelled (SIGTERM). On
// cancellation it performs a bounded graceful drain: it stops accepting new work and lets
// in-flight reconciles finish under a context that is NOT cancelled by the signal, up to
// GracefulShutdownTimeout, before returning. See Options.GracefulShutdownTimeout for the knob.
func (c *Controller) Run(ctx context.Context, numWorkers int) error {
	defer utilruntime.HandleCrash()
	// Idempotent (CAS-guarded): guarantees the queue is shut down on any early return below
	// (e.g. cache-sync failure). The drain path also calls ShutDown explicitly.
	defer c.queue.ShutDown()

	c.logger.Info("Starting controller")
	// The informer runs under the caller's ctx, so event intake stops the instant SIGTERM
	// cancels it — no new work is admitted once shutdown begins.
	go c.informer.Run(ctx.Done())

	// Start metrics server in goroutine so it doesn't block
	if c.metricsServer != nil {
		go func() {
			if err := c.metricsServer.WithLogger(c.logger).Start(ctx); err != nil {
				c.logger.Error(err, "metrics server failed")
			}
		}()
	}

	// Wait for all involved caches to be synced, before
	// processing items from the queue is started
	c.logger.Info("waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced) {
		err := fmt.Errorf("failed to wait for informers caches to sync")
		utilruntime.HandleError(err)
		return err
	}

	// reconcileCtx is derived from context.Background(), NOT from the caller's ctx, so an
	// in-flight reconcile keeps a live context (its API writes complete) even after SIGTERM
	// cancels ctx. It is cancelled only once the drain finishes or the grace period expires.
	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	defer reconcileCancel()

	// stopWorkers stops wait.Until from relaunching runWorker once we begin draining.
	stopWorkers := make(chan struct{})
	var wg sync.WaitGroup

	c.logger.Info(fmt.Sprintf("Starting workers: %d", numWorkers))
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// wait.Until preserves the panic-restart-with-2s-backoff behavior. runWorker
			// returns when the queue is shut down (GetWithPriority reports shutdown), and
			// closing stopWorkers stops the relaunch. Neither cancels reconcileCtx, so an
			// in-flight reconcile is never severed by loop teardown — only by the explicit
			// reconcileCancel() below once the grace period is exhausted.
			wait.Until(func() {
				c.runWorker(reconcileCtx)
			}, 2*time.Second, stopWorkers)
		}()
	}
	c.logger.Info("Controller ready.")

	<-ctx.Done() // SIGTERM (or caller cancel): begin the bounded drain.
	timeout := c.getGracefulShutdownTimeout()
	c.logger.Info("Stopping controller; draining in-flight reconciles", "gracePeriod", timeout.String())

	// Stop relaunching workers and wake any worker parked in GetWithPriority.
	close(stopWorkers)
	c.queue.ShutDown()

	if timeout == 0 {
		// Graceful shutdown disabled: reproduce the pre-drain behavior exactly — cancel in-flight
		// reconciles and return immediately WITHOUT awaiting workers. Awaiting them here would let a
		// reconcile that ignores ctx hold the process open indefinitely, which the old fire-and-forget
		// code never did. The workers exit under the cancelled reconcileCtx / shut-down queue as the
		// process tears down.
		reconcileCancel()
		return nil
	}

	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()

	if timeout < 0 {
		<-drained // wait forever for in-flight reconciles
		c.logger.Info("All workers drained cleanly")
		return nil
	}

	select {
	case <-drained:
		c.logger.Info("All workers drained cleanly")
	case <-time.After(timeout):
		// Grace period expired: cancel in-flight reconciles and return. A reconcile that
		// honors ctx unwinds promptly; one that ignores it is abandoned (the process exits
		// and the pod's terminationGracePeriodSeconds is the hard ceiling).
		c.logger.Info("Graceful shutdown period expired; cancelling in-flight reconciles")
		reconcileCancel()
	}
	return nil
}

// SetGracefulShutdownTimeout overrides the drain window at runtime. It exists as the seam for
// a future leader-election runnable to force an immediate release on lease loss
// (SetGracefulShutdownTimeout(0) in OnStoppedLeading, mirroring controller-runtime), avoiding a
// split-brain double-writer during HA rollouts.
func (c *Controller) SetGracefulShutdownTimeout(d time.Duration) {
	c.gracefulShutdownMu.Lock()
	defer c.gracefulShutdownMu.Unlock()
	c.gracefulShutdownTimeout = d
}

func (c *Controller) getGracefulShutdownTimeout() time.Duration {
	c.gracefulShutdownMu.RLock()
	defer c.gracefulShutdownMu.RUnlock()
	return c.gracefulShutdownTimeout
}
