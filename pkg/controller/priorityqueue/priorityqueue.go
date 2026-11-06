package priorityqueue

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"github.com/krateo-platformops/unstructured-runtime/pkg/logging"
	"github.com/krateo-platformops/unstructured-runtime/pkg/workqueue/metrics"
)

// AddOpts describes the options for adding items to the queue.
type AddOpts struct {
	After       time.Duration
	RateLimited bool
	Priority    int
}

// PriorityQueue is a priority queue for a controller. It
// internally de-duplicates all items that are added to
// it. It will use the max of the passed priorities and the
// min of possible durations.
type PriorityQueue[T comparable] interface {
	workqueue.TypedRateLimitingInterface[T]
	AddWithOpts(o AddOpts, Items ...T)
	GetWithPriority() (item T, priority int, shutdown bool)
}

// Opts contains the options for a PriorityQueue.
type Opts[T comparable] struct {
	// Ratelimiter is being used when AddRateLimited is called. Defaults to a per-item exponential backoff
	// limiter with an initial delay of five milliseconds and a max delay of 1000 seconds.
	RateLimiter    workqueue.TypedRateLimiter[T]
	MetricProvider workqueue.MetricsProvider
	Log            logging.Logger
}

// Opt allows to configure a PriorityQueue.
type Opt[T comparable] func(*Opts[T])

// New constructs a new PriorityQueue.
func New[T comparable](name string, o ...Opt[T]) PriorityQueue[T] {
	opts := &Opts[T]{}
	for _, f := range o {
		f(opts)
	}

	if opts.RateLimiter == nil {
		opts.RateLimiter = workqueue.NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second)
	}

	if opts.MetricProvider == nil {
		opts.MetricProvider = metrics.WorkqueueMetricsProvider{}
	}

	if opts.Log == nil {
		opts.Log = logging.NewNopLogger()
	}

	pq := &priorityqueue[T]{
		log:          opts.Log,
		items:        map[T]*item[T]{},
		queue:        btree.NewG(32, less[T]),
		blockForever: make(chan time.Time),
		metrics:      newQueueMetrics[T](opts.MetricProvider, name, clock.RealClock{}),
		// itemOrWaiterAdded indicates that an item or
		// waiter was added. It must be buffered, because
		// if we currently process items we can't tell
		// if that included the new item/waiter.
		itemOrWaiterAdded: make(chan struct{}, 1),
		rateLimiter:       opts.RateLimiter,
		locked:            sets.Set[T]{},
		lockedPriority:    make(map[T]int),
		done:              make(chan struct{}),
		get:               make(chan item[T]),
		now:               time.Now,
		tick:              time.Tick,
	}

	go pq.spin()
	go pq.logState()
	if _, ok := pq.metrics.(noMetrics[T]); !ok {
		go pq.updateUnfinishedWorkLoop()
	}

	return pq
}

type priorityqueue[T comparable] struct {
	log logging.Logger
	// lock has to be acquired for any access any of items, queue, addedCounter
	lock  sync.Mutex
	items map[T]*item[T]
	queue bTree[*item[T]]

	// addedCounter is a counter of elements added, we need it
	// because unixNano is not guaranteed to be unique.
	addedCounter uint64

	// blockForever is the wake-up channel when no delay is pending: nothing ever sends on it.
	blockForever chan time.Time
	metrics      queueMetrics[T]

	itemOrWaiterAdded chan struct{}

	rateLimiter workqueue.TypedRateLimiter[T]

	// locked contains the keys we handed out through Get() and that haven't
	// yet been returned through Done().
	locked     sets.Set[T]
	lockedLock sync.RWMutex

	lockedPriority map[T]int

	shutdown atomic.Bool
	done     chan struct{}

	get chan item[T]

	// waiters is the number of routines blocked in Get, we use it to determine
	// if we can push items.
	waiters atomic.Int64

	// Configurable for testing
	now  func() time.Time
	tick func(time.Duration) <-chan time.Time
}

func (w *priorityqueue[T]) AddWithOpts(o AddOpts, items ...T) {
	if w.shutdown.Load() {
		return
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	for _, key := range items {
		after := o.After
		if o.RateLimited {
			rlAfter := w.rateLimiter.When(key)
			if after == 0 || rlAfter < after {
				after = rlAfter
			}
		}

		var readyAt *time.Time
		if after > 0 {
			readyAt = ptr.To(w.now().Add(after))
			w.metrics.retry()
		}
		if _, ok := w.items[key]; !ok {
			item := &item[T]{
				Key:          key,
				AddedCounter: w.addedCounter,
				Priority:     o.Priority,
				ReadyAt:      readyAt,
			}
			w.items[key] = item
			w.queue.ReplaceOrInsert(item)
			if item.ReadyAt == nil {
				w.metrics.add(key, item.Priority)
			}
			w.addedCounter++
			continue
		}

		// The b-tree de-duplicates based on ordering and any change here
		// will affect the order - Just delete and re-add.
		item, _ := w.queue.Delete(w.items[key])
		if o.Priority > item.Priority {
			// Update depth metric only if the item in the queue was already added to the depth metric.
			if item.ReadyAt == nil {
				w.metrics.updateDepthWithPriorityMetric(item.Priority, o.Priority)
			}
			item.Priority = o.Priority
		}

		if item.ReadyAt != nil && (readyAt == nil || readyAt.Before(*item.ReadyAt)) {
			if readyAt == nil {
				w.metrics.add(key, item.Priority)
			}
			item.ReadyAt = readyAt
		}

		w.queue.ReplaceOrInsert(item)
	}

	if len(items) > 0 {
		w.notifyItemOrWaiterAdded()
	}
}

func (w *priorityqueue[T]) notifyItemOrWaiterAdded() {
	select {
	case w.itemOrWaiterAdded <- struct{}{}:
	default:
	}
}

func (w *priorityqueue[T]) spin() {
	var nextReady <-chan time.Time = w.blockForever

	for {
		select {
		case <-w.done:
			return
		case <-w.itemOrWaiterAdded:
		case <-nextReady:
		}

		func() {
			w.lock.Lock()
			defer w.lock.Unlock()

			w.lockedLock.Lock()
			defer w.lockedLock.Unlock()

			nextReady = w.promoteDueItems()

			// manipulating the tree from within Ascend might lead to panics, so
			// track what we want to delete and do it after we are done ascending.
			var toDelete []*item[T]
			w.queue.Ascend(func(item *item[T]) bool {
				// Delayed items sort last, so this is the end of what can be handed out.
				if item.ReadyAt != nil || w.waiters.Load() == 0 {
					return false
				}

				// Item is locked, we can not hand it out
				if w.locked.Has(item.Key) {
					return true
				}

				w.metrics.get(item.Key, item.Priority)
				w.locked.Insert(item.Key)
				w.lockedPriority[item.Key] = item.Priority
				w.waiters.Add(-1)
				delete(w.items, item.Key)
				toDelete = append(toDelete, item)
				// The hand-off is made selectable on w.done: a parked worker in GetWithPriority
				// now also selects on w.done, so on shutdown it may wake there instead of
				// receiving this item. A bare blocking send would then wedge forever while this
				// closure holds BOTH w.lock and w.lockedLock, hanging every subsequent Len()/Done()
				// and the graceful drain. On shutdown, abandon the hand-off and stop ascending so
				// the deferred Unlocks release the locks (the item stays "locked" but undelivered —
				// harmless during shutdown, it is simply dropped and re-listed by the next process).
				select {
				case w.get <- *item:
				case <-w.done:
					return false
				}

				return true
			})

			for _, item := range toDelete {
				w.queue.Delete(item)
			}
		}()
	}
}

// promoteDueItems turns every item whose delay has passed into an ordinary queued item, and returns
// the channel that fires when the next delay does.
//
// A DELAY HOLDS AN ITEM BACK UNTIL IT EXPIRES, AND NO LONGER. Delayed items sort behind undelayed ones
// so the walk can stop at the first that is not due. An expired item used to keep its ReadyAt and so
// its place behind them, whatever its priority, and it was reached only when nothing else was queued.
// In a controller whose resync demand exceeds its capacity the queue never empties, so it was never
// reached: the requeue of a lost status race, every rate-limited retry and every deferral starved. The
// digest they left stored then made each later resync of that object a duplicate, and the object went
// silent until a restart, with no error and every condition green.
//
// Callers must hold w.lock.
func (w *priorityqueue[T]) promoteDueItems() <-chan time.Time {
	var next <-chan time.Time = w.blockForever
	var due []*item[T]
	w.queue.Ascend(func(item *item[T]) bool {
		if item.ReadyAt == nil {
			return true
		}
		if remaining := item.ReadyAt.Sub(w.now()); remaining > 0 {
			next = w.tick(remaining)
			return false
		}
		due = append(due, item)
		return true
	})

	// The b-tree orders on ReadyAt, so an item cannot change it in place.
	for _, item := range due {
		w.queue.Delete(item)
		item.ReadyAt = nil
		// It joins its priority's queue now, so items come out in the order they became due.
		item.AddedCounter = w.addedCounter
		w.addedCounter++
		w.queue.ReplaceOrInsert(item)
		w.metrics.add(item.Key, item.Priority)
	}
	return next
}

func (w *priorityqueue[T]) Add(item T) {
	w.AddWithOpts(AddOpts{}, item)
}

func (w *priorityqueue[T]) AddAfter(item T, after time.Duration) {
	w.AddWithOpts(AddOpts{After: after}, item)
}

func (w *priorityqueue[T]) AddRateLimited(item T) {
	w.AddWithOpts(AddOpts{RateLimited: true}, item)
}

func (w *priorityqueue[T]) GetWithPriority() (_ T, priority int, shutdown bool) {
	if w.shutdown.Load() {
		var zero T
		return zero, 0, true
	}

	w.waiters.Add(1)

	w.notifyItemOrWaiterAdded()

	select {
	case <-w.done:
		// The queue was shut down while this worker was parked waiting for an item.
		// Without this branch a worker blocked on <-w.get is never woken by ShutDown()
		// and a graceful drain would hang until its timeout on every shutdown.
		//
		// We intentionally do NOT decrement w.waiters here. The counter is only consulted by spin()
		// to decide whether to hand out items; at shutdown any resulting stale count merely lets
		// spin() lock-and-abandon one extra item (harmless — the item is dropped and re-listed by the
		// next process), and spin()'s hand-off is itself selectable on w.done so it can never wedge.
		var zero T
		return zero, 0, true
	case item := <-w.get:
		return item.Key, item.Priority, w.shutdown.Load()
	}
}

func (w *priorityqueue[T]) Get() (item T, shutdown bool) {
	key, _, shutdown := w.GetWithPriority()
	return key, shutdown
}

func (w *priorityqueue[T]) Forget(item T) {
	w.rateLimiter.Forget(item)
}

func (w *priorityqueue[T]) NumRequeues(item T) int {
	return w.rateLimiter.NumRequeues(item)
}

func (w *priorityqueue[T]) ShuttingDown() bool {
	return w.shutdown.Load()
}

func (w *priorityqueue[T]) Done(item T) {
	w.lockedLock.Lock()
	defer w.lockedLock.Unlock()

	priority := w.lockedPriority[item]
	w.locked.Delete(item)
	delete(w.lockedPriority, item)
	w.metrics.done(item, priority)
	w.notifyItemOrWaiterAdded()
}

func (w *priorityqueue[T]) ShutDown() {
	// CompareAndSwap makes ShutDown idempotent: it may be called both from the
	// controller's drain path and a deferred cleanup. Closing w.done wakes every
	// worker parked in GetWithPriority and stops the spin()/logState()/unfinished-work
	// background goroutines.
	if w.shutdown.CompareAndSwap(false, true) {
		close(w.done)
	}
}

// ShutDownWithDrain just calls ShutDown, as the draining
// functionality is not used by controller-runtime.
func (w *priorityqueue[T]) ShutDownWithDrain() {
	w.ShutDown()
}

// Len returns the number of items that are ready to be
// picked up. It does not include items that are not yet
// ready.
func (w *priorityqueue[T]) Len() int {
	w.lock.Lock()
	defer w.lock.Unlock()

	var result int
	var count int
	w.queue.Ascend(func(item *item[T]) bool {
		count++
		if item.ReadyAt == nil || item.ReadyAt.Compare(w.now()) <= 0 {
			result++
			return true
		}
		return false
	})

	return result
}

func (w *priorityqueue[T]) logState() {
	t := time.Tick(10 * time.Second)
	for {
		select {
		case <-w.done:
			return
		case <-t:
		}

		w.lock.Lock()
		items := make([]*item[T], 0, len(w.items))
		w.queue.Ascend(func(item *item[T]) bool {
			items = append(items, item)
			return true
		})
		w.lock.Unlock()

		w.log.Debug("workqueue items", "items", items)
	}
}

func less[T comparable](a, b *item[T]) bool {
	if a.ReadyAt == nil && b.ReadyAt != nil {
		return true
	}
	if b.ReadyAt == nil && a.ReadyAt != nil {
		return false
	}
	if a.ReadyAt != nil && b.ReadyAt != nil && !a.ReadyAt.Equal(*b.ReadyAt) {
		return a.ReadyAt.Before(*b.ReadyAt)
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}

	return a.AddedCounter < b.AddedCounter
}

type item[T comparable] struct {
	Key          T          `json:"key"`
	AddedCounter uint64     `json:"addedCounter"`
	Priority     int        `json:"priority"`
	ReadyAt      *time.Time `json:"readyAt,omitempty"`
}

func (w *priorityqueue[T]) updateUnfinishedWorkLoop() {
	t := time.Tick(500 * time.Millisecond) // borrowed from workqueue: https://github.com/kubernetes/kubernetes/blob/67a807bf142c7a2a5ecfdb2a5d24b4cdea4cc79c/staging/src/k8s.io/client-go/util/workqueue/queue.go#L182
	for {
		select {
		case <-w.done:
			return
		case <-t:
		}
		w.metrics.updateUnfinishedWork()
	}
}

type bTree[T any] interface {
	ReplaceOrInsert(item T) (_ T, _ bool)
	Delete(item T) (T, bool)
	Ascend(iterator btree.ItemIteratorG[T])
}
