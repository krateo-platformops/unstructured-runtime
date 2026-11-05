package workqueue

import (
	"math"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
)

var _ workqueue.TypedRateLimiter[string] = &TypedItemExponentialTimedFailureRateLimiter[string]{}

type FailureRequest struct {
	// The number of times the request has been attempted
	Attempts int
	// The time at which the request was last attempted
	LastAttempt time.Time
}

type TypedItemExponentialTimedFailureRateLimiter[T comparable] struct {
	failuresLock sync.Mutex
	failures     map[T]FailureRequest

	baseDelay time.Duration
	maxDelay  time.Duration

	// now is injectable so a test can cross the long-gap boundary without sleeping through it.
	now func() time.Time
}

func NewExponentialTimedFailureRateLimiter[T comparable](baseDelay time.Duration, maxDelay time.Duration) workqueue.TypedRateLimiter[T] {
	return &TypedItemExponentialTimedFailureRateLimiter[T]{
		failures:  map[T]FailureRequest{},
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		now:       time.Now,
	}
}

func (r *TypedItemExponentialTimedFailureRateLimiter[T]) When(item T) time.Duration {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	failreq, ok := r.failures[item]
	if !ok {
		r.failures[item] = FailureRequest{Attempts: 1, LastAttempt: r.now()}
		return r.baseDelay
	}

	// EVERY FAILURE IS COUNTED, however long since the last one. This used to return 0 — without
	// updating Attempts or LastAttempt — once 2*maxDelay had passed since the last attempt. Because
	// LastAttempt then never moved, every later call took the same branch: the delay stayed 0 and
	// NumRequeues froze, so maxRetries could never fire and a failing event was retried with NO
	// backoff, forever. A gap that long is ordinary for a slow reconcile behind a long queue (the
	// cdc runs 1s/60s, so 120s), and in the unit test it froze within one slow retry.
	//
	// NOT "reset to baseDelay after a quiet period", the other reading of the old intent. That keeps
	// the backoff but makes maxRetries unreachable for any event whose retries are always slower
	// than 2*maxDelay — the drop is a contract, and it must hold for slow failures too. Recovery is
	// Forget's job, which runWorker calls on success (#52).
	exp := failreq.Attempts
	failreq.Attempts = failreq.Attempts + 1
	failreq.LastAttempt = r.now()
	r.failures[item] = failreq

	// The backoff is capped such that 'calculated' value never overflows.
	backoff := float64(r.baseDelay.Nanoseconds()) * math.Pow(2, float64(exp))
	if backoff > math.MaxInt64 {
		return r.maxDelay
	}

	calculated := time.Duration(backoff)

	if calculated > r.maxDelay {
		return r.maxDelay
	}
	return calculated
}

func (r *TypedItemExponentialTimedFailureRateLimiter[T]) NumRequeues(item T) int {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	return r.failures[item].Attempts
}

func (r *TypedItemExponentialTimedFailureRateLimiter[T]) Forget(item T) {
	r.failuresLock.Lock()
	defer r.failuresLock.Unlock()

	// UNCONDITIONAL. The idle-time guard existed because runWorker used to call Forget after EVERY
	// pass, failures included, and deleting then would have wiped each failing item's history. Since
	// #52 Forget is called on success only, so the guard did nothing useful and kept every
	// failed-then-recovered entry in the map: a leak, one entry per event instance.
	delete(r.failures, item)
}
