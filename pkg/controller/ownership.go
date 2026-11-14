package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Ownership makes a controller reconcile only while it holds a coordination.k8s.io Lease, so two
// processes started for the same resource never reconcile it at the same time
// (core-provider's docs/design/cdc-pooling.md, step 2).
//
// Two writers for one resource arise in two ordinary ways. A rolling update runs the old and the new
// pod side by side until the new one is Ready. Moving a resource between a dedicated controller and
// a shared one (the pool) briefly runs both. Each process would reconcile every object, and when
// their versions disagree each undoes the other's writes.
//
// With Ownership set, Run:
//   - starts the informer and waits for its first list as before, so ReadyzCheck keeps meaning "this
//     controller can see its resource". A controller waiting for the Lease is Ready: under a
//     Deployment's default rolling update (maxUnavailable 0) the old pod is stopped only once the new
//     one is Ready, so a readiness that waited for the Lease would never let the old pod go;
//   - then waits for the Lease before starting any worker. The informer keeps queuing meanwhile, so
//     the first worker starts with every object already queued;
//   - on shutdown, drains in-flight reconciles and only THEN releases the Lease, so the next owner
//     starts after this one has stopped writing (stop-before-start);
//   - on losing the Lease (it could not be renewed within RenewDeadline, so another process may
//     already own it), cancels in-flight reconciles at once and returns ErrOwnershipLost.
//
// A Lease that cannot be read or written (for example, the controller lacks the RBAC) makes
// ReadyzCheck fail with that error, so a controller that can never own is not reported Ready.
//
// Nil Ownership is the behaviour before this option existed: no Lease, workers start after the
// first list.
type Ownership struct {
	// Leases reads and writes the Lease. Required.
	Leases coordinationv1client.LeasesGetter
	// Namespace and Name identify the Lease. Required. Every process that may reconcile the same
	// resource must be given the same pair; that is the whole guarantee.
	Namespace string
	Name      string
	// Identity names this process as the holder. Required, and unique per process: the pod name.
	Identity string

	// LeaseDuration, RenewDeadline and RetryPeriod are client-go's leader-election timings. Zero
	// takes the default (15s, 10s, 2s), the same as controller-runtime's.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

const (
	defaultOwnershipLeaseDuration = 15 * time.Second
	defaultOwnershipRenewDeadline = 10 * time.Second
	defaultOwnershipRetryPeriod   = 2 * time.Second
)

// ErrOwnershipLost is returned by Run when the controller lost its ownership Lease while
// reconciling. The workers were stopped at once. A process should exit, so that it restarts and
// waits for the Lease again.
var ErrOwnershipLost = errors.New("lost the ownership lease: stopped reconciling, another process may own this resource")

func (o *Ownership) validate() error {
	if o == nil {
		return nil
	}
	switch {
	case o.Leases == nil:
		return fmt.Errorf("ownership: a Leases client is required")
	case o.Namespace == "" || o.Name == "":
		return fmt.Errorf("ownership: the Lease namespace and name are required")
	case o.Identity == "":
		return fmt.Errorf("ownership: an identity is required")
	}
	return nil
}

func (o *Ownership) timings() (lease, renew, retry time.Duration) {
	lease, renew, retry = o.LeaseDuration, o.RenewDeadline, o.RetryPeriod
	if lease == 0 {
		lease = defaultOwnershipLeaseDuration
	}
	if renew == 0 {
		renew = defaultOwnershipRenewDeadline
	}
	if retry == 0 {
		retry = defaultOwnershipRetryPeriod
	}
	return lease, renew, retry
}

func (o *Ownership) describe() string { return o.Namespace + "/" + o.Name }

// ownershipState is what the Lease attempts have shown: the last error reading or writing the
// Lease (nil once an attempt succeeds) and the holder last seen.
type ownershipState struct {
	mu     sync.Mutex
	err    error
	holder string
}

func (s *ownershipState) set(err error, holder string, seen bool) (holderChanged bool, prev string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
	if !seen || holder == s.holder {
		return false, s.holder
	}
	prev, s.holder = s.holder, holder
	return true, prev
}

func (s *ownershipState) lastErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// recordingLock is the Lease lock, recording what each attempt shows. client-go's elector retries
// every failure silently except for a log line, so without this a controller that can never take
// the Lease would wait forever while reporting Ready.
type recordingLock struct {
	resourcelock.Interface
	c *Controller
}

func (l *recordingLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	rec, raw, err := l.Interface.Get(ctx)
	switch {
	case err == nil:
		l.c.observeOwnership(nil, rec.HolderIdentity, true)
	case apierrors.IsNotFound(err):
		// Nobody has taken it yet; the elector creates it next.
		l.c.observeOwnership(nil, "", false)
	default:
		l.c.observeOwnership(err, "", false)
	}
	return rec, raw, err
}

func (l *recordingLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Create(ctx, ler)
	switch {
	case err == nil:
		l.c.observeOwnership(nil, ler.HolderIdentity, true)
	case apierrors.IsAlreadyExists(err):
		// Another process created it first; the next attempt reads it.
		l.c.observeOwnership(nil, "", false)
	default:
		l.c.observeOwnership(err, "", false)
	}
	return err
}

func (l *recordingLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Update(ctx, ler)
	switch {
	case err == nil:
		l.c.observeOwnership(nil, ler.HolderIdentity, true)
	case apierrors.IsConflict(err):
		// Another process wrote it between our read and our write; the next attempt re-reads.
		l.c.observeOwnership(nil, "", false)
	default:
		l.c.observeOwnership(err, "", false)
	}
	return err
}

func (c *Controller) observeOwnership(err error, holder string, seen bool) {
	changed, prev := c.ownershipState.set(err, holder, seen)
	if err != nil {
		c.logger.Error(err, "Cannot read or write the ownership lease; this controller does not reconcile until it can",
			"lease", c.ownership.describe(), "identity", c.ownership.Identity)
		return
	}
	if changed && holder != c.ownership.Identity {
		c.logger.Info("Ownership lease is held by another process; waiting for it",
			"lease", c.ownership.describe(), "holder", holder, "previousHolder", prev, "identity", c.ownership.Identity)
	}
}

// ownershipReadyzError is the readiness half of Ownership: an error reading or writing the Lease.
func (c *Controller) ownershipReadyzError() error {
	if c.ownership == nil {
		return nil
	}
	if err := c.ownershipState.lastErr(); err != nil {
		return fmt.Errorf("cannot read or write the ownership lease %s: %w", c.ownership.describe(), err)
	}
	return nil
}

// acquireOwnership waits for the Lease. It returns a context that is cancelled if the Lease is lost,
// and a release func that gives the Lease up and waits until it has. Both are nil when ctx ended
// first, in which case nothing was acquired and nothing needs releasing.
//
// The elector runs under its own context, not ctx, so that a SIGTERM does not release the Lease
// while in-flight reconciles are still draining: release is called after the drain.
func (c *Controller) acquireOwnership(ctx context.Context) (context.Context, func(), error) {
	o := c.ownership
	lease, renew, retry := o.timings()
	lock := &recordingLock{
		Interface: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Namespace: o.Namespace, Name: o.Name},
			Client:     o.Leases,
			LockConfig: resourcelock.ResourceLockConfig{Identity: o.Identity},
		},
		c: c,
	}
	acquired := make(chan context.Context, 1)
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   lease,
		RenewDeadline:   renew,
		RetryPeriod:     retry,
		ReleaseOnCancel: true,
		Name:            o.describe(),
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(lead context.Context) { acquired <- lead },
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("ownership: %w", err)
	}

	electCtx, stopElect := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		le.Run(electCtx)
	}()
	release := func() {
		stopElect()
		<-done
	}

	c.logger.Info("Waiting for the ownership lease before reconciling", "lease", o.describe(), "identity", o.Identity)
	select {
	case lead := <-acquired:
		c.logger.Info("Holding the ownership lease; starting workers", "lease", o.describe(), "identity", o.Identity)
		return lead, release, nil
	case <-ctx.Done():
		release()
		c.logger.Info("Stopped before taking the ownership lease; nothing was reconciled", "lease", o.describe())
		return nil, nil, nil
	}
}
