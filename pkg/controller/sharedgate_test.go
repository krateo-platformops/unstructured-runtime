package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/priorityqueue"
	"github.com/krateo-platformops/unstructured-runtime/pkg/ratelimit"
	"github.com/stretchr/testify/require"
)

// core-provider#561. Two controllers share one governor with a process total of ONE slot, as two
// members of a cdc pool do. The idle one's worker is started first and blocks in its queue; the busy
// one has an item. The busy one must run it. With the slot taken before the pop, the idle worker holds
// the only slot while it waits for work that never comes, and the busy controller never runs.
//
// Control: the same item on the same shared governor, with no idle controller competing, runs, so a
// pass here is not just the busy controller being broken in some other way.
func TestSharedGovernor_AnIdleControllerDoesNotStarveABusyOne(t *testing.T) {
	for _, withIdle := range []bool{false, true} {
		name := "control: busy controller alone"
		if withIdle {
			name = "an idle controller shares the only slot"
		}
		t.Run(name, func(t *testing.T) {
			g := ratelimit.New(ratelimit.Options{Ceiling: 1, Shared: true})
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			var queues []func()
			t.Cleanup(func() {
				cancel()
				for _, q := range queues {
					q()
				}
				wg.Wait()
			})

			if withIdle {
				idle, _, _ := gateController(t, g, &countingClient{})
				idle.runCtx = ctx
				queues = append(queues, idle.queue.ShutDown)
				wg.Add(1)
				go func() { defer wg.Done(); idle.runWorker(ctx) }()
				time.Sleep(50 * time.Millisecond) // the idle worker is in its queue's Get by now
			}

			cc := &countingClient{}
			busy, evt, _ := gateController(t, g, cc)
			busy.runCtx = ctx
			queues = append(queues, busy.queue.ShutDown)
			busy.items.Store(ctrlevent.DigestForEvent(evt), struct{}{})
			busy.queue.AddWithOpts(priorityqueue.AddOpts{}, evt)
			wg.Add(1)
			go func() { defer wg.Done(); busy.runWorker(ctx) }()

			waitFor(t, "the busy controller's item to run", func() bool { return cc.calls.Load() > 0 })
		})
	}
}

// The slot is given back on every way out of a shared pass, and a worker that is shut down while it
// waits for a slot puts its item back instead of dropping it.
func TestSharedGovernor_SlotReturnedAndItemKeptOnShutdown(t *testing.T) {
	g := ratelimit.New(ratelimit.Options{Ceiling: 1, Shared: true})
	cc := &countingClient{}
	ctrl, evt, _ := gateController(t, g, cc)
	t.Cleanup(ctrl.queue.ShutDown)
	ctrl.items.Store(ctrlevent.DigestForEvent(evt), struct{}{})
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, evt)
	ctrl.runCtx = context.Background()
	require.True(t, ctrl.processNext(context.Background(), "test"))
	release, ok := g.(ratelimit.LocalGate).AcquireLocal(deadline(t))
	require.True(t, ok, "the shared slot was not returned after a pass")

	// The slot is now held elsewhere; a worker that pops and then cannot get it before shutdown must
	// leave the item in the queue.
	ctrl.queue.AddWithOpts(priorityqueue.AddOpts{}, evt)
	ctx, cancel := context.WithCancel(context.Background())
	ctrl.runCtx = ctx
	done := make(chan bool)
	go func() { done <- ctrl.processNext(context.Background(), "test") }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.False(t, <-done, "a worker shut down while waiting for a slot carried on")
	require.Equal(t, 1, ctrl.queue.Len(), "the item it held was dropped")
	release()
}
