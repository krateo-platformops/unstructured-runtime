package controller

import (
	"sync"

	ctrlevent "github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
)

// objectGate lets at most ONE pass run per object at a time (#62).
//
// The queue cannot do this on its own. Its key is the whole ctrlevent.Event, QueuedAt included, so two
// events for one object are two keys, and the workqueue's one-at-a-time guarantee is per key. The
// `items` dedup hashes eventType+objectRef, so a Create and an Observe for one object do not dedup
// either. Two workers then reconciled one object concurrently. The case that made it visible: the
// runtime's own external-create annotation write fired an informer event while Create was still
// running, a second pass read the object before Create recorded its inventory, and its status write
// replaced Create's (core-provider#359).
//
// An event for an object that is already in flight is PARKED, not dropped and not run: one per
// event type, so a Delete that arrives during an Observe is never lost behind it. When the running
// pass ends, everything parked for that object goes back on the queue, where the next worker
// processes it against the object as it now is. Different objects still run in parallel.
type objectGate struct {
	mu      sync.Mutex
	running map[objectref.ObjectRef]bool
	parked  map[objectref.ObjectRef]map[ctrlevent.EventType]parkedEvent
}

type parkedEvent struct {
	event    ctrlevent.Event
	priority int
}

func newObjectGate() *objectGate {
	return &objectGate{
		running: map[objectref.ObjectRef]bool{},
		parked:  map[objectref.ObjectRef]map[ctrlevent.EventType]parkedEvent{},
	}
}

// begin claims ev's object. It returns false, having parked ev, when another pass holds the object.
// A later event of the same type replaces the parked one, keeping the HIGHER priority of the two.
func (g *objectGate) begin(ev ctrlevent.Event, priority int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.running[ev.ObjectRef] {
		g.running[ev.ObjectRef] = true
		return true
	}
	byType := g.parked[ev.ObjectRef]
	if byType == nil {
		byType = map[ctrlevent.EventType]parkedEvent{}
		g.parked[ev.ObjectRef] = byType
	}
	if prev, ok := byType[ev.EventType]; ok && prev.priority > priority {
		priority = prev.priority
	}
	byType[ev.EventType] = parkedEvent{event: ev, priority: priority}
	return false
}

// end releases ev's object and returns what was parked for it, for the caller to requeue.
func (g *objectGate) end(ev ctrlevent.Event) []parkedEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.running, ev.ObjectRef)
	byType := g.parked[ev.ObjectRef]
	delete(g.parked, ev.ObjectRef)
	out := make([]parkedEvent, 0, len(byType))
	for _, p := range byType {
		out = append(out, p)
	}
	return out
}
