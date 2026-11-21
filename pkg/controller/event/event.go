package event

import (
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/objectref"
)

type EventType string

const (
	Observe EventType = "Observe"
	Create  EventType = "Create"
	Update  EventType = "Update"
	Delete  EventType = "Delete"
)

type Event struct {
	EventType EventType
	ObjectRef objectref.ObjectRef
	QueuedAt  time.Time
	// ObservedExisting is set on an Update that handleObserve queued after it OBSERVED the external
	// resource to exist (and drift). Every other Update (the informer's CRUpdated, which maps to
	// Update by default) carries no such knowledge. When krateo.io/management-policy withholds create
	// but allows update (a verb set such as "observe,update", #110 D1), such an Update is dispatched
	// as Observe first: calling the handler's Update on an absent resource can create it (an upsert
	// PUT), the one write that policy forbids. Not part of the dedup digest.
	ObservedExisting bool
}

func (e Event) CompareTo(other Event) int {
	if e.QueuedAt.Before(other.QueuedAt) {
		return -1
	}
	if e.QueuedAt.After(other.QueuedAt) {
		return 1
	}
	return 0
}
