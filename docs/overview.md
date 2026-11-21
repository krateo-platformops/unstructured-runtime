---
type: Architecture
title: unstructured-runtime — overview
description: The reconcile loop and recovery semantics of Krateo's composition runtime, traced to pkg/controller source.
resource: github.com/krateo-platformops/unstructured-runtime
tags: [managed-reconciler, external-create, priority-queue, recovery]
timestamp: 2026-08-07T00:00:00Z
---

# Overview

unstructured-runtime is a **managed-reconciler framework**: you implement four
methods against an external system, the runtime supplies everything else — the
watch, the queue, retry, state tracking, recovery, telemetry. It works purely
on `unstructured.Unstructured` through a `dynamic.Interface`, which is what
lets one binary reconcile CRDs that are generated at runtime (Krateo
compositions) with no compiled types. This is Krateo's own runtime, shared by
every composition on the platform through `composition-dynamic-controller`.

```
informer (listwatcher) ──classify──▶ dedup map ──▶ priority queue ──▶ workers ──▶ processItem
                                                        ▲                             │
                                                        └──── rate-limited retry ◀────┘
                                                                            ExternalClient
                                                                    Observe / Create / Update / Delete
```

## The contract: `ExternalClient`

`pkg/controller/controller.go:49` — four methods, all taking the managed CR as
`*unstructured.Unstructured`:

- `Observe` returns `ExternalObservation{ResourceExists, ResourceUpToDate}`.
- `Create`, `Update`, `Delete` act on the external system.

The interface documentation states the two hard requirements: **none of the
calls may block** and **all must be idempotent** (`Create` must not return
AlreadyExists on a repeat call; `Delete` must not error when the resource is
already gone). A third requirement is enforced by the recovery design below:
**`Observe` must be a pure read — it must never write the CR** (see
[the side-effect-free rule](#observe-must-be-side-effect-free)).

## Event intake and classification

`controller.New` (`pkg/controller/controller.go:174`) builds a plain informer
over a `cache.ListWatch` (`pkg/listwatcher`, optional label/field selectors and
namespace scoping) with the configured resync interval. The handlers classify
every informer callback into a typed `ctrlevent.Event`
(`Observe|Create|Update|Delete` + an `ObjectRef` + `QueuedAt`):

- **Add** → an Observe event at Normal priority — or Low priority when any
  `krateo.io/external-create-*` annotation is already present, i.e. the
  resource is mid-handshake and this is a replayed add, not fresh user intent
  (`controller.go:229-255`).
- **Update** with a deletion timestamp → Delete event at High priority.
- **Update** matching a **watched annotation** rule (`WatchAnnotations`) →
  the configured event type at Normal priority. The builder registers two by
  default: any change of `krateo.io/paused` → Observe, and deletion of
  `krateo.io/external-create-pending` → Create (the operator's manual-recovery
  lever) (`pkg/controller/builder/options.go:51-52`).
- **Update** with a **spec diff** (`cmp.Diff` of old vs new `spec`) → Update
  event at High priority (`controller.go:358-406`).
- **Update** with an unchanged ResourceVersion → the informer's periodic
  resync → Observe event at Low priority.
- **Update** with a changed ResourceVersion but identical spec → a
  **status-only write, ignored** — this is the controller observing its own
  status updates, and queueing it would self-trigger a loop
  (`controller.go:435-445`).
- **Delete** → Delete event at High priority, with tombstone
  (`DeletedFinalStateUnknown`) recovery; an object that vanishes **without** a
  deletion timestamp (e.g. it stopped matching the label selector) is logged
  and skipped — external cleanup is not run for resources that merely left the
  controller's view (`controller.go:448-504`).

**Dedup**: every event is hashed (`ctrlevent.DigestForEvent`) and recorded in a
`sync.Map`; an event already queued or in flight is not queued again. The digest
is deleted after processing (`worker.go:202`), so the next classification for
that object queues normally.

## Queue, priorities, retry

The queue is a generic **priority queue with rate limiting**
(`pkg/controller/priorityqueue`, btree-based). Three priorities
(`controller.go:36-38`): `HighPriority` (100) for user intent
(create-missing / spec-change / delete), `NormalPriority` (0), `LowPriority`
(-100) for background work (resync observes, mid-handshake replays, recovery
re-observes) so patient work never crowds out user actions.

Fresh events are added **without** rate limiting. Retries are always
rate-limited (`worker.go:238-241`): on error `handleErr` requeues at the same
priority through the configured limiter until `MaxRetries` (default 5) is
reached, then drops the event — the next resync will pick the object up again.
The builder's default limiter is the max of a per-item exponential backoff
(3s → 180s) and a global 10 qps / burst 100 bucket
(`pkg/controller/builder/options.go:66-68`).

## The worker loop

Each worker (`runWorker`, `pkg/controller/worker.go:110`) pops an event,
stamps a trace id, and calls `processItem` (`worker.go:245`), which runs the
per-event pipeline **in this order**:

1. **Fetch** the CR via the dynamic client; if it is gone, drop the event
   silently (normal after deletion).
2. **Start the reconcile span**, continuing any distributed trace carried on
   the CR's `krateo.io/traceparent` annotation (`pkg/telemetry/tracing.go:67`).
3. **Pause**: if `krateo.io/paused: "true"`, set the `ReconcilePaused`
   condition and stop; removing the annotation retriggers (it is a default
   watched annotation).
4. **Orphan delete**: if the CR is being deleted and the management decision
   withholds delete (`krateo.io/deletion-policy` is `orphan` or unrecognised, or
   the management policy has no `delete`), just remove the
   `composition.krateo.io/finalizer` — the external resource is left alone.
5. **Incomplete-create recovery** — see below.
6. **Finalizer**: ensure `composition.krateo.io/finalizer` on any live CR.
7. **Dispatch** on the event type to `handleObserve` / `handleCreate` /
   `handleUpdate` / `handleDelete`.

`handleObserve` (`worker.go:451`) is the decision step: it calls
`ExternalClient.Observe` and queues a High-priority **Create** event if the
external resource does not exist, a High-priority **Update** event if it is
not up to date, or sets `ReconcileSuccess` on the status if it is. All
condition updates go through `tools.UpdateStatus` (status subresource), and
failures are surfaced as Kubernetes events (warnings through a throttled
recorder, `worker.go:915-927`).

## The external-create handshake

`handleCreate` (`worker.go:562`) brackets every external create with the
`krateo.io/external-create-*` annotations (`pkg/meta/meta.go`):

1. Write `external-create-pending: <now>` to the CR **before** calling
   `Create`.
2. Call `ExternalClient.Create`.
3. On failure write `external-create-failed: <now>`; on success write
   `external-create-succeeded: <now>`.

The pending write is **deliberately allowed to fail on a 409 conflict**
(`worker.go:575-581`): it is a plain update, not a retry-on-conflict write,
because the 409 is the point — it *guarantees the reconciler is operating on
the latest version of the resource* before it performs a non-idempotent-in-
practice external create, and it ensures that critical information the
`Create` call may produce (e.g. `krateo.io/external-name`) will be persisted
onto a fresh object rather than silently lost on a stale one. A 409 here is
not an incident; the event is retried and the create simply happens one
reconcile later, on the current version.

`ExternalCreateIncomplete` (`pkg/meta/meta.go:265`) derives the handshake
state: creation is *incomplete* iff the `pending` timestamp is newer than both
`succeeded` and `failed` — i.e. a create started and the process never
persisted its outcome (crash, API timeout, restart mid-create).

## Incomplete-create recovery

Historically an incomplete create meant "refuse forever until a human removes
the pending annotation" — which permanently wedged the resource and everything
depending on it. The runtime instead **reconciles the annotations against
reality by observing** (`worker.go:338-410`):

- **Observe errors** → keep the conservative refusal (condition
  `Creating` + `ReconcileError: cannot determine creation result…`): the
  runtime genuinely cannot tell whether the create landed, and guessing risks
  leaking an orphan external resource.
- **Resource observed present** → the create actually succeeded and only the
  success marker was lost: stamp `external-create-succeeded` and proceed.
- **Resource observed absent, pending is *recent*** → **confirm the negative
  first**: within a 2-minute grace period
  (`externalCreateRecoveryGracePeriod`, `worker.go:91`) an absent resource is
  *not* proof the create failed — eventually-consistent external APIs may not
  show it yet, and recreating now would duplicate it. The runtime requeues a
  Low-priority re-observe every 15s (`worker.go:95`) until the resource
  appears or the grace period elapses.
- **Resource observed absent, grace period elapsed** → the create genuinely
  never landed: roll the handshake back by removing the
  `external-create-pending` marker and proceed to a fresh create.

Manual escape hatch: deleting `krateo.io/external-create-pending` by hand is a
watched annotation that immediately queues a Create event.

## Observe must be side-effect-free

The recovery above — and `handleObserve` itself — call `Observe` with whatever
version of the CR the reconcile is holding. If an `Observe` implementation
writes the CR (labels, annotations, anything via an update), two design
invariants break at once:

1. Any concurrent writer (another controller re-applying the instance, a human
   `kubectl` edit, the reconcile's own annotation write) makes the embedded
   write 409. The recovery treats an `Observe` **error** as "cannot determine
   creation result" and conservatively refuses — so a transient conflict
   becomes a **permanent wedge**: the CR sits with `external-create-pending`
   set, looping on `Operation cannot be fulfilled…`, forever.
2. Every successful `Observe` write advances the ResourceVersion, defeating
   the runtime's intentional fail-on-409 pending write (whose entire job is to
   prove the reconciler holds the latest version).

This is not theoretical: it wedged live Krateo compositions until
[core-provider#67](https://github.com/krateo-platformops/core-provider/pull/67)
made the composition handler's `Observe` a pure read (deterministic metadata
moved into `Create`/`Update`, which are allowed to write). The rule for every
`ExternalClient`: **`Observe` reads external state and returns an observation;
all CR writes belong to the mutating phases.** Status writes performed *by the
runtime* after `Observe` returns are fine — the runtime retries those on its
own event-retry path.

## Shutdown: bounded graceful drain

`Controller.Run` (`controller.go:538`) blocks until its context is cancelled
(SIGTERM), then drains: event intake stops immediately (the informer runs
under the caller's context), but in-flight reconciles keep a **live** context
derived from `context.Background()` so their API writes complete, bounded by
`GracefulShutdownTimeout` (default 30s; `0` = abrupt exit, negative = wait
forever). The timeout must stay below the pod's
`terminationGracePeriodSeconds` or the kubelet SIGKILLs mid-drain
(`controller.go:90-98`).

## Observability

- **Prometheus**: `controller_reconcile_total` / `_duration_seconds`
  (`controller.go:151-167`) plus workqueue, client-go and leader-election
  adapters (`pkg/metrics`), served by an optional metrics server
  (`pkg/metrics/server`, disabled by default).
- **OTel**: `pkg/telemetry` exports metrics (per-phase durations/failures,
  queue depth/wait/age, in-flight), traces (per-reconcile root span continuing
  `krateo.io/traceparent` across the composition tree, batch export only —
  a dead collector never blocks a reconcile) and OTLP logs
  (`pkg/logging.NewOTelHandler` tee). All gated off by default.
- **Kubernetes events**: normal events through a plain recorder, warnings
  through a throttled recorder, both from the
  [plumbing](https://github.com/krateo-platformops/plumbing) library.
