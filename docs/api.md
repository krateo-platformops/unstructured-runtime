---
type: API
title: unstructured-runtime — API
description: The exported Go API surface, package by package, with the load-bearing types.
resource: https://pkg.go.dev/github.com/krateo-platformops/unstructured-runtime
tags: [go-api, packages]
timestamp: 2026-08-07T00:00:00Z
---

# API

The contract this library exposes is its exported Go surface. Authoritative
reference:
[pkg.go.dev/github.com/krateo-platformops/unstructured-runtime](https://pkg.go.dev/github.com/krateo-platformops/unstructured-runtime).
The load-bearing packages:

## Core

- **`pkg/controller`** — the runtime.
  - `ExternalClient` — the four-method contract you implement:
    ```go
    type ExternalClient interface {
        Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error)
        Create(ctx context.Context, mg *unstructured.Unstructured) error
        Update(ctx context.Context, mg *unstructured.Unstructured) error
        Delete(ctx context.Context, mg *unstructured.Unstructured) error
    }
    ```
  - `ExternalObservation{ResourceExists, ResourceUpToDate bool}`
  - **What a nil `Create` means.** The runtime reads a nil `Create` as "a create was requested". It
    stamps `krateo.io/external-create-succeeded`, emits the Normal event `CreatedExternalResource`
    and sets `Synced` to `ReconcileSuccess`. `Ready` is not written on that path, so a `Ready` the
    client persisted before returning survives. A non-nil error sets `Ready=False/Creating` (with
    the error) and `Synced` to `ReconcileError`, and stamps `krateo.io/external-create-failed`.
  - `ResultCreator` (optional): a client whose create can return without requesting anything (for
    example a credential lacking the create verb, or a referent not yet present) implements
    `CreateWithResult(ctx, mg) (CreateResult, error)`, with
    `CreateResult{Requested bool; Reason string; Conditions []metav1.Condition}`. The runtime calls it
    instead of `Create`. With a nil error and `Requested: false`, the runtime clears
    `external-create-pending`, does not stamp `external-create-succeeded`, emits no
    `CreatedExternalResource` event, and leaves `Ready` as the client wrote it. The next pass observes
    again. A non-nil error takes the error path whatever the result says. Without it, a nil return
    would durably claim a create that never happened, and keep the object out of incomplete-create
    recovery.
  - `CreateResult.Conditions`, on either nil-error outcome, follow the same rule as
    `ExternalObservation.Conditions`: they are upserted by Type, and the runtime stamps its default
    `Synced=ReconcileSuccess` only when they carry no `Synced`. A refusal that will not resolve
    without a person (no create verb, a credential that may not create) should return `Ready=False`
    and `Synced=False` with its own reason. A wait (an async create, a referent not yet present) can
    leave `Synced` to the default. Return conditions; don't write status from inside the call.
  - `ExistenceChecker` (optional): `Exists(ctx, mg) (bool, error)`, preferred by incomplete-create
    recovery over `Observe`.
  - `Options`, `New(sid, Options) (*Controller, error)`
  - `(*Controller).SetExternalClient`, `Run(ctx, numWorkers)`,
    `SetGracefulShutdownTimeout(d)`
  - priorities `LowPriority` (-100) / `NormalPriority` (0) / `HighPriority` (100)
- **`pkg/controller/builder`** — `Configuration`, `Build(ctx, conf, opts…)`,
  `GetConfig()` and the `With…` functional options
  (see [configuration](./configuration.md)).
- **`pkg/controller/event`** — `EventType` (`Observe|Create|Update|Delete`),
  `Event`, `DigestForEvent`, `AnnotationEvent(s)` (trigger rules
  `OnCreate|OnDelete|OnChange|OnAny`), `ActionsEvent` (CR action → event-type
  mapping).
- **`pkg/controller/objectref`** — `ObjectRef{APIVersion, Kind, Name, Namespace}`.
- **`pkg/controller/priorityqueue`** — the generic rate-limited priority queue
  (`PriorityQueue[T]`, `AddOpts{After, RateLimited, Priority}`).

## Managed-resource helpers

- **`pkg/meta`** — the `krateo.io/*` annotation vocabulary and helpers:
  external-name, the `external-create-pending/-succeeded/-failed` handshake
  (`ExternalCreateIncomplete`, `ExternalCreatePendingDuring`, setters/getters),
  `IsPaused`, deletion policy (`ShouldDelete`/orphan), management policy
  (`IsActionAllowed`), traceparent keys, finalizer/label/annotation utilities.
- **`pkg/tools`** — `Update` / `UpdateStatus` of unstructured CRs through the
  dynamic client (GVK→GVR via a `Pluralizer`).
- **`pkg/tools/unstructured`** — condition plumbing on unstructured objects:
  `SetConditions`, `GetCondition(s)`, `IsAvailable`, failed-object-ref helpers.
- **`pkg/tools/unstructured/condition`** — the condition constructors:
  `Available`, `Unavailable`, `Creating`, `Deleting`, `ReconcileSuccess`,
  `ReconcileError(err)`, `ReconcilePaused`, `FailWithReason`.
- **`pkg/tools/statusprojection`** — the jq-based status projection engine:
  `Mapping{For, Expression}` (JSON-tagged wire format), `Project(ctx, cr,
  resolved, mappings)`, `SetObservedGeneration`.
- **`pkg/pluralizer`** — `PluralizerInterface` (`GVKtoGVR`) and the
  discovery-backed, TTL-cached default `New()`.

## Infrastructure

- **`pkg/logging`** — the `Logger` interface (Info/Debug/Warn/Error,
  WithName/WithValues) with `NewLogrLogger`, `NewSlogLogger`, `NewNopLogger`;
  OTel structured-JSON slog handlers `NewOTelJSONHandler` and the OTLP tee
  `NewOTelHandler`.
- **`pkg/telemetry`** — `Config`, `Setup` (OTel metrics; returns the `Metrics`
  handle the controller records into), `SetupTracing`, `SetupOTLPLogs`,
  `StartReconcileSpan`, `InjectTraceparent`.
- **`pkg/metrics`** + **`pkg/metrics/server`** — Prometheus registry,
  workqueue/client-go/leader-election adapters, and the optional metrics
  `Server` (`NewServer(Options, *rest.Config, *http.Client)`).
- **`pkg/listwatcher`** — `Create(CreateOption)` → `*cache.ListWatch` over a
  dynamic client with label/field selectors.
- **`pkg/workqueue`** — rate-limiter helpers and workqueue metrics provider.
- **`pkg/context`** — context plumbing: `BuildContext`, `WithLogger`,
  `WithTraceId`, `Logger(ctx)`, `TraceId(ctx, generate)`.
- **`pkg/signals`** — `SetupSignalHandler()` (SIGTERM/SIGINT stop channel;
  second signal exits 1).
- **`pkg/certwatcher`** — TLS certificate hot-reload from disk for servers.
- **`pkg/errors`** — the wrap/cause error helpers used across Krateo
  controllers (`New`, `Wrap(f)`, `WithMessage(f)`, `Cause`, `Is`, `As`).

`pkg/internal/**` is not part of the public surface.

## Compatibility

The module follows Go semver on `vX.Y.Z` tags (currently `v1.x`): no breaking
changes to the exported surface within the major version. See
[release](./release.md).
