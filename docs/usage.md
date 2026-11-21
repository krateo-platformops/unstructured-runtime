---
type: Usage
title: unstructured-runtime — usage
description: go get + the minimal code from zero to a running controller; the ExternalClient contract.
resource: github.com/krateo-platformops/unstructured-runtime
tags: [go-get, builder, external-client]
timestamp: 2026-08-07T00:00:00Z
---

# Usage

This is a Go library, consumed with `go get` — there is no image, chart or CLI.

```sh
go get github.com/krateo-platformops/unstructured-runtime
```

Requires Go `1.25` (see [go.mod](../go.mod)). The module path is
`github.com/krateo-platformops/unstructured-runtime`.

## From zero to a running controller

Three steps: implement `ExternalClient`, build a controller for your GVR with
the builder, run it.

```go
package main

import (
	"context"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/builder"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type myClient struct{}

func (c *myClient) Observe(ctx context.Context, mg *unstructured.Unstructured) (controller.ExternalObservation, error) {
	// Pure read of external state. NEVER write mg here (see overview.md).
	return controller.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
}
func (c *myClient) Create(ctx context.Context, mg *unstructured.Unstructured) error { return nil }
func (c *myClient) Update(ctx context.Context, mg *unstructured.Unstructured) error { return nil }
func (c *myClient) Delete(ctx context.Context, mg *unstructured.Unstructured) error { return nil }

func main() {
	cfg, err := builder.GetConfig() // in-cluster; or build a *rest.Config yourself
	if err != nil {
		panic(err)
	}
	ctrl, err := builder.Build(context.Background(), builder.Configuration{
		Config:       cfg,
		GVR:          schema.GroupVersionResource{Group: "samples.krateo.io", Version: "v1", Resource: "samples"},
		ProviderName: "sample-provider", // the event-recorder source name
	})
	if err != nil {
		panic(err)
	}
	ctrl.SetExternalClient(&myClient{})
	_ = ctrl.Run(context.Background(), 2) // blocks until ctx cancel, then drains
}
```

The builder wires sane defaults for everything (queue, rate limiter, event
recorders, pluralizer, no-op logger, disabled metrics server); every knob is a
`builder.With…` functional option — the full list is in
[configuration](./configuration.md). For out-of-cluster runs, build the
`*rest.Config` from a kubeconfig instead of `builder.GetConfig()` — the
[quickstart example](../examples/quickstart/main.go) does exactly that.

## The ExternalClient contract

- **Non-blocking and idempotent** (`pkg/controller/controller.go:44-48`):
  `Create` must tolerate being called again after a success whose marker was
  lost; `Delete` must not error when the resource is already gone.
- **`Observe` is a pure read.** It must never write the managed CR: a write
  inside `Observe` can 409 under concurrent modification and permanently wedge
  the incomplete-create recovery. Persist deterministic metadata in
  `Create`/`Update` instead. Rationale and history:
  [overview](./overview.md#observe-must-be-side-effect-free).
- Return errors as-is; the runtime sets conditions, emits events and retries
  through the rate limiter (up to `MaxRetries`, default 5).
- **When the existence check cannot answer** (the incomplete-create recovery, while
  `krateo.io/external-create-pending` is the newest create marker), say which kind
  of "cannot":
  - **transient**, wrap the error with `controller.ExistenceUndetermined`: a 5xx,
    a timeout, an async operation still in flight, a resource reported Pending.
    Ready is `Creating`, the error is returned and the check runs again with
    backoff.
  - **permanent**, wrap it with `controller.ExistenceUnanswerable` (since v1.32.0,
    #119): the check can NEVER answer for this object, e.g. the read needs a
    server-assigned id that was never recorded and the kind declares no lookup.
    - Not being deleted: the object stays stopped, with the marker kept and no
      create retried, so no duplicate can be made. Ready is
      `False/CreateOutcomeUnknown`, and its message names the two ways out:
      verify by hand and remove `krateo.io/external-create-pending`, or delete
      the object.
    - Being deleted: the finalizer is released **without** calling `Delete`. The
      external resource, if one was created, is kept, and a Warning event
      `ExternalResourceKeptExistenceUnanswerable` says so. Before, `kubectl
      delete` hung on such an object for ever.
  - **unmarked**, the default: `False/CreateOutcomeUnknown`, waiting until the
    check answers, on reconcile and on delete alike.

## Operating a resource under this runtime

The runtime reads these annotations on every managed CR (`pkg/meta/meta.go`; the two policy
annotations are defined by plumbing's `managementpolicy` package, shared with provider-runtime):

| Annotation | Meaning |
|---|---|
| `krateo.io/paused: "true"` | pause reconciliation (condition `ReconcilePaused`); remove to resume |
| `krateo.io/deletion-policy: orphan\|delete` | on CR delete, leave or delete the external resource (default: delete). Any other value (`orphaned`, `Orphan`, ...) **keeps** it |
| `krateo.io/management-policy` | the verbs the runtime may perform: a comma-separated set such as `observe,create`, or one of `default`, `observe`, `observe-create-update`, `observe-delete` (below) |
| `krateo.io/external-name` | name of the resource on the external system |
| `krateo.io/external-create-pending\|-succeeded\|-failed` | the create handshake — **managed by the runtime**; deleting `-pending` by hand queues a fresh Create (recovery lever). Values are RFC3339 in **UTC**, second precision (since v1.28.2; earlier versions wrote the process's local offset). **Compare parsed times, never strings**: an older value may carry an offset, and markers already on an object are never rewritten, so across an upgrade from v1.28.1 or earlier one object can hold a pending marker at `+01:00` beside a clearer at `Z`, the very pair a string compare misorders |
| `krateo.io/traceparent`, `krateo.io/tracestate` | W3C trace context continued across the composition tree |

### What each management policy does

Since #110 Phase 0, create and update honour the policy, as delete always did. Before that, only
delete honoured it: a resource annotated `observe` was still created and updated. The outcomes
mirror provider-runtime's managed reconciler and are pinned by `pkg/controller/testdata/management-policy-table.tsv`
(policy × deletion policy × state):

Since #110 D1 the value is a **set of verbs**, comma-separated, in any order, from `observe`,
`create`, `update`, `delete`: `observe,create` means "observe and create, never update or
delete". `observe` is always implied (D3), so `create` means `observe,create`. The four historical
values are aliases for their sets: `default` = all four, `observe-create-update`, `observe-delete`,
`observe`.

| policy | absent | drifted | on CR delete |
|---|---|---|---|
| `default` (or unset) | create | update | delete, or orphan with `deletion-policy: orphan` |
| `observe-create-update` | create | update | orphan |
| `observe,create` (or `create`) | create | nothing, `Synced=True` | orphan |
| `observe-delete` | nothing, `Synced=True` | nothing, `Synced=True` | delete, or orphan with `deletion-policy: orphan` (D5: orphan always wins; before v1.30.0 it deleted anyway) |
| `observe` | `Synced=False/ReconcileError` "external resource does not exist", `ExternalResourceAbsentUnderPolicy` event (v1.28.1; v1.28.0 used `CannotObserveExternalResource`, the generic observe-error reason) | nothing, `Synced=True` | orphan |

A withheld verb is withheld whichever way it arrives: from the observe pass, from a CR update
(dispatched as `Update` by default), or from removing `krateo.io/external-create-pending`. It is
dispatched as Observe instead.

A value that does not parse (an unknown verb such as `observe-create` or `creat`, or a stray comma)
**acts on nothing** (D2): it is treated as `observe`, so nothing is created, updated or deleted. A
typo meant to restrict never widens into full management. It is **reported** as
`Synced=False/ManagementPolicyInvalid`, whose message names the annotation and the bad token, on
every pass (since v1.31.0; before, an absent resource read `Synced=False/ReconcileError`).

An unrecognised `krateo.io/deletion-policy` (`orphaned`, `Orphan`, ...) **keeps** the external
resource on delete, and is reported as `Synced=False/DeletionPolicyInvalid` on **every** pass while it
is set, not only while deleting, so the typo shows before the delete it decides (Diego, 2026-11-21).

**Synced precedence** (Diego, 2026-11-21): `ManagementPolicyInvalid` replaces whatever the pass
concluded (`ReconcileSuccess` or `ReconcileError`); otherwise `DeletionPolicyInvalid` does; when both
are invalid the `ManagementPolicyInvalid` message also names the deletion-policy problem. The pass's
own error is still returned (same backoff) and its events still fire, e.g.
`ExternalResourceAbsentUnderPolicy`. `ReconcilePaused` and `ReconcileDeferred` keep their reasons: no
pass ran. Both reason literals are plumbing `managementpolicy`'s, shared with provider-runtime.

**Deleting an object whose external resource is already absent** still calls the handler's
`Delete` whenever the policy allows delete (the `deleting-absent` rows of the table), so the
handler, which alone can tell "verified gone" from "could not determine", decides when the
finalizer goes. This is unstructured-runtime's contract (Diego, 2026-11-21, #110); provider-runtime
instead removes the finalizer without a `Delete`.

## Who consumes it

Krateo's `composition-dynamic-controller` (the per-CompositionDefinition
controller deployed by
[core-provider](https://github.com/krateo-platformops/core-provider)) is the
primary consumer — every Krateo composition reconciles through this runtime.
