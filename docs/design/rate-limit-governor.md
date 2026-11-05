---
type: Architecture
title: unstructured-runtime — automatic reconcile slow-down under server-side rate limits
description: Design for a per-endpoint governor that slows reconciliation when GitHub Enterprise, Azure DevOps or any server expresses a rate limit, replacing hand-tuned worker counts and resync intervals.
resource: github.com/krateo-platformops/unstructured-runtime
tags: [rate-limit, governor, backoff, deferral, conditions]
timestamp: 2026-11-05T00:00:00Z
---

# Automatic reconcile slow-down under server-side rate limits

**Status:** design, not implemented. Revised once after review by the provider-runtime session,
which found a circular dependency and a self-contradiction in the first draft (§3).
**Owner:** unstructured-runtime.
**Motivation:** server-side rate limiting on GitHub Enterprise and Azure DevOps currently has to be
absorbed by hand — operators tune worker counts and resync intervals per installation, per provider,
and re-tune when either side changes. The runtime should do it.

---

## 1. The constraint that shapes everything: the runtime cannot see a 429

Measured, not assumed. `net/http` appears in exactly three non-test files in `pkg/`:

```
pkg/metrics/server/server.go     its own metrics endpoint
pkg/internal/httpserver/server.go  its own health/probe server
pkg/controller/builder/builder.go  wiring
```

**The runtime makes no outbound HTTP calls at all.** Every external request is made inside a
handler, behind this interface:

```go
type ExternalClient interface {
	Observe(ctx context.Context, mg *unstructured.Unstructured) (ExternalObservation, error)
	...
}
```

So the signal (`429`, `Retry-After`, `X-RateLimit-*`) originates in the **handler**, and the controls
that could act on it (how many reconciles run concurrently, how soon an item is retried) live in the
**runtime**. There is no path between them today.

Two consequences that rule out the obvious designs:

- **The runtime must never parse HTTP.** Not "should not" — it has no response to parse. Any design
  that has the runtime inspect status codes or vendor headers requires giving it the transport,
  which is a much larger change and puts vendor header drift in the runtime's blast radius.
- **Per-item exponential backoff is the wrong instrument.** `TypedItemExponentialTimedFailureRateLimiter`
  backs off *one item*. A server-side limit is a property of the *endpoint*, shared by every item
  that talks to it — so backing off one item while the other workers keep calling makes the
  situation worse, and the recovery is uneven because each item carries its own independent counter.

---

## 2. Signal: a sink in the context, reported by the handler

The runtime places a reporter in the context before each handler call. The handler reports whenever
it sees rate-limit information — **on success as well as on failure**.

```go
package ratelimit

// Signal is one observation of an endpoint's rate-limit state, as the handler saw it.
type Signal struct {
	// Key identifies the rate-limited scope: normally the endpoint host, because that is what a
	// server-side limit is a property of. See §5 on why this is not global.
	Key string

	// RetryAfter is how long the server asked us to wait, when it said so explicitly
	// (Retry-After). Zero when the server gave a reset instant instead.
	RetryAfter time.Duration

	// ResetAt is when the window resets, when the server gave an instant rather than a delay.
	// Exactly one of RetryAfter/ResetAt is normally set; if both are, the later wins.
	ResetAt time.Time

	// Remaining and Limit are the quota counters when the server publishes them. Optional, and
	// used only for the ADVISORY path in §3 — never required for the throttled path.
	Remaining, Limit int
	HasQuota         bool

	// Throttled distinguishes "the server refused this request" from "the server is telling us we
	// are close". Both slow us down; only the first means the call did not happen.
	Throttled bool
}

// Report records a signal. Safe on a context with no sink: a handler that reports into a runtime
// that has the governor disabled is a no-op, which is what keeps this opt-in.
func Report(ctx context.Context, s Signal)
```

### Why a context sink rather than a typed error

A typed error checked with `errors.As` is the pattern this codebase uses elsewhere and was my first
choice. It does not work here, for a reason that is specific to Azure DevOps:

> **Azure DevOps publishes `X-RateLimit-Delay` on a SUCCESSFUL response.** It tells you it is
> delaying you *before* it starts refusing you. Reacting to that is the entire value of automatic
> slow-down — an error-only channel can only react after throttling has already begun.

So the channel must carry information from a **200**. The alternatives were:

| mechanism | why not |
|---|---|
| typed error only | cannot carry a signal from a successful call — misses the advisory case, which is the one worth having |
| field on `ExternalObservation` | works on success, but not on the error path, so the invariant lives in two places that must agree. That is the coupling I rejected in core-provider#307's design for the same reason |
| both of the above | two mechanisms, two things to keep in sync, and a handler that implements one and not the other looks correct |
| **context sink** | one mechanism, both paths, **no change to `ExternalClient`** |

The last point is practical as well as aesthetic: changing the handler interface means a signature
change across every call site, which is exactly the churn that made unstructured-runtime#47 split in
two.

**The known weakness, stated:** a context side channel is implicit, and a handler can forget to
report. That is why it fails *open* — no reports means no throttling, i.e. today's behaviour — and
why §6 proposes a conformance test handlers can run against themselves rather than a runtime-side
assertion that cannot see inside them.

---

## 3. Control: a per-key governor with a resizable gate

```go
// Governor decides how much concurrency a key currently gets, and how long to defer.
type Governor interface {
	// TryAcquire NEVER BLOCKS. It either grants capacity (returning a release func) or returns a
	// Deferral saying why and when to come back. See "why non-blocking" below — a blocking
	// acquire makes this design contradict itself.
	TryAcquire(key string) (release func(), d *Deferral)
	Observe(Signal)
}

type Deferral struct {
	Key    string
	Reason DeferReason
	// Until is the server's OWN deadline, and is set only when we have one. When the key is merely
	// at capacity it stays ZERO and the caller requeues with a short jittered delay instead — a
	// deadline we invented is a deadline a consumer reads as the server's.
	Until time.Time
}

type DeferReason string

const (
	// DeferThrottled: the server refused or told us to wait. Until is set.
	DeferThrottled DeferReason = "Throttled"
	// DeferAtCapacity: the governor is holding us below its own ceiling. Until is ZERO.
	DeferAtCapacity DeferReason = "AtCapacity"
)

// RateLimitKeyer lets a handler name the scope BEFORE a call is made. Optional: a handler that
// does not implement it is never gated.
//
// THIS CLOSES A CIRCULAR DEPENDENCY in the first draft of this design, found by the
// provider-runtime session. TryAcquire needs the key before the call; the key arrives in the Signal
// reported DURING the call. Both halves were written and neither could be satisfied.
//
// It must do NO I/O — it runs before every gated call, and a keyer that made a request to decide
// whether a request is allowed would be its own rate-limit problem.
//
// Learning keys from past signals was the alternative and is worse: every object's first call would
// be ungated, and an object that changes endpoint would be gated against the wrong key until it
// next failed.
type RateLimitKeyer interface {
	RateLimitKey(mg *unstructured.Unstructured) (string, error)
}
```

### Why non-blocking, and why the first draft was self-contradictory

The first version of this section had `Acquire` *block* until the key had capacity. That is wrong,
and wrong in a way this document already argued against two sections later:

> §5: key by endpoint, **not globally**, because a GitHub Enterprise limit must not slow Azure
> DevOps reconciles.

With a fixed worker pool and a blocking acquire, every worker can park on one throttled key — and
then nothing processes the healthy keys either. The blocking gate reproduces exactly the global
stall that keying exists to prevent. The design contradicted itself and I did not notice; the
provider-runtime session did.

So `TryAcquire` returns immediately, and the worker requeues:

```
DeferThrottled   -> AddAfter(item, until)            the server's deadline
DeferAtCapacity  -> AddAfter(item, jitter(200ms..2s)) no deadline claimed
```

Each worker resolves the key with `RateLimitKeyer` and calls `TryAcquire` immediately before
`externalClient.Observe` (and the other verbs), releasing after. The gate is a **resizable semaphore**, so effective parallelism changes at runtime
without restarting anything — which is what replaces the manual worker tuning. `Run(ctx, numWorkers)`
keeps its signature and its goroutine count; what changes is how many of them may be *in a handler
call* at once.

### Two states, and the advisory one is the point

```
THROTTLED   a Signal with Throttled=true, or RetryAfter/ResetAt in the future
            -> the gate is CLOSED for that key until the deadline; workers get a Deferral
ADVISORY    a Signal with HasQuota and Remaining low relative to Limit, Throttled=false
            -> capacity is REDUCED but not closed; calls keep flowing more slowly
```

The advisory state is the difference between this design and a retry loop. A retry loop can only
discover the limit by hitting it; the advisory state avoids hitting it.

### Recovery: AIMD, with the decrease tied to the server's own deadline

- **On a throttled signal:** capacity for that key drops to the floor (1) and the gate holds until
  `max(RetryAfter, ResetAt)`. Not halved — the server has told us a specific instant, and guessing a
  smaller delay than the one we were given is how a client gets banned rather than throttled.
- **On an advisory signal:** capacity halves, floor 1.
- **On a clean window** (no signal for `RecoveryInterval`, default 30s): capacity +1, ceiling
  `numWorkers`. Additive so recovery cannot itself cause a burst.

A cooldown prevents oscillation: capacity rises at most once per `RecoveryInterval` per key.

### The requeue must not use the failure backoff

A deferred item is requeued with `AddAfter(item, until)` — **not** `AddRateLimited`. Using the
existing exponential-failure limiter would conflate *"this item is failing"* with *"the endpoint is
busy"*, so a global limit would inflate every item's personal backoff and the recovery would be
uneven and much slower than the server's own window. The item's failure count must be untouched: a
deferral is not a failure, and `Forget` semantics stay with real errors.

---

## 4. Observability: a deferral must be visible, and must not read as an assessment

This is the half most likely to be got wrong, and the failure modes are known from core-provider's
condition work this week.

**A deferred pass must not write an assessment.** No `ObserveFailed`, no `NotObserved`, no
`ReconcileError`-driven Ready change. The handler was not called; nothing about the resource was
learned, and writing a reason that means "we looked and it was bad" is a false statement about state.

**A deferred pass must not write silence either**, and this is the part that is easy to miss. A
deferral that records nothing is indistinguishable from a pass that ran and found everything fine —
so on a long backoff the conditions go stale while reporting healthy. That is the shape of three
separate core-provider defects: a condition computed from an inventory nothing refreshed (#273), a
reason with no success arm so it could never clear (#331), and a self-heal returning before the
status write so `Ready` was never written at all (#337).

So a deferral writes:

```
Synced = False   reason = ReconcileDeferred   message = "<key> rate-limited by the server; next attempt <RFC3339>"
Ready  = <unchanged>
```

- **`Synced=False` is correct and load-bearing.** Nothing is converging this object right now, which
  is the same semantic as the pause conditions, and it means a dependency gate that requires
  `Synced=True` already handles this with no change. (That gate change is under discussion in
  installer for pause; it would cover deferral for free.)
- **`Ready` is left alone**, because readiness is the handler's finding and the handler did not run.
  Overwriting it would be the same defect as runtime#45, where `ObserveFailed` overwrote a handler's
  finding.
- **The message carries the deadline.** A "deferred" with no deadline becomes a lie purely through
  the passage of time — the single most common way a status field stops being true without anything
  changing.

### A deferral is also the explanation for a stalled async poll

A handler that drives a long-running operation by requeueing ("still pending, come back soon") has
TWO deadlines once its endpoint is deferred: its own "poll again soon" and the governor's "do not
call until T". The governor's wins — a poll is a call — so the operation will look stuck, and a
handler that writes its own `Pending` condition will show that beside it. The `ReconcileDeferred`
reason on Synced, with its message naming the key and the deadline, IS the explanation for that
stall; consumers should read it before diagnosing the operation. Raised by the oasgen-provider
review, from rest-dynamic-controller's requeue-based async mode.

### The reason string is a consumer contract, and must not repeat today's collision

`ReconcileDeferred` must be **a value no other reason shares**. Today this package has:

```go
ReasonReconcilePausedReady string = "ReconcilePaused"   // Ready
ReasonReconcilePaused      string = "ReconcilePaused"   // Synced
```

Two identifiers, **one string**. A consumer reading a reason cannot tell Ready-paused from
Synced-paused; only the condition *type* distinguishes them. That collision cost a wrong answer
today: core-provider's own comment describes the emitted value as `ReconcilePausedReady`, which is
the Go identifier and not the wire value, and I repeated it to a peer as the thing a cluster emits.
The installer session printed the object and the Ready reason is `ReconcilePaused`.

So: one identifier, one distinct value, pinned by a test naming its consumers — the pattern
`ReasonZeroEvidence` already documents, where the string is explicitly a cross-repo contract whose
rename breaks installer's gate silently.

### Metrics

```
unstructured_runtime.ratelimit.capacity          gauge   per key, current effective concurrency
unstructured_runtime.ratelimit.deferrals         counter per key
unstructured_runtime.ratelimit.seconds_to_reset  gauge   per key, 0 when not throttled
unstructured_runtime.ratelimit.signals           counter per key, by throttled|advisory
```

The capacity gauge is the one an operator actually wants: it is the number they used to tune by
hand, now reported rather than configured.

---

## 5. Keyed by endpoint, not global

A GitHub Enterprise limit must not slow Azure DevOps reconciles. The limit is a property of the
server (and often of the credential), so the governor is keyed, and the key comes from the handler
because only the handler knows which endpoint an object talks to.

Default key: the endpoint host. Handlers that authenticate as several identities against one host
should key by `host|identity`, since the quota is usually per credential — the `Key` field is a
string precisely so that is the handler's choice rather than a shape the runtime imposes.

**A global governor is the wrong default and would be the easy mistake:** it makes one noisy provider
throttle every reconcile in the process, which on a Krateo installation means one misbehaving
blueprint slowing the whole portal.

---

## 6. What has to be true for this to work, and how each is checked

| requirement | check |
|---|---|
| a deferral does not write an assessment | unit: a deferred pass leaves Ready untouched and writes `Synced=False/ReconcileDeferred` |
| a deferral is visible | unit: the condition IS written, with a parseable deadline in the message |
| capacity actually bounds concurrency | unit: with capacity 1 and N workers, at most one handler call is in flight — asserted by a handler that records overlap |
| a throttled key does not slow other keys | unit: two keys, one throttled, the other's call latency unchanged |
| the server's deadline is respected, not guessed | unit: `RetryAfter` 30s produces a deferral until ~30s, never shorter |
| recovery is additive | unit: capacity returns to the ceiling in N `RecoveryInterval`s, not in one |
| a deferral does not consume failure backoff | unit: an item deferred twice has `NumRequeues` 0 |
| the reason string has not drifted | contract test pinning the literal and naming consumers |
| **handlers actually report** | a conformance helper handlers import: feed it a 429 and an Azure advisory 200, assert a `Signal` was reported. The runtime cannot assert this from outside, so the test belongs where the handler is |
| the governor is off by default | unit: with no `Governor` configured, no gate, no deferrals, identical behaviour |

**Mutation targets** (each must compile, and an inert mutant is inconclusive):
drop the gate acquisition at the call site; make `Acquire` always return capacity; return a deferral
without writing the condition; write `ObserveFailed` instead of `ReconcileDeferred`; use
`AddRateLimited` instead of `AddAfter`; make the key global.

---

## 7. What this design does NOT do

- **It does not parse vendor headers.** Handlers do, and §1 explains why that separation is
  structural rather than stylistic: the runtime has no response. A consequence worth stating plainly
  — **vendor header drift is a handler concern.** GitHub's secondary-limit behaviour and Azure's
  TSTU accounting can change without this package changing, and the exact header names should be
  verified against current vendor documentation by whoever writes the handler, not taken from this
  document.
- **It does not change `Run`'s signature or the worker count.** `numWorkers` stays the ceiling; the
  governor only decides how many may be in a handler call.
- **It does not retry inside a pass.** A deferral requeues; it does not sleep in the worker. Sleeping
  would hold a queue slot and a goroutine for the server's whole window.
- **It does not give an operator a knob to force capacity.** Deliberate: the point is to remove the
  tuning, and a manual override is a setting whose users are the people who would be better served
  by the metric telling them what the governor already chose.
- **It does not address client-side QPS** (`rest.Config` QPS/Burst against the apiserver). Different
  limiter, different authority, out of scope.

---

## 8. Open questions for review

1. **Should an advisory signal with no quota counters do anything?** A server that sends
   `Retry-After` on a 200 with no `X-RateLimit-*` is unusual but possible. Current proposal: treat it
   as advisory with capacity halved.

   **Not to be confused with GitHub's secondary limits, which are refusals.** The oasgen-provider
   review pointed out that a bare `Retry-After` is EXPECTED GitHub traffic, not a corner: secondary
   limits arrive as 403 or 429 carrying only `Retry-After` (confirmed by the github-provider-kog
   session; all 32 of its kinds call api.github.com natively). Those requests were REFUSED, so a
   handler must report them `Throttled: true` — a hold until the deadline — and never as advisory.
   Reported as advisory they would only halve capacity while the server keeps refusing. This
   question stays about the 200 case alone.

   **And a healthy GitHub response must not slow anything down.** GitHub sends `X-RateLimit-Reset`,
   always in the future, on every response. The first governor read that boundary as a delay and
   halved capacity on every successful call (fixed in #57, reported by the provider-runtime
   session): counters above a quarter are now a no-op of their own, and only a cut restarts
   recovery.
2. **Per-verb keying.** Observe is read-only and cheap; Create/Update are not. A quota expressed in
   cost units (Azure's TSTUs) is not evenly consumed across verbs. Proposal: ignore for v1 and
   revisit with real numbers, because guessing the weights is worse than not weighting.
3. **Should `Signal` and the reason string live in a shared module imported by both runtimes?**
   **Answered: no — duplicated declarations with a contract test in each repo.** Asked by the
   provider-runtime session; decided as plumbing's owner, and the reasons are specific rather than
   a general preference for duplication:

   - plumbing is already a release-cadence choke point. core-provider and the cdc sit on v1.21.0
     while chart-inspector is on v1.23.0, so a vocabulary change would need a plumbing release plus
     two bumps before either runtime could move.
   - the coupling is **release cadence, not a new dependency edge.** An earlier version of this
     reasoning said provider-runtime does not depend on plumbing; it does — `go.mod` line 10,
     `github.com/krateo-platformops/plumbing v1.13.0`, verified in its repository after the
     provider-runtime session corrected me. I asserted a fact about another repo that was one file
     read away. The decision is unchanged and one of its three reasons was wrong: sharing would not
     add an edge, it would make a vocabulary change need a plumbing release and two bumps — and
     v1.13.0 against this repo's consumers on v1.21.0/v1.23.0 shows those bumps do not happen
     together.
   - **the compiler would not enforce the thing that matters.** A shared type guarantees the field
     names agree; it does not guarantee both runtimes interpret `Throttled` or `ResetAt` the same
     way, which is the divergence that would actually confuse an operator. That needs a test either
     way.

   So: a contract test per repo pinning the literal and naming the other runtime as a consumer, in
   the shape `ReasonZeroEvidence` already documents here. If the two are ever changed in lockstep
   more than once, that is the evidence for a shared module.

4. **Should the deferral condition be `Synced=False` or a new typed condition?** `Synced=False`
   reuses an existing contract and gets dependency gates for free. A typed `ReconcileDeferred`
   condition is more precise and is a new thing for every consumer to learn. Proposal: `Synced=False`,
   on the same reasoning that made the graceful pause adopt the runtime's existing shape rather than
   invent a third vocabulary.
