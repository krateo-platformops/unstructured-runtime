---
type: Log
title: unstructured-runtime — log
description: Curated chronological history of notable changes and decisions.
resource: github.com/krateo-platformops/unstructured-runtime
tags: [history]
timestamp: 2026-08-07T00:00:00Z
---

# Log

Curated history (newest first); release notes stay on the git tags.

- **2026-11-13 — CPU saturation step measured over a 10s window (#82)**: ported
  provider-runtime v1.19.0's step. Pressure is the share of the last 10s in
  which some task waited for CPU, from cpu.pressure's cumulative `some total`,
  not the kernel's `avg10` (a 10s exponential average that kept one stall over
  the threshold for several 1s samples and halved capacity several times,
  16→1). A cut needs a whole window of evidence and the next a window after
  it; after a CPU cut capacity rises +1 per `StartupRampInterval` (2s), not
  per `RecoveryInterval`. The threshold keeps its name and default (50), now
  the 10s share. Opt-in rule (off by default in core-provider).
- **2026-08 — `v1.4.0`, module identity migration**: Go module path moved to
  `github.com/krateo-platformops/unstructured-runtime` (full org
  independence); OTLP log export (tee slog handler + `SetupOTLPLogs`).
- **2026-07 — create-pending hardening (#9)**: confirm-the-negative in the
  incomplete-create recovery — a 2-minute grace period with Low-priority
  re-observes before an absent external resource is treated as a genuine miss
  and recreated (guards eventually-consistent external APIs against
  duplicates).
- **2026-07 — graceful drain (#8)**: `Run` performs a bounded graceful drain
  of in-flight reconciles on SIGTERM (`GracefulShutdownTimeout`: default 30s,
  0 = abrupt, negative = forever), with reconcile contexts decoupled from the
  signal context so in-flight API writes complete.
- **2026-06 — KOS-1 observability**: OTel structured-JSON log handler (#4),
  then metrics resource attributes + per-reconcile tracing with
  `krateo.io/traceparent` continuation across the composition tree (#5,
  `v1.3.0`).
- **2026 — status projection engine**: jq-based `pkg/tools/statusprojection`
  (`Mapping`/`Project`), the wire format core-provider ships to
  composition-dynamic-controller ConfigMaps; hardened against non-JSON-safe
  sources and empty jq output.
- **2026 — incomplete-create recovery via observe (#2)**: replaced the
  historical "refuse forever until a human removes
  `krateo.io/external-create-pending`" behavior with reconciling the
  handshake annotations against observed reality (present → record success;
  absent → recreate; observe error → conservative refusal). Companion lesson
  shipped later in core-provider#67: `Observe` must be side-effect-free or
  this recovery wedges — see
  [overview](./overview.md#observe-must-be-side-effect-free).
- **2025 — OTel queue/reconcile metrics (#56)**, throttled warning event
  recorder (#54/#55), ResourceVersion-aware update classification so
  status-only writes no longer self-trigger reconciles (#52).
- **2025 — event queueing revision (KRA-761, #32)** and the priority queue +
  metrics server (KRA-631, #28): dedup'd events, Low/Normal/High priorities,
  rate-limited retries only.
- **2024 — origin (KRA-203, #16 and earlier)**: initial extraction of the
  dynamic-client managed reconciler — builder, pluralizer via plumbing,
  pause annotation, finalizer and external-create annotation handshake.
