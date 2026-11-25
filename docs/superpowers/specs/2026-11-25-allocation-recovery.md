# Bounded sandbox allocation recovery

## Approved direction

The user approved independent create/reconciliation scheduling, a 30-second
allocation lease renewed every 10 seconds, a 90-second uncertain-create recovery
window, at most one replacement allocation for an unstarted business Job, and
a five-minute initial sandbox TTL extended after successful adoption. Subsequent
questions require explicit distinction between create concurrency, local Pool
capacity, durable resource reservations and cloud quota. Preserve normal
capacity waiting and all existing hard resource limits.

## Current failure and ownership

AllocationService waits for an entire batch of create RPCs before reconciliation.
UNKNOWN selection has a fixed limit without due-time rotation, and missing
inventory never resolves a request. A stalled create therefore delays unrelated
recovery, and a business Job can wait until its full task deadline.

The current Pool limit is 13 per process (API and JobWorker have separate pools),
create/admission concurrency is 8, and Scheduler reservations are durable.
Do not describe the process-local semaphore as a global cloud quota. A new
allocation must pass both existing local capacity and durable resource checks;
an uncertain old allocation continues to count until authoritative cleanup.

## Desired behavior

1. AllocationWorker retains one deployment and runs bounded create tasks and
   reconciliation independently. New requests refill free create slots without
   waiting for the slowest request in a batch. Shutdown settles its owned tasks.
2. Persist per-allocation reconciliation scheduling, attempts and fenced claims.
   Rotate due records fairly; back off unavailable/empty/ambiguous observations.
   A late authoritative create receipt cannot be overwritten by a stale lookup.
3. Make the allocation lease configurable, default 30 seconds with heartbeat at
   one third. Lease expiry authorizes observation, never blind create replay.
4. Freeze recovery policy with each new allocation. The 90-second recovery
   window applies to an issued create whose result is uncertain, not ordinary
   queue/capacity waiting. At expiry, a transaction may supersede only a source
   attempt that has no Runtime submission intent or accepted business execution.
5. Queue at most one new attempt of the same business Job. Persist cleanup of
   the old generation and retain its reservation/Pool slot. Generation checks
   prevent old Workers from starting business execution. Known/accepted Runtime
   executions retain existing reattachment behavior and are never replaced here.
6. Replacement must acquire additional capacity. If unavailable, terminate with
   an explicit resource/recovery error rather than overcommitting or looping.
   Regular pre-accept capacity waits keep their existing generation and policy.
7. Freeze a short provisioning TTL (default 300 seconds) for new dispatched
   allocations, and renew to the normal configured TTL before business use.
   Failed adoption preserves durable cleanup ownership. TTL passage alone never
   changes UNKNOWN to ABSENT or releases a reservation.
8. Existing cancellation, pause, owner isolation, artifact and continuation
   contracts remain authoritative. Late resources for superseded/cancelled Jobs
   are cleanup-only. Thread deletion must not discard unresolved ownership.
9. Surface structured recovery/capacity reasons and persist bounded diagnostics.
   No provider-, Operation- or case-specific shared state-machine branches.

## Verification and delivery

Use red/green tests for slow-create independence, due-row fairness, stale lookup
and late-receipt races, cross-process claiming, schema upgrades, replacement
fences, exhausted capacity, cancellation and zero duplicate Runtime submissions.
Run isolated PostgreSQL races and fresh full Python checks before release.

Real fault tests kill AllocationWorker itself around the actual provider create
boundary, including after provider acceptance but before durable handle storage.
Test real delayed visibility/replacement and cleanup using owned test scopes.
Exercise OpenSandbox and Ali through the common contracts; preserve unaffected
case evidence and rerun the selected affected cases only. Report real faults,
fake transports and actual model tests separately.

All mutations remain in the Ali worktree and `/data2/wujing03`. The seven
historical unresolved allocations are not declared absent by this migration.
Publish small reviewed commits, update docs, deploy only affected Ali services,
verify health/recovery, clean owned temporary data, and push the authorized
GitHub branch. The original sibling deployment remains untouched.
