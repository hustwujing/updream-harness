# Bounded Allocation Recovery Implementation Plan

**Goal:** Keep sandbox creation/reconciliation progressing independently and recover unstarted business Jobs with one capacity-accounted replacement.

**Architecture:** PostgreSQL allocation records retain creation identity and frozen recovery policy. AllocationService independently schedules creation and fenced reconciliation. JobWorker may atomically supersede only unstarted allocation attempts; old generations remain owned by durable cleanup.

**Tech Stack:** Python 3.13, SQLAlchemy/PostgreSQL, existing SandboxBackend protocol and JobWorker.

**Spec:** [Approved design](../specs/2026-11-25-allocation-recovery.md).

**Execution:** Native TDD in this session, followed by independent review. The user approved this design and execution; do not request the same approval again. Referenced execution/TDD helper skills are not in the available catalog; preserve the established native workflow and the read verification/debugging instructions.

## Global constraints and review focus

- Ali worktree/branch only; all generated files under `/data2/wujing03`.
- Never infer absence from empty inventory, timeout, lease expiry or TTL passage.
- Capacity waits before dispatch must not start the uncertain-create deadline.
- Fence a late original Worker before replacement can start business work.
- Preserve original reservation and cleanup identity across replacement/restart.
- Bound extra allocations; unavailable capacity must not deadlock recovery.
- Reconciliation claims must survive process death and cannot regress READY.
- Preserve supported provider switching and existing accepted execution recovery.

## Task 1: Independent and fair allocation observation

Files: `allocation_dispatch.py`, `allocation_service.py`, `store.py`, `config.py`,
`tests/test_allocation_dispatch.py`, `tests/test_allocation_dispatch_postgres.py`.

- [x] Reproduce a held create blocking UNKNOWN reconciliation and fixed-limit starvation.
- [x] Add additive scheduling/claim columns with populated SQLite/PostgreSQL upgrade tests.
- [x] Implement bounded `AllocationService.run(stop_event, concurrency=...)` and due/fenced reconciliation, keeping `run_once` for deterministic callers.
- [x] Configure allocation lease at 30 seconds and renew every third; test cancellation and stale owners.
- [x] Run focused and PostgreSQL gates; commit this independently useful fix. Evidence: 80 focused tests, one additional shutdown test, 12 isolated PostgreSQL tests; two red regressions in `.data/tmp/allocation-scheduling-red.log`.

## Task 2: Bounded recovery of unstarted business Jobs

Files: `allocation_dispatch.py`, `job_allocation.py`, `job_adapters.py`,
`job_worker.py`, `store.py`, `scheduler.py` if required, `pool.py`, `job_errors.py`,
`config.py`, `job_service.py`, related worker/store/capacity tests.

- [x] Add red tests for the 90-second uncertain boundary, old-generation cleanup and one replacement.
- [x] Freeze per-allocation policy and introduce a typed recovery-expiry signal.
- [x] Atomically queue replacement only before Runtime submission; retain the old cleanup/reservation and bound replacements independently of capacity waits.
- [x] Require new capacity and fail replacement explicitly when unavailable; preserve normal capacity waiting.
- [x] Use a 300-second provisioning TTL and renew before business use; test renewal failure and no local quota release on mere TTL expiry.
- [x] Test cancellation, stale writes, late READY, Thread ownership and PostgreSQL races. Expanded focused gate: 163 passed; isolated PostgreSQL: 14 passed. Whole-change review remains in Task 3.

## Task 3: Real faults, release and publication

Files: real capacity/allocation test runners, deployment settings, README,
development guidance and test-evidence report.

- [ ] Add actual AllocationWorker SIGKILL cases and count provider creates/Runtime submissions separately.
- [ ] Exercise delayed visibility, one replacement, insufficient capacity and authoritative owned cleanup.
- [ ] Record the affected-case assessment; run new cases and affected existing recovery/capacity cases.
- [ ] Run full Python and required frontend/build gates; obtain independent whole-change review.
- [ ] Commit, freeze release, deploy affected Ali services and verify live health, recovery and command execution.
- [ ] Preserve all case revisions, clean owned test resources, scan credentials and push GitHub.
