# Durable Thread Pause Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add restart-safe pause and resume for the Primary Harness and parallel child Harness Jobs without duplicating durable external effects.

**Architecture:** Persist a Thread pause saga and per-Harness participant checkpoints in PostgreSQL. Fence new Job claims and continuation delivery while paused, quiesce sandboxes before checkpointing, and resume each participant from its own durable snapshot.

**Tech Stack:** Python 3.13, FastAPI, SQLAlchemy asyncio, PostgreSQL/SQLite tests, React, OpenSandbox runtime.

**Spec:** `docs/superpowers/specs/2026-11-17-durable-thread-pause-design.md`

## Global Constraints

- Pause and hard cancellation remain distinct operations.
- PostgreSQL and object storage are authoritative; Redis is notification-only.
- Existing Provider acceptance receipts must never be recreated by resume.
- All runtime and test storage remains under `/data2/wujing03`.
- No compatibility shim is required for an older public API.

## Review Focus

- Restart after pause commit but before runtime interruption must finish the same generation.
- Restart after checkpoint upload but before sandbox release must not duplicate the participant.
- Provider completion while paused must remain durable without creating a continuation Turn.
- Resume must requeue only paused Harness Jobs and never terminal Jobs.
- A missing checkpoint must produce degraded recovery rather than a false exact-resume claim.

---

### Task 1: Persistent Pause Contract

**Files:** `src/sandbox_harness/domain.py`, `src/sandbox_harness/store.py`, `tests/test_thread_pause.py`

- [ ] Add Thread control and participant rows plus public views and state enums.
- [ ] Add idempotent `request_thread_pause`, participant transition, `finish_thread_pause`, and `resume_thread` Store methods.
- [ ] Test generation fencing, idempotency, queued Harness pausing, Provider preservation, and terminal Job preservation.

### Task 2: Primary Harness Safe Point

**Files:** `src/sandbox_runtime/app.py`, `src/sandbox_harness/runtime.py`, `src/sandbox_harness/broker.py`, `tests/test_broker.py`

- [ ] Make runtime quiesce return the current native session identity.
- [ ] Add `Broker.pause_thread` and checkpoint the Primary Harness before sandbox release.
- [ ] Mark the Turn `PAUSED` and record degraded pause when checkpointing is unavailable.
- [ ] Test that cancellation remains destructive while pause persists checkpoint and native session identity.

### Task 3: Parallel Child Harness Pause

**Files:** `src/sandbox_harness/job_worker.py`, `src/sandbox_harness/harness_job.py`, `src/sandbox_harness/store.py`, `tests/test_harness_job.py`, `tests/test_job_worker.py`

- [ ] Quiesce and checkpoint running child Harness Jobs into their participant record.
- [ ] Requeue paused Harness Jobs on resume and restore their checkpoint/native session.
- [ ] Fence claims for paused Threads and leave Provider/Operation receipts intact.
- [ ] Test queued, running, lost-sandbox, and already-terminal participants.

### Task 4: API, Continuation, And UI

**Files:** `src/sandbox_harness/app.py`, `src/sandbox_harness/job_continuation.py`, `web/src/api.js`, `web/src/main.jsx`, `web/src/styles.css`, `tests/test_job_api.py`, `tests/browser_thread_pause.cjs`

- [ ] Add Thread pause/status/resume endpoints and keep stop-all as explicit hard cancellation.
- [ ] Exclude paused Threads from continuation claims.
- [ ] Display pausing/paused/resuming state and route the main control to pause/resume.
- [ ] Test page refresh and resume after transport reconnect.

### Task 5: Restart Reconciliation And Real-Machine Case

**Files:** `src/sandbox_harness/pause_reconciler.py`, `src/sandbox_harness/main.py`, `tests/test_pause_reconciler.py`, `tests/run_real_parallel_protocol.py`, `docs/test-evidence/durable-pause-report.md`

- [ ] Reconcile incomplete pause sagas on startup and by periodic lease-safe polling.
- [ ] Add a real case with parallel Harness participants and a durable external Job.
- [ ] Run targeted unit/browser tests, deploy the changed services once, execute the real case, and record evidence.
