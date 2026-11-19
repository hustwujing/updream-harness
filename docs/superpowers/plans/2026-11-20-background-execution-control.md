# Background Execution Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded Sandbox-local background execution while keeping cross-Turn, Runtime, Sandbox and machine work on host Durable Jobs with Continuations.

**Architecture:** Extend the existing Runtime background Job and subreaper supervisor rather than adding another queue. The supervisor owns process-tree deadline enforcement and atomic receipts; Runtime owns API validation, events and recovery; the CLI owns a single wait invocation and structured user-facing failures. Existing host JobCoordinator/JobWorker remains the only cross-boundary execution plane.

**Tech Stack:** Python 3.12, FastAPI, asyncio, Linux process groups/subreaper, pytest, OpenSandbox/gVisor.

**Spec:** `docs/superpowers/specs/2026-11-20-background-execution-control-design.md`

## Global Constraints

- Sandbox-local background Jobs never claim durability across Sandbox or machine loss.
- Cross-Turn, Runtime, Sandbox or machine work uses host Durable Job plus Continuation.
- Child Harnesses cannot delegate another Harness.
- Product code contains no case-specific command, media or timing special case.
- All mutable evidence and test data remain under `/data2/wujing03`.

## Review Focus

- A command ignores `SIGTERM`: deadline must send `SIGKILL`, reap it and report `timed_out`.
- Runtime restarts while work is active: process identity is checked, work is quiesced and reported `interrupted`.
- An observation wait expires: Job remains running and can later complete normally.
- Cancellation cleanup exits zero: public state must remain `cancelled`, never `completed`.
- API rejects a request: CLI prints one structured JSON error and no traceback.

---

### Task 1: Runtime Job Contract And CLI

**Files:**
- Modify: `src/sandbox_runtime/app.py`
- Modify: `src/sandbox_runtime/background_cli.py`
- Test: `tests/test_background_cli.py`
- Test: `tests/test_runtime_boundaries.py`

**Interfaces:**
- Produces: `BackgroundJobCreate.timeout_seconds`, `terminate_grace_seconds`; `GET /v1/background-jobs/{job_id}/wait`; `sandbox-background wait`.

- [x] Write failing tests for start options, structured HTTP errors, terminal wait and observation timeout.
- [x] Run the focused tests and confirm the new assertions fail.
- [x] Add persisted deadline/grace fields, wait endpoint and CLI commands.
- [x] Run the focused tests and confirm they pass.

### Task 2: Supervisor Deadline And Terminal Receipts

**Files:**
- Modify: `src/sandbox_runtime/background_supervisor.py`
- Modify: `src/sandbox_runtime/app.py`
- Test: `tests/test_runtime_boundaries.py`

**Interfaces:**
- Consumes: absolute deadline and termination grace from Task 1.
- Produces: atomic supervisor receipts with `status`, `returncode`, termination signals and completion time.

- [x] Write failing tests for normal completion, timeout escalation and cancelled-state preservation.
- [x] Run the tests and confirm failure.
- [x] Enforce deadline in the subreaper and make Runtime receipt state authoritative.
- [x] Run Runtime boundary/configuration tests and confirm they pass.

### Task 3: Harness Guidance And Cross-Boundary Contract

**Files:**
- Modify: `src/sandbox_runtime/instructions.py`
- Modify: `docs/development/sandbox-and-harness.md`
- Modify: `docs/development/testing-and-real-cases.md`
- Test: `tests/test_runtime_configuration.py`

**Interfaces:**
- Consumes: `start/wait/cancel` contract from Tasks 1-2.
- Produces: stable model guidance selecting sync, Sandbox background, Provider Worker or host Durable Job.

- [x] Pin generic guidance assertions in a focused test.
- [x] Update guidance and development rules without domain-specific heuristics.
- [x] Run focused instruction and coordinator tests.

### Task 4: Real OpenSandbox Cases

**Files:**
- Create: `tests/run_real_background_control.py`
- Modify: `docs/test-evidence/full-product-case-design.md`

**Interfaces:**
- Produces: real-machine evidence for B04 success/wait, B05 observation timeout, and B06 forced deadline cleanup.

- [x] Create an isolated real OpenSandbox runner using a synthetic Harness fixture and the deployed Runtime bundle.
- [ ] Verify B04 waits to successful terminal state and captures output.
- [ ] Verify B05 observation timeout leaves the Job running and a later wait succeeds.
- [ ] Verify B06 times out a process ignoring `SIGTERM`, reports `timed_out`, and leaves no live process.
- [ ] Persist revision, image, elapsed time and cleanup evidence under `.data/evidence`.

### Task 5: Release

**Files:**
- Modify: `docs/test-evidence/full-product-report.md`
- Generated evidence: `.data/evidence/gate/*.json`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: immutable release, deployed verification and GitHub commit.

- [ ] Run focused suites, then commit the implementation.
- [ ] Run the complete gate once on the clean commit.
- [ ] Record and deploy the immutable release; refresh warm Sandboxes for the new Runtime bundle.
- [ ] Run B04-B06 against the deployed environment and verify cleanup.
- [ ] Update the report, commit evidence documentation, rerun affected checks, push `sandbox-harness-platform-parallel`, and verify deployed health revision.
