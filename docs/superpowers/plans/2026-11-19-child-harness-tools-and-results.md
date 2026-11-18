# Child Harness Tools and Results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable scoped durable Operation/Provider calls from Child Harnesses and close the result, recovery and UI failures exposed by thread `71b72c1e2e3c481caee04e2f2abcb0da`.

**Architecture:** Reuse JobGroup/Job/Outbox infrastructure with explicit parent Job ownership and target-aware continuation. Compile frozen Skill requirements into bounded grants; persist Child inputs/results and suspend/resume state rather than allowing LLM polling.

**Tech Stack:** FastAPI, SQLAlchemy/PostgreSQL, OpenSandbox Runtime, React, Dagre, Pytest, Vitest, Playwright

**Spec:** `docs/superpowers/specs/2026-11-19-child-harness-tools-and-results-design.md`

## Global Constraints

- Preserve current database compatibility policy: no legacy dual reads or writes.
- Ordinary Children cannot create `kind=harness` descendants.
- Authorization, budget, artifact identity, generation and lease fences remain authoritative.
- Infrastructure retries and waits never return to the LLM as polling work.
- Preserve unrelated working-tree changes.

## Review Focus

- Parent Turn terminalizes while Child model requests remain active.
- Sandbox creation races orphan reaping before acceptance receipt.
- Child reports a blocker or required Artifact set is empty.
- Multiple Children propose Provider work concurrently or across restart.
- Structured activity payloads and wide/deep DAGs render without blank pages or occlusion.

---

### Task 1: Immediate correctness boundaries

**Files:** `src/sandbox_runtime/job_cli.py`, `src/sandbox_runtime/instructions.py`, `src/sandbox_harness/harness_job.py`, `src/sandbox_harness/store.py`, focused tests.

- [ ] Add failing tests for artifact cardinality, frozen input paths, bounded Child summary/blocker, and Job-scoped model activity surviving origin Turn completion.
- [ ] Implement the minimal contract and result fixes.
- [ ] Run focused CLI, Harness Job, Store and continuation tests.

### Task 2: Job Sandbox provisioning ownership

**Files:** `src/sandbox_harness/store.py`, `src/sandbox_harness/broker.py`, `src/sandbox_harness/harness_job.py`, namespace/recovery tests.

- [ ] Reproduce reaper deletion during Job Sandbox configure.
- [ ] Persist/query live Job identity throughout SUBMITTING and retain matching namespace Sandboxes.
- [ ] Verify terminal/abandoned provisioning is still reaped.

### Task 3: Target-aware subordinate JobGroups

**Files:** domain/contracts/store/Gateway/Runtime CLI/worker/continuation modules and tests.

- [ ] Add `parent_job_id`, bounded depth and continuation target contracts.
- [ ] Authorize only Operation/Provider descendants from ordinary Child grants.
- [ ] Route subordinate terminal results back to the same Child session.
- [ ] Verify cancellation, restart, idempotency and top-level continuation remain correct.

### Task 4: Provider approval and Skill dependency compilation

**Files:** catalog, Runtime CLI, grants, media confirmation/worker, Child lifecycle and tests.

- [ ] Compile frozen Skill capabilities/network requirements and reject unsatisfied profiles before scheduling.
- [ ] Persist Child Provider proposals and one confirmation wave per owning top-level group.
- [ ] Suspend/checkpoint and resume the same Child after Provider results.
- [ ] Adapt `codex-ppt` to platform `image.generate` without public SDK fallback.

### Task 5: Browser and execution graph correction

**Files:** `web/src/ExecutionGraph.jsx`, `web/src/execution-graph-layout.js`, CSS and browser/unit tests.

- [ ] Reproduce structured-detail blank page and right-edge occlusion.
- [ ] Add safe structured rendering and a local error boundary.
- [ ] Wrap DAG ranks into viewport-aware serpentine bands while preserving factual edges and drag behavior.
- [ ] Verify desktop/mobile screenshots, stable URL and no page errors.

### Task 6: Full verification and deployment

- [ ] Run focused and release-level suites for Store, Broker, Worker, Gateway, Runtime and browser changes.
- [ ] Add the PDF + image live case and verify real Provider/Operation execution and three deliveries.
- [ ] Deploy an immutable release, restart through active Child wait states, and verify recovery/cleanup/telemetry.
