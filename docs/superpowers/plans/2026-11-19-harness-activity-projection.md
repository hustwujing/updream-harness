# Harness Activity Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve complete Child Harness streams while presenting stable, phase-level execution activities and terminal semantic segmentation errors.

**Architecture:** Keep `job_activities` as the raw ledger and transactionally maintain a separate display projection keyed by native operation identity. The API serves display activities by default and an explicit raw diagnostic view; the frontend merges display updates by stable ID.

**Tech Stack:** Python 3.13, SQLAlchemy async, PostgreSQL/SQLite tests, FastAPI, React/Vitest.

**Spec:** `docs/superpowers/specs/2026-11-19-harness-activity-projection-design.md`

## Global Constraints

- Do not parse natural-language text to infer operation identity.
- Preserve every raw event and existing Child summary behavior.
- Bound projected payloads and tool-output tails.
- Keep display projection replay-idempotent and owner-scoped.
- Store all generated evidence under `/data2/wujing03/sandbox-harness-platform-parallel/.data`.

## Review Focus

- A tool with hundreds of output chunks remains one display activity.
- Concurrent tools update distinct stable rows.
- Missing native tool IDs do not merge unrelated operations.
- Warning/error events cannot be hidden by compaction.
- Recovery replay does not duplicate display rows or regress cursor order.
- Large groups of native tool calls remain a few structured, expandable phases.
- Technical mode never renders native payload JSON as timeline copy.
- `no_target_found` never repeats the same frozen selector.

---

### Task 1: Semantic activity projector

**Files:**
- Create: `src/sandbox_harness/activity_projection.py`
- Modify: `src/sandbox_harness/store.py`
- Test: `tests/test_job_activity_projection.py`

**Interfaces:**
- Produces: `projection_instruction(kind, payload, status, sequence) -> ProjectionInstruction`.
- Produces: `Store.list_job_display_activities(job_id, after, limit)`.

- [ ] Write failing tests for text exclusion, tool lifecycle pairing, bounded output, errors and replay.
- [ ] Run the focused test and verify failure.
- [ ] Add the display table and transactional projector while retaining raw rows.
- [ ] Run focused Store/projector tests and verify pass.
- [ ] Commit the coherent backend projection change.

### Task 2: API and execution graph counts

**Files:**
- Modify: `src/sandbox_harness/app.py`
- Modify: `src/sandbox_harness/execution_graph.py`
- Test: `tests/test_job_api.py`
- Test: `tests/test_execution_graph.py`

**Interfaces:**
- Consumes: `Store.list_job_display_activities`.
- Produces: default display activity API and explicit `view=raw` diagnostic API.

- [ ] Write failing API ownership/view and display-count tests.
- [ ] Run the focused tests and verify failure.
- [ ] Serve projected rows by default and count projected rows in graph nodes.
- [ ] Run focused API/graph tests and verify pass.
- [ ] Commit the API/graph contract.

### Task 3: Stable frontend merge and raw details

**Files:**
- Create: `web/src/execution-activities.js`
- Modify: `web/src/api.js`
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/styles.css`
- Test: `web/src/execution-activities.test.js`
- Test: `tests/browser_execution_graph.cjs`

**Interfaces:**
- Consumes: activity `id`, `sequence`, `cursor_sequence`, `raw_count`.
- Produces: `mergeExecutionActivities(current, incoming)` and a raw technical-details toggle.

- [ ] Write failing merge tests proving in-place lifecycle updates and cursor monotonicity.
- [ ] Run Vitest and verify failure.
- [ ] Merge by stable ID, advance by cursor sequence and render compact/raw modes.
- [ ] Run Vitest and focused browser execution-graph tests.
- [ ] Commit the frontend behavior.

### Task 4: Recovery, release and documentation

**Files:**
- Modify: `docs/development/recovery-and-observability.md`
- Modify: `docs/development/api-and-frontend.md`

**Interfaces:**
- Consumes: completed backend/frontend contracts.
- Produces: deployable release and verification evidence.

- [ ] Add a recovery test replaying a raw tool lifecycle without duplicate display rows.
- [ ] Run all affected backend and frontend tests.
- [ ] Update development rules for raw versus display activity ownership.
- [ ] Commit, record release, `SIGKILL` affected services, and verify recovery.
- [ ] Run the final full gate once and publish the verified revision.

### Task 5: Phase-level presentation and semantic segmentation failures

**Files:**
- Modify: `web/src/execution-activities.js`
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/styles.css`
- Modify: `src/sandbox_harness/controlled_media_adapter.py`
- Modify: `src/sandbox_runtime/instructions.py`
- Modify: `deploy/job-catalog.example.json`
- Test: `web/src/execution-activities.test.js`
- Test: `tests/browser_execution_graph.cjs`
- Test: `tests/test_controlled_media_adapter.py`
- Test: `tests/test_job_worker.py`
- Test: `tests/test_job_catalog.py`

**Interfaces:**
- Consumes: projected display activities and structured native tool metadata.
- Produces: `groupExecutionActivities(rows)` and terminal `no_target_found` failures.

- [ ] Write failing unit/browser tests for phase grouping, localized raw summaries and non-retryable selectors.
- [ ] Run the focused tests and verify the new assertions fail.
- [ ] Implement structured phase grouping and remove direct JSON rendering from the timeline.
- [ ] Mark `no_target_found` non-retryable and clarify inspected geometry/media-processing routing.
- [ ] Run affected frontend/backend tests, deploy, verify the real thread, then run the full gate once.
