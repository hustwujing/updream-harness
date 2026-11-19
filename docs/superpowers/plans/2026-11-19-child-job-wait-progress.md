# Child Job Wait Progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show durable `completed/total` child-task wait progress in the transcript and execution graph whenever the Primary Harness is waiting for a JobGroup.

**Architecture:** `ExecutionGraphProjector` computes progress from authoritative Job and JobGroup rows and emits a deterministic wait barrier. The main browser refresh derives transcript wait steps from the same graph snapshot and polls only while a barrier is nonterminal.

**Tech Stack:** Python, SQLAlchemy, FastAPI, React, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-19-child-job-wait-progress-design.md`

## Global Constraints

- PostgreSQL Job state is the only progress truth.
- Every terminal outcome counts as returned but not necessarily successful.
- No prompt parsing and no case-specific behavior.
- Only nonterminal barriers animate.
- Refresh and restart must reconstruct the same count without duplicate writes.

## Review Focus

- Mixed success/failure/cancelled terminal outcomes produce exact counts.
- A Job transition replay cannot increment progress twice.
- A group becomes terminal before its continuation Turn exists.
- A continuation exists while the browser is viewing an older Turn.
- Multiple JobGroups from one origin Turn remain separate steps.

---

### Task 1: Authoritative Wait Barrier Projection

**Files:**
- Modify: `src/sandbox_harness/execution_graph.py`
- Test: `tests/test_execution_graph.py`

**Interfaces:**
- Consumes: persisted `JobGroupRow`, `JobRow`, `JobOutboxRow`.
- Produces: `job_group.progress` and `wait_barrier:<group_id>` graph nodes.

- [x] Add failing tests for partial and mixed terminal progress.
- [x] Compute state counts once per snapshot and attach bounded progress.
- [x] Connect member Jobs to the barrier and the barrier to continuation.
- [x] Run `pytest tests/test_execution_graph.py -q`.

### Task 2: Transcript Wait Step

**Files:**
- Modify: `web/src/main.jsx`
- Modify: `web/src/styles.css` or `web/src/enhancements.css`
- Modify: `web/src/ExecutionGraph.jsx`
- Test: `tests/browser_execution_graph.cjs`

**Interfaces:**
- Consumes: wait barrier nodes from `getExecutionGraph`.
- Produces: transcript steps with `terminal/total` and outcome details.

- [x] Extend browser fixture from `0/3` through `3/3`.
- [x] Load current graph facts in the workspace refresh path.
- [x] Poll once per second only while a barrier is nonterminal.
- [x] Render active spinner and terminal check without changing scroll follow.
- [x] Run the focused browser test and inspect desktop/mobile screenshots.

### Task 3: Verification And Release

**Files:**
- Modify: documentation only if implementation changes the contract.

- [x] Run focused Python, frontend unit, production build and browser tests.
- [ ] Commit the coherent change.
- [ ] Record release and restart affected API service after capturing active work.
- [ ] Run a real JobGroup and record observed progress and restart recovery.
