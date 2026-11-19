# Live Execution Graph And Delegation Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make execution progress truthful and continuously visible across continuations, delegated Jobs and media Jobs while preventing avoidable delegated-model failures.

**Architecture:** PostgreSQL remains authoritative and `ExecutionGraphProjector` becomes the unified read model. The frontend only controls follow/history behavior and layout; job preflight validates exact child Harness/model routes before resources are scheduled.

**Tech Stack:** Python, SQLAlchemy, FastAPI, React, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-19-live-execution-graph-and-delegation-reliability-design.md`

## Global Constraints

- Do not infer lifecycle state from transcript text.
- Do not expose raw Provider error bodies or credentials.
- Preserve owner scoping and bounded graph snapshots.
- Do not add Thread-specific or media-domain-specific special cases.

## Review Focus

- A continuation arrives while the user is viewing the active Turn.
- A user intentionally selects history while new work starts.
- A media batch is partly running, partly succeeded and partly failed.
- A child model is absent, unhealthy or becomes rate-limited after preflight.
- A terminal node retains an in-progress activity row.

---

### Task 1: Unified Execution Graph Projection

**Files:**
- Modify: `src/sandbox_harness/execution_graph.py`
- Test: `tests/test_execution_graph.py`

- [ ] Add failing tests for continuation edges, media nodes and safe errors.
- [ ] Query and project the authoritative rows with bounded limits.
- [ ] Run `pytest tests/test_execution_graph.py -q`.

### Task 2: Follow-current Full-screen Graph UI

**Files:**
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/execution-graph-layout.js`
- Modify: `web/src/enhancements.css`
- Test: `web/src/execution-graph-layout.test.js`
- Test: `tests/browser_execution_graph.cjs`

- [ ] Add failing reducer/layout and browser expectations for follow mode.
- [ ] Implement explicit follow-current state and current-execution action.
- [ ] Expand desktop and mobile graph dimensions.
- [ ] Run Vitest, production build and the focused browser script.

### Task 3: Delegated Runtime Inheritance And Error Truth

**Files:**
- Modify: job draft/coordinator and model catalog modules selected by inspection.
- Test: focused Job API/coordinator/model health tests.

- [ ] Add a failing test for a Harness-authored stale child model.
- [ ] Freeze ordinary delegation to the origin Turn's Harness, model and reasoning effort.
- [ ] Preserve stable rate-limit versus model-unavailable public errors.
- [ ] Run focused Job and model tests.

### Task 4: Bounded Continuation And Quality Guidance

**Files:**
- Modify: continuation prompt/runtime skill guidance selected by inspection.
- Test: continuation and runtime skill tests.

- [ ] Assert continuation payloads omit raw activities and Provider bodies.
- [ ] Add general document-editability and semantic-validation guidance.
- [ ] Run focused continuation/runtime tests.

### Task 5: Integrated Verification

**Files:**
- Modify: relevant real-case evidence only; no generated data outside `.data`.

- [ ] Run focused Python and frontend suites.
- [ ] Run one isolated real continuation case if no active user work would be interrupted.
- [ ] Record program, model and external outcomes separately.
