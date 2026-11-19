# Harness Run Details Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace generic Harness node details and raw diagnostics with a live semantic run view.

**Architecture:** Execution Graph nodes explicitly identify Harness runs and their objective. A focused frontend utility converts durable display activities into user-facing timeline spans; a dedicated component renders overview, progress, timeline and outputs without fetching raw events.

**Tech Stack:** Python 3.13, SQLAlchemy, React 19, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-19-harness-run-details-design.md`

## Global Constraints

- Never infer intent from model text or command text.
- Preserve the raw event ledger and owner-scoped raw API.
- Do not show raw JSON, internal paths or stream chunks in the product view.
- Failed semantic activities remain independently visible.

## Review Focus

- A running Harness with only text deltas still visibly waits for the model.
- Concurrent/in-progress tools produce one truthful current stage.
- Refresh merges lifecycle updates without duplicate timeline rows.
- Primary and Child Harness nodes both use the friendly panel.
- Non-Harness graph nodes keep their existing details and error contracts.

---

### Task 1: Explicit Harness graph contract

**Files:**
- Modify: `src/sandbox_harness/execution_graph.py`
- Test: `tests/test_execution_graph.py`

**Interfaces:**
- Produces: graph node fields `harness_view`, `objective`, `harness`, `model`.

- [ ] Add failing Primary/Child graph assertions.
- [ ] Add the fields from immutable Turn/Job request data.
- [ ] Run focused graph tests.

### Task 2: Semantic Harness timeline

**Files:**
- Create: `web/src/harness-activity.js`
- Create: `web/src/harness-activity.test.js`
- Modify: `web/src/execution-activities.js`

**Interfaces:**
- Consumes: display activities plus Harness graph node state.
- Produces: `buildHarnessTimeline(rows,node,now)` and `currentHarnessStage(...)`.

- [ ] Add failing tests for model wait, structured grouping, terminal state and errors.
- [ ] Implement semantic spans without raw/text parsing.
- [ ] Run focused Vitest tests.

### Task 3: Friendly Harness panel

**Files:**
- Create: `web/src/HarnessRunDetails.jsx`
- Create: `web/src/harness-run-details.css`
- Modify: `web/src/ExecutionGraph.jsx`
- Test: `tests/browser_execution_graph.cjs`

**Interfaces:**
- Consumes: Task 1 graph fields and Task 2 timeline functions.
- Produces: objective/current stage/progress/timeline/output Harness details.

- [ ] Update browser fixtures and add failing user-surface assertions.
- [ ] Remove raw-activity fetching and the diagnostic toggle from the browser.
- [ ] Render the dedicated panel for Primary/Child Harness nodes only.
- [ ] Run Vitest, build and focused Playwright checks at desktop/mobile sizes.

### Task 4: Documentation and release

**Files:**
- Modify: `docs/development/api-and-frontend.md`
- Modify: `docs/development/recovery-and-observability.md`

**Interfaces:**
- Produces: documented raw/display/user-view ownership and deploy evidence.

- [ ] Update development rules.
- [ ] Run affected backend/frontend/browser regression.
- [ ] Commit, record release, deploy affected services and verify a real Harness node.

### Task 5: Product identity and graph role icons

**Files:**
- Create: `web/src/assets/brand/conductor-mark.svg`
- Modify: `web/src/main.jsx`
- Modify: `web/index.html`
- Modify: `src/sandbox_harness/execution_graph.py`
- Modify: `web/src/ExecutionGraph.jsx`
- Test: `tests/test_execution_graph.py`
- Test: `tests/browser_execution_graph.cjs`
- Test: `tests/browser_auth.cjs`

**Interfaces:**
- Produces: product name `Updream Conductor` and graph node `visual_role`.

- [ ] Add failing graph/browser assertions for stable visual roles and product name.
- [ ] Add the vector mark and visible product copy.
- [ ] Render role-specific Lucide icons with independent status badges.
- [ ] Verify all role families, responsive layout and browser screenshots.
