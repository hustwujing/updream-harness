# Execution DAG and API Errors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render the factual execution topology as a readable DAG and prevent raw proxy HTML from reaching user-visible errors.

**Architecture:** Normalize fetch failures in the shared API layer. Derive a Turn-scoped graph view and use Dagre for left-to-right layout, while the existing component owns polling, history and details.

**Tech Stack:** React 19, `@dagrejs/dagre`, SVG, Vitest, Playwright

**Spec:** `docs/superpowers/specs/2026-11-18-execution-dag-and-api-errors-design.md`

## Global Constraints

- Render only server-provided factual nodes and edges.
- Never display non-JSON response bodies or proxy HTML.
- Active animation must terminate with the authoritative Turn/node state.
- Mobile must not create document-level horizontal overflow.
- Blank-canvas panning and desktop node dragging must not change factual edges.
- Clicking every node type must keep the URL stable and show non-empty details.
- Preserve unrelated working-tree changes.

## Review Focus

- Cyclic or dangling server edges must not crash layout; covered by layout unit tests.
- A group with many children must remain usable collapsed and expanded; covered by browser tests.
- Multiple Turns must default to the latest active/current Turn without losing history access; covered by browser tests.
- HTML 502 and plain-text 503 responses must produce bounded messages; covered by API unit tests.
- Reduced-motion and terminal nodes must not animate indefinitely; covered by browser assertions.

---

### Task 1: Normalize API failures

**Files:**
- Modify: `web/src/api.js`
- Modify: `web/src/api.test.js`

**Interfaces:**
- Produces: `publicApiError(response, raw)` behavior used by `api()`.

- [ ] Add failing tests for HTML 502, text 503, JSON detail and request IDs.
- [ ] Run `npm --prefix web test -- --run` and confirm the new tests fail.
- [ ] Implement bounded status/content-type error classification in `api.js`.
- [ ] Re-run the unit tests and confirm they pass.

### Task 2: Derive and lay out the factual DAG

**Files:**
- Create: `web/src/execution-graph-layout.js`
- Create: `web/src/execution-graph-layout.test.js`
- Modify: `web/package.json`
- Modify: `web/package-lock.json`

**Interfaces:**
- Produces: `executionGraphView(graph, options)` returning visible nodes, edges, dimensions and Turn/group selectors.

- [ ] Add failing tests for branch/join ranks, dangling edges, Turn filtering and collapsed groups.
- [ ] Install `@dagrejs/dagre` using the project-owned npm cache.
- [ ] Implement deterministic left-to-right layout and collapsed-group projection.
- [ ] Run unit tests and confirm they pass.

### Task 3: Replace the serpentine renderer

**Files:**
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/enhancements.css`
- Modify: `tests/browser_execution_graph.cjs`

**Interfaces:**
- Consumes: `executionGraphView(graph, options)` from Task 2.

- [ ] Update the browser fixture to include real `submitted_by`, `contains`, `reviewed_by` and `review_verdict` edges.
- [ ] Confirm the updated browser test fails against the serpentine renderer.
- [ ] Render ranked SVG edges and positioned node cards, Turn selector, group controls, canvas panning, session-local node dragging and responsive detail UI.
- [ ] Assert branches, joins, every node type's non-navigation details, collapse, canvas/node dragging, terminal animation closure and mobile bounds.
- [ ] Inspect desktop and mobile screenshots and confirm no overlap or document overflow.

### Task 4: Release verification

**Files:**
- Modify only release evidence under ignored `.data` paths.

- [ ] Run frontend unit tests, production build and focused browser test.
- [ ] Create a clean detached release containing only this change and existing intended source state.
- [ ] Confirm no active Turn is interrupted, record the release and restart only the API.
- [ ] Verify API/Gateway health, observability counters, worker state and actual Nginx ingress browser behavior.
