# Execution Graph Readability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show factual operation details in the execution graph and fit more nodes in desktop viewports.

**Architecture:** Add one pure frontend activity-detail formatter and reuse it in both detail surfaces. Adjust the existing Dagre constants and wrapped-rank spacing without changing graph semantics.

**Tech Stack:** React, JavaScript, Vitest, Dagre, Playwright browser regression.

**Spec:** `docs/superpowers/specs/2026-11-25-execution-graph-readability-design.md`

## Global Constraints

- No backend/API/schema or Sandbox-provider-specific behavior.
- Preview text is bounded and full detail is user-expanded.
- Mobile remains readable and all text stays inside its container.

## Review Focus

- Multiline/heredoc commands: preview stays bounded and full command remains accessible.
- Missing or malformed payloads: render status fallback without throwing.
- File actions without paths: identify the action count instead of a blank row.
- Very long paths and tool names: wrap or truncate without resizing nodes.
- Narrow desktop and mobile: compact desktop layout must not introduce overlap.

---

### Task 1: Shared Activity Detail Formatting

**Files:**
- Modify: `web/src/execution-activities.js`
- Modify: `web/src/execution-activities.test.js`
- Modify: `web/src/HarnessRunDetails.jsx`
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/execution-activities.css`
- Modify: `web/src/harness-run-details.css`

**Interfaces:**
- Produces: `executionActivityDetail(row, options?) -> {preview, full, kind}`.
- Consumes: existing structured activity payloads.

- [ ] Write failing formatter tests for commands, multiline commands, actions, paths, external tools, queries, and fallback.
- [ ] Run `npm test -- --run web/src/execution-activities.test.js` and confirm the new tests fail.
- [ ] Implement `executionActivityDetail` and reuse it in both detail panels.
- [ ] Add a semantic expandable `<code>` detail for values where `full !== preview`.
- [ ] Run the formatter and component tests and confirm they pass.
- [ ] Commit the formatter change.

### Task 2: Compact Graph Layout

**Files:**
- Modify: `web/src/execution-graph-layout.js`
- Modify: `web/src/execution-graph-layout.test.js`
- Modify: `web/src/enhancements.css`

**Interfaces:**
- Produces: `GRAPH_NODE_WIDTH=170`, `GRAPH_NODE_HEIGHT=82`, compact rank/node/wrap gaps.
- Consumes: unchanged graph nodes and edges.

- [ ] Write failing layout tests for exact dimensions and representative desktop column density.
- [ ] Run `npm test -- --run web/src/execution-graph-layout.test.js` and confirm failure.
- [ ] Apply the compact dimensions and spacing while preserving mobile CSS minimums.
- [ ] Run all web unit tests.
- [ ] Run the browser execution-graph regression at desktop and mobile widths and inspect screenshots for overlap.
- [ ] Commit the layout change.

### Task 3: Integrated Verification

**Files:**
- Modify if required: `tests/browser_execution_graph.cjs`
- Evidence: `.data/evidence/`

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: browser evidence for detailed commands and denser nodes.

- [ ] Extend the browser fixture assertion so expanding `执行处理命令` exposes `python /workspace/project/render.py`.
- [ ] Build the web bundle and run the focused browser regression.
- [ ] Run the full web test suite and repository frontend gate.
- [ ] Review the final diff for provider/case-specific logic.
- [ ] Commit any verification fixture change.

