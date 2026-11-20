# Execution Graph Harness Token Usage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show per-Harness token usage in Execution Graph nodes and animated details.

**Architecture:** Aggregate normalized model activity in the graph projector, add a compact node total, and animate detail counters only for updates to the same selected node.

**Tech Stack:** SQLAlchemy, React, requestAnimationFrame, Playwright

**Spec:** `docs/superpowers/specs/2026-11-20-execution-graph-token-usage-design.md`

## Global Constraints

- Do not change accounting or persist presentation state.
- Do not merge Primary and Child usage.
- Respect reduced-motion preferences.

## Review Focus

- Multiple model calls aggregate once each.
- Child and Reviewer `job_id` values remain isolated.
- Node switching never animates across identities.
- Active SSE refresh animates from the current rendered value.
- Bounded history reports truncation.

---

### Task 1: Project per-Harness usage

**Files:**
- Modify: `src/sandbox_harness/execution_graph.py`
- Test: `tests/test_execution_graph.py`

- [x] Add failing Primary and Child aggregation tests.
- [x] Add bounded usage projection keyed by Turn and Job.

### Task 2: Render and animate usage

**Files:**
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/HarnessRunDetails.jsx`
- Modify: `web/src/harness-run-details.css`
- Test: `tests/browser_execution_graph.cjs`

- [x] Add failing node/detail/animation geometry assertions.
- [x] Render compact total and four animated counters.
- [x] Verify desktop, mobile and reduced motion.

### Task 3: Verify and release

- [x] Run focused backend/frontend/browser tests.
- [ ] Run full gate, deploy, verify and push.
