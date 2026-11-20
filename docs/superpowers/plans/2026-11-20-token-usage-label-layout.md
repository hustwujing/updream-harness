# Token Usage Label Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep all four Harness token-usage labels fully visible at supported execution-graph widths.

**Architecture:** Remove metric cells from the generic execution-activity `article` selector by using plain metric containers. Pin the visible geometry with a browser overflow assertion.

**Tech Stack:** React, CSS Grid, Playwright

**Spec:** `docs/superpowers/specs/2026-11-20-token-usage-label-layout-design.md`

## Global Constraints

- Preserve four columns on desktop and two columns on mobile.
- Do not change token accounting or animation.
- Do not add text-specific width exceptions.

## Review Focus

- All exact Chinese labels fit without overflow.
- Values remain aligned below their labels.
- Desktop and mobile graph layouts remain within the viewport.
- Token animation still reaches its durable final value.
- Generic execution activity rows remain unchanged.

---

### Task 1: Isolate metric markup and verify geometry

**Files:**
- Modify: `web/src/HarnessRunDetails.jsx`
- Modify: `tests/browser_execution_graph.cjs`

- [x] Add a failing browser assertion for label overflow.
- [x] Replace metric `article` elements with plain metric containers.
- [x] Run the execution-graph browser test and frontend checks.
- [ ] Commit, release, deploy and verify.
