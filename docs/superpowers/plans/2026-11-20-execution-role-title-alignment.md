# Execution Role Title Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align each Execution Graph role icon and role name on one horizontal title row with clearer spacing.

**Architecture:** Preserve the existing DOM and graph dimensions. Apply explicit grid coordinates in role-specific CSS and verify rendered geometry with Playwright.

**Tech Stack:** CSS Grid, React, Playwright

**Spec:** `docs/superpowers/specs/2026-11-20-execution-role-title-alignment-design.md`

## Global Constraints

- Do not change node dimensions or graph layout.
- Preserve icon status badges, dragging and responsive behavior.

## Review Focus

- Active and terminal role icons align identically.
- Long role names retain ellipsis behavior.
- Mobile nodes keep the same title alignment.

---

### Task 1: Align role title row

**Files:**
- Modify: `web/src/role-icons.css`
- Test: `tests/browser_execution_graph.cjs`

- [x] Add a failing browser geometry assertion for vertical center alignment and horizontal gap.
- [x] Assign icon/name/title/status to explicit grid cells and add 3px name padding.
- [x] Run frontend tests, production build and the focused browser case.
