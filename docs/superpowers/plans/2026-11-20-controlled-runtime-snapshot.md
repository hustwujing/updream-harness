# Controlled Runtime Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the controlled CPU/GPU runtime as a project-owned immutable snapshot that survives source deletion.

**Architecture:** The manifest builder materializes and verifies a digest directory before atomically updating the stable entry. `configure-local.py` installs that snapshot and freezes its digest in the environment.

**Tech Stack:** Python 3.13, Linux `cp --reflink`, pathlib, pytest

**Spec:** `docs/superpowers/specs/2026-11-20-controlled-runtime-snapshot-design.md`

## Global Constraints

- All service storage remains below `/data2/wujing03/sandbox-harness-platform-parallel/.data`.
- No published snapshot may depend on a predecessor checkout or `/data` path.
- Publication is atomic and repeatable.

## Review Focus

- Source deletion after install leaves the entry usable.
- An absolute source symlink is materialized, not preserved.
- A corrupt existing digest directory fails closed.
- A repeated install does not recopy or change the entry.
- A failed staging copy never replaces the current entry.

---

### Task 1: Snapshot materializer

**Files:**
- Modify: `scripts/build_controlled_runtime_manifest.py`
- Test: `tests/test_controlled_runtime_manifest.py`

**Interfaces:**
- Produces: `materialize_entry(source: Path, entry: Path, manifest: dict) -> Path`

- [x] Replace the symlink-alias test with source-deletion, absolute-link,
  corruption and idempotency tests; verify they fail.
- [x] Implement staging copy, external-link materialization, manifest row
  verification and atomic entry publication.
- [x] Run `pytest -q tests/test_controlled_runtime_manifest.py` and commit.

### Task 2: Installer and operational standard

**Files:**
- Modify: `deploy/configure-local.py`
- Modify: `docs/development/deployment-and-collaboration.md`
- Test: `tests/test_deployment_isolation.py`

**Interfaces:**
- Consumes: `materialize_entry(...)` from Task 1.

- [x] Require the installer to call `materialize_entry` and assert the owned
  snapshot path in deployment tests.
- [x] Document project-owned controlled Runtime storage and source cleanup.

### Task 3: Relocated Conda execution

**Files:**
- Modify: `src/sandbox_harness/controlled_media_runner.py`
- Test: `tests/test_controlled_media_runner.py`

- [x] Bind each project-owned environment read-only at its compiled prefix only
  inside bwrap; do not expose the host `/data` tree.
- [x] Prove GPU discovery and a controlled real operation after source cleanup.

- [ ] Run the focused deployment and controlled-worker suites, commit, run the
  full gate, record the release, restart services and verify recovery.
