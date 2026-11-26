# Child Artifact Identity Bindings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve platform-authoritative Artifact filenames in Child Harness and Reviewer input bindings.

**Architecture:** Resolve metadata in `SandboxChildBackend.start`, derive a safe materialized path from the stored Artifact basename, and pass immutable bindings to prompt construction and runtime upload. Keep the external Job contract unchanged.

**Tech Stack:** Python, Pydantic contracts, pytest, sandbox runtime streaming API.

**Spec:** `docs/superpowers/specs/2026-11-26-child-artifact-identity-bindings-design.md`

## Global Constraints

- Artifact ID and Core metadata are authoritative.
- Do not add case, operation or sandbox-provider branches.
- Do not expose host paths or allow path traversal.
- Keep caller-facing `JobRequest` backward compatible within this revision.

## Review Focus

- Two inputs with generic roles keep the correct distinct filenames.
- A stored nested path contributes only its basename.
- Prompt and uploaded runtime path are identical.
- Reviewer prompts receive the same binding evidence.
- Missing Artifacts still fail before runtime submission.

---

### Task 1: Freeze resolved input bindings

**Files:**
- Modify: `src/sandbox_harness/services/job_worker/harness_job.py`
- Test: `tests/test_harness_job.py`

**Interfaces:**
- Consumes: `JobArtifactInput` and the Artifact returned by `store.get_artifact`.
- Produces: an internal immutable binding used by `execution_prompt` and `put_project_file_stream`.

- [ ] **Step 1: Write failing prompt and runtime path tests**
- [ ] **Step 2: Run the focused tests and confirm the old ID-only path fails**
- [ ] **Step 3: Implement safe resolved bindings and use them in both consumers**
- [ ] **Step 4: Run Child Harness, Reviewer and JobWorker tests**
- [ ] **Step 5: Commit the isolated fix**

### Task 2: Publish and verify the affected real case

**Files:**
- Modify: deployment release pointer under `.data` only
- Evidence: `.data/evidence/real-campaign/`

**Interfaces:**
- Consumes: the tested JobWorker release.
- Produces: an R03 report whose source-order contract passes.

- [ ] **Step 1: Build the immutable JobWorker artifact**
- [ ] **Step 2: Stop the live JobWorker, select the new release and restart it**
- [ ] **Step 3: Verify recovery and service health**
- [ ] **Step 4: Rerun R03 only and inspect Artifact bindings and final pixels**
- [ ] **Step 5: Resume only the cases not yet started by the interrupted gate**
