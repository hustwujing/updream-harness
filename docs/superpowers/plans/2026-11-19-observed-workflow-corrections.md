# Observed Workflow Corrections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent invalid Operation contracts, collapse media authorization into one batch action, show truthful waiting state, and remove avoidable PPT workflow retries.

**Architecture:** Reuse Descriptor normalization and existing Turn-keyed media batches. Persist only semantic provenance needed by the UI; keep Provider and Job execution boundaries unchanged. Tighten the signed Skill bundle rather than adding domain rules to the Manager host.

**Tech Stack:** Python, FastAPI, PostgreSQL, React, Playwright, Codex-PPT scripts.

**Spec:** `docs/superpowers/specs/2026-11-19-observed-workflow-corrections-design.md`

## Global Constraints

- No case IDs, filenames or user phrases in production logic.
- No database schema change.
- Provider work still requires explicit user confirmation.
- Batch confirmation is atomic and owner/Turn scoped.

## Review Focus

- Mixed models or an expired Draft in one batch must confirm none.
- Replayed batch confirmation must return existing Jobs without duplicates.
- A pending batch must not animate the execution graph as running.
- Explicit Artifact filenames must remain unchanged.
- Existing level-2 speaker-note headings must still parse.

### Task 1: Descriptor Validator Contract

- [ ] Add failing catalog/coordinator tests.
- [ ] Add `output_validators` to Operation Descriptor and frozen catalog entries.
- [ ] Reject undeclared validator IDs during normalization.
- [ ] Run catalog, coordinator and controlled-operation tests.

### Task 2: Atomic Media Batch Decisions

- [ ] Add failing Store/API batch confirmation and cancellation tests.
- [ ] Implement Turn-scoped atomic confirm/cancel methods and routes.
- [ ] Preserve individual Draft endpoints and idempotent replay.
- [ ] Run media Store/API tests.

### Task 3: Media Batch UI And Waiting State

- [ ] Add a browser test with multiple Drafts from one Turn.
- [ ] Group cards with one confirm/cancel action and optimistic lock.
- [ ] Show `等待确认 N 项媒体生成` in the Thread header.
- [ ] Run browser and frontend build checks.

### Task 4: Semantic Artifact Presentation

- [ ] Add failing publication/UI tests for generic Job filenames.
- [ ] Persist `local_job_id` and derive a semantic display name in the UI.
- [ ] Preserve explicit deliverable names and Markdown path resolution.

### Task 5: PPT Skill Corrections

- [ ] Add assembler and prompt-generation regression tests.
- [ ] Accept level-1 speaker-note headings and document the canonical level-2 form.
- [ ] Require sparse generated text and prohibit duplicate Provider authorization.
- [ ] Verify and freeze the signed Skill bundle.

### Task 6: Release

- [ ] Run focused tests, one full gate, browser regressions and frontend build.
- [ ] Commit, record the signed Content Bundle, deploy affected services and run targeted real checks.

### Task 7: Complex Parallel Real-Machine Cases

- [ ] Add catalog assertions for three executable business-shaped parallel cases.
- [ ] Add a presentation case covering `image.segment`, asynchronous image generation and `codex-ppt` aggregation.
- [ ] Add a multimodal report case covering transcription, media normalization, asynchronous image generation and `docx` aggregation.
- [ ] Add a mixed in-flight recovery case that restarts the Job Worker and verifies no duplicate Provider submission and one Primary continuation.
- [ ] Regenerate the catalog, run its unit tests, deploy, and execute the three cases against the isolated environment.
