# Provider Contract Plugins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Isolate external Adapter validation and return safe, actionable errors from every asynchronous executor to users and model continuations.

**Architecture:** A pure Provider contract registry supplies public schema, preflight and request normalization. Durable workers keep execution ownership; public projections derive `error_reason` only from a trusted diagnostic envelope.

**Tech Stack:** Python 3.13, Pydantic, SQLAlchemy, FastAPI, React, pytest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-27-provider-contract-plugins-design.md`

## Global Constraints

- No Provider-, model- or case-specific branches in durable state machines.
- Never expose raw Provider bodies, prompts, URLs, request IDs or credentials.
- All generated evidence and caches remain under `/data/wujing02/.../.data`.
- Use focused tests while iterating; deploy before real Provider probes.

## Review Focus

- Unknown adapters fail before any external side effect.
- Provider text containing URLs or credentials cannot become `error_reason`.
- A voice-ID collision retries only once.
- Continuation payloads remain bounded and contain no `error_detail`.
- Explicit image dimensions retain aspect ratio and honor pixel limits.

---

### Task 1: Public External Error Envelope

**Files:**
- Modify: `src/sandbox_harness/common/contracts/public_errors.py`
- Test: `tests/test_public_error_projection.py`

- [x] Add `external_error_detail()` and `public_error_reason()` with bounded redaction.
- [x] Project the safe reason through public Job results.
- [x] Run the focused public error tests and commit.

### Task 2: Provider Contract Registry

**Files:**
- Create: `src/sandbox_harness/common/providers/contracts.py`
- Create: `src/sandbox_harness/common/providers/ark.py`
- Create: `src/sandbox_harness/common/providers/minimax.py`
- Create: `src/sandbox_harness/common/providers/tripo.py`
- Create: `src/sandbox_harness/common/providers/dashscope.py`
- Modify: `src/sandbox_harness/common/catalog/media_catalog.py`
- Test: `tests/test_media_catalog.py`

- [x] Define the pure contract protocol and registry keyed by adapter.
- [x] Move every enabled Media Provider's public schema and validation into plugins.
- [x] Reject unregistered adapters and run catalog/preflight tests.

### Task 3: Provider Executors

**Files:**
- Modify: `src/sandbox_harness/services/media_worker/media_worker.py`
- Modify: `src/sandbox_harness/services/media_worker/minimax_voice.py`
- Test: `tests/test_media_worker.py`
- Test: `tests/test_minimax_voice.py`

- [x] Compile Seedream sizes through the Ark contract without tier conversion.
- [x] Rotate MiniMax duplicate voice identity once and preserve safe Provider reason.
- [x] Run worker tests and commit.

### Task 4: Async Error Delivery

**Files:**
- Modify: `src/sandbox_harness/common/db/job_terminal.py`
- Modify: `src/sandbox_harness/common/db/media_jobs.py`
- Modify: `src/sandbox_harness/common/db/media_drafts.py`
- Modify: `src/sandbox_harness/services/platform_worker/platform_service.py`
- Modify: `src/sandbox_harness/services/api/execution_graph.py`
- Modify: `src/sandbox_harness/common/contracts/domain.py`
- Modify: `web/src/main.jsx`
- Modify: `web/src/ExecutionGraph.jsx`

- [x] Add the same safe failure fields to Job and Media continuations.
- [x] Project trusted reasons for Operation, Provider, Child Harness and Site jobs without exposing raw detail.
- [x] Show `error_reason` in Media detail and execution graph.
- [x] Run continuation, graph, frontend and browser tests and commit.

### Task 5: Deploy And Real Regression

**Files:**
- Modify: provider documentation and deployment config only if verified behavior changed.

- [x] Run focused Python and frontend gates.
- [x] Build and deploy affected services, then deliberately restart workers.
- [x] Run real Ark image, MiniMax reference-voice and reference-video cases.
- [x] Verify public UI, model continuation and logs contain safe actionable reasons.
- [x] Commit documentation and record evidence paths. GitHub push is the final publication step.
