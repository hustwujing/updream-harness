# Inherited D-Case Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate, adapt and execute Plus `D01-D51`, rerun model-limited cases, then gate and release the complete 149-case Parallel platform.

**Architecture:** Preserve every original D definition while routing each invariant through a real Parallel boundary. Artifact cases use deployed Harness/Provider execution; deterministic lifecycle and fault cases use API, Store and systemd evidence through a dedicated inherited protocol runner.

**Tech Stack:** Python 3.13, FastAPI, SQLAlchemy/PostgreSQL, OpenSandbox, systemd, Playwright, pytest.

**Spec:** `docs/superpowers/specs/2026-11-18-inherited-d-case-migration-design.md`

## Global Constraints

- All mutable state and evidence remain under `/data2/wujing03/sandbox-harness-platform-parallel/.data`.
- Keep original D IDs, prompts, expected contracts and checks; record adaptation explicitly.
- No case-specific product logic or natural-language regex routing.
- Concurrency is 8; stop dispatch on program or optimizable prompt defects.
- Commit every coherent fix; run the full gate only after real-case convergence.

## Review Focus

- A migrated case that has no implemented handler must fail before dispatch.
- Fault cases must target only Parallel-prefixed services and restore them before returning.
- Provider uncertainty must reuse a durable handle and never duplicate submission.
- Artifact/replacement cases must prove lineage from stored identities, not filenames alone.
- User rejection and stale input cases must prove that no forbidden side effect was created.

---

### Task 1: Catalog Completeness And Provenance

**Files:**
- Modify: `scripts/build_real_case_catalog.py`
- Modify: `tests/real_cases/cases.json`
- Modify: `tests/test_real_campaign_runner.py`

**Interfaces:**
- Produces: 149 unique cases and exact D provenance/adaptation metadata.

- [ ] Add a failing catalog test requiring D01-D51 and counts `S31/C30/R17/D51/P20`.
- [ ] Import all Plus D definitions and append Parallel execution metadata without rewriting source fields.
- [ ] Regenerate the catalog, run the catalog tests and commit.

### Task 2: Inherited Protocol Driver

**Files:**
- Create: `tests/run_real_inherited_protocol.py`
- Modify: `tests/run_real_campaign.py`
- Test: `tests/test_inherited_protocol_runner.py`

**Interfaces:**
- Consumes: `--case D01..D51`, selected Harness/model/reasoning and deployed base URL.
- Produces: an atomic per-case JSON report with observed durable identities and assertions.

- [ ] Add failing tests requiring handlers for all D01-D51 and rejecting missing handlers.
- [ ] Implement shared setup, artifact, JobGroup, fault injection and cleanup helpers.
- [ ] Route `executor=inherited_protocol` through the campaign runner with model settings intact.
- [ ] Run driver tests and commit.

### Task 3: D01-D20 Execution Boundaries

**Files:**
- Modify: `tests/run_real_inherited_protocol.py`
- Test: `tests/test_inherited_protocol_runner.py`

**Interfaces:**
- Produces: staged/join, pause/cancel, authorization, isolation, budget and Skill evidence.

- [ ] Implement D01-D08 and focused tests.
- [ ] Implement D09-D20 and focused tests.
- [ ] Run D01-D20 on the isolated deployment, fix generic defects, rerun affected cases and commit.

### Task 4: D21-D41 Contract And Interaction Boundaries

**Files:**
- Modify: `tests/run_real_inherited_protocol.py`
- Test: `tests/test_inherited_protocol_runner.py`

**Interfaces:**
- Produces: replacement lineage, ordered input, Artifact/review and delivery evidence.

- [ ] Implement D21-D35 and focused tests.
- [ ] Implement D36-D41 and focused tests.
- [ ] Run D21-D41, fix generic defects, rerun affected cases and commit.

### Task 5: D42-D51 Recovery And Usage Authorization

**Files:**
- Modify: `tests/run_real_inherited_protocol.py`
- Test: `tests/test_inherited_protocol_runner.py`

**Interfaces:**
- Produces: hard-kill takeover, Provider-handle reuse, attachment/successor and approval evidence.

- [ ] Implement D42-D46 with bounded service restart cleanup.
- [ ] Implement D47-D51 with delivery and side-effect assertions.
- [ ] Run D42-D51, fix generic defects, rerun affected cases and commit.

### Task 6: Rate-Limit Rerun, Final Gate And Release

**Files:**
- Create: `.data/evidence/real-campaign/inherited-d*.json` (untracked)
- Modify: `.data/evidence/real-campaign/final-report.md` (untracked)

**Interfaces:**
- Produces: latest results for all 149 cases, full gate receipt and released service evidence.

- [ ] Rerun latest model-limited cases with the healthiest configured Claude 5.5 route.
- [ ] Aggregate 149 latest results and require zero program/optimizable failures.
- [ ] Run `uv run --offline python tests/gate.py` with the API port available.
- [ ] Record release, restart all Parallel services, verify Nginx/live browser and push the branch.
