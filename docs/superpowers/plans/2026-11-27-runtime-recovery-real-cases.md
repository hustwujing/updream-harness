# Runtime Recovery Real Cases Implementation Plan

> **For agentic workers:** Implement natively in this checkout; sub-agent delegation is disabled for this task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add EVO16-EVO18 to the deployed Ali gate and produce evidence for checkpoint replay, recovered internal context and media-output promotion.

**Architecture:** Extend the existing evolution runner with case-local transport and database fault fixtures while all successful paths use deployed services and real Ali sandboxes/Providers. Generate the canonical catalog from its builder and keep runner/catalog contract tests authoritative.

**Tech Stack:** Python 3.13, asyncio, HTTPX, SQLAlchemy, pytest, Ali Sandbox backend.

**Spec:** `docs/superpowers/specs/2026-11-27-runtime-recovery-real-cases.md`

## Global Constraints

- All state, evidence and temporary data stay under the Ali checkout's `.data` tree.
- Faults are isolated to case-owned Threads, sandboxes and clients.
- Never replay Harness or Provider operations with unknown side effects.
- Preserve current user-message scheduling and one-active-Turn serialization.

## Review Focus

- The first restore may have completed remotely before its response is lost; repeating it must be idempotent.
- A second pre-execution restore failure must leave the completion context eligible for a later user Turn.
- Runtime execution registration must make context forwarding final and exactly once.
- Media promotion must use the existing cross-domain command and preserve one Provider identity.
- Case cleanup must not delete unrelated retained sandboxes or objects.

---

### Task 1: Catalog Contract

**Files:**
- Modify: `scripts/build_real_case_catalog.py`
- Modify: `tests/test_real_case_catalog.py`
- Modify: `tests/test_real_campaign_runner.py`
- Regenerate: `tests/real_cases/cases.json`

- [x] Add EVO16-EVO18 definitions and update exact 193-case/group assertions.
- [x] Run the catalog tests and verify they fail before runner handlers exist.

### Task 2: Evolution Runner

**Files:**
- Modify: `tests/run_real_evolution.py`
- Test: `tests/test_real_case_catalog.py`
- Test: `tests/test_real_campaign_runner.py`

- [x] Add the case-local lost-response transport and `evo16` checkpoint replay assertions.
- [x] Add `evo17` durable setup, deployed user Turns, exact-once assertions and zero external-work audit.
- [x] Add `evo18` real image draft, Project File promotion, continuation and one-execution assertions.
- [x] Extend `HANDLERS` through EVO18 and run runner syntax/contract tests.

### Task 3: Deployed Evidence

**Files:**
- Create under ignored data: `.data/evidence/real-campaign/EVO16.json`
- Create under ignored data: `.data/evidence/real-campaign/EVO17.json`
- Create under ignored data: `.data/evidence/real-campaign/EVO18.json`

- [x] Deploy the affected API, PlatformWorker and JobWorker revision by SIGKILL restart and verify recovery.
- [x] Run EVO16-EVO18 individually and inspect authoritative database/log evidence.
- [x] Rerun the affected existing P18 checkpoint case; K13/K14 were proven outside the Direct Media boundary and retained.
- [x] Run release gates and refresh the 193-case composite report; publication is tracked by the final repository commit.
