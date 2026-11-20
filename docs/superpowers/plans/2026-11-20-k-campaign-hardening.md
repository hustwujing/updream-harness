# K Campaign Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the accepted K-series improvements across durable recovery, capability selection, offline Skill execution, truthful real-case evidence and user-visible progress.

**Architecture:** Reuse the existing Job generation/lease model for collection retries, add one shared readiness/preflight component to API and Gateway coordinators, and keep immutable dependencies in Runtime Profiles while scaffolds remain in the Skill bundle. Extend the K protocol runner rather than introducing another test framework.

**Tech Stack:** Python 3.13, FastAPI, SQLAlchemy/PostgreSQL, httpx, OpenSandbox/gVisor, React/Vitest/Playwright, Docker BuildKit.

**Spec:** `docs/superpowers/specs/2026-11-20-k-campaign-hardening-design.md`

## Global Constraints

- Do not add Claude/DeepSeek K matrix execution.
- Never resubmit an accepted Provider execution for collection recovery.
- No confirmation card may expose a Descriptor that is currently unavailable or a Provider request that fails deterministic preflight.
- Runtime package installation is forbidden for `web-artifacts-builder`.
- K product verification must not trust only Harness-authored JSON.
- All mutable state, build cache and evidence remain under `/data2/wujing03`.

## Review Focus

- Worker dies after collection retry is persisted: next generation must reuse one external ID.
- Readiness changes during draft confirmation: confirm must revalidate and reject stale availability.
- Provider reference bytes do not match role/mode: draft creation must fail without a side effect.
- Offline Web Skill runs with network unavailable: initialization and bundle must still succeed.
- Thousands of child events arrive: UI must show bounded semantic groups and accurate remaining Job progress.

---

### Task 1: Durable Collection Retry

**Files:**
- Modify: `src/sandbox_harness/store.py`
- Modify: `src/sandbox_harness/job_worker.py`
- Modify: `deploy/job-catalog.example.json`
- Test: `tests/test_job_worker.py`
- Test: `tests/test_store.py`

**Interfaces:**
- Produces: `Store.defer_collection_retry(job_id, worker_id, generation, ...)`; Descriptor `retry_policy.max_collection_attempts`.

- [x] Add failing tests for same-worker retry, worker restart takeover, one external ID and retry exhaustion.
- [x] Implement persisted `COLLECTION_RETRY` Attempt closure and bounded reclaim.
- [x] Run worker/store recovery suites and commit.

### Task 2: Execution Readiness And Provider Preflight

**Files:**
- Create: `src/sandbox_harness/execution_readiness.py`
- Create: `src/sandbox_harness/provider_preflight.py`
- Modify: `src/sandbox_harness/job_catalog.py`
- Modify: `src/sandbox_harness/catalog.py`
- Modify: `src/sandbox_harness/job_coordinator.py`
- Modify: `src/sandbox_harness/app.py`
- Modify: `src/sandbox_harness/gateway.py`
- Test: `tests/test_job_catalog.py`
- Test: `tests/test_job_coordinator.py`
- Test: `tests/test_media_job_adapter.py`

**Interfaces:**
- Produces: `ExecutionReadiness.snapshot()` / `require_available()` and async `ProviderPreflight.validate(request)`.

- [x] Add failing tests for unavailable/recovered Catalog state and stale draft confirmation.
- [x] Add failing tests proving invalid Provider references fail before draft persistence.
- [x] Implement shared readiness and pure Provider validation used by API and Gateway.
- [x] Run coordinator/catalog/media tests and commit.

### Task 3: Offline Web Skill Closure

**Files:**
- Modify: `runtime/skills/web-artifacts-builder/scripts/init-artifact.sh`
- Modify: `runtime/skills/web-artifacts-builder/scripts/bundle-artifact.sh`
- Create: `runtime/skills/web-artifacts-builder/template/*`
- Modify: `runtime/skills/manifest.json`
- Modify: `runtime/profiles/web-browser.Dockerfile`
- Modify: `runtime/toolchain.lock.json`
- Test: `tests/test_runtime_skill_dependencies.py`
- Test: `tests/test_runtime_image_contract.py`

**Interfaces:**
- Produces: network-free `init-artifact.sh PROJECT` and `bundle-artifact.sh` using pinned Profile packages.

- [ ] Add dependency-closure and no-network-script assertions.
- [ ] Implement offline scaffold/link/bundle scripts and pinned Profile dependencies.
- [ ] Build the affected image using existing `/data2` caches and run its Skill probe.
- [ ] Commit image source and signed Profile Catalog update.

### Task 4: Truthful K Protocol Evidence

**Files:**
- Modify: `tests/run_real_parallel_protocol.py`
- Modify: `scripts/build_real_case_catalog.py`
- Modify: `tests/real_cases/cases.json`
- Modify: `tests/test_real_case_catalog.py`
- Modify: `docs/development/testing-and-real-cases.md`

**Interfaces:**
- Produces: independently validated `skill_product` archives and explicit `clean_run` / `composite_verified` report scope.

- [ ] Add tests for safe/nonempty product archives and report-scope classification.
- [ ] Extend K01-K06 objectives/contracts and host verification.
- [ ] Make campaign reports refuse to label composite reruns as one clean success.
- [ ] Run catalog/runner tests and commit.

### Task 5: E2E K Cases And Long-tail UI

**Files:**
- Modify: `src/sandbox_harness/execution_graph.py`
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/HarnessRunDetails.jsx`
- Test: `tests/test_execution_graph.py`
- Test: `web/src/harness-activity.test.js`
- Test: `tests/browser_execution_graph.cjs`
- Modify: `tests/run_real_parallel_protocol.py`
- Modify: `scripts/build_real_case_catalog.py`

**Interfaces:**
- Produces: wait-barrier `remaining_jobs` and K12-K14 user-path evidence with one bounded continuation per group.

- [ ] Add failing graph/browser tests for remaining names, elapsed time and bounded semantic grouping.
- [ ] Implement wait-barrier payload/UI without adding graph nodes per raw event.
- [ ] Add K12 Skill continuation, K13 mixed group and K14 two-stage Provider cases.
- [ ] Run focused UI/API tests and commit.

### Task 6: Real Verification And Release

**Files:**
- Modify: `docs/test-evidence/final-complex-report.md`
- Evidence: `.data/evidence/real-campaign/*`
- Evidence: `.data/evidence/gate/*`

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: deployed release and remote branch evidence.

- [ ] Run focused real recovery/readiness/offline Skill cases.
- [ ] Run representative complex K cases and the three new E2E cases; fix only generic defects.
- [ ] Run the complete gate once on the final clean revision.
- [ ] Deploy signed Runtime/Profile bundles and affected services/images.
- [ ] Verify Nginx, cleanup, active leases and deployed revision; push GitHub.
