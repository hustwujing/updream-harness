# Cross-Database Real Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add XDB01-XDB06 to the product catalog and produce a verified 175-case Ali report.

**Architecture:** A dedicated real runner connects to the deployed service databases and uses existing API/protocol helpers for real external work. The campaign runner invokes it through a new executor while retaining the current progress, classification, resume and stop-on-defect behavior.

**Tech Stack:** Python 3.11, asyncio, httpx, SQLAlchemy async repositories, PostgreSQL, pytest, systemd.

**Spec:** `docs/superpowers/specs/2026-11-26-cross-database-real-gate-design.md`

## Global Constraints

- Keep all data, logs and temporary files below `/data2/wujing03/sandbox-harness-platform-parallel-ali/.data`.
- Use the deployed Ali services and separate Accounts, Core, Media and Sites databases.
- Do not add provider-, operation- or case-specific production behavior.
- Preserve existing successful case evidence unless an implementation change affects it.

## Review Focus

- Provider acceptance racing the JobWorker kill must be observed before termination.
- Multi-output comparison must reject missing, duplicate or reordered output identities.
- Cancellation must be checked after a possible late Media completion.
- Site manifest comparison must include every file identity and published version.
- Cleanup must inspect all four databases after lifecycle completion.

---

### Task 1: Catalog And Campaign Routing

**Files:**
- Modify: `tests/real_cases/cases.json`
- Modify: `tests/run_real_campaign.py`
- Modify: `tests/test_real_campaign_runner.py`

**Interfaces:**
- Produces: executor name `cross_database` and catalog IDs `XDB01` through `XDB06`.

- [ ] Add failing assertions for 175 cases, the XDB group and campaign routing.
- [ ] Run `pytest tests/test_real_campaign_runner.py -q` and confirm failure.
- [ ] Add six catalog rows and route the executor to the dedicated runner.
- [ ] Rerun the focused tests and commit.

### Task 2: Cross-Database Real Runner

**Files:**
- Create: `tests/run_real_cross_database.py`
- Create: `tests/test_cross_database_real_runner.py`

**Interfaces:**
- Consumes: existing protocol helpers, domain repositories and campaign service controls.
- Produces: one JSON outcome with `classification`, evidence and timing for each case.

- [ ] Add failing structural tests for six handlers, live repositories, real provider use and JobWorker kill/restart.
- [ ] Run `pytest tests/test_cross_database_real_runner.py -q` and confirm failure.
- [ ] Implement XDB01-XDB06 with unique identities, bounded waits and exact cross-database assertions.
- [ ] Run the focused runner tests and commit.

### Task 3: Real Execution And Defect Loop

**Files:**
- Modify only production/test files required by an observed defect.

**Interfaces:**
- Consumes: `tests/run_real_cross_database.py --case XDBnn`.
- Produces: six current-revision real case outcomes.

- [ ] Run cases individually, report progress every two minutes and stop on a program defect.
- [ ] For each defect, add a focused failing unit test, fix the shared contract, commit and immediately restart the affected service.
- [ ] Rerun the failed case and any existing cases affected by the fix.
- [ ] Confirm XDB01-XDB06 all succeed.

### Task 4: 175-Case Gate And Documentation

**Files:**
- Modify: `README.md`
- Create: `docs/test-evidence/ali-cross-database-175-case-report.md`
- Create: `.data/evidence/real-campaign/all-final-175-<revision>.json`

**Interfaces:**
- Consumes: verified 169-case report and six successful XDB results.
- Produces: one report whose IDs exactly equal the 175-case catalog.

- [ ] Merge current-revision XDB results into the verified 169-case baseline.
- [ ] Validate counts, classifications, catalog identity and average duration.
- [ ] Update current README gate wording and write the evidence report.
- [ ] Commit documentation.

### Task 5: Final Verification And Publication

**Files:**
- Modify: no source files unless verification exposes a defect.

**Interfaces:**
- Produces: tested branch and public GitHub publication.

- [ ] Run the full Python suite and frontend test/build checks.
- [ ] Verify deployed health, page, sandbox takeover/recovery and command execution.
- [ ] Build/restart any pending immutable service release and verify health.
- [ ] Remove obsolete test data, releases, caches and images while retaining selected rollback material.
- [ ] Push `sandbox-harness-platform-parallel-ali` to the public GitHub remote.

