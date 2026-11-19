# Capability Names And Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split Provider capabilities by truthful media action and add eleven complex real-machine cases that cover every Skill, Operation, Provider adapter and Provider mode.

**Architecture:** Job Catalog descriptors freeze semantic action boundaries while Media Catalog entries select concrete providers and models inside those boundaries. The coverage catalog declares exact Skill, Operation and Provider-mode sets, and real protocol drivers verify jobs, overlap, artifacts, authorization and continuation evidence.

**Tech Stack:** Python, FastAPI, PostgreSQL, OpenSandbox, signed Runtime Skill bundles, React, durable JobGroups and Media Providers.

**Spec:** `docs/superpowers/specs/2026-11-19-capability-names-and-coverage-design.md`

## Global Constraints

- Capability IDs describe business actions, never vendors or models.
- `image.generate` accepts image requests only.
- No database migration or backwards-compatibility alias.
- No case-specific production logic.
- All mutable data and evidence remain under `/data2/wujing03`.
- Expensive Provider calls require explicit whole-group confirmation.

## Review Focus

- A video request using `image.generate` must fail before Draft or Provider work.
- A mode outside its semantic capability must fail even when the selected model supports it.
- Child Skill grants must not silently gain unrelated Provider capabilities.
- Provider recovery must retain one external execution identity across Worker loss.
- Coverage must fail when a future Skill, Operation, adapter or generation mode is added without a case.

---

### Task 1: Semantic Provider Descriptors

**Files:**
- Modify: `deploy/job-catalog.example.json`
- Modify: `src/sandbox_harness/job_service.py`
- Modify: `deploy/profile-catalog.example.json`
- Test: `tests/test_job_catalog.py`
- Test: `tests/test_job_coordinator.py`
- Test: `tests/test_profile_resolver.py`

- [ ] Add failing tests for the six exact capability IDs and wrong kind/mode rejection.
- [ ] Add the five new descriptors and restrict `image.generate` to image modes.
- [ ] Register all six descriptors to `MediaProviderJobAdapter` and update frozen Profiles.
- [ ] Run catalog, coordinator, profile and Adapter tests.
- [ ] Commit the semantic capability boundary.

### Task 2: Runtime And Gateway Capability Enforcement

**Files:**
- Modify: `src/sandbox_harness/media_job_adapter.py`
- Modify: `src/sandbox_runtime/instructions.py`
- Modify: `runtime/skills/codex-ppt/SKILL.md`
- Modify: `runtime/skills/metadata.json`
- Test: `tests/test_media_job_adapter.py`
- Test: `tests/test_job_cli_delegation.py`
- Test: `tests/test_grants.py`

- [ ] Add failing tests that bind descriptor ID to kind and mode.
- [ ] Enforce capability semantics before creating any Media Draft.
- [ ] Update Runtime guidance and signed Skill metadata without granting unrelated capabilities.
- [ ] Freeze and verify the signed Skill bundle.
- [ ] Commit Runtime and Gateway enforcement.

### Task 3: Exhaustive Coverage Catalog

**Files:**
- Modify: `scripts/build_real_case_catalog.py`
- Modify: `tests/real_cases/cases.json`
- Modify: `tests/test_real_case_catalog.py`
- Create: `tests/test_capability_coverage_catalog.py`

- [ ] Add failing set-equality tests for 33 Skills, 7 Operations, 6 adapters and all enabled modes.
- [ ] Add `S01`-`S06`, `O01`-`O02`, `V01`-`V03` declarations with exact coverage metadata.
- [ ] Regenerate the deterministic catalog and pass coverage tests.
- [ ] Commit catalog coverage.

### Task 4: Skill And Operation Protocol Drivers

**Files:**
- Modify: `tests/run_real_parallel_protocol.py`
- Test: `tests/test_real_campaign_runner.py`

- [ ] Add deterministic helpers for Skill jobs, frozen inputs, output contracts and attempt-overlap evidence.
- [ ] Implement `S01`-`S06` with isolated Child jobs and artifact validation.
- [ ] Implement `O01` and `O02` with exact Operation IDs and staged Artifact handoff.
- [ ] Run protocol static tests and focused fake-backend tests.
- [ ] Commit Skill and Operation drivers.

### Task 5: Provider Protocol Drivers

**Files:**
- Modify: `tests/run_real_parallel_protocol.py`
- Modify: `tests/run_real_campaign.py`
- Test: `tests/test_real_campaign_runner.py`

- [ ] Implement `V01` with every enabled adapter and required reference modes.
- [ ] Implement `V02` with all remaining video modes.
- [ ] Implement two-stage `V03` with every 3D mode.
- [ ] Assert confirmation, unique submission identity, mode, adapter, artifact and recovery evidence.
- [ ] Commit Provider drivers.

### Task 6: Focused Verification And Deployment

- [ ] Run focused Python, Skill bundle, frontend and browser tests.
- [ ] Commit, record the release and deploy the isolated services.
- [ ] Verify health, release digest and service storage roots.

### Task 7: Real-Machine Campaign

- [ ] Run required cases `S01`-`S06`, `O01`-`O02`, `V01`, stopping on program or optimizable prompt defects.
- [ ] Fix only general defects and rerun the failed plus affected cases.
- [ ] Run expensive `V02` and `V03`; classify external outages separately.
- [ ] Repeat affected cases until no program or clearly optimizable prompt defect remains.

### Task 8: Final Gate And Publication

- [ ] Run one complete non-browser gate and one complete browser gate.
- [ ] Record and deploy the final release.
- [ ] Push the branch and publish the coverage/evidence report.
