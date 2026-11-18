# General Delegation And Skill Metadata Implementation Plan

> **For agentic workers:** Execute tasks in order with focused red/green tests. Do not commit from the shared dirty worktree unless explicitly requested.

**Goal:** Make appended-input state truthful, expose a compact generic Child Harness delegation command over the durable Job platform, and freeze complete metadata for every retained Runtime Skill.

**Architecture:** Preserve the existing `JobGroupCreate` protocol and compile a smaller metadata-independent task schema into it inside the Runtime CLI. Keep business decomposition in the Primary Harness and all authorization/durability enforcement in the host. Freeze explicit Skill metadata in the signed content manifest and use it only to narrow routing and runtime requirements.

**Tech Stack:** Python 3.13, Pydantic, SQLAlchemy/PostgreSQL, FastAPI, React/Vitest, signed Runtime bundles

**Spec:** `docs/superpowers/specs/2026-11-18-input-routing-delegation-and-ppt-design.md`

## Global Constraints

- Preserve unrelated working-tree changes.
- PostgreSQL remains authoritative; Redis is notification/replay only.
- The host must not classify business intent or create a task DAG.
- Child Harnesses cannot delegate.
- No Skill prose grants network, Provider, GPU, filesystem, budget or credential authority.
- `pptx` is removed without an old-Skill compatibility path.
- Generated state and test output remain under `.data` on `/data2`.

## Review Focus

- A delivered input with a completion receipt followed by Turn failure remains applied.
- A delivered input without a completion receipt becomes unknown, not falsely applied or failed.
- Compact delegation cannot select foreign/non-frozen Artifacts or widen Child Profile authority.
- Missing Skill metadata does not block generic delegation and never grants missing capabilities.
- Removed `pptx` cannot remain reachable through a Profile, bundle manifest, checkpoint materialization or prompt.

---

### Task 1: Monotonic Appended Input State

**Files:**
- Modify: `src/sandbox_harness/broker.py`
- Modify: `src/sandbox_harness/store.py`
- Modify: `src/sandbox_harness/domain.py`
- Test: `tests/test_runtime_boundaries.py`
- Test: `tests/test_store.py`
- Test: affected browser input-status tests

**Interfaces:**
- Produces: monotonic `TurnInput.status` transitions and separate Turn failure presentation.

- [ ] Add failing tests for applied input followed by failed Runtime result, delivered input without completion, and delivery failure before write.
- [ ] Make Runtime input-completion receipts mark their listed appended inputs applied independently of result error.
- [ ] Terminalize pending inputs as failed and unresolved delivered inputs as unknown without regressing applied rows.
- [ ] Update public Items/API and browser copy to separate input processing from Turn outcome.
- [ ] Run focused Store, Broker and browser tests.

### Task 2: Model Error Normalization

**Files:**
- Modify: Runtime adapter/result normalization modules identified by the failing test.
- Test: `tests/test_runtime_boundaries.py`
- Test: relevant adapter/Gateway tests

**Interfaces:**
- Produces: `normalize_runtime_error_code(value: object) -> str` or the established equivalent at the adapter boundary.

- [ ] Add a failing test with an upstream HTTP 429 object in `error_code`.
- [ ] Normalize string/dict/list/exception-shaped errors to bounded stable codes before `RuntimeResult` validation.
- [ ] Preserve only allowlisted structural diagnostics and map 429 to `model_rate_limited`.
- [ ] Run focused Runtime/Gateway tests.

### Task 3: Compact Generic Delegation Compiler

**Files:**
- Modify: `src/sandbox_runtime/job_cli.py`
- Modify: `src/sandbox_harness/catalog.py`
- Modify: `src/sandbox_runtime/instructions.py`
- Test: `tests/test_job_api.py`
- Test: `tests/test_job_catalog.py`
- Test: `tests/test_runtime_bundle.py`

**Interfaces:**
- Produces: `DelegationTaskSpec`, `DelegationGroupSpec`, and deterministic compilation to existing `JobGroupCreate`.
- Consumes: `/v1/execution/catalog`, Runtime identity environment and frozen `input_artifacts`.

- [ ] Add failing CLI tests for metadata-free task compilation, environment binding, Artifact path/ID resolution, deterministic keys and invalid inputs.
- [ ] Add a `sandbox-job delegate --request JSON_OR_PATH` command with a compact task schema.
- [ ] Expose Child Profile allowed Skill IDs and all frozen template fields needed by the compiler.
- [ ] Compile to `child-general`, inherited Harness/model, generic `child_deliverables` candidate contract and bounded defaults.
- [ ] Keep `prepare` unchanged as the low-level exact interface and submit compiled groups through the same endpoint.
- [ ] Shorten/highlight generic delegation guidance without requiring delegation for every task.
- [ ] Run focused CLI/catalog/bundle tests.

### Task 4: Complete Skill Metadata Schema

**Files:**
- Modify: `runtime/skill_bundle.py`
- Modify: `runtime/skills/sources.json`
- Regenerate: `runtime/skills/manifest.json`
- Modify: `src/sandbox_harness/profiles.py`
- Modify: `src/sandbox_harness/catalog.py`
- Test: add or extend Runtime Skill and catalog tests

**Interfaces:**
- Produces: validated frozen routing, dependency, network, capability and GPU metadata for every retained Skill. Skill-level delegation/result hints were later removed because task-level `expected_outputs` is authoritative.

- [ ] Add failing schema tests for missing fields, unknown fields, invalid supersedes references/cycles and policy widening.
- [ ] Implement typed validation for source metadata and dependency profiles in `skill_bundle.py`.
- [ ] Fill explicit complete metadata for every retained Skill in `sources.json`.
- [ ] Freeze the manifest and update ProfileResolver/catalog to consume frozen fields rather than hard-coded defaults.
- [ ] Verify generic delegation still works for a Skill with no routing/default-result optimization.
- [ ] Run bundle/profile/catalog tests and `runtime/skill_bundle.py verify`.

### Task 5: Retire Generic PPTX And Connect Codex PPT

**Files:**
- Delete: `runtime/skills/pptx/`
- Modify: `runtime/skills/sources.json`
- Modify: Profile catalog sources and generated manifests containing `pptx`
- Modify: `runtime/skills/codex-ppt/SKILL.md`
- Modify: relevant `codex-ppt/docs/` and worker prompt
- Modify: `docs/skills.md`
- Test: Runtime bundle, profile and image contract tests

**Interfaces:**
- Produces: one PPT creation Skill (`codex-ppt`) whose leaf-worker instructions use `sandbox-job` durable Jobs.

- [ ] Add failing assertions that `pptx` is retired, absent from the bundle and absent from every allowed Skill list.
- [ ] Remove the tracked `pptx` Skill and catalog/profile references; record the retired identity.
- [ ] Rewrite `codex-ppt` subagent instructions to use `sandbox-job catalog/delegate/group/job`, with Primary-only delegation and leaf slide workers.
- [ ] Preserve underlying PPT libraries as Runtime dependencies unless a separate dependency check proves they are unused.
- [ ] Run Skill bundle/profile/image contract tests.

### Task 6: Persisted Public Terminology

**Files:**
- Modify: execution graph/activity projection modules as required.
- Modify: `web/src/` transcript/execution graph components.
- Test: execution graph/API tests and Vitest/browser tests.

**Interfaces:**
- Consumes: persisted Tool Items, Job Groups, Jobs and Attempts.
- Produces: distinct public labels for tool concurrency, Child Harness and Job Group execution.

- [ ] Add failing projection/UI tests that model prose alone cannot create Child/Group status.
- [ ] Project labels only from authoritative entity types and keep assistant prose as transcript content.
- [ ] Display message processing and Turn outcome independently.
- [ ] Run focused API/Vitest/browser tests at desktop and mobile sizes.

### Task 7: Integrated Verification And Documentation

**Files:**
- Modify: `docs/development/add-agent-skill.md`
- Modify: `docs/development/skill-and-image-lifecycle.md`
- Modify: `docs/development/add-runtime-tool.md`
- Modify: `docs/skills.md`
- Add/modify: real-case driver and evidence documentation as required.

**Interfaces:**
- Produces: executable Skill intake/development rules and evidence for the complete change.

- [x] Document the metadata schema, provenance, routing versus authority, dependency profile selection, retirement and completion gates.
- [x] Add a general compact-delegation real protocol alongside the existing direct, tool-concurrency, fallback and input-ordering protocols; keep input/Turn presentation deterministic in browser/component tests.
- [ ] Run focused suites from Tasks 1-6, full Python tests, frontend tests/build and bundle verification.
- [ ] Build changed Runtime bundles/images only when dependency analysis requires it; otherwise verify signed Code/Content bundle paths.
- [ ] Run a real OpenSandbox regression with exact code/content/image digests and record undeployed/deployed status honestly.
