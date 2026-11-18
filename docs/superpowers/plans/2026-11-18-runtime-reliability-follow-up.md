# Runtime Reliability Follow-up Implementation Plan

> **For agentic workers:** Execute each task with focused red/green tests and preserve the current JobGroup/Job/JobAttempt authority boundaries.

**Goal:** Keep semantic decomposition and Skill choice with the Primary while making metadata honest and moving retry, authorization reuse, GPU health and lifecycle correctness into deterministic host behavior.

**Architecture:** Do not add ExecutionPlan or programmatic Skill routing. Retain existing compact delegation and exact JobGroup paths. Skill metadata supplies provenance, dependencies and permission requirements; task output remains authoritative in `JobRequest.expected_outputs`. Host services own infrastructure retry, scope comparison and resource health.

**Tech Stack:** Python 3.13, Pydantic, SQLAlchemy/PostgreSQL, FastAPI, systemd, bubblewrap, CUDA, OpenSandbox, pytest and real campaign drivers

**Spec:** `docs/superpowers/specs/2026-11-18-input-routing-delegation-and-ppt-design.md`

## Global Constraints

- No host-generated business DAG and no required ExecutionPlan.
- No case-specific PDF, PPT or dolphin routing rules.
- JobGroup, Job and JobAttempt remain authoritative.
- Primary prompt guidance is best effort; retry, authorization and health are deterministic.
- Existing worktree changes and `/data2` storage boundaries are preserved.

### Task 1: Remove Unused Skill Delegation Metadata

**Files:** `runtime/skill_bundle.py`, `runtime/skills/metadata.json`, generated manifest, Profile/Catalog models, Skill tests and current development documentation.

- [x] Remove `DelegationMetadata` and every per-Skill `delegation` row.
- [x] Stop exposing delegation hints through the execution catalog.
- [x] Keep compact delegation behavior unchanged and `JobRequest.expected_outputs` authoritative.
- [x] Regenerate and verify the signed Skill manifest; run Profile/Catalog/Skill tests.

### Task 2: Prompt-guided Parallel Decomposition

**Files:** Runtime instructions, delegation guidance and decision-layer campaign cases.

- [ ] Add one generic rule to start ready independent durable work before long direct work.
- [ ] Keep JobGroup tools available; do not introduce a planning schema or keyword classifier.
- [ ] Add telemetry that distinguishes an autonomous Primary delegation decision from direct compiler tests.
- [ ] Add a user-message-driven real case proving independent work overlaps in time.

### Task 3: Deterministic Infrastructure Retry

**Files:** Job error contracts, controlled/normal workers, Store attempt transitions, continuation guidance and tests.

- [ ] Define stable failure phase, retry class, retryability and node-health generation fields.
- [ ] Retry an unchanged request as another JobAttempt, never as an LLM-created replacement.
- [ ] Prevent semantic parameter changes when guidance identifies infrastructure failure.
- [ ] Bound attempts and terminalize every non-retryable or exhausted path.

### Task 4: Authorization Scope Inheritance

**Files:** authorization snapshots, JobCoordinator replacement validation, confirmation API/UI and PostgreSQL race tests.

- [ ] Freeze operation/capability, Artifact, output, network, resource and budget scope.
- [ ] Reuse confirmation for unchanged attempts and replacement requests contained by prior scope.
- [ ] Require a new confirmation for every scope expansion.
- [ ] Preserve replacement lineage and concurrent confirmation safety.

### Task 5: Controlled GPU Health Boundary

**Files:** controlled runner/worker, node inventory, systemd unit, observability and real GPU probes.

- [ ] Reproduce CUDA access with the exact production bubblewrap/UID/device/library boundary.
- [ ] Fix GPU exposure and add a minimal allocation probe per advertised GPU.
- [ ] Suppress unhealthy GPU capacity until a new successful health generation.
- [ ] Map runtime CUDA unavailability separately from process startup failure and retain bounded diagnostics.

### Task 6: Decision-layer Real Campaign

**Files:** real case catalog, protocol/Primary drivers, evidence reports and browser checks.

- [ ] Verify Primary-initiated parallel decomposition, not only compiler correctness.
- [ ] Verify prompt-guided Skill choice and adherence to the selected Skill workflow.
- [ ] Verify 429 after durable submission still closes through continuation.
- [ ] Verify infrastructure failure creates no duplicate authorization or speculative replacement.
- [ ] Verify a real GPU-backed transparent output and complete cleanup.

### Task 7: Release Verification

- [ ] Run focused tests, the full Python suite, frontend build and affected browser tests.
- [ ] Build only the changed Code/Content Bundles unless native/systemd changes require an image or unit rollout.
- [ ] Deploy from a clean release revision after confirming no active work.
- [ ] Record release, Bundle/Profile/image digests, real-case receipts and zero residual active work.
