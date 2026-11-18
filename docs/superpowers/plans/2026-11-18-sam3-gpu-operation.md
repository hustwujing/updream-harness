# SAM3 GPU Operation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the missing durable `image.segment` SAM3/ZIM GPU Operation and `audio.transcribe` offline ASR Operation so Harnesses can request recoverable controlled media work without receiving host model or device access.

**Architecture:** A root-only controlled Job Worker claims only signed `image.segment` and `audio.transcribe` adapters, reserves concrete CPU/GPU resources through the existing Scheduler, and executes already-downloaded Plus model runtimes inside fixed offline bwrap. All requests, attempts, budgets, artifacts, validation receipts, cleanup and continuation remain authoritative in Parallel PostgreSQL/ObjectStore; only immutable model programs and weights are reused read-only.

**Tech Stack:** Python 3.13, asyncio, SQLAlchemy/PostgreSQL, existing Durable Job/Artifact Gateway/Scheduler APIs, bwrap, NVIDIA runtime, SAM3, ZIM, systemd.

**Spec:** `docs/superpowers/specs/2026-11-16-primary-harness-durable-job-platform-design.md`

## Global Constraints

- All mutable state stays below `/data2/wujing03/sandbox-harness-platform-parallel/.data`.
- Provider model environments and weights may be reused read-only from a configured `/data2` runtime root; no root-filesystem downloads.
- The Primary Harness receives only a signed Job descriptor and scoped input Artifact IDs, never a GPU device, host path, credential or model path.
- The unprivileged general Job Worker must never claim `image.segment`; only the dedicated root GPU Worker may execute it.
- Submit/reconcile/cancel/cleanup remain idempotent and generation-fenced; a killed Worker must not duplicate an accepted execution.
- Product code must stay case-independent; `S09` is verification, not a routing special case.

## Review Focus

- Text selection returns no object: publish no artifact and return a precise `no_selection` terminal error with a bounded box/point retry path.
- Worker death before, during and after bwrap: recover completed files or safely retry local work without duplicate publication.
- GPU exhaustion or wrong GPU identity: remain in one infrastructure-wait Attempt and preserve business retry budget.
- Malformed selectors, unauthorized Artifact IDs and unsupported output contracts: reject before GPU/model execution.
- Cleanup after success, failure, cancellation and process death: release GPU reservation, process, nonce and temporary files exactly once.

---

### Task 0: Original Runtime Capability Parity

**Files:**
- Modify: `runtime/common.Dockerfile`
- Create: `runtime/original_compatibility_probe.py`
- Modify: `runtime/build-images.sh`
- Modify: `tests/test_runtime_image_contract.py`

**Interfaces:**
- Produces: a machine-checkable baseline covering the original image's commands, Python modules, fonts and browser/graphics libraries; every `primary-full-cpu` image must pass it.

- [ ] Write failing tests for the original baseline and the missing Chinese/Emoji/Liberation fonts plus EGL/GLES/Mesa/XAuth packages.
- [ ] Run the focused image-contract tests and verify failures.
- [ ] Add stable shared packages through domestic Debian mirrors and implement the executable compatibility probe.
- [ ] Build one changed `primary-full-cpu` development image and run the probe inside it.
- [ ] Commit: `feat(runtime): restore original image capability parity`.

### Task 1: Signed Operation Contract And Harness Discovery

**Files:**
- Modify: `deploy/job-catalog.example.json`
- Modify: `deploy/profile-catalog.example.json`
- Modify: `deploy/configure-local.py`
- Test: `tests/test_job_catalog.py`
- Test: `tests/test_deployment_isolation.py`

**Interfaces:**
- Produces: signed descriptor `image.segment@1`, adapter `image.segment`, input role `source`, output roles `foreground`, `mask`, `segmentation_report`, runtime profile `gpu-operation`, and one-GPU resource requirements.

- [ ] Write failing catalog tests for prompt/box/points selectors, source Artifact binding, bounded output roles, no network origins, one GPU, and `sam3` guidance.
- [ ] Run the focused catalog/deployment tests and verify the descriptor is absent.
- [ ] Add the descriptor/profile configuration and generated local configuration support.
- [ ] Run the focused tests and require all pass.
- [ ] Commit: `feat(gpu): register image segment operation`.

### Task 2: Durable SAM3/ZIM And ASR Adapters

**Files:**
- Create: `src/sandbox_harness/sam3_job.py`
- Create: `src/sandbox_harness/transcribe_job.py`
- Create: `src/sandbox_harness/controlled_media_adapter.py`
- Modify: `src/sandbox_harness/config.py`
- Test: `tests/test_sam3_adapter.py`
- Test: `tests/test_job_worker.py`
- Test: `tests/test_scheduler.py`

**Interfaces:**
- Consumes: `JobExecutionContext`, `Scheduler.reserve/hold/release`, Parallel `ObjectStore`, `ArtifactGateway`, and configured read-only SAM3 runtime root.
- Produces: `ControlledMediaOperationAdapter.submit/recover_submission/status/cancel/collect/cleanup_external/release_capacity` for both descriptor IDs and digest-bound artifacts/validation receipts.

- [ ] Write failing fake-runner tests for selector validation, transcription parameters, scoped input lookup, GPU/CPU reservation identity, both successful three-role collections, `no_selection`, process failure, cancellation and exact cleanup.
- [ ] Write worker-loss tests proving completed work is adopted and incomplete local work is safely retried without a second visible result.
- [ ] Run the adapter/worker/scheduler tests and verify failures.
- [ ] Port the fixed offline SAM3/ZIM bwrap job, replacing Plus Store/WorkUnit coupling with Parallel Job/Artifact interfaces and keeping all work directories under Parallel `.data`.
- [ ] Implement the adapter with deterministic idempotency, fenced GPU reservation and validator receipts for image size, alpha and nontrivial mask content.
- [ ] Run focused tests and require all pass.
- [ ] Commit: `feat(gpu): execute durable sam3 operations`.

### Task 3: Root GPU Worker And Recovery

**Files:**
- Create: `src/sandbox_harness/gpu_job_service.py`
- Modify: `pyproject.toml`
- Create: `deploy/systemd/sandbox-harness-parallel-gpu-worker.service`
- Modify: `deploy/install-units.sh`
- Modify: `deploy/configure-local.py`
- Test: `tests/test_job_service.py`
- Test: `tests/test_systemd_units.py`
- Test: `tests/test_deployment_isolation.py`

**Interfaces:**
- Produces: `sandbox-harness-parallel-gpu-worker` entrypoint/service claiming only `image.segment`; it uses the existing node inventory and never owns Primary/Provider/Harness adapters.

- [ ] Write failing service tests for adapter allowlist, root requirement, read-only runtime root, `/data2` mutable paths, graceful shutdown and takeover.
- [ ] Run focused service/systemd/deployment tests and verify failures.
- [ ] Implement the isolated GPU Job Worker and hardened systemd unit without broad network or filesystem access.
- [ ] Run focused tests and require all pass.
- [ ] Commit: `feat(gpu): add isolated gpu job worker`.

### Task 4: Real GPU Verification And Campaign Resume

**Files:**
- Create: `tests/run_real_sam3_operation.py`
- Modify: `docs/development/testing-and-real-cases.md`
- Modify: `.data/evidence/real-campaign/final-report.md` (generated evidence, not source contract)

**Interfaces:**
- Consumes: deployed descriptor, GPU Worker, existing `portrait.png`, and `S09/C05` campaign definitions.
- Produces: real evidence for text selection, SAM3 mask, ZIM foreground, Worker-kill recovery, cleanup and browser-visible delivery.

- [ ] Run unit/deployment verification and restart only the changed GPU/API catalog services.
- [ ] Execute a direct real `image.segment` smoke and verify mask is nontrivial, foreground has alpha, report binds the same source digest, and no GPU reservation remains.
- [ ] Kill the GPU Worker during one execution and verify takeover/retry stays bounded and publishes one collection.
- [ ] Rerun `S09`, then affected `C05/C20`; stop on any program or clearly optimizable prompt defect.
- [ ] Record evidence and commit: `test(gpu): verify real sam3 recovery`.

## Self-Review

- Spec coverage: implements the registered GPU Operation Worker and strict Harness grant required by the approved design; no direct-GPU Harness profile is introduced.
- Type consistency: all execution is a normal Durable Job using existing Adapter, Scheduler, Artifact and continuation interfaces.
- Failure coverage: no-selection, invalid input, GPU capacity, worker loss and cleanup each have a named test owner above.
- Scope: no general Capability framework or additional GPU model is introduced; those remain separate descriptors built on the same adapter/service pattern.
