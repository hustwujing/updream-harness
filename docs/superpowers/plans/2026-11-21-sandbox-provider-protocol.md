# Sandbox Provider Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a provider-neutral sandbox lifecycle protocol, migrate OpenSandbox to it without behavioral regression, add four abnormal real-machine protocol cases, pass K01-K14 and release the Ali deployment.

**Architecture:** Keep the existing signed `sandbox-harness-runtime` HTTP protocol. New lifecycle request/result objects represent provider profile, idempotency, metadata, TTL and endpoint access; OpenSandbox becomes the first adapter. The pool and runtime depend on those contracts while provider inventory remains recovery support.

**Tech Stack:** Python 3.13, asyncio, httpx, Pydantic settings, PostgreSQL, OpenSandbox, pytest, existing real campaign runner and transient systemd deployment.

**Spec:** `docs/superpowers/specs/2026-11-21-sandbox-provider-protocol-design.md`

## Global Constraints

- Preserve all application state under `/data2/wujing03/sandbox-harness-platform-parallel-ali`.
- Do not modify the original Parallel checkout, shared OpenSandbox service, registry, Redis or object storage.
- Do not implement Alibaba or Tencent credentials/API calls in this phase.
- Keep runtime protocol, durable Turn/Job semantics and public APIs behaviorally compatible.
- Use TDD for every production change and commit coherent stages.
- Real cases use unique ownership metadata and delete only their own sandboxes.

## Review Focus

- Transport failure after provider acceptance must reconcile exactly one sandbox and never duplicate creation.
- A provider-deleted attached sandbox must release the local capacity slot and not poison thread reuse.
- Expired runtime endpoints must refresh without replacing a healthy sandbox.
- Repeated delete must be idempotent while real provider errors remain visible.
- Provider profile differences must prevent cross-profile thread reuse even when Harness matches.

---

### Task 1: Provider-Neutral Contracts

**Files:**
- Create: `src/sandbox_harness/sandbox_contracts.py`
- Test: `tests/test_sandbox_contracts.py`

**Interfaces:**
- Produces: `SandboxProfile`, `SandboxCreateRequest`, `SandboxUpdate`, `SandboxEndpoint`, `Sandbox`, `SandboxCapabilities`, `SandboxBackend` and typed provider errors.

- [ ] Write contract tests for immutable profile compatibility, request identity, bounded metadata/provider diagnostics, endpoint expiry/redaction and typed error metadata.
- [ ] Run the tests and verify they fail because the module is absent.
- [ ] Implement the value objects and Protocol with `create(request)`, `get`, `find`, `list`, `update`, `renew`, `delete`, `endpoint(force_refresh)` and `aclose`.
- [ ] Run focused tests and static import checks.
- [ ] Commit `feat: define sandbox provider protocol`.

### Task 2: OpenSandbox Adapter

**Files:**
- Create: `src/sandbox_harness/sandbox_opensandbox.py`
- Modify: `src/sandbox_harness/sandbox.py`
- Modify: `tests/test_sandbox_namespace.py`
- Create: `tests/test_sandbox_provider_contract.py`

**Interfaces:**
- Consumes: Task 1 contracts.
- Produces: `OpenSandboxBackend` implementing the provider-neutral protocol and compatibility exports for existing imports.

- [ ] Add a named shared conformance matrix for create/get/update/delete/endpoint, metadata patch/removal, exact filtering, pagination, idempotency, client shutdown and native-error mapping.
- [ ] Verify the tests fail against the current positional create and OpenSandbox-specific result model.
- [ ] Move HTTP mapping into `sandbox_opensandbox.py`, map profile references to immutable image details and make the request idempotency key explicit.
- [ ] Retain temporary legacy properties/method overloads only where existing external tests require them; mark them outside the Protocol.
- [ ] Run namespace, cancellation and contract tests.
- [ ] Commit `refactor: adapt opensandbox to provider protocol`.

### Task 3: Pool, Runtime And Factory Integration

**Files:**
- Create: `src/sandbox_harness/sandbox_factory.py`
- Modify: `src/sandbox_harness/pool.py`
- Modify: `src/sandbox_harness/runtime.py`
- Modify: `src/sandbox_harness/app.py`
- Modify: `src/sandbox_harness/job_service.py`
- Modify: `src/sandbox_harness/config.py`
- Test: `tests/test_sandbox_namespace.py`
- Test: `tests/test_runtime_client.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: Task 1 protocol and Task 2 adapter.
- Produces: one `build_sandbox_backend(settings)` construction path; `SandboxProfileResolver`; pool requests based on resolved profiles; RuntimeClient consuming `SandboxEndpoint`.

- [ ] Add failing tests proving both API and Job Service use the same factory, frozen image overrides resolve to exact OpenSandbox profiles, compatibility mismatch blocks reuse, and endpoint refresh does not renew TTL implicitly.
- [ ] Replace positional backend creation with request objects and `SandboxUpdate` in the pool.
- [ ] Resolve endpoint access through the backend-owned, skewed/coalesced endpoint cache while retaining no-replay runtime behavior.
- [ ] Centralize backend construction and production capability validation.
- [ ] Run broker, pool, runtime, inspector and Child Harness focused suites.
- [ ] Commit `refactor: consume provider-neutral sandbox lifecycle`.

### Task 4: Remove Vendor Naming From Shared Surfaces

**Files:**
- Modify: `src/sandbox_harness/harness_job.py`
- Modify: `src/sandbox_harness/execution_graph.py`
- Modify: `src/sandbox_harness/app.py`
- Modify: `src/sandbox_harness/config.py`
- Modify: affected browser and execution graph tests.

**Interfaces:**
- Produces: neutral shared labels (`sandbox`, `Cloud Sandbox`) while adapter diagnostics retain `opensandbox` as provider.

- [ ] Add failing assertions that common execution graph and health supervision text is provider-neutral.
- [ ] Rename the Child backend and shared log/UI labels without changing API shape beyond label values.
- [ ] Run execution graph, browser and config tests.
- [ ] Commit `refactor: make shared sandbox surfaces provider neutral`.

### Task 5: Abnormal Real-Machine Protocol Cases

**Files:**
- Create: `tests/run_real_sandbox_provider_protocol.py`
- Create: `tests/test_real_sandbox_provider_protocol.py`
- Modify: `docs/development/testing-and-real-cases.md`

**Interfaces:**
- Consumes: deployed OpenSandbox adapter.
- Produces: cases `SP01`–`SP04` with JSON evidence under `.data/evidence/sandbox-provider/`.

- [ ] Add offline runner contract tests for case catalog, unique metadata, cleanup ownership and evidence fields.
- [ ] Implement SP01 using a forwarding response-drop transport, SP02 external deletion plus capacity reacquire, SP03 forced endpoint-cache expiry with real refresh and SP04 repeated delete.
- [ ] Run offline tests, then execute each real case individually and retain first-failure evidence.
- [ ] Rerun the complete SP01–SP04 set cleanly on one revision.
- [ ] Commit `test: add real sandbox provider fault cases`.

### Task 6: K Campaign And Release

**Files:**
- Modify only evidence/report documentation required by observed results.
- Generated ignored evidence: `.data/evidence/real-campaign/*`, `.data/evidence/sandbox-provider/*`.

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: released Ali deployment with exact revision and evidence receipts.

- [ ] Run focused sandbox/pool/runtime/broker suites and the complete Python suite.
- [ ] Run frontend tests and production build.
- [ ] Commit current documentation and require a clean worktree.
- [ ] Build and record release bundles if deployable runtime content changed.
- [ ] Preflight zero active Turns, deploy only affected Ali transient services and verify health.
- [ ] Run `python tests/run_real_sandbox_provider_protocol.py --base-url http://127.0.0.1:24767 --cases SP01,SP02,SP03,SP04 --report .data/evidence/sandbox-provider/release.json` against the deployed revision.
- [ ] Run `python tests/run_real_campaign.py --base-url http://127.0.0.1:24767 --case K01 --case K02 --case K03 --case K04 --case K05 --case K06 --case K07 --case K08 --case K09 --case K10 --case K11 --case K12 --case K13 --case K14 --report .data/evidence/real-campaign/provider-protocol-k-clean.json`; on failure, fix the shared boundary, rerun affected cases, then restart the clean matrix.
- [ ] Run `python tests/gate.py` and retain its receipt, then verify direct plus FRPC ingress.
- [ ] Push only if a configured remote branch exists and the gate receipt matches the deployed revision.
- [ ] Record commit, deployed revision, bundle digests, image digests, SP evidence and K campaign report.
