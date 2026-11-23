# Sandbox Protocol v2 Implementation Plan

**Goal:** Make provider identity, recovery, profiles and Runtime routing fully
provider-neutral while preserving current OpenSandbox behavior.

**Design:** `docs/superpowers/specs/2026-11-23-sandbox-protocol-v2-design.md`

## Task 1: Value objects and behavioral capabilities

**Files:** `sandbox_contracts.py`, provider adapters, contract tests

1. Add failing tests for `SandboxHandle`, `SandboxRuntimeRoute` and capability gate.
2. Replace metadata/idempotency request fields with `allocation_id`.
3. Add backend `reconcile_create`, `list_owned` and `runtime_route` operations.
4. Keep provider metadata private to adapters.
5. Run contract and adapter suites; commit.

## Task 2: Durable Handle persistence

**Files:** `domain.py`, `store.py`, `broker.py`, store/broker recovery tests

1. Add `sandbox_handle_json` to Turns with SQLite/PostgreSQL schema upgrade.
2. Persist a complete Handle before Runtime injection.
3. Recover, cancel and clean by Handle provider plus ID.
4. Reject incomplete new nonterminal allocations.
5. Run Store/Broker/cancel/recovery suites; commit.

## Task 3: Job, cleanup and Inspector handles

**Files:** `harness_job.py`, `job_worker.py`, `store.py`, `sandbox_inspector.py`

1. Put full Handle in provisional/final Job receipts and cleanup obligations.
2. Add Handle JSON to sandbox controls and port grants.
3. Dispatch exact get/delete/endpoint through provider identity.
4. Prove worker death, terminal cleanup and reservation ownership.
5. Run Job/Inspector/scheduler suites; commit.

## Task 4: Backend Registry

**Files:** `sandbox_factory.py`, `pool.py`, app/job-service assembly

1. Add a registry with one active provider and provider dispatch.
2. Move Pool get/adopt/retire paths to Handle dispatch.
3. Preserve the no-provider-switch-with-active-work invariant.
4. Test two fake providers and wrong-provider IDs; commit.

## Task 5: Profile Catalog schema v2

**Files:** `profiles.py`, coordinator/scheduler, deploy materialization, catalogs

1. Add failing schema/selection tests for provider releases.
2. Generate OpenSandbox provider releases from the current immutable images.
3. Replace image suffix recovery checks with Handle profile/revision checks.
4. Update catalog digest/materialization and deployment isolation tests.
5. Run catalog/coordinator/job/image suites; commit.

## Task 6: Provider Runtime Route

**Files:** contracts, adapters, Broker, Child Harness, Settings/deploy

1. Add backend-resolved Gateway/proxy route.
2. Remove direct Settings Gateway injection from execution paths.
3. Add explicit OpenSandbox and Ali route configuration without wildcard bind.
4. Test route isolation and capability injection; commit.

## Task 7: Shared conformance and release evidence

**Files:** provider contract tests, real runners, development docs

1. Parameterize the deterministic conformance suite across Fake/Open/Ali clients.
2. Add crash, cancellation, endpoint refresh and cleanup fault cases.
3. Run full Python suite and OpenSandbox real provider protocol.
4. Run K campaign before any Ali backend switch.
5. Request fresh review and commit final evidence/docs.

## Cloud Gate (after manual success only)

1. Confirm ACR VPC binding/ENI/security-group 443.
2. Run the minimal dry-run-reviewed request manually.
3. Only after READY, codify the Ali provisioner and provider release manifest.
4. Keep Ali warm targets zero through protocol and K canaries.
