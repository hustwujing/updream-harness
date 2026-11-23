# Aliyun Sandbox Backend Implementation Plan

**Goal:** Implement a fail-closed Alibaba Cloud Agent Sandbox Express adapter
behind the provider-neutral lifecycle protocol without changing the deployed
backend.

**Design:** `docs/superpowers/specs/2026-11-23-aliyun-sandbox-backend-design.md`

## Task 1: Lock the capability gap with tests

**Files:** `tests/test_sandbox_aliyun.py`, `tests/test_sandbox_factory.py`

1. Add an in-memory fake E2B transport boundary.
2. Write failing lifecycle tests for create, inventory, TTL, endpoint and delete.
3. Write a failing test proving real metadata mutation is rejected.
4. Run the focused tests and record the expected import/factory failures.

## Task 2: Implement the adapter

**Files:** `src/sandbox_harness/sandbox_aliyun.py`, `src/sandbox_harness/sandbox.py`

1. Add a small E2B SDK boundary so unit tests do not call the cloud.
2. Map profiles, metadata, pagination, states and typed errors.
3. Reconcile uncertain creates by immutable idempotency metadata.
4. Resolve authenticated HTTPS endpoints without exposing tokens.
5. Run the focused adapter tests until green.

## Task 3: Wire configuration and dependency

**Files:** `src/sandbox_harness/config.py`, `src/sandbox_harness/sandbox_factory.py`,
`pyproject.toml`, `uv.lock`, `tests/test_sandbox_factory.py`

1. Add explicit Aliyun API URL/domain/key configuration.
2. Add the pinned E2B SDK dependency and refresh the lock file.
3. Construct Aliyun only when selected and retain all mandatory capability gates.
4. Verify Fake and OpenSandbox construction remains unchanged.

## Task 4: Add immutable-metadata Pool behavior

**Files:** `src/sandbox_harness/sandbox_contracts.py`, `src/sandbox_harness/pool.py`,
`tests/test_sandbox_contracts.py`, `tests/test_sandbox_factory.py`

1. Make metadata mutation an optional capability rather than a primary gate.
2. Claim application warm instances by durable ID without provider metadata mutation.
3. Reuse an attached immutable-metadata Sandbox for the same Thread by renewing
   TTL and updating platform state.
4. Prove OpenSandbox mutable-metadata reuse remains unchanged.

## Task 5: Verify the regression boundary

**Files:** all touched files

1. Run adapter, contracts, factory, pool and namespace tests.
2. Run the full Python suite.
3. Inspect the diff for secrets and unrelated deployment changes.
4. Request a fresh code review and address findings.
5. Commit the verified implementation on
   `sandbox-harness-platform-parallel-ali`; do not deploy or switch backend.
