# Sandbox Protocol v2 - Stage 1 Report

## Scope

Stage 1 establishes provider-neutral value objects, behavioral capabilities, and the v2 backend lifecycle contract. It does not change deployment, cloud resources, or the active provider.

Implementation commit: `e7e8305bbb9ddd09b623ab3399401e73b6f51f4b`

## Design

- Introduced immutable `SandboxHandle(provider, sandbox_id, allocation_id, profile_key, revision)`.
- Introduced validated `SandboxRuntimeRoute(gateway_url, proxy_url)`.
- Replaced implementation-shaped capabilities with behavioral capabilities: native idempotency, scoped inventory, TTL renewal, arbitrary ports, authenticated runtime HTTP, delete convergence, and pause/resume.
- Replaced public metadata mutation/filter operations with `create`, `get`, `reconcile_create`, `list_owned`, `renew`, `endpoint`, `delete`, and `runtime_route`.
- Replaced request-level business metadata and `idempotency_key` with the provider-neutral `allocation_id`.

## Development

- Migrated Fake, OpenSandbox, and Aliyun backends to the v2 lifecycle surface.
- Migrated Pool, Broker orphan cleanup, media cleanup, Inspector, and the real-provider protocol runner away from public metadata lookup/update APIs.
- Fake now implements one-sandbox-per-allocation semantics and rejects profile conflicts.
- OpenSandbox derives its bounded provider request ID from a SHA-256 digest of the complete allocation ID; full allocation identity remains available for scoped reconciliation.
- Aliyun deletion now converges on exact-ID absence rather than inventory-list absence.
- Provider-owned labels remain internal adapter details and are no longer part of the business contract.

## Migration And Rollback

- This stage contains transitional `get(str)` and legacy create overloads so later stages can migrate durable ownership without an all-at-once cutover.
- Rollback is the single implementation commit above; no schema or cloud rollback is required.
- No deployment, backend switch, template creation, or cloud mutation was performed.

## Tests

- Initial focused protocol/lifecycle suite: `76 passed`.
- Post-review focused suite, including new idempotency, collision, and delete-convergence cases: `87 passed` except one outdated delete mock; after updating the mock, the exact affected set passed `3 passed`.
- Full suite after migration: `1057 passed, 5 skipped`; one unrelated SQLite setup test failed once with `sqlite3.OperationalError: disk I/O error` under `.data/pytest` on `/data2`.
- The environmental failure was rerun alone and passed together with the affected delete tests: `3 passed`.
- `git diff --check`: passed.
- Storage verification: test and repository state remained under `/data2`; root filesystem was not used for generated project data.

## Review

Independent review found no Critical issues and six Important issues.

Closed in Stage 1:

1. Fake capability/behavior mismatch for native idempotency.
2. Aliyun false delete convergence on transient inventory absence.
3. OpenSandbox 64-character prefix collision in provider request IDs.

Tracked for later stages:

1. Complete Handle reconstruction and fence validation: Stage 2.
2. Removal of raw-ID production call paths: Stage 2 and Stage 3.
3. Receipt-less media operation cleanup using durable ownership instead of transient `turn_id`: Stage 3.

## Residual Risks

- Until Stage 2, a Handle is not yet durably stored and adapters retain a transitional raw-ID bridge.
- Until Stage 3, job and operation cleanup does not yet have its final durable Handle ownership path.
- The full-suite SQLite disk I/O event was transient, but `/data2` was at 95% utilization with 278 GB available; subsequent stages continue to keep all test paths on `/data2` and record any recurrence.
