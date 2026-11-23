# Sandbox Protocol v2 - Stage 2 Report

## Scope And Design

Stage 2 makes `SandboxHandle` the durable Turn allocation identity. The existing indexed `sandbox_id` remains a projection; `sandbox_handle_json` is the authoritative provider, allocation, profile, and revision fence.

Implementation commit: `a60ef5a0fb4862452db21f781c7b6cd8a4d8aef4`

## Development

- Added `sandbox_handle_json` to the Turn ORM and internal Turn view.
- Added idempotent SQLite/PostgreSQL online schema upgrade logic.
- Persisted the complete Handle in the same state transition that records `sandbox_id`, before Runtime injection.
- Changed restart, cancellation, and recovery inventory to use the complete Handle.
- Rejected nonterminal durable sandbox references with missing or malformed Handles.
- Added provider/profile/allocation/revision fence validation in Fake, OpenSandbox, and Aliyun `get(handle)` paths.
- Added a provider-safe revision fingerprint so adapters can validate a Handle after process restart without treating provider metadata as business state.

## Migration And Rollback

- New databases create the column through ORM metadata; existing databases receive an idempotent `ALTER TABLE`.
- New nonterminal allocations require complete Handles. There is intentionally no legacy fallback because this deployment has not launched the v2 data model.
- Rollback requires reverting the implementation commit. The additive database column may remain unused safely.

## Tests

- Store, Broker recovery, cancellation, provider fence, and namespace suites: `120 passed`.
- Explicit coverage includes Handle JSON round-trip, restart reattachment, missing-Handle rejection, allocation mismatch rejection, and Aliyun profile reconstruction with the original revision.
- `git diff --check`: passed.

## Review Findings

- Closed the Stage 1 findings for incomplete Handle reconstruction and unvalidated provider/profile/allocation fences.
- Closed the production Turn recovery raw-ID path; Job and Inspector raw-ID paths are intentionally assigned to Stage 3.
- No deployment or cloud mutation was performed.

## Residual Risks

- Job receipts, sandbox controls, and port grants still require Handle persistence; Stage 3 owns that migration.
- Backend selection still uses a single active backend object; provider dispatch is Stage 4.
