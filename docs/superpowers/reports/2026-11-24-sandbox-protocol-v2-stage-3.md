# Sandbox Protocol v2 - Stage 3 Report

## Scope And Design

Stage 3 extends durable Handle ownership from Turns to Jobs, user sandbox controls, and port grants. Any operation that can outlive one process now stores provider identity and compatibility fences before execution or control is exposed.

Implementation commit: `35e6c714599f929e16ff24552d6659393cde4eca`

## Development

- Added complete `sandbox_handle` data to provisional and final child Job acceptance receipts before Runtime injection.
- Changed child recovery, status, collect, cancel, and cleanup lookup to require the recorded Handle.
- Replaced receipt-less media cleanup's transient `turn_id` scan with allocation reconciliation by Job ID.
- Added Handle JSON to sandbox controls and port grants, including SQLite/PostgreSQL online migration.
- Changed Inspector control recovery, release, terminal, and port preview paths to use the stored Handle.
- Retained `sandbox_id` only as an indexed/API projection and revocation header value.

## Migration And Rollback

- The control/grant columns are additive and created idempotently.
- New Job receipts and controls require complete Handles; incomplete new records fail closed.
- Rollback reverts the implementation commit. Additive columns can remain safely.

## Tests

- Job, Job Worker, Job Store, media cleanup, Inspector, and auth suites: `77 passed`.
- Tests cover provisional receipt persistence, recovered submission, missing sandbox behavior, control ownership, port-grant fencing, and child artifact collection.
- `git diff --check`: passed.

## Review Findings

- Closed the Stage 1 finding where receipt-less media cleanup depended on in-process `turn_id` and could leak a sandbox after worker death.
- Closed remaining known Job and Inspector raw-ID lookup paths.
- No deployment or cloud mutation was performed.

## Residual Risks

- Dispatch still targets the single active backend object. Stage 4 adds provider-based registry dispatch for mixed cleanup.
- Profile compatibility still derives from the current catalog shape. Stage 5 introduces provider releases.
