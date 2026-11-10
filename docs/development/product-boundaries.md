# Product Boundaries

## Applies to

Read this for every feature proposal and whenever logic could be placed either
on the host or inside a Harness sandbox.

## Implemented boundary

The browser talks to the API. The API persists user-owned Threads and Turns and
asks `Broker` to execute. `Broker` leases the active Turn, acquires or adopts an
OpenSandbox instance, configures its Runtime and streams normalized Runtime
events. The chosen native Harness decides how to perform the user's work.

The host may make deterministic infrastructure decisions:

- identity and owner checks;
- one-active-Turn serialization and idempotency;
- resource limits, safe paths and MIME/download behavior;
- sandbox allocation, recovery, cancellation and cleanup;
- capability routes, budgets, model routing and network policy;
- checkpoint and artifact persistence;
- transport normalization, durable event ordering and error classification.

The host must not:

- classify business intent or construct a task DAG;
- rewrite a user's task into a host-owned plan;
- judge media quality or approve a Harness result;
- execute a task with a host model or shell when OpenSandbox is unavailable;
- put durable Provider, database, Redis or object-store credentials in a
  sandbox;
- replay a Harness action whose external outcome is unknown.

## Placement rule

If behavior expresses how to complete a user task, it belongs to the selected
Harness, its prompt or a sandbox tool. If behavior protects ownership,
durability, isolation, quotas or deterministic protocol correctness, it belongs
to the host.

## Review questions

- Does this change introduce task-specific semantics into `Broker`, `Store` or
  Gateway?
- Could a sandbox gain a long-lived credential or a route unrelated to its
  current Turn?
- Does a failure create a hidden host fallback?
- Can the same behavior be implemented as a sandbox tool while retaining host
  validation at the boundary?

Changes that move this boundary require an explicit product decision and an
update to `AGENTS.md`, architecture docs and isolation tests.
