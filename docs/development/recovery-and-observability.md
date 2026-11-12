# Recovery and Observability

## Applies to

Read this for Broker, Store, Runtime stream, pool, cleanup, cancellation,
Gateway lifecycle or telemetry changes.

## Non-graceful recovery model

Do not assume shutdown hooks run. PostgreSQL leases and OpenSandbox metadata are
the recovery anchors. Startup repairs stale Thread heads, claims only expired
active Turns and reconciles by namespace plus Turn metadata.

- `QUEUED` may be scheduled normally.
- `RESTORING` and `STARTING` delete/recreate before Harness execution.
- `RUNNING`, `AWAITING_USER`, `DRAINING_INPUTS` and `CHECKPOINTING` reattach the
  recorded Runtime execution after its durable sequence.
- A missing Runtime execution after work began fails explicitly; the request is
  not replayed.
- `CLEANING` resumes the persisted `pending_final_state`, capability revocation
  and exact sandbox deletion.

Cleanup retains capacity ownership until delete converges. OpenSandbox DELETE
must be followed by absence confirmation. Namespace cleanup never touches
resources without the product namespace.

Warm and attached sandboxes retain the expiration timestamp from their
original creation unless explicitly renewed. Every bind must renew a full
Sandbox TTL, and the active Turn lease heartbeat must continue renewing long
executions. A reused sandbox must never inherit only the warm pool instance's
remaining lifetime.

## Observability contract

Follow [docs/observability.md](../observability.md). Use the shared `event`,
`operation`, `observed` and request middleware helpers. Business IDs remain
attributes; Trace IDs are SDK-generated. A recovered processing attempt starts
a new Trace linked to the previous attempt.

Allowed telemetry is structural: lifecycle, IDs, state, model identity, timing,
counts, bounded error codes and code locations. Prompts, replies, commands,
files, credentials, cookies, arbitrary headers, full URLs and raw exception
messages are forbidden.

Telemetry failure must not change the business result. Queues, disk, retention
and external export are bounded; PostgreSQL remains authoritative. `/health`
exposes writer and loss counters.

## Required fault checks

- API/Broker SIGKILL while a Runtime execution continues.
- Lease expiry and stale-generation write rejection.
- Transport loss with Runtime event replay and no second process.
- Runtime endpoint loss across the configured reattach window. Retries read the
  same execution after its durable cursor and never replay the initial POST.
- A primary model/Runtime failure followed by diagnostic collection or sandbox
  deletion failure. Cleanup remains a secondary warning and must not overwrite
  the primary failure code or user message.
- Warm sandbox claimed near its original expiration and a Turn longer than one
  Sandbox TTL; both must renew instead of receiving `SIGKILL` at the old time.
- Cancellation racing completion.
- Capability revoke or sandbox delete failure followed by recovery.
- Redis unavailable with PostgreSQL SSE replay.
- Checkpoint/object-store failure without false success.
- Best-effort partial checkpoint after an execution exception; a later retry
  must inspect current project artifacts rather than trust stale Harness memory.
- Telemetry queue/export/disk failure without business-state mutation.

Use deterministic Harness fixtures for recovery mechanics and label them as
such. A separate real Provider case is required for claims about model behavior.
