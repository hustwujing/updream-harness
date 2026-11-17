# Recovery and Observability

## Pause Is A Durable Saga

Never implement user pause by only cancelling a process. Commit the Thread control generation and participant set first, quiesce each Harness, save its native session identity and workspace checkpoint, then release the sandbox. A service restart must claim the existing generation and finish it; it must not resume business execution while the desired state is paused.

Primary and child Harnesses are separate pause participants. Accepted Provider Jobs keep their external identity and may be reconciled while paused, but their terminal Outbox cannot wake the Primary Harness until resume. Unaccepted Jobs remain fenced. Any missing checkpoint is reported as degraded recovery, never as an exact resume.

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

Once a Harness result is known, `begin_turn_cleanup` durably records the
original `pending_final_state`. A revoke/delete failure leaves the Turn in
`CLEANING`; it does not rewrite a successful result to `cleanup_failed` and it
does not publish the terminal state while resources remain. A replacement
Broker retries cleanup and publishes exactly that pending outcome after
convergence. This makes uninterrupted and crash-during-cleanup behavior equal.

Turn inventory lookup classifies every matching Sandbox independently. Recovery
selects one ready Sandbox with the recorded Runtime execution, deletes permanent
failures and duplicates, and waits for `starting` or transiently unhealthy
matches instead of failing/replaying the Turn. Cancellation deletes every match
regardless of health. A replacement process must `adopt` the selected Sandbox
before using Pool release semantics.

Warm and attached sandboxes retain the expiration timestamp from their
original creation unless explicitly renewed. Every bind must renew a full
Sandbox TTL, and the active Turn lease heartbeat must continue renewing long
executions. A reused sandbox must never inherit only the warm pool instance's
remaining lifetime.

Warm-pool health is not automatically optional. A failed convergence attempt is
`degraded` only when the Pool still contains a verified ready warm or attached
Sandbox; otherwise the execution backend is `unavailable` and `/api/health`
returns 503. Retry/backoff continues until a fill succeeds. This distinguishes a
missing optional slot for one Harness from a control-plane/authentication/image
failure that prevents every Turn. Verification is continuous: Pool entries are
boundedly re-read through the backend and Runtime health, confirmed missing
entries are retired, and a post-startup outage changes readiness back to 503.

All recovery deletion goes through `SandboxPool.retire`. It removes matching
attachment/expiry/idle ownership, preserves a failed deletion in `_retiring`,
and releases exactly one user slot only after backend absence is confirmed.
The ownership transfer and `_retiring.user_slot` OR-merge happen under one lock,
so concurrent/cancelled retirement cannot lose the capacity-release bit.
Direct backend deletion is reserved for namespace objects proven not to be
owned by this Pool.

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

## Runtime child durability

Native Interaction delivery is tri-state. A successful stdin drain is
`delivered`; a failure before write is retryable; a drain failure after write is
`unknown` and must never be replayed as accepted or blindly written again. The
control-plane Interaction remains `responding` with the frozen answer until
authoritative completion or explicit recovery.

Supervised background commands persist a crash-safe `starting` intent before
spawn and then bind PID, boot ID and `/proc` start time before acknowledgement.
Every post-spawn failure kills and waits the process group. Runtime recovery
validates that complete process identity before SIGTERM/SIGKILL, so a reused PID
is never targeted. Surviving groups are quiesced before their records become
`interrupted` or a replacement execution can start.

For streaming control Harnesses, model-start/idle timing is per input, not per
process lifetime. Every explicit input-completion boundary resets the next
pending input to the model-start timeout; substantive output switches only that
input to the idle timeout.

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
