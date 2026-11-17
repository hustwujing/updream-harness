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

Job execution has two leases. The business lease is heartbeated across every
adapter call and is fenced by generation. A persisted pre-submit intent makes
process death conservative: a replacement may reconcile or report unknown but
cannot resubmit. Terminal business state atomically creates a cleanup
obligation. The cleanup lease is independently heartbeated and retried across
restart; cleanup degradation is visible without changing a successful result.
Scheduler ownership is released only after external absence is confirmed.
Reservation expiry is diagnostic, not permission to double-allocate a live
exclusive GPU; the durable cleanup obligation is the recovery authority.

Thread archival is a durable barrier: active Jobs/Groups and ready or claimed
continuation outboxes prevent archive. The final Job transaction locks the
Thread while creating its outbox, closing the archive race. Legacy blocked
outboxes remain quiescent while archived and return to ready on unarchive;
permanent dispatcher errors never hot-loop.

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

Authenticated Runtime telemetry always inherits non-empty Turn, Thread and
Sandbox identities from its capability after mismatch rejection. Local related
timeline queries scan each telemetry segment once, deduplicate with stable
fallback identities and expand related Turns in memory under one global limit.

Execution Graph projection uses SQL aggregate counts and bounded summaries. It
limits history, nodes, edges and Artifact details, returns truncation/continuation
metadata, and removes every edge whose endpoint is absent. Active nodes retain
their ancestor closure. Review Gate nodes expose the publication outcome rather
than inferring it from Reviewer Job state. Turn retries are takeovers after the
first claim, not raw lease generation.

## Runtime child durability

Native Interaction delivery is tri-state. A successful stdin drain is
`delivered`; a failure before write is retryable; a drain failure after write is
`unknown` and must never be replayed as accepted or blindly written again. The
control-plane Interaction remains `responding` with the frozen answer until
authoritative completion or explicit recovery.

Supervised background commands persist a crash-safe `starting` intent before
spawn. They launch through `sandbox_runtime.background_supervisor`, which fsyncs
its PID/boot/start-time receipt before executing user argv and acts as a Linux
child subreaper. Double-fork and `setsid()` descendants remain owned by that
supervisor; parent exit is not Job completion until the descendant boundary is
empty. Every post-spawn failure kills and waits the supervisor boundary. Runtime
recovery can recover identity from the supervisor receipt, validates it before
SIGTERM/SIGKILL, and never targets a reused PID. Surviving boundaries are
quiesced before records become `interrupted` or replacement work can start.

For streaming control Harnesses, model-start/idle timing is per input, not per
process lifetime. Every explicit input-completion boundary resets the next
pending input to the model-start timeout; substantive output switches only that
input to the idle timeout.

Codex control interactions emit `harness.interaction.applied` only after the
native app-server response write succeeds. Runtime journals that execution-
scoped receipt and reconciles an UNKNOWN drain to delivered; Store atomically
marks the matching Interaction/item responded. Likewise every
`harness.input.completed` event immediately commits per-input applied/failed
state, so a later input failure cannot overwrite an earlier completion.

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
