# Recovery and Observability

## Sandbox Job allocation ownership

`job_allocation.SandboxAllocationOwner` is shared by Child/Reviewer execution and
sandbox Operations. Before resource reservation, persist `reserving_resources`.
Before the provider create call, persist the allocation ID and frozen provider,
profile and original reservation generation. Inventory can restore identity but
must not replace the frozen profile with provider-side placeholder fields.

Safe pre-execution recovery releases only the original reservation, with the
current Job/lease generation checked in the same transaction. Empty legacy
receipts are uncertain, not evidence of absence. Terminal cleanup owns previous
generations; the stale reservation reaper must preserve unresolved submissions.

An Operation records `runtime_submission` before its RPC and persists the full
completed result before returning acceptance. A lost response is not permission
to repeat the RPC. Cleanup confirms deletion before capacity release, and
preparation recovery persists absence before releasing the original reservation.

When `SANDBOX_HARNESS_JOB_ALLOCATION_DISPATCHER_ENABLED=true`, run
`python -m sandbox_harness.allocation_service` independently of the JobWorker.
`allocation_dispatch.py` stores provider create requests and responses in
`sandbox_allocations`. Job deadlines and Worker replacement stop waiting without
cancelling that service's in-flight create. Its own lost SENDING lease remains
UNKNOWN and is reconciled without another create. Keep this service in the same
product's isolated database/namespace and deployment start/stop/release controls.
New dispatched provider allocation IDs include attempt generation, while retries
within a capacity wait retain one ID. Frozen handles and exact identities fence
cleanup; a superseded generation must never release a replacement allocation.

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
- Primary adoption reconstructs compatibility with the release revision in the
  durable SandboxHandle plus the Turn's frozen provider reference and Bundle
  digests. A provider reference is not a release revision, and the current
  release mapping must not replace the revision of an already running sandbox.
  The Pool still verifies the full compatibility key before reattachment.
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

An accepted execution and its collected Artifacts are separate durable
boundaries. If submission/execution completed but Artifact collection failed,
the next worker generation reuses the persisted external ID and acceptance
receipt. It may query and collect the same execution, but may not submit a
replacement. The collection retry count is persisted and bounded. Deterministic
validation and Reviewer contract failures are terminal, because another read of
the same immutable result cannot repair them.

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
fallback identities and build a temporary SQLite correlation index under the
configured data root. Direct/related filtering happens after the scan, so old
matches are not discarded by newer unrelated records; one global result limit
still bounds output.

Execution Graph projection uses SQL aggregate counts and bounded summaries. It
limits history, nodes, edges and Artifact details, returns truncation/continuation
metadata, and removes every edge whose endpoint is absent. Active nodes retain
their ancestor closure. Review Gate nodes expose the publication outcome rather
than inferring it from Reviewer Job state. Turn retries are takeovers after the
first claim, not raw lease generation.
Normalized model activity is also grouped by `(turn_id, job_id)` for per-Harness
ordinary input, cache-read input, cache-write input and output usage. Empty
`job_id` belongs only to the Primary Harness; Child and Reviewer usage is keyed
by its durable Job ID. The projection is bounded and deduplicates stable model
request IDs. If the selected usage window is truncated, Harness nodes are
conservatively labelled as partial rather than presenting an incomplete value
as a complete total.
History uses a stable `history_before` cursor and frontend merge/load-more
without duplicate nodes. When node ceilings apply, pending interactions,
required/warning Review Gates, active Jobs and their ancestors are retained
before terminal history; truncation is visible in the UI.

Child Harness activity has two durable grains. `job_activities` is the raw
Runtime event ledger used for reconnect, child summaries and diagnostics;
`job_display_activities` is a rebuildable semantic projection used by the
Execution Graph. Text deltas never become display steps. Native tool IDs pair
start/output/finish into one stable row, late output cannot regress a terminal
status, and warnings/errors remain visible. Raw replay and display projection
are committed together for new events; historical Jobs are projected lazily and
idempotently before display. Never delete raw rows or infer activity identity
from natural-language text to improve the UI.

The browser adds a presentation-only phase grouping over display activities.
This grouping cannot mutate the durable raw ledger or projection, and it uses
only native tool type plus structured command-action types. Raw diagnostics stay
available through the owner-scoped API; the ordinary timeline never becomes a
JSON/log viewer.

Primary and Child Harness details are a user projection, not a diagnostic
surface. A running Harness with no active projected tool remains visibly in
`等待模型响应`; text deltas never become rows. The operator raw API remains
available for replay and incident analysis even though the browser does not
request it.

Model usage telemetry retains logical input plus ordinary/cache-read/cache-write
dimensions and output. A local budget rejection is persisted before it reaches
the native Harness, so recovery and the user-facing failure do not depend on
whether Codex, Claude or DeepSeek preserves an HTTP response body. The latest
failed model activity may refine only a generic native rate-limit/harness error;
it cannot override a different explicit upstream failure.

The failed-Turn UI tip reads the sanitized Error Item. Raw Turn `error_detail`,
Provider bodies, credentials and command output remain diagnostic-only. Tip
dismissal is presentation state and never mutates Turn recovery or retry state.

## Runtime child durability

Runtime Code and Content Bundle status reads use the same bounded startup retry
as their idempotent uploads. Connection/transport failures and HTTP
429/502/503/504 retry at most eight times; permanent responses return immediately
for normal error handling. Configuration begins only after both Bundle stages
succeed. Harness execution submission is outside this helper and is never
replayed by bootstrap retry. Runtime client tests cover both reads, permanent
rejection and retry exhaustion.

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
- Worker death between accepted execution and collection retry, with the same
  external ID and no duplicate submission.
- Reviewer malformed verdict/schema failure, with Job and Review Gate reaching
  the correct terminal state in one transaction rather than waiting externally.
- Best-effort partial checkpoint after an execution exception; a later retry
  must inspect current project artifacts rather than trust stale Harness memory.
- Telemetry queue/export/disk failure without business-state mutation.

Use deterministic Harness fixtures for recovery mechanics and label them as
such. A separate real Provider case is required for claims about model behavior.
