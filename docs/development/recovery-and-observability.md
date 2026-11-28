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
`python -m sandbox_harness.services.allocation_worker.allocation_service` independently of the JobWorker.
`allocation_dispatch.py` stores provider create requests and responses in
`sandbox_allocations`. Job deadlines and Worker replacement stop waiting without
cancelling that service's in-flight create. Its own lost SENDING lease remains
UNKNOWN and is reconciled without another create. Keep this service in the same
product's isolated database/namespace and deployment start/stop/release controls.
New dispatched provider allocation IDs include attempt generation, while retries
within a capacity wait retain one ID. Frozen handles and exact identities fence
cleanup; a superseded generation must never release a replacement allocation.

AllocationWorker schedules creation and reconciliation independently, each with
bounded concurrency. Slow creates cannot block UNKNOWN observation or refill of
another available creation slot. `allocation_lease_seconds` defaults to 30;
the create owner renews every third of that interval. An expired create lease
permits observation only. Reconciliation has its own persisted claim token,
lease, attempt count and due time, with exponential delay capped at 30 seconds.
Empty/unavailable inventory retains UNKNOWN. Late authoritative creation and
stale observation cannot regress READY or RELEASED. Shutdown joins its owned
tasks before closing the backend and Store. See `test_allocation_scheduling.py`
and the isolated PostgreSQL allocation tests.

`max_sandboxes` is a per-Pool process limit, not a global cloud quota. The API
and JobWorker have separate Pools. `job_worker_concurrency` bounds each
AllocationWorker's create lane; durable Scheduler reservations independently
limit registered Job resources. A Pool waits for a local slot before submitting
a create request, so this wait is not an uncertain cloud creation outcome.
JobWorker restores charged allocation identities and frozen profile keys from
the ledger and legacy cleanup receipts before admission. UNKNOWN, late READY
and accepted executions therefore retain their slots across process loss.
Adoption consumes the retained slot; confirmed deletion/absence releases it.
If a configured limit is reduced below existing ownership, capacity stays closed
until enough retained allocations have been retired. This does not turn the
per-process Pool setting into a global provider quota. Verify restart, accepted
reattachment and lowered-limit debt in `test_allocation_capacity_restore.py` and
`test_allocation_recovery.py`.

New dispatched allocations freeze a 90-second recovery window and a 300-second
provisioning TTL, configurable through `allocation_recovery_seconds` and
`allocation_provision_ttl_seconds`. The window begins with issued creation,
never while queued for local capacity. Adoption renews the sandbox through the
backend's normal TTL before business preparation. Renewal failure retains the
exact handle for cleanup. Neither TTL nor an empty inventory proves absence.

`Store.replace_unstarted_allocation` may fence an allocating attempt before
Runtime submission and queue at most one replacement of the same logical Job.
It atomically creates cleanup of the old generation and retains its reservation.
The replacement acquires additional Scheduler and Pool capacity; local, node or
provider capacity rejection fails it explicitly. Regular capacity waits retain
their existing generation. Runtime submission intent or acceptance prohibits
replacement. Exhaustion survives a crash before terminal publication, and late
cleanup cannot remove the new session or its event collector. See
`tests/test_allocation_recovery.py` and the PostgreSQL submission/replacement race.
Legacy unknown allocations keep their previous conservative recovery policy.

An explicit allocation ID passed to `SandboxPool.acquire` must match any reused
attachment. A replacement ID retires the old sandbox before reallocation; normal
same-Thread reuse without a replacement ID keeps its existing behavior. Check
`test_explicit_replacement_allocation_never_reuses_previous_attachment` in
`tests/test_allocation_dispatch.py` when changing this boundary.

## Pause Is A Durable Saga

Never implement user pause by only cancelling a process. Commit the Thread control generation and participant set first, quiesce each Harness, save its native session identity and workspace checkpoint, then release the sandbox. A service restart must claim the existing generation and finish it; it must not resume business execution while the desired state is paused.

Primary and child Harnesses are separate pause participants. Accepted Provider Jobs keep their external identity and may be reconciled while paused, but their terminal Outbox cannot wake the Primary Harness until resume. Unaccepted Jobs remain fenced. Any missing checkpoint is reported as degraded recovery, never as an exact resume.

## Applies to

Read this for Broker, Store, Runtime stream, pool, cleanup, cancellation,
Gateway lifecycle or telemetry changes.

## Non-graceful recovery model

Do not assume shutdown hooks run. PostgreSQL leases, allocation receipts and
frozen SandboxHandles are the recovery anchors. Startup repairs stale Thread
heads and claims expired work. Provider inventory can locate an allocation but
does not own lifecycle transitions or replace persisted execution identity.

Runtime preparation requests that are idempotent use the configured bounded
request policy: checkpoint restore, Runtime configure/bundle activation,
attachment and replayable Project File upload, Project File
reset/manifest/preserve and bounded checkpoint/artifact downloads. Defaults are
four total attempts with 1, 2 and 4 second waits. A streaming download discards
its partial staging file and restarts at byte zero; an object-backed upload
reopens the immutable object for every attempt. Deterministic 4xx rejection is
not retried.

Checkpoint restore is replayable from immutable object storage. Archives up to
8 MiB use the bounded whole-request path. Larger archives use an isolated
Runtime staging protocol with fixed 1 MiB chunks, immutable offsets, replay-safe
receipts and an exact total size/SHA-256 commit before extraction. A lost chunk
or commit response reuses the same upload identity and bytes; it never restarts
Harness execution. Chunks may arrive out of order with configured concurrency
(`runtime_checkpoint_upload_concurrency`, default 4, range 1-16); overlap or
conflicting replay is rejected and commit requires gap-free coverage. Runtime
replacement invalidates unfinished staging. Setting concurrency to 1 preserves
the serial rollback path.

A fresh sandbox may write a new Thread checkpoint only after the previous
workspace restore is confirmed. A transport or archive restore failure skips
partial save entirely and preserves the last good pointer. Partial checkpoint
after a later execution error remains best effort, but must not convert an
unrestored empty workspace into the authoritative Thread state.

Codex completion is durable only after its app-server has observed stdin EOF
and exited. Before Runtime receives `harness.input.completed`, the Runner starts
a fresh app-server and verifies `thread/resume` for the same native ID. A failed
verification reports `native_session_unavailable`; checkpoint publication may
not claim a native session that a new process cannot resume.

Harness execution start, Provider submission, PTY creation and media-render
submission do not use this policy because a lost response can hide an accepted
external side effect. Those paths retain their accepted-handle reconciliation
contracts instead.

Runtime assigns every normalized event a monotonic sequence and appends it to a
private segmented execution journal before publication. Primary and Child
consumers retain only an in-memory aggregation window, publish bounded live text
through Redis, and commit aggregates at `stream_persist_seconds` (five seconds
by default). After commit they ACK the exact high-water mark; Runtime then
compacts fully acknowledged nonterminal segments. Terminal ACK schedules the
execution journal for delayed removal. A Runtime restart replays retained rows
and explicitly terminates an execution that lost its Harness reader; it never
starts the Harness again. Journal corruption, gaps or capacity exhaustion fail
closed instead of discarding uncommitted events.

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
JobWorker admission saturation must not stop cleanup or accepted-execution
recovery. One independent cleanup batch and a bounded observation capacity
(`job_recovery_concurrency`, default one) keep these paths progressing. Recovery
observations cannot submit work, and round-robin ordering uses the Job's update
time. Only acceptance newer than the latest applicable resume boundary is valid.
`Store.claim_job_cleanups` filters registered adapters before locking/limiting
rows; an empty adapter set claims nothing. Recovery-only claims also require a
valid accepted identity, and `JobWorker` rechecks it before any adapter work.
On shutdown or dispatch failure, the Worker joins its cleanup, admission and
recovery tasks before closing their Store or Pool. Validate these contracts in
`tests/test_job_worker.py` and `tests/test_job_postgres_concurrency.py`.
Reservation expiry is diagnostic, not permission to double-allocate a live
exclusive GPU; the durable cleanup obligation is the recovery authority.

`Broker._database_call` settles an in-flight database operation before propagating
even repeated cancellation. `_claim_turn_safely` records or releases a completed
claim; lease release and matching-generation bookkeeping settle together.
An explicit cancellation-handler marker prevents heartbeat, API stop and close
from interrupting cancellation handling again. `Task.cancelling()` alone does
not cover an explicitly raised `CancelledError`. Keep the database-read,
startup and repeated-cancellation barriers in `tests/test_cancel_boundaries.py`.

Unknown creation results remain charged and cannot be declared absent on the
strength of an empty inventory, lease expiry or audit-access error. An authorized
unstarted Job replacement uses additional capacity and retains the old cleanup;
it never reissues the old allocation or replays a business execution. Record the missing
provider evidence separately from current functional case results. Reclaim only
after authoritative reconciliation or verified retirement of the exclusively
owned test scope; do not change a historical result to make cleanup look green.

Startup orphan cleanup includes every non-RELEASED `sandbox_ownership` handle in
its protected set, including `RETAINED` sessions whose keepalive has not
expired. A Worker restart must not delete a retained provider sandbox merely
because no Turn is currently nonterminal. If provider lookup authoritatively
returns not found, Pool commits direct release of the stale ownership and a
normal Primary Turn may create a fresh allocation; it must not return the stale
404 indefinitely.

An accepted execution and its collected Artifacts are separate durable
boundaries. If submission/execution completed but Artifact collection failed,
the next worker generation reuses the persisted external ID and acceptance
receipt. It may query and collect the same execution, but may not submit a
replacement. The collection retry count is persisted and bounded. Deterministic
validation and Reviewer contract failures are terminal, because another read of
the same immutable result cannot repair them.

Structured output contracts are publication allowlists. Child manifests and
Operation role maps publish only explicitly authorized paths or roles; private
scratch and auxiliary outputs remain inside the sandbox. Every authorized role
must still be present exactly once, and missing, duplicate or identity-mismatched
outputs fail before publication.

Child completion and nested continuation delivery are concurrent durable
boundaries. After observing the original Runtime execution complete and no open
subordinate group, JobWorker must re-read the Child continuation before
terminalizing or collecting. A newly ready continuation keeps the parent Job
nonterminal and resumes the same native session on the next fenced observation.

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

Recovered Runtime completion uses the Turn's persisted execution ID when
applying appended-input receipts. Both replayed completion and an already
completed status response enter this path; a fresh `Runtime.run` is unnecessary.
Keep lease/generation and execution-ID checks on every input status write.
`test_broker.py` covers both receipt paths with an already delivered input and
asserts that recovery never resubmits the accepted execution.

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

`RuntimeClient.put_project_file` uses the same bounded retry only for immutable
byte uploads. Every attempt resends the complete original body; cancellation
propagates and deterministic rejection is not retried. One-shot streaming
uploads are not replayed by this helper. The Runtime project-file endpoint uses
a request-unique sibling temporary file, verifies the length, then atomically
replaces the destination. Cleanup owns only that request's temporary file,
including when an older disconnected upload overlaps a retry. Keep lost-reply,
bounded exhaustion, cancellation and overlapping-upload regressions in
`test_runtime_client.py` and `test_runtime_boundaries.py`.

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

Disposable writable directories are not durable state. Long-running services
must reassert their log, diagnostic, artifact staging, Runtime download, media
staging, site staging, preview cache and local object shard directories at the
write boundary. Recreation is allowed only beneath the configured data root;
symbolic links and path escape fail closed. PostgreSQL, persisted object data,
Controlled work results and sandbox workspaces are authoritative or
non-recreatable and must never be replaced with empty directories.

`AS01` is the supplemental real gate for this boundary. It removes the Child
Harness and Controlled Operation artifact staging roots while their services
remain running, then executes the real P24 nested Child/`image.segment` flow.
It requires successful publication, zero `COLLECTION_RETRY` attempts and one
external execution identity per Job. AS01 is separate from the frozen 175-case
business catalog.

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
- Restore failure before Harness execution, proving the previous checkpoint
  reference remains unchanged and no partial checkpoint request occurs.
- A checkpoint larger than the monolithic threshold through an ingress that
  rejects requests above 1 MiB, including lost chunk and commit responses.
- Telemetry queue/export/disk failure without business-state mutation.

Use deterministic Harness fixtures for recovery mechanics and label them as
such. A separate real Provider case is required for claims about model behavior.
