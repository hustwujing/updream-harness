# Data Model

This document describes the tables implemented in
`src/sandbox_harness/store.py`. `domain.py` defines API/runtime models; not every
domain concept has a separate table.

## Threads

`threads` stores browser owner, frozen Harness defaults, title, checkpoint
reference, native Harness thread ID, active Turn pointer, last-read Turn, the
next transcript Item sequence and nullable `archived_at`.

One Thread belongs to one hashed browser owner. `active_turn_id` serializes
execution; a queued follower is not claimable until this pointer advances.
Archived Threads are filtered from the default list and cannot accept a new
Turn until restored.

## Turns

`turns` stores the original message and attachments, frozen model/reasoning
selection, delivery mode, ingress-derived network access, state, sandbox ID, error, lease owner/expiry,
recovery generation, Runtime execution/cursor, capability nonce, trace context,
cancellation intent and pending final state.
`project_files_frozen` distinguishes an intentionally empty ProjectFile
snapshot from a Turn that has not frozen inputs yet.

`network_access` is frozen as `public` for a Turn submitted with a valid account
Session. Anonymous direct submissions are `restricted`. It is part
of the idempotency contract and survives queueing, restart and lease recovery.

`(thread_id, idempotency_key)` is unique. Implemented states are:

```text
QUEUED, RESTORING, STARTING, RUNNING, AWAITING_USER,
DRAINING_INPUTS, CHECKPOINTING, CLEANING,
WAITING_USER, COMPLETED, FAILED, CANCELLED
```

`WAITING_USER`, `COMPLETED`, `FAILED` and `CANCELLED` are terminal.
`AWAITING_USER` retains a live Runtime execution.

There is no separate sandbox-attempt or capability-grant table. Current sandbox,
lease and capability identity are fields on the Turn; OpenSandbox metadata and
telemetry provide additional audit evidence.

## Turn inputs

`turn_inputs` stores appended messages with per-Turn sequence and idempotency
constraints. Status is `pending`, `delivered`, `unknown`, `applied` or `failed`.
`delivery_mode` records the native delivery result such as an immediate steer or
deferred safe point.

## Items and events

`items` is the durable Thread transcript. `(thread_id, sequence)` is unique.
Types are `user_message`, `agent_message`, `reasoning`, `plan`, `command`,
`tool`, `file_change`, `artifact`, `approval`, `warning` and `error`.

`events` is the ordered per-Turn SSE/replay log with primary key
`(turn_id, sequence)`. Runtime-originated events additionally store execution ID
and source sequence so replay is idempotent. PostgreSQL is authoritative; Redis
Streams only wake clients and retain a bounded replay aid.

## Files

`uploads` associates immutable uploaded objects with a browser owner.
`artifacts` indexes immutable `/workspace/outputs` exports by Turn. Its
non-null `partial` flag records whether that Turn exposed the file before a
degraded or failed publication boundary. Once partial within a Turn it cannot
be downgraded; a later successful Turn creates a distinct complete history row.
`project_files` versions logical `assets/...` and `artifacts/...` paths per
Thread. Deletion writes a tombstone version; older versions remain queryable.
Artifact publication atomically creates the Artifact, its user-visible Item and
the corresponding artifact ProjectFile under the owning Turn lease.
Startup adds `artifacts.partial` with a false server default to an existing
database under the schema initialization lock, preserving and backfilling all
existing Artifact rows. Repeated and concurrent initialization is idempotent.

`turn_project_files` is the immutable per-Turn input manifest. It stores the
selected ProjectFile ID, path, version, source, SHA-256, size, MIME and object
reference. The Broker creates it once after registering initial attachments and
materializes only these rows. Later ProjectFile versions cannot change a running
or recovering Turn, including when the frozen manifest is empty.

Object bytes live in the configured content-addressed local or S3/BOSS store.
References are `sha256:<digest>` and reads verify both size and digest.

The latest checkpoint is not a table row. Its object reference and native
Harness thread ID are committed on `threads` after the content-addressed archive
write succeeds.

## Media references

`media_drafts` and `media_jobs` freeze `generation_mode` plus reference
snapshots containing ProjectFile ID/path/version, SHA-256, object reference,
role, usage and inspected media metadata. Public API responses omit the internal
object reference.

`media_reference_uploads` stores owner-scoped, expiring presigned PUT tickets
and the ProjectFile created after full upload verification.

`media_reference_assets` is the cross-Thread content cache keyed by SHA-256. It
stores the stable `aniforge/...` BOSS key/public URL, BOSS publication status,
Ark Asset ID/status/error and a renewable preparation lease. The table prevents
concurrent Workers from uploading or registering the same content twice.

`media_job_outputs` records every immutable result of a Job. Most image, video
and audio Jobs have one primary output; a Tripo Job can atomically register a
primary model and rendered preview. Each row points to its Artifact and, for
`workspace`/`delivery`, its ProjectFile. `media_jobs` retains the primary output
columns for the Runtime fetch contract and stores the external task ID plus
reported credit consumption.

## Static site deployment

`site_projects` owns a globally unique public slug, Thread/owner identity,
status and current immutable version pointer. `site_drafts` stores the
content-addressed validated tar and confirmation state; `site_draft_files`
indexes its individually content-addressed files for efficient owner-scoped
preview. `site_deploy_jobs` stores asynchronous phase, progress, lease, error
and terminal timestamps for the independent Site Worker. Its generation fences
every mutation; attempt count and next-attempt timestamp survive restart and
drive bounded retry/manual continuation. `site_drafts.request_digest` covers the
complete frozen request for concurrent database idempotency.

`site_versions` is unique by project/version and project/bundle digest.
`site_files` maps each version path to its immutable object reference, MIME,
size and SHA-256. The Worker writes objects before one transaction inserts the
version/files, advances the project pointer and completes the Job. Partial
object writes are never visible. Rollback updates only the project pointer;
downline changes project status while retaining version history.
Activation follows successful streaming verification of every referenced
object. Missing/corrupt objects are repaired from the validated staged archive
before this transaction.

## Interactions

`interactions` stores execution-bound approvals/questions, request/response,
idempotency key, responding owner and response lease expiry. The unique native
identity is `(turn_id, runtime_interaction_id)`. Only the matching active Runtime
execution can receive a response.

## Sandbox inspection

`sandbox_controls` is the durable exclusive user-control record for one Thread.
It binds owner, live Sandbox, generation, short-lived capability nonce, Execd
PTY session and expiry. `sandbox_port_grants` records private, revocable port
preview access. `sandbox_audit_events` records structural control/file/process/
port actions without commands, terminal output or file content.

These rows do not make a Sandbox persistent. `threads.checkpoint_ref` and
ProjectFiles remain the restore source after a Sandbox is deleted.
Release compares the live managed-directory manifest with current ProjectFiles.
The selected resolution is audited; preserved terminal edits enter checkpointed
`project/work`, while saved versions create new immutable ProjectFile rows.

## Users and account Sessions

`users` stores stable account ID/owner ID, normalized unique username, Argon2id
password hash, optional avatar object reference/MIME/version, disabled flag and
timestamps. Username changes do not change the owner ID used by Threads and uploads.

`auth_sessions` stores only the SHA-256 hash of a random Cookie token, account
ID, expiry, creation and last-seen timestamps. Password reset/disable revokes
Sessions by deleting rows. Anonymous browser identities remain stateless signed
Cookies and do not create `users` rows.

## Capability authority

`capability_ledger` is the PostgreSQL authority for signed nonce identity,
expiry, revocation, normal/public request counters, and token
reserved/consumed totals. A parent nonce links Job spend to its Turn/Group
ceiling. `model_token_reservations` records one Gateway model-request identity,
the requested output limit, conservative reservation, final charge and
terminal state. Finalization is idempotent; an unfinalized reservation remains
charged against availability after process restart rather than reopening
budget.

## Media execution receipts

`media_jobs.generation` fences every claimed Worker generation.
All single-Job Worker mutations require this value; no owner-only compatibility
path exists. Heartbeat failure invalidates the active generation locally.
`media_job_executions` stores the immutable sanitized Provider result receipt
and content-addressed staged output list. It is written before download and
before terminal Artifact binding respectively, allowing retry/restart without
another billable Provider submission or another download after object staging.
Pending `media_reference_uploads` are archive/delete obligations only until
their durable `expires_at`; expired rows remain queryable for audit.

## Schema upgrades

`Store.initialize` creates missing tables and performs explicit additive ALTERs
for existing installations. There is currently no external migration runner.
Changing a SQLAlchemy model alone is insufficient for an existing database; see
[database migration guidance](development/database-migration.md).
