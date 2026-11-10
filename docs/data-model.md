# Data Model

This document describes the tables implemented in
`src/sandbox_harness/store.py`. `domain.py` defines API/runtime models; not every
domain concept has a separate table.

## Threads

`threads` stores browser owner, frozen Harness defaults, title, checkpoint
reference, native Harness thread ID, active Turn pointer, last-read Turn and the
next transcript Item sequence.

One Thread belongs to one hashed browser owner. `active_turn_id` serializes
execution; a queued follower is not claimable until this pointer advances.

## Turns

`turns` stores the original message and attachments, frozen model/reasoning
selection, delivery mode, state, sandbox ID, error, lease owner/expiry,
recovery generation, Runtime execution/cursor, capability nonce, trace context,
cancellation intent and pending final state.

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
`artifacts` indexes immutable `/workspace/outputs` exports by Turn.
`project_files` versions logical `assets/...` and `artifacts/...` paths per
Thread. Deletion writes a tombstone version; older versions remain queryable.

Object bytes live in the configured content-addressed local or S3/BOSS store.
References are `sha256:<digest>` and reads verify both size and digest.

The latest checkpoint is not a table row. Its object reference and native
Harness thread ID are committed on `threads` after the content-addressed archive
write succeeds.

## Interactions

`interactions` stores execution-bound approvals/questions, request/response,
idempotency key, responding owner and response lease expiry. The unique native
identity is `(turn_id, runtime_interaction_id)`. Only the matching active Runtime
execution can receive a response.

## Schema upgrades

`Store.initialize` creates missing tables and performs explicit additive ALTERs
for existing installations. There is currently no external migration runner.
Changing a SQLAlchemy model alone is insufficient for an existing database; see
[database migration guidance](development/database-migration.md).
