# State and Data Contracts

## Applies to

Read this before changing `domain.py`, `store.py`, Broker state transitions, API
write endpoints, SSE, project files, interactions or cancellation.

## Actual durable schema

`store.py` currently defines these tables:

`threads`, `turns`, `events`, `turn_inputs`, `artifacts`, `items`, `uploads`,
`project_files` and `interactions`.

There is no separate sandbox-attempt, checkpoint or capability-grant table.
Sandbox identity, capability nonce, lease and Runtime cursor live on `turns`;
the latest checkpoint reference and native Harness thread ID live on `threads`.
OpenSandbox metadata and structured telemetry provide additional evidence but
are not substitutes for database state.

## Thread and Turn serialization

- Browser ownership is `ThreadRow.owner_id`, derived from the signed anonymous
  session cookie.
- `ThreadRow.active_turn_id` is the serialization head.
- `Store.create_turn` rejects `delivery_mode=start` while a nonterminal active
  Turn exists. Queued Turns do not become claimable until `claim_next_turn`
  advances the Thread pointer.
- `(thread_id, idempotency_key)` is unique. Reusing the key with different
  message, attachments, profile or delivery mode is a conflict.
- A worker may claim only the Turn referenced by its Thread's active pointer.

## Implemented states

`TurnState` defines:

```text
QUEUED → RESTORING → STARTING → RUNNING
RUNNING ↔ AWAITING_USER
RUNNING → DRAINING_INPUTS → RUNNING
RUNNING → CHECKPOINTING → CLEANING → terminal
terminal = WAITING_USER | COMPLETED | FAILED | CANCELLED
```

This diagram describes Broker behavior, not a database CHECK constraint.
Changes must inspect all state sets in `store.py` and `broker.py`; duplicated
terminal/acceptable-state sets are contract-sensitive.

`AWAITING_USER` means a live Runtime execution is blocked on an interaction and
is recoverable. `WAITING_USER` is a terminal Runtime result after checkpoint and
cleanup. Do not interchange them.

## Lease and recovery fencing

`lease_owner`, `lease_expires_at` and `recovery_generation` form the worker
fence. A new owner increments generation. Heartbeat, state, Runtime sequence,
execution start, checkpoint and cleanup writes validate owner and generation.
Never add an authoritative worker write without the same fence.

Cancellation is a persisted `cancel_requested` decision. Completion and cancel
serialize in the database; when cancellation already won, a later completed,
waiting or failed outcome becomes `CANCELLED` unless cleanup itself failed.

## Events, Items and inputs

- Events are per-Turn replay records ordered by `(turn_id, sequence)`.
- Runtime replay identity is `(source_execution_id, source_sequence)`.
- Items are the durable, Thread-ordered transcript. Text deltas update an
  existing agent Item instead of inserting one row per token.
- Input status is `pending`, `delivered`, `unknown`, `applied` or `failed`.
  Transport loss after delivery becomes `unknown`; it is not permission to send
  the input again as new work.
- Interaction identity includes the Runtime execution. Late responses from an
  old execution cannot reach a replacement process.

## Files and objects

Uploads and artifacts are immutable object references verified by SHA-256 and
byte limits. `project_files` versions logical `assets/...` and `artifacts/...`
paths; deletion is a tombstone. Managed project files are materialized into the
sandbox and excluded from checkpoint archives to avoid duplicate storage.

## Required checks

- Transaction and idempotency tests in `test_store.py`.
- Scheduling/recovery behavior in `test_broker.py` and
  `test_event_bus_boundaries.py`.
- Cancellation races in `test_cancel_boundaries.py`.
- Owner boundaries in `test_user_isolation.py` and `test_thread_read.py`.
- Runtime receipt/replay behavior when the change crosses that boundary.
