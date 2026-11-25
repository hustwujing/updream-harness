# State and Data Contracts

External execution failures use the canonical phase, retry and side-effect
fields defined by [External execution error contract](external-error-contracts.md).
Do not encode retry safety in free-form `error_detail`.

## Applies to

Read this before changing `domain.py`, `store.py`, Broker state transitions, API
write endpoints, SSE, project files, interactions or cancellation.

## Actual durable schema

`store.py` currently defines these tables:

`threads`, `turns`, `events`, `turn_inputs`, `artifacts`, `items`, `uploads`,
`project_files`, `turn_project_files`, `interactions`, media draft/job/output
tables, and static-site project/draft/job/version/file tables.

There is no separate sandbox-attempt, checkpoint or capability-grant table.
Sandbox identity, capability nonce, lease and Runtime cursor live on `turns`;
the latest checkpoint reference and native Harness thread ID live on `threads`.
OpenSandbox metadata and structured telemetry provide additional evidence but
are not substitutes for database state.

## Thread and Turn serialization

- Browser ownership is `ThreadRow.owner_id`, derived from the signed anonymous
  session cookie.
- `ThreadRow.active_turn_id` is the serialization head.
- `ThreadRow.archived_at` removes a Thread from the default list. Archiving is
  allowed only after every Turn is terminal; archived Threads reject new Turns.
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

Optional multi-format source outputs may use `ArtifactGateway.emit_matching`
with the frozen collection's MIME patterns. This is content-based selection
before object persistence; default `emit` remains strict. Selected outputs must
have dense positions and stable item keys across retries. Store's generation,
role, disposition, MIME and cardinality checks remain authoritative; selection
never widens the contract or suppresses declared/content mismatch errors.

Uploads and artifacts are immutable object references verified by SHA-256 and
byte limits. `project_files` versions logical `assets/...` and `artifacts/...`
paths; deletion is a tombstone. Managed project files are materialized into the
sandbox and excluded from checkpoint archives to avoid duplicate storage.
Artifact, transcript Item and artifact ProjectFile publication commit in one
lease-fenced transaction after the immutable object write.
Failed or degraded publication atomically marks every Artifact and visible
Artifact Item from that Turn partial. The state is monotonic within one Turn;
a successful retry in a new Turn retains separate historical evidence.

Checkpoint restore streams into bounded staging storage and exposes stable
public error codes. Invalid tar data, unsafe member paths and attempts to
traverse an existing workspace link all return `checkpoint_archive_invalid`;
the Runtime must not expose parser- or host-specific exception text. Tests for
these cases must also assert that no workspace or image-managed Skill bytes
were modified. Size framing failures use `checkpoint_content_length_invalid`,
`checkpoint_size_mismatch` or `checkpoint_too_large` as applicable.

Durable Jobs write a pre-submit `SUBMITTING` intent under the active Job lease
before any external call. A heartbeat covers submit, status, reconcile,
cancel, collect and pause. If a process dies after intent but before acceptance,
a replacement generation reconciles the durable evidence; it never blindly
replays submit. A confirmed unstarted allocation may be retired and retried,
while an accepted Runtime execution must be reattached.
Descriptor timeout is frozen into `JobRequest` and measured from the first
Attempt across restarts. Unsupported cancellation becomes a soft cancel that
continues observing the accepted execution; unsupported reconciliation is
rejected before group commit.

The business Job outcome and cleanup are separate durable facts.
`job_cleanups` retains the immutable adapter/external acceptance receipt,
claim lease, retry count and last error. A successful Job remains successful
while cleanup is pending; workers retry idempotent revoke/Sandbox/resource
release across restart until completed. Group terminalization releases unused
budget and creates its continuation outbox in the same transaction.
Resource reservations remain capacity-authoritative while their state is
`reserved` or `running`; TTL expiry alone never makes an exclusive GPU reusable.
Adapter start/status/collect/pause/cancel/cleanup heartbeats the reservation.
Cleanup confirms capability revocation and external Sandbox absence before the
Scheduler release, which is always the last step.
Cleanup commits `external_quiescence -> capacity_release -> completed` as
separate claim-fenced transactions. A crash after external absence skips
quiescence on retry; a crash after Scheduler release repeats only the
idempotent release and then commits completion, without trying to renew an
already released reservation.

Child/Reviewer allocation receipts freeze provider, allocation ID, compatible
profile, planned execution ID and original reservation identity. Phases
`awaiting_capacity`, `allocating`, `preparing`, and `runtime_submission` distinguish
resource allocation from execution acceptance. Only the final boundary may have
started user work. `record_job_submission_allocation` rejects a Runtime submission
marker when durable cancellation already won. Allocation lookup checks cancel
during both initial start and replacement-worker recovery.

Pool callers opt into `retain_unresolved_allocation` only with a durable cleanup
owner. `before_create` persists the create phase after capacity acquisition.
Pending capacity is keyed by `(provider, allocation_id)`; adoption consumes the
existing slot. Empty inventory is not proof of absence. Cleanup persists a
discovered handle under its claim fence before deletion and does not require
Runtime health for unstarted allocations. Confirmed absence is durable before
Scheduler release, so another Worker can finish the retry decision. Original
attempt generation survives terminal cleanup. Pending allocation IDs also fence
orphan deletion and provider switching. Relevant regressions live in
`test_harness_job.py`, `test_sandbox_namespace.py`, and `test_broker.py`.

## Reviewer gates

Operation Descriptors declare the validator IDs each output role can produce.
The Job Coordinator rejects an undeclared requested validator before creating a
Group or reserving GPU/Provider capacity. Adapter receipts remain authoritative
at publication; prompt text cannot invent a validator contract.

Descriptor `review_policy` is frozen into each output contract. Required and
optional policy both keep the validated collection as candidates and create a
read-only Reviewer Job; disabled policy does not infer review from prompt text.
`review_gates` binds that Job to one source Job generation, artifact role and
exact ordered Artifact/digest set. Deterministic validator receipts are loaded
from the source collection and frozen into Reviewer context before scheduling.
The Job and Gate are inserted by the same Store transaction; no Reviewer Job is
claimable without its gate. Every terminal Reviewer transition applies an
availability outcome even if Adapter collection was never reached. Non-success
Job state, Gate state, optional publication/warning, Job result and group
continuation outbox commit in one transaction, so continuation cannot observe a
terminal Reviewer with a pending Gate.
Continuation outboxes contain only bounded Job facts: identity, state, result
digest, result availability, stable error code and compact Review Gate
state/warning. Arbitrary Adapter result bodies remain addressable by Job ID and
are never copied into the bounded continuation batch.
The continuation prompt defines `result_available=true` as an intentional
omission and names the one-shot `sandbox-job job --job-id` / `sandbox-job group
--group-id` reads. Harnesses must wait for continuation before reading; they do
not poll Job state in a model loop.

A digest-bound `PASS` atomically promotes candidates and publishes late
delivery Items if the source group already terminated. `REVISE`,
`EVIDENCE_REQUIRED` and `BLOCKED` retain candidates. Required execution failure
is visibly unavailable and retryable; optional execution failure promotes the
otherwise valid collection with an explicit availability warning. Gate replay
is idempotent and a changed source identity is never accepted.
Required gates can attach only to an unpublished `awaiting_review` candidate
collection. Optional advisory review may group immutable Artifacts from
multiple Jobs/roles plus generic Artifacts, but has no publication target and
cannot roll a published collection back to blocked.

Child Harness inputs stream from object storage to Runtime with a declared
length. Count, per-item, aggregate bytes and reserved disk are checked before
acquiring a Sandbox; neither Host nor Runtime may buffer a full valid input.

Thread deletion uses the same Thread row barrier as the last Job transition.
It rejects nonterminal Jobs, incomplete cleanup, active resource reservations,
open budget and pending continuation. Once all obligations are closed it
deletes Job activities, Artifacts, cleanups, leases, Attempts, reservations,
outboxes, budgets, snapshots, drafts, groups and Jobs in dependency order; no
generic execution row may outlive its Thread.

Before first materialization a leased worker freezes `turn_project_files` and
sets `turns.project_files_frozen`, even for an empty manifest. Recovery and
normal execution must use that snapshot rather than the Thread's later current
versions. Runtime output carries the owning Turn batch ID; publishing a batch
owned by another Turn is a program error.

## Required checks

- Transaction and idempotency tests in `test_store.py`.
- Scheduling/recovery behavior in `test_broker.py` and
  `test_event_bus_boundaries.py`.
- Cancellation races in `test_cancel_boundaries.py`.
- Owner boundaries in `test_user_isolation.py` and `test_thread_read.py`.
- Runtime receipt/replay behavior when the change crosses that boundary.

## Live input attachment bindings

After materializing a pending TurnInput's authorized attachments, Broker calls
`prepare_turn_input_artifacts` before Runtime append. The Store checks the active
Turn lease/generation, execution identity, cancellation and input identity.
Deferred execution freezes the same bindings in `start_runtime_execution`.
This publishes immutable input Artifact identities without asserting input
receipt or completion; those remain separate Runtime-backed transitions.

Bindings have stable IDs per input/name. Repeated preparation and applied
receipts cannot duplicate them. `turn_input_artifacts` retains every immutable
ID so an existing Child grant still sees its original version, while path lookup
selects the latest prepared input in input-sequence order. Initial Turn project
snapshots are never replaced by mutable Thread files. Owner checks also apply
to attachment-only catalog rows. Broker, Store and Gateway tests cover live and
deferred consumption, recovery, same-name replacements and old grants.
