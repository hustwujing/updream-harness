# Architecture

## Non-goals

The platform does not plan tasks, create a DAG, review outputs, recover individual media steps or decide whether a result satisfies the user. Those decisions belong to the selected Harness.

## Broker Responsibilities

The broker authenticates users, serializes turns per session, restores and saves checkpoints, controls OpenSandbox lifecycle, forwards ordered events and exports output files. Deterministic resource and security checks are infrastructure controls rather than business logic.

PostgreSQL is authoritative for every event. Redis Streams under the product key prefix wake connected clients and retain a bounded replay window; reconnects always reconcile against PostgreSQL, so Redis loss does not lose transcript state or cause continuous database polling.

## Runtime Responsibilities

The runtime daemon starts or resumes the selected Harness, sends the user message and attachments, normalizes native streaming events, maintains `/workspace`, and reports completion or a user question. Harness-specific behavior stays behind the adapter interface.

## Project Files

`/workspace/project` is included in checkpoints. The host additionally maintains a Thread-scoped manifest for immutable uploads and exported artifacts. Reusing a logical path with different content creates a new version; identical content is idempotent. Current uploads are materialized under `project/assets`, while current outputs are materialized under `project/artifacts` after checkpoint restore and before the Harness starts.

`inbox` remains the current-message delivery area and `outputs` remains the current-Turn publication area. Their contents do not need to survive a Sandbox because the object store and project manifest are authoritative. Managed `project/assets` and `project/artifacts` are excluded from checkpoint tar files to avoid storing large media twice, then restored from the manifest. Sandboxes receive file bytes through the Runtime control endpoint and never receive object-store credentials.

## Recovery

Every nonterminal Turn has a renewable owner lease and a monotonically increasing recovery generation. State writes are fenced by both values, so a delayed worker from an older generation cannot overwrite the current owner. A startup coordinator and periodic scanner claim expired leases.

Sandbox creation is keyed by Turn ID. An unknown create result is reconciled by namespace and Turn metadata before another create is allowed. `RESTORING` and `STARTING` can restart from the last committed checkpoint because the Harness has not started. Once `RUNNING` begins, the broker never blindly resends the user message.

The Sandbox Runtime owns the Harness subprocess reader. It continues draining stdout when the Browser or Broker HTTP stream disconnects, assigns a sequence to every normalized event and retains the terminal result. PostgreSQL stores the active Runtime execution ID and last committed Runtime sequence. A replacement Broker finds the exact Sandbox, resumes after that sequence and proceeds through checkpoint, artifact export and cleanup. Runtime-derived events carry their execution ID and source sequence so replay does not duplicate already committed events.

If the Runtime execution cannot be found, the Turn fails with an explicit interruption instead of replaying unknown public or Provider side effects. Provider handles are ordinary Harness state under `/workspace/state`; the broker does not interpret or replay them.

Cleanup first persists the intended terminal state, then revokes the capability token, terminates Gateway streams, quiesces the runtime, deletes the exact sandbox ID, confirms absence and only then releases the pool slot. A new Broker can finish an interrupted cleanup without changing that intended result. Artifact registration is idempotent by Turn, path and content hash.

Each conversation freezes one Harness. Codex uses app-server `turn/steer`, Claude uses realtime stream-json input, and DeepSeek drains appended input in the same SDK Session at its idle boundary. Inputs are persisted with a sequence and idempotency key before native delivery. A draining state closes the race between the last accepted input and checkpoint publication.

The product keeps a bounded pristine warm pool and a separate attached-session set. Used sandboxes are never returned to the warm pool. Total product capacity is bounded; excess Turns wait for a released slot.

Graceful API shutdown preserves only currently executing sandboxes. On startup, stale warm sandboxes and sandboxes whose Turns are terminal are removed. A periodic namespace-scoped reaper removes later orphans without touching another product's OpenSandbox resources.

## Transport loss and failed-turn artifacts

The Runtime result and its journal entry are published under the same condition lock.
A Broker that sees EOF or a transient transport error reconnects to the existing
execution's GET event endpoint (up to three reconnections), starting after its last
consumed sequence. It does not replay the POST or create another Harness process.
Exhaustion is reported as a transport interruption, not a model-quality failure.

A terminal Harness failure still triggers a bounded checkpoint/artifact save before
sandbox deletion. Exported files are marked `partial` in transcript Items, and the
Turn remains FAILED. Saving partial work is best effort; it cannot recover files
from a sandbox that disappeared before they were exported. Original upstream errors
remain in the Turn audit record; public errors distinguish overload, timeouts and
transport interruption. Protocol completion events preserve failure status.

The UI retains one EventSource per active Turn, honours terminal closure and reads
all Item pages. SSE reconnects honour Last-Event-ID. A delivery conflict only creates
a queued Turn for the explicit `turn_not_accepting_input` outcome, never for arbitrary
409 errors. Pending and failed appended messages have visible transcript states.

Object storage calls run outside the asyncio event loop so slow BOSS transfers do
not block lease heartbeats. Sandbox deletions retain their capacity reservation and
retry record until absence is confirmed. Namespace cleanup includes unhealthy
sandboxes; exact Turn lookup validates metadata returned by OpenSandbox.

## Concurrent recovery and event commits

Only the Turn referenced by a Thread's active pointer may acquire an execution
lease. Claims use one conditional UPDATE, including on SQLite. The recovery scan
repairs stale terminal pointers before selecting eligible work; queued followers
cannot bypass the active Turn. Heartbeats continue during cleanup. A worker that
lost its lease does not cancel the Runtime now owned by its replacement.

Runtime transcript mutations, event receipts and replay cursors commit together.
Duplicate source coordinates are idempotent, including a retry after a failed
transaction. Checkpoint and artifact publication also check the worker's lease.
Unknown input delivery receipts remain explicitly `unknown`; only explicit input
completion receipts can mark them applied. Selecting a pending deferred input is
not itself evidence of delivery.

The transcript renders Markdown without raw HTML and resolves sandbox file links
only against the current Thread's exported artifacts/managed files. Step timers
use persisted Item timestamps: live steps tick once a second, completed steps
retain their duration, and terminal cleanup stops unfinished step timers.
