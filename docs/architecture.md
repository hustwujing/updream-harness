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

`inbox` remains the current-message delivery area and `outputs` remains the current-Turn publication area. Their contents do not need to survive a Sandbox because the object store and project manifest are authoritative. Sandboxes receive file bytes through the Runtime control endpoint and never receive object-store credentials.

## Recovery

Every nonterminal Turn has a renewable owner lease and a monotonically increasing recovery generation. State writes are fenced by both values, so a delayed worker from an older generation cannot overwrite the current owner. A startup coordinator and periodic scanner claim expired leases.

Sandbox creation is keyed by Turn ID. An unknown create result is reconciled by namespace and Turn metadata before another create is allowed. `RESTORING` and `STARTING` can restart from the last committed checkpoint because the Harness has not started. Once `RUNNING` begins, the broker never blindly resends the user message.

The Sandbox Runtime owns the Harness subprocess reader. It continues draining stdout when the Browser or Broker HTTP stream disconnects, assigns a sequence to every normalized event and retains the terminal result. PostgreSQL stores the active Runtime execution ID and last committed Runtime sequence. A replacement Broker finds the exact Sandbox, resumes after that sequence and proceeds through checkpoint, artifact export and cleanup. Runtime-derived events carry their execution ID and source sequence so replay does not duplicate already committed events.

If the Runtime execution cannot be found, the Turn fails with an explicit interruption instead of replaying unknown public or Provider side effects. Provider handles are ordinary Harness state under `/workspace/state`; the broker does not interpret or replay them.

Cleanup first persists the intended terminal state, then revokes the capability token, terminates Gateway streams, quiesces the runtime, deletes the exact sandbox ID, confirms absence and only then releases the pool slot. A new Broker can finish an interrupted cleanup without changing that intended result. Artifact registration is idempotent by Turn, path and content hash.

Each conversation freezes one Harness. Codex uses app-server `turn/steer`, Claude uses realtime stream-json input, and DeepSeek drains appended input in the same SDK Session at its idle boundary. Inputs are persisted with a sequence and idempotency key before native delivery. A draining state closes the race between the last accepted input and checkpoint publication.

The product keeps a bounded pristine warm pool and a separate attached-session set. Used sandboxes are never returned to the warm pool. Total product capacity is bounded; excess Turns wait for a released slot.

Graceful API shutdown preserves only currently executing sandboxes. On startup, stale warm sandboxes and sandboxes whose Turns are terminal are removed. A periodic namespace-scoped reaper removes later orphans without touching another product's OpenSandbox resources.
