# Architecture

## Non-goals

The platform does not plan tasks, create a DAG, review outputs, recover individual media steps or decide whether a result satisfies the user. Those decisions belong to the selected Harness.

## Broker Responsibilities

The broker authenticates users, serializes turns per session, restores and saves checkpoints, controls OpenSandbox lifecycle, forwards ordered events and exports output files. Deterministic resource and security checks are infrastructure controls rather than business logic.

PostgreSQL is authoritative for every event. Redis Streams under the product key prefix wake connected clients and retain a bounded replay window; reconnects always reconcile against PostgreSQL, so Redis loss does not lose transcript state or cause continuous database polling.

## Runtime Responsibilities

The runtime daemon starts or resumes the selected Harness, sends the user message and attachments, normalizes native streaming events, maintains `/workspace`, and reports completion or a user question. Harness-specific behavior stays behind the adapter interface.

## Recovery

Sandbox creation is keyed by Turn ID. An unknown create result is reconciled by metadata before another create is allowed. A Turn can resume only from its last committed checkpoint. Provider handles are ordinary Harness state under `/workspace/state`; the broker does not interpret or replay them.

Cleanup revokes the capability token, terminates Gateway streams, quiesces the runtime, deletes the exact sandbox ID, confirms absence and only then releases the pool slot.
