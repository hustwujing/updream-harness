# Durable sandbox allocation dispatch

## Problem and decision

Real P05 jobs lost sandbox creation responses when another protocol killed the
JobWorker roughly 100 ms after allocation began. A real provider-capacity timeout
also interrupted an in-flight create. The provider exposes no authoritative
negative allocation lookup. Empty inventory cannot prove that a request was not
accepted. Keep the historical unknown requests unresolved; do not replay them.

Execute new Job sandbox creates in an independent allocation service. The
JobWorker submits a fenced request to PostgreSQL and waits for its result. Worker
loss or a Job deadline stops waiting but does not cancel the supplier request.
The service records its result independently, so recovery and cleanup can consume
the original response. This applies to Child, Reviewer and Operation jobs through
their common allocation owner and works with any configured SandboxBackend.

## Contracts

- A row is unique by namespace, provider and allocation ID, with an immutable
  request digest. Enqueue checks the current Job/lease generation under a row lock.
  New dispatched allocations include the attempt generation in their provider
  allocation ID. Capacity retries keep that ID; a replacement attempt gets its
  own ID so an older cleanup cannot adopt or release the replacement's sandbox.
- State is PENDING, SENDING, READY, REJECTED, CANCELLED, UNKNOWN or RELEASED.
  Only a definitely rejected/released request may be queued again. A lost service
  lease in SENDING becomes UNKNOWN; never issue another create automatically.
- Provider authentication/configuration/capacity rejections retain stable typed
  error codes. Transport ambiguity remains UNKNOWN. Persist handles and frozen
  profiles, never arbitrary error text or secrets in telemetry.
- Recovery can prove absence from a fenced missing queue row or a definite
  REJECTED/CANCELLED/RELEASED state. Cleanup records that evidence before releasing
  capacity. A READY handle is deleted and confirmed before marking RELEASED.
- The service has independent process lifetime, but uses this product's isolated
  database, namespace, configuration and `/data2` paths. It runs no models or user
  commands and receives no sandbox credentials from another deployment.
- Legacy receipts without dispatch ownership retain conservative inventory
  reconciliation. New ownership does not reinterpret historical unknown creates.

## Verification

TDD covers waiter cancellation with a late create result, Worker replacement,
definitive rejection and retry, stale enqueue fencing, service loss without
duplicate creation, profile/handle identity, and terminal cleanup after late
rejection. Run PostgreSQL race checks and the existing allocation/Job suites.
Extend the real isolated quota gate to kill a waiting Worker while the allocation
service completes the supplier call, then verify one create/one execution and
zero owned resources. Rerun P05 and other directly affected recovery cases.
