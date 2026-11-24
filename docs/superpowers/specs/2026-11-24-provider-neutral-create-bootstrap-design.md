# Provider-Neutral Create And Bootstrap Design

## Intent

Remove two protocol-level assumptions exposed by the Ali canary without adding
Ali- or case-specific branches: a fresh allocation must not pay a full
inventory reconciliation window before create, and Runtime bootstrap must not
assume an image-time UID or a persistent `/run` directory.

## Constraints

- Preserve the Sandbox Protocol lifecycle, durable handle fencing, namespace
  isolation, accepted-create reconciliation, and delete convergence.
- OpenSandbox, Aliyun and future providers use the same create outcome states.
- Provider-specific SDK and endpoint behavior remains inside adapters.
- Runtime state contains no credentials and remains sandbox-lifetime ephemeral.
- No root-partition storage is introduced on the host.

## Approaches

1. Keep pre-create inventory polling and tune Ali retry counts. Rejected: it
   keeps provider latency on every fresh allocation and is Ali-specific.
2. Move idempotency entirely to the provider. Rejected: providers without
   native idempotency still need accepted-response-loss recovery.
3. Use the protocol's allocation handle as the local first fence, create once,
   and reconcile only after an ambiguous create outcome. Selected: it preserves
   at-most-one submission while removing unnecessary control-plane reads.

For bootstrap storage, hardcoding another image path is rejected. The Runtime
will resolve an explicit `SANDBOX_RUNTIME_STATE_DIR` when configured and
otherwise create a private, current-UID directory below the platform temporary
directory. All bootstrap and compatibility CLI consumers use the same resolver.

## Behavior

- A fresh `create(request)` performs one provider create without pre-polling
  inventory.
- A repeated request already known to the adapter reuses the cached allocation.
- A create call that fails with an unambiguous authentication, rate, or profile
  error fails immediately.
- Any other create exception is reconciled by allocation ID without replaying
  create. No match produces `SandboxCreateOutcomeUnknown`.
- Bootstrap code releases are activated in a private directory writable by the
  current runtime UID. Missing or unwritable `/run/harness` is not an error.
- OpenSandbox behavior and release integrity checks remain unchanged.

## Verification

- Unit tests prove no inventory call occurs before a successful fresh create.
- Existing response-loss and repeated-create tests continue proving no replay.
- Bootstrap tests run with an absent/unwritable legacy `/run/harness` equivalent
  and prove signed activation, child startup and CLI dispatch.
- Focused adapter/runtime suites, full suite, real Ali provider protocol cases,
  then K-series canary run in that order.

