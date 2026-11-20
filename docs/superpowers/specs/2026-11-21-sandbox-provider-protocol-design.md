# Sandbox Provider Protocol Design

## Intent

Replace the OpenSandbox-shaped lifecycle seam with a provider-neutral protocol
that preserves the platform's existing `sandbox-harness-runtime` contract. The
first production implementation remains OpenSandbox. Alibaba Cloud Agent
Sandbox and Tencent Cloud Agent Sandbox research defines the portability
boundary, but neither cloud provider is integrated in this phase.

## Goals

- Express lifecycle, identity, recovery, lease and endpoint semantics without
  image-, template- or vendor-specific fields in the public protocol.
- Move OpenSandbox behind the new protocol without changing deployed behavior.
- Keep `Broker`, `SandboxPool`, `RuntimeClient`, checkpointing, takeover and
  Child Harness behavior intact.
- Make provider creation idempotency and unknown outcomes explicit.
- Establish one conformance suite reusable by future providers.
- Add real-machine abnormal lifecycle cases and pass the complete K campaign
  before release.

## Non-Goals

- Implement Alibaba Cloud or Tencent Cloud credentials and API clients.
- Replace `sandbox-harness-runtime` with E2B or AGS command/filesystem APIs.
- Migrate PostgreSQL, Redis, object storage or the Gateway.
- Change the user-facing thread, turn, job or artifact contracts.

## Research Summary

OpenSandbox creates from immutable image references, stores mutable metadata,
renews expiration and returns authenticated HTTP endpoints for arbitrary ports.

Alibaba Cloud Agent Sandbox Express uses the E2B SDK. It creates from a
template, supports connect/list/kill/timeout and port hosts, and separates
template/warm-pool management from the sandbox data plane.

Tencent Cloud AGS creates from a Tool, Snapshot or source sandbox. Its
`StartSandboxInstance` accepts a 64-character `ClientToken`, timeout, metadata,
mounts and auth mode. It supports list/get/update metadata and timeout, stop,
connect and authenticated arbitrary-port hosts.

The common product boundary is therefore a profile-based sandbox lease with
explicit idempotency, metadata, TTL and endpoint resolution. Provider-native
command, code and filesystem clients are optional accelerators, not the
platform protocol.

## Approaches

### Provider-neutral lifecycle plus existing Runtime (selected)

Introduce request/result value objects and a lifecycle protocol. A provider
maps `profile_ref` to an image, template, Tool or Snapshot internally. The
runtime remains an HTTP service reached through a resolved endpoint. This
preserves all durable execution semantics and minimizes release risk.

### Generic command/filesystem sandbox API

Replace RuntimeClient with the intersection of E2B, Tencent AGS and other
command/filesystem SDKs. This loses the platform's streaming event, execution
reattach, interaction, capability, checkpoint and takeover contracts and would
require a broad rewrite. Rejected.

### Keep the OpenSandbox API as the common protocol

Make every provider emulate image creation, metadata query strings and
OpenSandbox response shapes. This is quick for a second provider but makes
template- and snapshot-based providers increasingly brittle. Rejected.

## Protocol

`SandboxProfile` contains a portable `profile_id`, an immutable `revision`, a
provider-owned `provider_ref` and a `compatibility_key`. OpenSandbox resolves
`provider_ref` to an immutable image digest; Alibaba resolves it to a template
version; Tencent resolves it to a Tool or Snapshot. The compatibility key is a
hash of provider, provider reference, runtime contract and frozen Runtime
Bundle/Code Bundle digests. Only equal keys may reuse an attached or warm
sandbox. Existing per-Turn and Child Job image overrides become explicit
OpenSandbox profiles and retain their exact digest.

`SandboxCreateRequest` contains:

- `harness`: logical Harness identity.
- `profile`: resolved immutable `SandboxProfile` selected by a
  `SandboxProfileResolver`.
- `environment`: runtime environment injected at creation.
- `metadata`: bounded platform ownership and recovery labels.
- `ttl_seconds`: requested lease duration.
- `idempotency_key`: stable key for exactly one logical provision request.

`SandboxInstance` contains provider-neutral observed state: ID, provider,
Harness, profile and compatibility key, metadata, health, expiry and local reuse markers.
It does not expose image or template as the portable identity. Provider details
may be retained in a bounded `provider_details` diagnostic map.

`SandboxEndpoint` contains URL, secret request headers excluded from repr and
telemetry, and an optional monotonic expiration deadline. The backend owns the
endpoint cache. `RuntimeClient` receives the backend as an endpoint resolver and
resolves port 8080 before every operation. The backend refreshes once the
deadline is within a 30-second skew and coalesces concurrent refreshes per
sandbox/port. Runtime HTTP calls are never automatically replayed after 401/403;
the next operation forces endpoint refresh, avoiding duplicate mutating calls.

Required operations:

- `create(request)`
- `get(sandbox_id)`
- `list(scope)` and `find(metadata)` for reconciliation; both exhaust provider
  pagination and return complete results for the exact namespace scope
- `update(sandbox, metadata, ttl_seconds)`
- `delete(sandbox)`
- `endpoint(sandbox, port, force_refresh=False)`
- `aclose()`

The backend exposes immutable `provider` and `capabilities`. Primary Harness
execution requires metadata update, exact metadata filtering, arbitrary ports,
TTL renewal, idempotent create, authenticated Runtime HTTP and client shutdown.
Startup fails if any are absent; there is no silent fallback. Metadata queries
are exact AND matches within one namespace. A patch value of `None` removes the
key. Providers validate at most 64 metadata entries, 128 characters per key and
512 characters per value before sending a request.

Lifecycle state is `PROVISIONING`, `READY`, `STOPPING`, `STOPPED`, `FAILED` or
`UNKNOWN`. `create` returns only after the Runtime health endpoint is ready.
`get` reconnects and returns a ready instance or reports the observed terminal
condition; `list/find` are nonblocking inventory observations. Expiry is an
aware UTC timestamp on the instance and a monotonic cache deadline on endpoint
access. `update` returns the refreshed instance. `delete` is idempotent and
returns only after absence is observed or the provider confirms terminal
deletion.

## Error Contract

All providers map native errors into:

- `SandboxNotFound`
- `SandboxCapacityUnavailable`
- `SandboxRateLimited`
- `SandboxAuthenticationFailed`
- `SandboxProfileUnavailable`
- `SandboxEndpointUnavailable`
- `SandboxProviderUnavailable`
- `SandboxCreateOutcomeUnknown`

Errors carry `retryable`, `code`, provider and optional request ID. The action
contract is: NotFound removes local ownership and permits a fresh acquire;
Capacity/RateLimited wait with bounded backoff; ProviderUnavailable retries on
the supervisor/recovery cadence; Authentication/Profile failures mark backend
health unavailable and are terminal for the current request; Endpoint failure
forces resolution for the next call without replaying a mutating Runtime call.
Unknown create outcome runs reconciliation immediately and on recovery scans,
but is never blindly replayed. Zero matches remain durably uncertain, one match
is adopted and multiple matches raise a duplicate-invariant failure.

## Recovery And Idempotency

The platform continues recording sandbox IDs on durable Turns and Jobs. Normal
recovery calls `get` with that durable ID first. Metadata search is used only
when create outcome is unknown, when an old record lacks an ID, and during
bounded startup orphan reconciliation. Duplicate matches are never selected by
sort order. Provider metadata is defense-in-depth, not the only source of
ownership. `idempotency_key` is always explicit: OpenSandbox maps it to the
request ID and turn metadata, Tencent maps it to `ClientToken`, and future E2B
adapters map it to supported metadata plus list reconciliation.

This phase keeps one configured provider for the complete deployment. A release
must reject provider changes while any Turn, Job or sandbox control is
nonterminal. That invariant avoids resolving a durable sandbox ID against the
wrong provider until provider identity is added to a future multi-provider
durable schema.

A create transport failure raises `SandboxCreateOutcomeUnknown` only after a
bounded provider reconciliation attempt. It must never be treated as a clean
pre-accept failure that can be blindly replayed.

## Pool And Runtime Boundary

`SandboxPool` keeps thread reuse and local capacity admission. A provider's
managed warm pool is an implementation detail behind `create`; it is not
represented by a second application pool abstraction.

Pool binding calls `update` to remove the warm marker, add thread/turn metadata
and renew TTL. Runtime endpoint refresh is separate from TTL renewal even when
one provider call can satisfy both.

`RuntimeClient` still speaks the existing signed HTTP protocol. It resolves a
fresh endpoint before each operation and keeps request replay semantics
unchanged.

## Module Layout

- `sandbox_contracts.py`: value objects, protocol, capabilities and errors.
- `sandbox_opensandbox.py`: OpenSandbox adapter and HTTP response mapping.
- `sandbox_factory.py`: the single configuration-to-backend construction path.
- `sandbox.py`: compatibility exports during migration, then Fake backend only.
- `pool.py`, `runtime.py`: consume the new values without vendor names.

## Real-Machine Abnormal Cases

Add four OpenSandbox provider-protocol cases:

1. A fault-injecting transport forwards a real create request, consumes the
   successful response and raises a synthetic read failure; reconciliation
   returns exactly one real sandbox for the idempotency key.
2. Sandbox is externally deleted between attachment and endpoint refresh; the
   protocol reports NotFound and the pool releases capacity without leakage.
3. The adapter's endpoint cache deadline is forced into the past while the real
   sandbox remains alive; a new real endpoint is resolved and the same sandbox
   continues to pass Runtime health.
4. Delete is repeated and the second delete converges successfully; namespace
   cleanup remains empty.

The external deletion case also proves a subsequent acquire succeeds, user
capacity is released and managed/retiring inventories settle empty. These cases
create unique metadata, retain request/evidence IDs and clean only their own
resources. They run before the K campaign.

## Release And Acceptance

- OpenSandbox contract tests and focused pool/runtime/broker tests pass.
- Four new abnormal real-machine cases pass against deployed OpenSandbox.
- K01-K14 complete as one clean campaign on the release revision.
- Full Python suite, frontend tests/build and release gate pass.
- Runtime bundles are rebuilt only if runtime deployable content changes.
- Ali transient services are restarted only after active-turn preflight.
- Direct and FRPC ingress, health, warm pool and cleanup verify after release.

Rollback deploys the previous revision and bundles. It does not change provider
resources, shared OpenSandbox state or the original Parallel deployment.
