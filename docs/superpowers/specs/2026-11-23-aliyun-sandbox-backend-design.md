# Aliyun Sandbox Backend Design

## Goal

Add an Alibaba Cloud Agent Sandbox Express adapter behind the existing
`SandboxBackend` protocol without changing the deployed OpenSandbox backend.
The adapter must support real lifecycle canaries now and preserve the previously
agreed immutable-metadata execution mode.

## Provider Boundary

Runtime lifecycle uses Alibaba Cloud's E2B-compatible data plane with
`e2b==2.31.0`. Template build and API-key management stay outside the runtime
adapter and continue to use the Alibaba Cloud control-plane OpenAPI. A
`SandboxProfile.provider_ref` is an existing template ID or immutable template
release reference; the adapter never creates or mutates templates.

The adapter receives explicit `api_key`, `api_url`, `domain`, namespace and
timeouts. It never reads Alibaba access-key credentials and never logs the E2B
API key or runtime traffic token.

## Supported Mapping

- `create`: `AsyncSandbox.create(template=provider_ref, timeout, metadata, envs)`
- `get`: `AsyncSandbox.get_info` plus `AsyncSandbox.connect` when an endpoint is
  required
- `find/list`: exhaust `AsyncSandbox.list` pagination and enforce an exact AND
  match locally inside the configured namespace
- `renew`: `AsyncSandbox.set_timeout`
- `delete`: `AsyncSandbox.kill`, treating missing instances as converged
- `endpoint`: `connect`, `get_host(port)`, HTTPS, and
  `e2b-traffic-access-token` from the returned traffic token
- `aclose`: clear adapter caches; the E2B SDK owns its HTTP client lifecycle

Create always enables `secure` control access and disables unauthenticated
public traffic. A missing traffic token is an endpoint failure, never a public
fallback. Create adds namespace, allocation/idempotency key, harness, profile ID
profile compatibility key and the original template `provider_ref` as immutable
control-plane metadata. Thread/Turn
ownership remains authoritative in PostgreSQL; provider metadata is only a
safety and reconciliation index. A transport or provider failure
after create may have been accepted is reconciled by the idempotency metadata.
The adapter never blindly replays create.

Delete returns only after namespace inventory confirms absence. Endpoint refresh calls
`connect` again so a rotated traffic token replaces the cached token. A repeated
create first reconciles the allocation key under a per-process lock and adopts
the unique compatible instance instead of submitting another create.
Because Ali inventory metadata is briefly eventually consistent after create,
reconciliation polls through a bounded visibility window. A process-local
allocation cache makes normal same-process repeats immediate. Reconciliation
requires both compatibility key and the frozen original provider reference;
the provider-reported template alias alone is not treated as immutable identity.

## Metadata Capability Gap

The current Agent Sandbox Express E2B-compatible API accepts control-plane
metadata at create time and supports metadata-filtered list, but exposes no
operation to update that metadata. The installed `e2b==2.31.0` generated client
also has no metadata update endpoint. This corrects the earlier provider survey
which conflated another Alibaba sandbox API with Agent Sandbox Express.

Consequences:

- `metadata_filter=True`, `metadata_update=False`.
- `update` may renew TTL, and may accept metadata only when it is already equal
  to the provider value. Any actual metadata mutation fails explicitly.
- Primary capability validation and Pool behavior do not require metadata
  mutation.
- Ali creates a Sandbox with immutable namespace, profile and allocation
  metadata. Durable Thread/Turn/Job Sandbox IDs are the only ownership source.
- A Sandbox may remain attached to the same Thread and be reused across Turns.
  Rebinding renews TTL and updates only platform-owned in-memory/durable state.
- Application warm instances and Alibaba's provider-side template warm pool are
  both supported. Claiming an application warm instance persists its exact ID
  before Runtime execution; no provider label changes.

OpenSandbox and Aliyun therefore expose the same application lifecycle. The
OpenSandbox adapter may support metadata mutation, but the application does not
depend on or invoke it for ownership transitions.

## Metadata Dependency Audit

The Ali deployment has no historical compatibility requirement. The normal
application path never writes or reads `sandbox-harness.io/thread`,
`sandbox-harness.io/turn` or `sandbox-harness.io/pool` provider metadata.

| Area | Authoritative source | Provider metadata use |
| --- | --- | --- |
| Pool claim/reuse | in-memory attachment plus durable Turn/Job Sandbox ID | none; bind renews TTL only |
| Primary recovery/cancel/cleanup | `TurnRow.sandbox_id` | none |
| Child Job recovery/cleanup | persisted acceptance receipt `sandbox_id` | none |
| Startup orphan cleanup | namespace inventory minus durable active Sandbox IDs | namespace boundary only |
| Inspector and port grants | owner-scoped grant/control rows and exact Sandbox ID | none |
| Runtime configuration/telemetry | explicit `Sandbox.thread_id` and `turn_id` assigned by Pool | none |
| Profile compatibility | frozen profile plus provider template/image identity | profile metadata only for inventory |
| Unknown create outcome | explicit allocation/idempotency key | exact allocation metadata query |

Missing durable identity is never repaired by guessing from Thread/Turn labels.
Before Runtime execution it can be recreated; after execution starts it fails
without replay. Provider inventory remains useful for bounded namespace cleanup
and operator diagnostics, not business ownership.

Child allocation is a two-stage durable boundary. Immediately after Pool
acquire, the current Job attempt records a provisional receipt containing the
Sandbox ID and deterministic execution ID; after capability issuance it adds
the nonce. Only then may Runtime injection/start occur. Terminal Jobs retain a
cleanup obligation and scheduler reservation until exact Sandbox cleanup
converges, even if the worker died before final acceptance was recorded.

## State And Errors

E2B `running` maps to `READY`, `paused` to `STOPPED`, and other observed states
map conservatively to protocol states. SDK errors map to the existing typed
provider errors without including native messages that may contain credentials
or endpoints. Authentication, rate limit, not-found, template, timeout/transport
and generic provider failures have distinct mappings.

`SandboxProfile` values created in this process are cached by sandbox ID. For
inventory recovered after restart, profile ID and compatibility key come from
provider metadata and the template ID becomes the recovered revision/reference.

## Acceptance

- Unit tests cover create/get/list pagination/exact filtering, timeout-only
  update, rejected metadata mutation, endpoint auth, delete convergence, error
  mapping and unknown-create reconciliation.
- Factory and Pool tests cover explicit Aliyun construction, warm claim without
  metadata mutation and same-Thread reuse.
- Existing OpenSandbox and Fake tests remain green.
- A real canary may use the existing ready Alibaba template, but no deployment
  or backend switch occurs in this phase.
