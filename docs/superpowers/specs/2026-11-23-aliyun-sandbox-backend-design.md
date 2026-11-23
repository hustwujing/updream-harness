# Aliyun Sandbox Backend Design

## Goal

Add an Alibaba Cloud Agent Sandbox Express adapter behind the existing
`SandboxBackend` protocol without changing the deployed OpenSandbox backend.
The adapter must support real lifecycle canaries now and must fail closed when
asked to serve as the primary Harness backend before all required capabilities
are available.

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
- `endpoint`: `connect`, `get_host(port)`, HTTPS, and `X-Access-Token` from the
  returned traffic token when present
- `aclose`: clear adapter caches; the E2B SDK owns its HTTP client lifecycle

Create always adds namespace, idempotency key, harness, profile ID and profile
compatibility key to control-plane metadata. A transport or provider failure
after create may have been accepted is reconciled by the idempotency metadata.
The adapter never blindly replays create.

## Metadata Capability Gap

The current Agent Sandbox Express E2B-compatible API accepts control-plane
metadata at create time and supports metadata-filtered list, but exposes no
operation to update that metadata. The installed `e2b==2.31.0` generated client
also has no metadata update endpoint. This corrects the earlier provider survey
which conflated another Alibaba sandbox API with Agent Sandbox Express.

Consequences for this phase:

- `metadata_filter=True`, `metadata_update=False`.
- `update` may renew TTL, and may accept metadata only when it is already equal
  to the provider value. Any actual metadata mutation fails explicitly.
- `SandboxCapabilities.require_primary_runtime()` continues to reject Aliyun,
  so configuration cannot silently switch the live Harness workload.
- Ali warm-pool binding and cross-turn metadata mutation are not enabled yet.

The follow-up protocol change will make mutable thread/turn ownership durable
in the platform database while retaining immutable provider metadata for
namespace ownership and create reconciliation. It must be completed before an
Aliyun production switch.

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
- Factory tests cover explicit Aliyun construction and the primary-runtime
  capability gate.
- Existing OpenSandbox and Fake tests remain green.
- A real canary may use the existing ready Alibaba template, but no deployment
  or backend switch occurs in this phase.
