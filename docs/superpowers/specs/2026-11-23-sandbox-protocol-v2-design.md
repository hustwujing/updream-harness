# Sandbox Protocol v2 Design

## Goal

Make sandbox lifecycle, durable recovery, Runtime routing and release profiles
provider-neutral. OpenSandbox remains the deployed provider while the protocol
is migrated. Aliyun is enabled only after its ACR/template and real-case gates
pass. A future provider must not require changes in Broker, Pool, Job recovery,
Inspector or RuntimeClient.

## Non-goals

- Do not replace the signed Runtime HTTP contract with provider command/files APIs.
- Do not fail over a running Turn or accepted Job to another provider.
- Do not store business ownership in provider metadata.
- Do not codify an Ali template builder before the minimal manual request succeeds.

## Durable Identity

Every allocated sandbox has an immutable handle:

```python
SandboxHandle(
    provider="aliyun",
    sandbox_id="sbx-...",
    allocation_id="turn-or-job-id",
    profile_key="...",
    revision="template-build-revision",
)
```

`provider + sandbox_id` is the lookup identity. `allocation_id` fences create
idempotency. `profile_key + revision` fences compatibility. Turns persist the
complete handle before Runtime injection. Job attempts persist it in the
provisional acceptance receipt before Runtime injection. Sandbox controls and
port grants persist it before ownership leaves the Broker.

Database rows keep the existing indexed `sandbox_id` projection for API and
operator queries, plus a versioned `sandbox_handle_json` value. New allocations
must have a complete handle. Provider switches remain forbidden while a Turn,
Job cleanup or sandbox control is active, so no runtime legacy fallback is
needed.

## Lifecycle Contract

The application-facing backend contract is:

```python
create(request) -> Sandbox
get(handle) -> Sandbox
reconcile_create(scope, allocation_id) -> Sandbox | None
list_owned(scope) -> list[Sandbox]
renew(sandbox, force=False) -> Sandbox
endpoint(sandbox, port, force_refresh=False) -> SandboxEndpoint
delete(sandbox) -> None
runtime_route(sandbox) -> SandboxRuntimeRoute
aclose() -> None
```

`SandboxCreateRequest` contains Harness, frozen Profile, environment, TTL and
`allocation_id`; it has no business metadata. Provider adapters may write
immutable namespace/allocation/profile labels internally. `find(metadata)` and
metadata mutation are not application contracts.

Create either uses provider-native idempotency or bounded allocation
reconciliation. Delete returns only after absence converges. Cancellation is
never translated into a provider error.

## Behavioral Capabilities

```python
SandboxCapabilities(
    native_idempotency=False,
    scoped_inventory=True,
    ttl_renewal=True,
    arbitrary_ports=True,
    authenticated_runtime_http=True,
    delete_convergence=True,
    pause_resume=False,
)
```

The primary gate requires scoped inventory, TTL renewal, arbitrary authenticated
Runtime HTTP and convergent deletion. Native idempotency and pause/resume are
optional. Providers without metadata remain valid when they offer ClientToken
or another native allocation identity.

## Backend Registry

`SandboxBackendRegistry` owns backends by provider and freezes one active
provider for new allocations. Dispatch of `get`, endpoint, renewal and deletion
uses `SandboxHandle.provider`, never the current configuration string. The
initial production registry contains only the configured provider; tests may
contain multiple providers. This supports future mixed cleanup without enabling
automatic execution failover.

## Provider Release Catalog

Profile Catalog schema v2 replaces `image_refs/image_digests` with provider
releases:

```json
{
  "provider_releases": {
    "opensandbox": {
      "codex": {
        "ref": "registry/runtime@sha256:...",
        "revision": "sha256:...",
        "artifact_digest": "..."
      }
    },
    "aliyun": {
      "codex": {
        "ref": "template-id",
        "revision": "template-build-revision",
        "artifact_digest": "source-image-sha256"
      }
    }
  }
}
```

The active provider is selected while resolving a profile. Scheduler receipts
retain `artifact_digest` for provenance but recovery compares Handle profile and
revision, not `image.endswith(@sha256)`.

Provisioning is outside the lifecycle backend:

- OpenSandbox provisioner publishes immutable images.
- Aliyun provisioner publishes ACR images and Templates.
- Future provisioners publish Tools, Snapshots or provider-native releases.

## Runtime Route

`SandboxRuntimeRoute` contains the sandbox-visible Gateway and proxy origins.
The backend returns the route for an allocated Sandbox. Broker and Child Harness
injection consume the route without checking provider names.

- OpenSandbox route may use `172.31.0.1`.
- Aliyun route uses the host private origin, currently `10.194.49.8`.
- Future providers may use PrivateLink, routed VPC or signed public ingress.

The Gateway may expose multiple explicitly configured listeners; it must never
bind all interfaces implicitly. Provider credentials are never injected into a
sandbox.

## Shared Conformance

Every backend runs the same contract cases:

1. create/get and complete Handle identity;
2. repeated allocation does not duplicate;
3. accepted response loss reconciliation;
4. namespace/scope isolation;
5. TTL threshold and forced renewal;
6. authenticated endpoint and refresh;
7. external deletion;
8. convergent and repeated delete;
9. warm claim and same-Thread reuse without provider ownership metadata;
10. cancellation propagation;
11. auth/rate/capacity/profile error mapping;
12. crash recovery and cleanup from the durable Handle.

Real-provider evidence is recorded separately from deterministic conformance.

## Rollout

1. Add v2 value objects and compatibility-preserving internal adapters.
2. Persist Handles and migrate all recovery/control dispatch.
3. Release on OpenSandbox and run provider protocol plus full K campaign.
4. Upgrade Profile Catalog and materialization to schema v2 on OpenSandbox.
5. Add Runtime Route resolution and verify both OpenSandbox network paths.
6. Manually validate the minimal Ali ACR/template request.
7. Publish Ali provider releases, starting with primary Codex.
8. Run Ali conformance, abnormal cases and K campaign with zero warm targets.
9. Switch the independent Ali deployment only after the no-active-work gate.

Rollback restores the previous release revision and Catalog. Durable Handles
retain the provider needed to delete or diagnose resources; rollback never
converts one provider's ID into another provider lookup.
