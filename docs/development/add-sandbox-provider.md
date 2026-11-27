# Adding a Sandbox Provider

Inventory reconstruction must preserve the immutable creation allocation ID in
`SandboxHandle.allocation_id`; it is distinct from the provider's sandbox ID.
Test `reconcile_create` with an empty process cache and compare both allocation
identity and profile key. The database restores the frozen release revision;
provider inventory does not own lifecycle or recovery decisions. Each adapter
supplies an explicit handle from its validated provider response. The shared
Sandbox fallback never interprets provider metadata; future adapters may use
another correlation representation without changing the common state machine.

## Applies to

Use this guide to add a sandbox infrastructure backend alongside OpenSandbox
and Ali Sandbox, or to change that backend's lifecycle, endpoints or release
mapping. Adding a model upstream follows [Model Provider guidance](add-model-provider.md);
adding a native Agent follows [Harness guidance](add-harness.md).

The existing implementation uses explicit registration. A new provider requires
code and deployment configuration changes; it is not an arbitrary URL or an
automatically discovered plugin. Once registered and verified, select it with
`SANDBOX_HARNESS_BACKEND` while preserving existing provider selections.

## Authoritative contracts

| Source | Responsibility |
| --- | --- |
| [sandbox_contracts.py](../../src/sandbox_harness/common/contracts/sandbox_contracts.py) | Backend protocol, capabilities, profiles, handles, endpoints, routes and typed errors |
| [sandbox_factory.py](../../src/sandbox_harness/common/sandbox/sandbox_factory.py) | Backend construction, capability gate, registry routing and frozen profile resolution |
| [config.py](../../src/sandbox_harness/common/configuration/config.py) / [profiles.py](../../src/sandbox_harness/common/catalog/profiles.py) | Supported backend selection, configuration and provider release matrix |
| [job_allocation.py](../../src/sandbox_harness/common/sandbox/job_allocation.py) / [allocation_dispatch.py](../../src/sandbox_harness/services/allocation_worker/allocation_dispatch.py) | Platform-owned allocation intent, create response, recovery and reservation ownership |
| [pool.py](../../src/sandbox_harness/common/sandbox/pool.py) / [sandbox_ownership.py](../../src/sandbox_harness/common/db/sandbox_ownership.py) | Attachment compatibility, capacity, durable handles and generation fencing |
| [runtime.py](../../src/sandbox_harness/common/sandbox/runtime.py) / [sandbox_inspector.py](../../src/sandbox_harness/common/execution/sandbox_inspector.py) | Runtime requests, endpoint refresh, takeover and terminal routing |

Use [sandbox_opensandbox.py](../../src/sandbox_harness/common/sandbox/sandbox_opensandbox.py) and
[sandbox_aliyun.py](../../src/sandbox_harness/common/sandbox/sandbox_aliyun.py) as adapter examples,
not as permission to copy provider assumptions into shared workers.

## Required capabilities

`SandboxCapabilities.require_primary_runtime()` requires all five capabilities:

- `scoped_inventory`: list and identify only resources owned by the product scope;
- `arbitrary_ports`: resolve the ports required by Runtime and the workbench;
- `ttl_renewal`: keep a running sandbox alive through bounded renewal;
- `authenticated_runtime_http`: authenticated access to Runtime endpoints;
- `delete_convergence`: confirm that a requested deletion actually completed.

`native_idempotency` and `pause_resume` are declared but are not required by
this gate. Platform pause may checkpoint and replace the sandbox. The absence
of native idempotency never permits replay of an uncertain create request.
Capability flags must reflect tested behavior; missing mandatory capabilities
reject the backend before it serves a Primary Harness.

## Implementation sequence

1. **Assess the provider.** Record creation/timeout semantics, ownership filters
   and pagination, TTL behavior, authentication, endpoint expiry, deletion
   confirmation, quota errors and how an allocation can be reconciled after
   response loss. Document the actual CPU/memory/disk/GPU shape and its mapping
   to Runtime Profiles and Scheduler reservations; logical admission capacity
   is distinct from the provider account's real quota. Mark unsupported or
   unverifiable behavior explicitly.
2. **Implement `SandboxBackend`.** Add a provider adapter, normally
   `src/sandbox_harness/sandbox_<provider>.py`. Keep SDK calls and native status
   mapping here. Expose a stable provider identifier, the configured `namespace`
   and truthful `capabilities`; registry and allocation dispatch also consume
   that namespace. The complete method protocol is summarized below.
3. **Register configuration.** Extend `Settings.backend` and add validated
   provider settings and host credential references. Register construction in
   `build_sandbox_backend`, run the capability gate and keep the registry's
   durable-handle dispatch. Update configuration examples and deployment
   launchers; do not add provider branches to Broker, Store, JobWorker or
   business Operations.
4. **Freeze releases.** Populate `provider_releases[provider][harness]` with
   `ref`, `revision` and `artifact_digest` for every supported Harness in each
   Runtime Profile. The Primary profile currently requires Codex, Claude and
   DeepSeek. Sign and verify the catalog. A template ID is a provider reference,
   not proof of an immutable image or revision. Retain image and both Runtime
   Bundle identities in the frozen profile and compatibility key.
5. **Connect Runtime and networking.** Preserve the signed Bootstrap/Code Bundle
   and Content Bundle contract described in [Sandbox and Harness](sandbox-and-harness.md).
   Return authenticated endpoints and an explicit `SandboxRuntimeRoute` for
   Gateway and proxy access reachable from the sandbox. Runtime and Inspector
   use these common interfaces, including terminal/replay and private ports.
   Reuse the current capability, revocation and public-network controls.
6. **Integrate lifecycle ownership.** Reuse Pool, Scheduler, Store and the
   independent allocation dispatcher. Persist the provider/allocation/profile
   intent before create, separately from Runtime acceptance. Keep one allocation
   identity across definite capacity refusals; preserve the original reservation
   and handle through worker replacement, cancellation and cleanup.
7. **Deploy in isolation.** Use dedicated configuration, database identity,
   namespace, application data, units and ports. In this workspace all writes
   stay under `/data2/wujing03`; the original sibling project stays read-only.
   Keep long-lived credentials on the host. Register provider routes in the
   selected launcher instead of assuming an OpenSandbox bridge address.
8. **Verify and publish.** Run the checks below, record the exact revision and
   image/profile/Bundle identities, then follow [Deployment and collaboration](deployment-and-collaboration.md).
   Preserve small, coherent commits and report remaining provider limitations.

| Protocol method | Required behavior |
| --- | --- |
| `validate_release` | Reject unusable or inconsistent provider reference/artifact identity before allocation |
| `create` | Honor the frozen request, TTL and allocation ID; return a `Sandbox` carrying a matching `SandboxHandle`; preserve uncertainty after a possible acceptance |
| `get` | Resolve the exact durable handle and enforce provider, scope and compatibility fences |
| `reconcile_create` | Find the original allocation without creating another; no match alone does not prove an uncertain request was absent |
| `list_owned` | Exhaust pagination and enforce exact scope, including when the provider's server-side filter is approximate |
| `renew` | Renew the same sandbox, retaining identity and respecting its lifecycle |
| `delete` | Support repeated cleanup and confirm absence; retain ownership when deletion is unresolved |
| `endpoint` | Return URL, authentication headers and expiry; honor `force_refresh` for expired credentials/endpoints |
| `runtime_route` | Return the verified Gateway/proxy route for this sandbox's provider |
| `aclose` | Close owned SDK/HTTP resources after in-flight work has been settled |

Provider credentials must not enter `SandboxProfile`, `SandboxHandle`, Runtime
environment, checkpoints or public responses. Endpoint authentication stays in
the host transport boundary. Do not log raw SDK errors, credentials, headers or
unfiltered URLs; use existing allowlisted telemetry and stable error codes.

## Failure and recovery rules

Map native failures to `SandboxProviderError` subclasses at the adapter boundary.
Distinguish not-found, definite capacity refusal/rate limiting, authentication,
profile incompatibility, unavailable endpoints and unknown create outcomes.
Do not infer acceptance from an HTTP status alone without the provider's
documented semantics. In particular, a capacity/readiness failure after creation
was accepted is not a safe pre-accept capacity rejection.

For an uncertain create, keep `SandboxCreateOutcomeUnknown`, the durable
allocation identity and the reservation. Reconcile through the platform ledger
and provider evidence; never issue another create simply because inventory is
temporarily empty. If the provider exposes no authoritative lookup, retain and
report that limitation rather than fabricating absence or successful cleanup.

Lifecycle state belongs to PostgreSQL. Creation metadata may support immutable
scope/identity lookup, but the provider is not required to support mutable
Thread, Turn, lease or pool labels. Recovery must not replace a frozen profile
with inventory placeholders or the newest release configuration. Provider plus
sandbox ID forms identity; identical native IDs from two providers must not
collide. Never reinterpret an old handle through a new active backend.

Confirm external deletion before releasing capacity. Admission saturation must
not block cleanup or observations of accepted executions. Preserve active
capability revocation, cancellation and database-operation settling. See
[Recovery and observability](recovery-and-observability.md) and the
[external error contract](external-error-contracts.md) for these shared rules.

Provider switching is guarded: `SandboxBackendRegistry` rejects switching away
from unresolved active work. The current factory builds only the configured
backend; it is not a live cross-provider migration mechanism. Resolve old
allocations and cleanup obligations before changing the selection, and record
the old and new release/route configuration for rollback.

## Acceptance checks

| Layer | Minimum evidence |
| --- | --- |
| Contracts/factory | `test_sandbox_contracts.py`, `test_sandbox_factory.py`, `test_sandbox_provider_contract.py`, provider-specific transport tests; missing capabilities/configuration fail closed |
| Scope and identity | Namespace rejection, full pagination, same native ID across providers, frozen release compatibility, no mutable-metadata requirement |
| Allocation and cleanup | `test_allocation_dispatch.py`, `test_allocation_dispatch_postgres.py`, `test_job_worker.py`, `test_job_postgres_concurrency.py`; uncertain create, delayed response, replacement, cancellation and confirmed deletion |
| Runtime and workbench | `test_runtime_client.py`, `test_sandbox_inspector.py`, `test_sandbox_namespace.py`; real configure/command, takeover, checkpoint restore, terminal input and reconnect replay |
| Provider faults | Real accepted-response loss, external deletion, endpoint/credential expiry and repeated deletion; distinguish injected transport faults from genuine quota refusal |
| Resource exhaustion | Equivalent RC01–RC07 behavior from the [capacity gate](../test-evidence/sandbox-capacity-gate.md), using an exclusively owned provider scope |
| Product/release | Affected Primary, Child, Reviewer and Operation cases, supported Harnesses, actual browser/health checks and existing-provider regressions |

Test filenames above are under `tests/`. Existing live drivers are examples,
not a universal certification switch: `run_real_sandbox_provider_protocol.py`
contains OpenSandbox-specific fault injection, and
`run_real_aliyun_sandbox_provider_protocol.py` contains Ali-specific fixtures.
`run_real_sandbox_capacity.py` currently uses the isolated Ali PostgreSQL port
28433 and a separately provisioned quota fixture. Adapt test setup for the new
provider while retaining common assertions; do not add provider-specific
production state-machine shortcuts to pass them.

Reproduce bugs with a failing regression before fixing the common boundary.
For subsequent fixes, rerun failed/affected cases and finish unrun cases under
the agreed [campaign policy](testing-and-real-cases.md). Retained results remain
labelled with their original provider and revision; OpenSandbox or Ali evidence
cannot certify the new backend. Report fake, real sandbox, real model/Provider
and browser checks separately. Completion includes owned-resource cleanup and
an explicit list of any unresolved historical allocations.
