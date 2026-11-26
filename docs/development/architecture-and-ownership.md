# Architecture and Ownership

## Applies to

Use this map to locate a change. Read the referenced module before relying on
the summary.

## Host control plane

| Module | Current ownership |
| --- | --- |
| `app.py` | FastAPI routes, browser identity middleware, owner checks, uploads/downloads, SSE and static web serving |
| `platform_service.py` | Multi-active Turn/continuation claims and database-fenced singleton supervisors |
| `domain.py` | Public Pydantic contracts, enums and identifier helpers |
| `common/db/accounts.py` | account, session and anonymous-principal registration authority |
| `common/db/core.py` | Thread, Turn, Job, artifact, lifecycle and sandbox-ownership authority |
| `common/db/media_repository.py` | media draft, execution, reference and output authority |
| `common/db/site_repository.py` | static-site draft, deployment and version authority |
| `common/db/handoff.py` / `lifecycle.py` | ordered cross-domain commands, receipts and lifecycle barriers |
| `turn_broker.py` | Turn scheduling, leases, recovery, Runtime orchestration, checkpoint/artifact publication and cleanup |
| `pool.py` | Pristine warm pool, Thread-attached sandboxes, capacity and deletion retry ownership |
| `sandbox_contracts.py` / `sandbox_factory.py` | provider-neutral lifecycle, capability gate and configured backend construction |
| `sandbox_opensandbox.py` / `sandbox_aliyun.py` | provider adapters; provider metadata is limited to namespace, allocation reconciliation and profile inventory |
| `sandbox.py` | fake backend and compatibility exports |
| `runtime.py` | Typed host client for the in-sandbox Runtime API and reconnecting event streams |
| `objects.py` | Content-addressed local and S3/BOSS storage with size/hash validation |
| `site_bundle.py` / `site_worker.py` | bounded static-site packages, durable deployment Jobs and atomic immutable version activation |
| `sessions.py` | Signed anonymous browser identity cookie; owner ID is its hash |
| `events.py` | Redis Streams wake-up/bounded replay aid; PostgreSQL remains authoritative |
| `job_coordinator.py` / `job_worker.py` | immutable JobGroup preflight and generic accepted-handle execution/recovery |
| `scheduler.py` / `node_agent.py` | registered Profile/resource inventory and fenced reservations |
| `job_allocation.py` | Shared Child/Reviewer/Operation allocation ownership, frozen identity and original reservation cleanup |
| `allocation_dispatch.py` / `allocation_service.py` | Durable provider create ledger and independent allocation service; unknown outcomes reconcile without another create |
| `harness_job.py` / `job_service.py` | Child/Reviewer execution through the configured SandboxBackend and production worker lifecycle |
| `job_continuation.py` | all-terminal outbox batching back into the Primary Harness Session |
| `image_decompose_adapter.py` | database-only handoff of the authorized Operation; exact legacy receipt import |
| `media_worker.py` / `media_provider.py` | shared media leases, submission boundaries, cancellation, output staging and terminal commit |
| `image_decompose_provider.py` | host-only HTTP/session transport for the independent image-layering inference service |

## Gateway

| Module | Current ownership |
| --- | --- |
| `gateway.py` | Capability authorization, upstream streaming, revocation and service lifecycle |
| `capability.py` | Signed versioned claims and route-prefix verification |
| `model_routes.py` | Exact Harness/model route overrides and credential environment references |
| `model_discovery.py` / `model_health.py` | Catalog discovery, protocol probes and persisted availability reduction |
| `model_activity.py` | Durable model wait Items and request timing |
| `public_proxy.py` | Authenticated HTTP/CONNECT proxy, public-address validation and destination policy |
| `telemetry_ingest.py` / `observability.py` | Runtime OTLP intake and bounded structured telemetry |

## Sandbox data plane

| Module | Current ownership |
| --- | --- |
| `sandbox_runtime/app.py` | Runtime HTTP API, Harness subprocess, event journal, inputs, interactions, jobs, workspace and outputs |
| `adapters.py` | Harness command construction and native-line normalization |
| `codex_runner.py` | Codex app-server bridge, steering and native interactions |
| `deepseek_runner.py` | DeepSeek SDK Session bridge and public notifications |
| `background_cli.py` / `interaction_cli.py` | Harness-facing Runtime control CLIs |
| `runtime/*.Dockerfile` | Immutable foundation, common Runtime and Harness-specific images |

## Frontend

`web/src/api.js` owns API calls and public error mapping. `main.jsx` owns Thread
selection, SSE, transcript, composer, interactions, project files and inspector.
`AgentMarkdown.jsx` is the Markdown/file-link boundary. CSS is split across
`styles.css`, `enhancements.css` and `mobile.css`.

## Dependency direction

Browser → API → domain repositories → PlatformWorker/Broker → Pool/Runtime/Object store. Core
emits durable commands to Media and Sites; no business transaction spans
database authorities. Sandbox Runtime → sandbox-gateway → configured upstream.
Gateway must not call the API to make business decisions, and Runtime must not
connect to PostgreSQL, Redis or object storage.

## Completion

A change stays within its owner module or explicitly updates every crossed
contract. Cross-boundary changes require tests at both sides; a unit test of one
side alone is insufficient.
