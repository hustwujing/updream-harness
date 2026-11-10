# Architecture and Ownership

## Applies to

Use this map to locate a change. Read the referenced module before relying on
the summary.

## Host control plane

| Module | Current ownership |
| --- | --- |
| `app.py` | FastAPI routes, browser identity middleware, owner checks, uploads/downloads, SSE and static web serving |
| `domain.py` | Public Pydantic contracts, enums and identifier helpers |
| `store.py` | SQLAlchemy tables, inline schema upgrades, transactions, event/item persistence and Redis notification integration |
| `broker.py` | Turn scheduling, leases, recovery, Runtime orchestration, checkpoint/artifact publication and cleanup |
| `pool.py` | Pristine warm pool, Thread-attached sandboxes, capacity and deletion retry ownership |
| `sandbox.py` | OpenSandbox HTTP adapter, metadata reconciliation and fake backend |
| `runtime.py` | Typed host client for the in-sandbox Runtime API and reconnecting event streams |
| `objects.py` | Content-addressed local and S3/BOSS storage with size/hash validation |
| `sessions.py` | Signed anonymous browser identity cookie; owner ID is its hash |
| `events.py` | Redis Streams wake-up/bounded replay aid; PostgreSQL remains authoritative |

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

Browser → API → Store/Broker → Pool/Runtime/Object store. Sandbox Runtime →
Capability Gateway → configured upstream. Gateway must not call the API to make
business decisions, and Runtime must not connect to PostgreSQL, Redis or object
storage.

## Completion

A change stays within its owner module or explicitly updates every crossed
contract. Cross-boundary changes require tests at both sides; a unit test of one
side alone is insufficient.
