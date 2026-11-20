# Parallel Platform Deployment

This product has an independent program directory, PostgreSQL cluster, ports, service names,
Redis/object prefixes, OpenSandbox namespace and Nginx path. Shared executables and immutable
images may be reused; mutable state may not.

| Resource | Isolated identity |
| --- | --- |
| Source | `/data2/wujing03/sandbox-harness-platform-parallel` |
| Writable state | `/data2/wujing03/sandbox-harness-platform-parallel/.data` only |
| API | `127.0.0.1:24766` |
| PostgreSQL | `127.0.0.1:28432`, role/database `sandbox_harness_parallel` |
| Redis | shared executable/server, key prefix `sandbox-harness-parallel:` |
| OpenSandbox | shared service, metadata namespace `sandbox-harness-parallel` |
| Gateway / proxy | `172.31.0.1:29880` / `172.31.0.1:29881` |
| Objects | shared service, prefix `sandbox-harness-parallel/` |
| Registry | `127.0.0.1:25001/sandbox-harness-parallel/*` |
| Nginx | `/sandbox-harness-platform-parallel/` |
| FRPC listener | loopback PROXY v2 `127.0.0.1:18091` |

The deployment never starts, stops or reconfigures shared Redis, Docker, Registry,
OpenSandbox or object storage. Systemd definitions may live under `/etc/systemd/system`; all
credentials, databases, logs, caches, temporary data, bundles and runtime state remain under
`.data` on `/data2`.

## Services

- `sandbox-harness-parallel-postgresql`: fresh schema authority; no migration framework.
- `sandbox-harness-parallel-network`: narrow bridge firewall grant for Gateway ports only.
- `sandbox-harness-parallel-gateway`: model, Capability, Artifact and proxy authorization.
- `sandbox-harness-parallel-api`: browser/API, Primary Harness Broker and continuation outbox.
- `sandbox-harness-parallel-job-worker`: NodeAgent plus Child/Reviewer Durable Job execution.
- `sandbox-harness-parallel-media-worker`: existing durable media Provider execution.
- `sandbox-harness-parallel-site-worker`: immutable static-site publication.

The Job Worker registers only digest-pinned `RuntimeDependencyProfile x Harness` combinations
from the frozen Profile Catalog. Missing or incomplete image matrices fail startup. Accepted Jobs
carry external handles and Sandbox identities in PostgreSQL; a replacement worker adopts the exact
Sandbox and resumes status collection. Graceful shutdown preserves active Child Sandboxes, while
`SIGKILL` relies on lease expiry and the same receipt-based adoption path.

## Runtime Delivery Units

1. L1 Bootstrap: non-root UID/GID 10001, `tini`, health endpoint and signed Code Bundle verifier.
2. L2 Common: shared Runtime dependencies and generic CLIs.
3. L3 Profile: one of `primary-full-cpu`, `general-cpu`, `media-cpu`, `office`, `web-browser` or
   `data-analysis`.
4. L4 Harness: only Codex, Claude or DeepSeek native client.

Runtime Code and Skill content are signed dynamic bundles, not image layers. Build only changed
Profile/Harness combinations during development. A release requires the complete signed matrix,
immutable registry digests and retained prior images/bundles for in-flight recovery.

## Installation

```bash
cd /data2/wujing03/sandbox-harness-platform-parallel
deploy/configure-local.py \
  --source-env /data2/wujing03/sandbox-harness-platform/.data/config/platform.env \
  --runtime-signing-key /data2/wujing03/sandbox-harness-platform/.data/config/runtime-bundle-signing.pem
bash deploy/install-units.sh
bash deploy/init-postgresql.sh
```

`configure-local.py` copies only explicitly allowlisted shared credentials. It generates new
database/session/Gateway secrets, rejects predecessor identities, rejects occupied dedicated ports
unless `--allow-running` is explicit, and writes only `.data/config/platform.env`.

Before service start:

1. Build the complete Profile/Harness image matrix and signed Profile Catalog.
2. Build the frontend and run `deploy/check-service-access.sh`.
3. Run `deploy/record-release.py` to freeze revision, the freshly built
   `web/dist` digest, Code Bundle, Skill Bundle and catalogs. The command builds
   the frontend from the selected `code_root`; a pre-existing dist is never
   trusted as release evidence.
4. Initialize the fresh database and start only the parallel-prefixed units.
5. Verify API/Gateway health, Job Worker node heartbeat, Nginx HTTP/SSE/WebSocket, file previews,
   terminal/takeover and restart recovery.

FRPC ingress always sets `X-Updream-Ingress: frpc`: anonymous API access is denied and registration
is unavailable. Direct loopback ingress retains the existing anonymous/account behavior. Neither
Nginx listener exposes PostgreSQL, Redis or Gateway bridge ports.

The standalone `deploy/nginx-frpc.conf` listens on `18091` when Parallel owns a
dedicated FRPC mapping. When the existing public `8090` endpoint is shared by
path, install `deploy/nginx-frpc-shared-location.conf` as an Nginx snippet and
include it inside the existing PROXY v2 server on `127.0.0.1:18090`. The shared
location keeps the same `frpc` authentication boundary and forwards only to the
independent Parallel API at `127.0.0.1:24766`.

The independent Ali deployment uses the same shared public `8090` endpoint at
`/sandbox-harness-platform-parallel-ali/`. Include the versioned
`deploy/nginx-frpc-ali-shared-location.conf` inside the existing PROXY v2 server
on `127.0.0.1:18090`; it forwards only to the Ali API at `127.0.0.1:24767`.
The root Nginx configuration contains only the include directive and must not
store Ali logs, caches, temporary files or application state.

## Release Safety

- Query active Turns and Jobs before a planned restart; do not treat graceful shutdown as a
  correctness requirement.
- Never delete namespace Sandboxes in `ExecStop`.
- Never replace an immutable image tag or remove a referenced Bundle during active/queued work.
- A release is complete only after current-revision real OpenSandbox and browser evidence passes.
- Rollback selects prior immutable Git/image/bundle/catalog digests; it never edits persisted facts.
