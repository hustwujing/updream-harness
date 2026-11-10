# Deployment

The product has its own API, Gateway, database objects and data namespace. It reuses the host's existing infrastructure programs. Isolation is enforced by names and ownership, not by running duplicate dependency services.

| Resource | Shared program | This product's isolated space |
| --- | --- | --- |
| PostgreSQL | `127.0.0.1:5432` | database and role `sandbox_harness` |
| Redis | `127.0.0.1:6379` | key prefix `sandbox-harness:` |
| Docker/runsc | `opensandbox-docker.service` | image repository `sandbox-harness/*` |
| image registry | `127.0.0.1:25001` | repository prefix `sandbox-harness/` |
| OpenSandbox | `sandbox-agent-opensandbox.service`, `127.0.0.1:28090` | metadata `sandbox-harness.io/namespace=sandbox-harness` |
| BOSS/S3 | existing endpoint and bucket | object prefix `sandbox-harness/` |
| local state | host filesystem | `/data2/wujing03/sandbox-harness-platform/.data` |
| web/API | this product | `127.0.0.1:22766`, route `/sandbox-harness/` at the reverse proxy |
| capability Gateway | this product | sandbox bridge `172.31.0.1:28600` |
| public HTTP/S proxy | this product | sandbox bridge `172.31.0.1:28601` |

The deployment must not start another PostgreSQL, Redis, Docker daemon, registry, object store or OpenSandbox server. It must not change the configuration, storage directory or lifecycle of an existing dependency.

## Isolation Rules

- Every OpenSandbox create request includes the product namespace. Reconciliation and cleanup query both namespace and Turn identity.
- Runtime images use immutable digests under `127.0.0.1:25001/sandbox-harness/*`.
- PostgreSQL migrations connect only as the `sandbox_harness` role to the `sandbox_harness` database. The role receives no privileges on other databases.
- Every Redis key starts with the configured prefix. Lua scripts and scans must include that prefix.
- Every checkpoint, attachment and artifact key starts with the configured BOSS/S3 prefix. Sandboxes never receive object-store credentials.
- API and Gateway secrets live in a product-specific environment file. Existing project environment files are read only during initial operator configuration and are never sourced by the running service.
- The shared OpenSandbox Docker network is internal and has no public NAT. Its host firewall permits `172.31.0.0/24` to reach only the existing runtime range plus this product's `172.31.0.1:28600-28601`; port 28601 proxies public HTTP/S only and rejects private destinations after DNS resolution. OpenSandbox rejects per-sandbox `networkPolicy` on this user-defined network, so create requests do not claim unsupported enforcement.

## Installation Order

1. Verify the shared services are healthy; do not restart them.
2. Create the product PostgreSQL role and database, then run this repository's schema initialization.
3. Build the three runtime images through the shared Docker socket and push them under the product registry namespace.
4. Record immutable image digests in the product environment file.
5. Install the product API, Gateway and its narrow bridge firewall oneshot. The installer leaves them stopped for configuration review.
6. Run a real OpenSandbox smoke Turn and verify its metadata namespace, image digest, event stream, checkpoint and artifact prefix.

The API uses 30-second Turn leases, 10-second heartbeats, a 15-second expired-lease scan and a 60-second orphan scan by default. Normal API shutdown preserves currently executing sandboxes so the replacement process can reattach them. Do not add an `ExecStop` command that deletes namespace sandboxes.

Local UI tests may use SQLite, local content-addressed objects and the explicit fake lifecycle backend. Those results do not count as a deployment or real OpenSandbox validation.

The foundation image tag includes the SHA-256 prefix of `runtime/foundation.Dockerfile`. `runtime/toolchain.lock.json` is verified while building the common runtime base, and the three final Harness images must expose the same locked toolchain before their immutable registry digests are deployed.

Before restarting API/Gateway after adding source files or building the frontend,
run `bash deploy/check-service-access.sh`. It grants the service user read/traverse
access only to `src` and `web/dist`, then imports both services and reads every web
asset as that user. Root-only unit tests do not detect unreadable new modules.
