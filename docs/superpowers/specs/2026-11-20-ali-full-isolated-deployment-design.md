# Ali Full Isolated Deployment Design

## Intent

Build a complete, independently operated deployment from the
`sandbox-harness-platform-parallel-ali` branch and checkout. The deployment is
the migration base for Alibaba Cloud Agent Sandbox work and must not modify the
original Parallel checkout, branch, database, namespaces or running services.

## Goals

- Run the full API, Gateway, PostgreSQL, Job, Controlled, Media and Site stack.
- Keep all application code and mutable state below
  `/data2/wujing03/sandbox-harness-platform-parallel-ali`.
- Share only preinstalled executables and shared infrastructure services.
- Isolate every shared service through a dedicated database, port, prefix or
  namespace.
- Replace the temporary SQLite and fake backend with a real, fenced execution
  deployment.
- Preserve direct and FRPC path ingress without changing existing routes.

## Non-Goals

- Implement `AliyunSandboxBackend` in this phase.
- Migrate the temporary SQLite trial Thread into PostgreSQL.
- Merge Ali changes back to the original `main` branch.
- Write persistent application state to the root partition.

## Hard Constraints

- The root filesystem is full and must not hold Ali data, logs, caches,
  databases, bundles, images, temporary files or service definitions.
- Persistent systemd units under `/etc/systemd` are prohibited.
- Existing Parallel services, ports and storage identities must remain unchanged.
- Provider and infrastructure credentials may be copied only from an explicit
  allowlist; deployment secrets are regenerated.
- Shared immutable binaries and services are permitted only with isolated
  resource identities.

## Resource Identity Matrix

| Resource | Ali identity |
| --- | --- |
| Checkout | `/data2/wujing03/sandbox-harness-platform-parallel-ali` |
| Branch | `sandbox-harness-platform-parallel-ali` |
| Writable root | checkout `.data` only |
| API | `127.0.0.1:24767` |
| PostgreSQL | `127.0.0.1:28433` |
| PostgreSQL role/database | `sandbox_harness_parallel_ali` |
| PostgreSQL data/socket/log | `.data/postgresql/{data,run,postgresql.log}` |
| Redis | shared server, prefix `sandbox-harness-platform-parallel-ali:` |
| OpenSandbox | shared service, namespace `sandbox-harness-platform-parallel-ali` |
| Gateway/proxy | `172.31.0.1:29882` / `172.31.0.1:29883` |
| Object storage | shared service, prefix `sandbox-harness-platform-parallel-ali/` |
| Registry | shared service, repository prefix `sandbox-harness-platform-parallel-ali/` |
| Job node | `ali-opensandbox` |
| Controlled node | `ali-controlled` |
| Direct ingress | `/sandbox-harness-platform-parallel-ali/` |
| FRPC ingress | `:8090/sandbox-harness-platform-parallel-ali/` |

## Application Isolation

All Ali source changes remain on the Ali branch and checkout. New deployment
implementation lives under `deploy/ali/`; Alibaba Cloud backend implementation
will live in an Ali-specific module and tests. Source updates move one way by
explicit cherry-pick. Ali commits are never merged into the original `main`.

The virtual environment, frontend dependencies, built frontend, runtime bundles,
code bundles, catalogs, signing material and controlled runtime snapshot are
materialized in the Ali checkout. No Ali path may point at another product's
mutable `.data` tree after configuration completes.

## Shared Infrastructure Rules

- PostgreSQL 16 executables are shared; Ali runs a separate server process and
  PGDATA.
- Redis is shared; every key uses the Ali prefix.
- OpenSandbox is shared temporarily; metadata namespace and Gateway ports are
  Ali-specific. This backend is replaced later by `AliyunSandboxBackend`.
- BOSS/S3 and registry services are shared; Ali uses dedicated key and repository
  prefixes.
- Immutable source images may seed Ali registry repositories, but runtime
  catalogs reference only Ali repository names.
- Provider credentials are shared only through the existing explicit allowlist.

## Configuration

`deploy/ali/configure.py` validates the exact checkout path and branch, checks
all dedicated ports, copies only allowlisted shared settings, generates database
and service secrets, materializes referenced immutable bundles/catalogs/runtime
snapshots into Ali `.data`, rewrites image references to the Ali registry
namespace and writes `.data/config/platform.env` atomically with mode `0600`.

The configurator rejects any path outside the Ali checkout, any original
Parallel identity, missing immutable artifact, digest mismatch or occupied port.
It supports `--check-only` for read-only validation and `--force` only for an
explicit reconfiguration.

## PostgreSQL

`deploy/ali/init-postgresql.sh` uses the shared PostgreSQL 16 executable and the
existing `sandbox-harness-db` OS user. It initializes only the Ali PGDATA,
creates only the Ali role/database and never contacts port 28432. Password files,
socket files and logs remain under `.data/postgresql`.

The temporary SQLite file is retained under `.data/runtime` as audit evidence;
the production environment no longer references it.

## Runtime Assets And Images

Only the bundle/code-bundle digests selected by the source release are copied to
Ali bundle directories. The controlled runtime snapshot is copied into an
immutable digest directory and Ali owns its `provider-runtime` symlink. Catalogs
are copied, digest-verified and rewritten where repository names differ.

The existing registry daemon is reused. Every required profile/harness manifest
is promoted to the `sandbox-harness-platform-parallel-ali` repository prefix.
The resulting profile catalog contains Ali image references with matching
manifest digests.

## Process Supervision

Persistent unit files are stored under `deploy/ali/units` in the checkout.
`deploy/ali/start.sh` uses `systemd-run` to create transient units in `/run`
tmpfs. No unit is installed in `/etc`. Units use Ali-specific names:

- `sandbox-harness-parallel-ali-postgresql`
- `sandbox-harness-parallel-ali-network`
- `sandbox-harness-parallel-ali-gateway`
- `sandbox-harness-parallel-ali-job-worker`
- `sandbox-harness-parallel-ali-controlled-worker`
- `sandbox-harness-parallel-ali-media-worker`
- `sandbox-harness-parallel-ali-site-worker`
- `sandbox-harness-parallel-ali-api`

`start.sh` is idempotent, starts dependencies in order and refuses to replace a
foreign process. `stop.sh` stops only Ali transient units. `status.sh` reports
unit, port, health, database, node and ingress state. After a host reboot,
`start.sh` restores the deployment.

## Network And Security

The Ali network rule opens only ports 29882-29883 from the validated shared
OpenSandbox bridge to `172.31.0.1`, with an Ali-specific iptables comment. It
does not modify or remove the original Parallel rule.

Direct ingress preserves the current anonymous/account product behavior. FRPC
ingress requires an account and disables registration. Both routes expose only
the API. PostgreSQL, Redis, Gateway and proxy ports are not exposed by Nginx.

## Cutover

1. Configure and validate Ali assets.
2. Initialize Ali PostgreSQL.
3. Promote images and freeze catalogs.
4. Stop the temporary nohup API only after the new PostgreSQL and dependencies
   are ready.
5. Start the full transient unit graph.
6. Verify direct and FRPC ingress, real OpenSandbox execution and cleanup.
7. Keep the SQLite file and pre-cutover logs unchanged for audit.

Rollback stops only Ali transient units and restarts the temporary API only if
explicitly requested. It never touches original Parallel services or data.

## Health And Acceptance

The deployment is accepted only when all of the following hold:

- API health uses `backend=opensandbox` and reports a real execution backend.
- PostgreSQL is reachable on 28433 and the configured role/database match.
- Redis keys are confined to the Ali prefix.
- Object and registry resources use Ali prefixes.
- Gateway and all workers are active with Ali node identities.
- A real Codex Turn creates an Ali-namespace sandbox, streams output, commits a
  checkpoint/artifact and deletes or retains the sandbox according to policy.
- Restart and worker replacement recover leases without duplicate execution.
- Existing Parallel direct and FRPC routes remain healthy.
- Full Python tests, frontend build, deployment checks and targeted browser
  evidence pass from the current revision.

## Approaches Considered

- **Transient systemd from `/data2` (selected):** supervision and dependency
  ordering without persistent root-partition writes; manual start after reboot.
- **tmux/nohup:** rejected because it cannot provide reliable restart and
  dependency convergence.
- **Persistent systemd units:** rejected because `/etc/systemd` violates the
  root-partition constraint.

