# Deployment and Collaboration

## Applies to

Read this for releases, systemd, nginx, database initialization, Runtime images,
environment changes, Git operations or concurrent workspaces.

## Isolation and storage

Production uses the project-owned PostgreSQL cluster and isolated namespaces in
shared infrastructure. All writable project state, caches, logs and databases
remain under the selected project's `.data` tree or an explicit
`/data2/wujing03` path. Do not create credentials or
state under `/etc`, `/root` or the root filesystem. Systemd units may reference
the ignored `.data/config/platform.env` only.

For Ali work, use `/data2/wujing03/sandbox-harness-platform-parallel-ali` and branch
`sandbox-harness-platform-parallel-ali`; inspect `pwd`, worktrees and status
first. The sibling original checkout is read-only. Use `deploy/ali/` and units
prefixed `sandbox-harness-parallel-ali-`; API 24767 and PostgreSQL 28433 are
separate from the original deployment's API 24766 and PostgreSQL 28432.

Do not restart or reconfigure shared Redis, Docker, registry, OpenSandbox or
object storage as part of this product's release.

The shared OpenSandbox lifecycle service owns all mutable server state under
`/data2/wujing03/opensandbox-stack/data/server`. Its unit and config must never
reference a product checkout or that product's runtime-data directory. Before
deleting any predecessor runtime tree, inspect `systemctl cat
sandbox-agent-opensandbox.service` and reject the cleanup if a path escapes the
OpenSandbox-owned tree.

The local immutable-image registry likewise owns its writable storage at
`/data2/wujing03/opensandbox-stack/data/registry`. Its container bind mount must
not reference a product checkout or predecessor `.sandbox-cluster` directory.
Before an image build, verify `docker inspect sandbox-agent-registry` and a small
registry upload when the mount was recreated or cleaned.

The controlled CPU/GPU Runtime is also project-owned. Installation materializes
the source into an immutable manifest-addressed directory below
`.data/controlled-runtime/` and atomically points `.data/provider-runtime` to
that snapshot. The stable entry must never resolve to a predecessor checkout,
shared user's environment or `/data` path. Before removing an installation
source, rebuild its manifest from the published snapshot and require identical
file rows; restart the controlled worker once to prove the snapshot is complete.
The manifest covers the complete SAM3, ZIM and libdirector import trees as well
as model and environment markers. bwrap mounts the snapshot at `/runtime` and
maps each copied Conda environment to its build-time prefix only inside the
namespace; it never exposes the host `/data` tree.

The Ali image-layering HTTP service remains a separate inference process. Its
startup applies `deploy/ali/isolate-image-service.py` to the already materialized
dependency: GPU selection and the shared lock inodes stay unchanged, but locks
are opened read-only and never truncated or recreated. Missing shared locks
leave work queued. ComfyUI input/output/temp/user directories are per Job below
the service's isolated runtime. Python bytecode is disabled; CUDA, Hugging Face,
Torch and Triton caches are explicitly below the Ali `.data/cache` directory.
The patch verifies its dependency source shape and fails closed after an
unrecognized upgrade. `tests/test_image_service_isolation.py` verifies lock
exclusion, unchanged shared file bytes and idempotent application without a GPU.

## Release sequence

1. Inspect `git status`; preserve concurrent edits.
2. Run focused tests, then the release-level suite required by the change.
3. Build `web/dist` when frontend or API static serving changed.
4. `record-release.py` builds deterministic signed Runtime Code and Content
   Bundles under `.data/runtime-code-bundles` and `.data/runtime-bundles`, then
   freezes both digests. Runtime Python, CLI, prompt and Skill changes stop
   here. Build/push images only when Bootstrap, native helpers, Harness clients,
   verification protocol or image dependencies changed.
5. Identify active work and its owner. For an authorized test restart, record
   current generations, allocation IDs and external execution IDs, restart the
   affected Ali services while work is active, and verify takeover with no
   duplicate execution. Do not require drain for this recovery validation;
   protect unrelated user work.
6. Run `deploy/check-service-access.sh`.
7. Use `deploy/record-release.py` with the selected `--code-root`, `--env-file`
   and `--record-file` to pin Git revision and `PYTHONPATH`.
8. Restart only affected services and verify API/Gateway health plus Job,
   Media and Site Worker systemd state. They are independent failure domains.
9. Verify the selected deployment's existing ingress. For Ali, request
   `/sandbox-harness-platform-parallel-ali/` with `Host: updream-lab`; the
   `90f51d5` release verified this entry. Inspect the actual nginx configuration
   before changing it and run `nginx -t` before reload. The root
   `deploy/nginx.conf` targets the original deployment and must not overwrite
   Ali or predecessor locations. Do not register another FRPC entry.
10. Run a focused deployed case and confirm state, logs and cleanup.

Before the final gate and push, refresh `README.md`, the affected documents in
`docs/development/` and the corresponding evidence report. Documentation must
describe current implemented behavior and link to source/tests; do not publish
planned behavior as a mandatory rule. For a code release, commit its documents
before the gate. A later documentation-only refresh is a separate commit on
the tested baseline: check links and claims, keep existing receipts at their
original revision, and do not redeploy or rewrite published history merely to
make documentation HEAD equal the running release.

For Ali, a release-record command must name the isolated paths explicitly and
use the cache/temp environment from [Getting started](getting-started.md):

```bash
.venv/bin/python deploy/record-release.py \
  --code-root "$PWD" \
  --env-file "$PWD/.data/config/platform.env" \
  --record-file "$PWD/.data/evidence/release-current.json"
```

`deploy/ali/start.sh` starts missing services; it does not reload an already
active process. Include the independent `allocation-worker` when
`SANDBOX_HARNESS_JOB_ALLOCATION_DISPATCHER_ENABLED=true`. Restart affected units
explicitly and record each process's loaded release. Unchanged components may
retain an earlier validated revision; an updated checkout or environment file
alone does not update a running process.

`record-release.py` refuses staged or unstaged changes to tracked files and
untracked deployable files in the selected checkout. Ignored `.data` evidence
is allowed. It rebuilds and hashes the selected checkout's frontend. Inspect `git status`
yourself. When another window owns uncommitted work, create a clean detached
worktree under `.data/releases`, build its frontend, grant the service user read
access and deploy with `--code-root`. Do not commit, discard or deploy another
window's partial edits merely to make a release command pass.

## Database and service changes

`Store.initialize` creates tables and applies explicit additive upgrades to
existing databases; there is no Alembic migration directory. Follow
[Database migration](database-migration.md) for idempotence, defaults and
SQLite/PostgreSQL checks. Do not reuse either predecessor database.
`install-units.sh` installs units but does not start them.
Root `deploy/init-postgresql.sh` targets the original port 28432 cluster;
`deploy/ali/init-postgresql.sh` targets only the Ali port 28433 cluster.

Never delete production data or reset the database unless the user explicitly
authorizes that exact environment. Never rely on graceful shutdown for
correctness.

## Git collaboration

- Commit only coherent, reviewed files. With unrelated edits, stage explicit
  paths.
- Do not reset, checkout or rewrite work you did not create.
- Pull/push only when requested or already authorized for the current delivery.
- Before pushing, scan tracked files and history for credentials and generated
  state.
- Report the commit, tests and deployment separately; they may have different
  revisions until rollout completes.

## Completion

The deployed health endpoint reports the intended revision, the worker has no
unexpected active leases, Gateway/API telemetry has no new loss/error counters,
the warm pool reports the intended image digests, Runtime reports the frozen
Bundle digest, the public entry works, and a targeted real case proves the
changed boundary.

Push only after deployed verification. Record the local release commit, remote
branch head, gate receipt, Runtime Code/Content Bundle digests and immutable
image digests separately. Equality applies to the claimed code release;
subsequent documentation commits preserve the original deployed/tested version.
For composite campaign coverage, retain each case's actual source revision and
the failed/affected/unrun selection instead of claiming a clean full run.

Cleanup follows verification: retain current image/profile and rollback
dependencies; delete only proven obsolete owned tags and test data without
active references. Manage registry/provider credentials on the host, never
through a sandbox. Preserve sanitized evidence, confirm provider deletion and
report unresolved historical allocations separately from reclaimed resources.
