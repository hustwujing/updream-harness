# Deployment and Collaboration

## Applies to

Read this for releases, systemd, nginx, database initialization, Runtime images,
environment changes, Git operations or concurrent workspaces.

## Isolation and storage

Production uses the project-owned PostgreSQL cluster and isolated namespaces in
shared infrastructure. All writable project state, caches, logs and databases
remain under the project `.data` tree on `/data2`. Do not create credentials or
state under `/etc`, `/root` or the root filesystem. Systemd units may reference
the ignored `.data/config/platform.env` only.

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

## Release sequence

1. Inspect `git status`; preserve concurrent edits.
2. Run focused tests, then the release-level suite required by the change.
3. Build `web/dist` when frontend or API static serving changed.
4. `record-release.py` builds deterministic signed Runtime Code and Content
   Bundles under `.data/runtime-code-bundles` and `.data/runtime-bundles`, then
   freezes both digests. Runtime Python, CLI, prompt and Skill changes stop
   here. Build/push images only when Bootstrap, native helpers, Harness clients,
   verification protocol or image dependencies changed.
5. Confirm no active Turn would be interrupted.
6. Run `deploy/check-service-access.sh`.
7. Use `deploy/record-release.py` to pin Git revision and `PYTHONPATH`.
8. Restart only affected services and verify API/Gateway health plus Job,
   Media and Site Worker systemd state. They are independent failure domains.
9. Install `deploy/nginx.conf` as the dedicated
   `/etc/nginx/snippets/sandbox-harness-platform-parallel.conf` include inside
   the existing `updream-lab` server. Run `nginx -t` before reload. Do not
   register FRPC or alter predecessor locations. Local verification must send
   `Host: updream-lab.bilibili.co`; bare `127.0.0.1` selects a different server.
10. Run a focused deployed case and confirm state, logs and cleanup.

Before the final gate and push, refresh `README.md`, the affected documents in
`docs/development/` and the corresponding evidence report. Documentation must
describe current implemented behavior and link to source/tests; do not publish
planned behavior as a mandatory rule. Commit documentation before running the
release gate so the gate receipt and deployed revision refer to the same tree.

`record-release.py` refuses staged or unstaged changes to tracked files and
untracked deployable files in the selected checkout. Ignored `.data` evidence
is allowed. It rebuilds and hashes the selected checkout's frontend. Inspect `git status`
yourself. When another window owns uncommitted work, create a clean detached
worktree under `.data/releases`, build its frontend, grant the service user read
access and deploy with `--code-root`. Do not commit, discard or deploy another
window's partial edits merely to make a release command pass.

## Database and service changes

This project is not online and intentionally has no migration framework.
`Store.initialize` creates the current schema in the fresh dedicated database;
do not add compatibility ALTERs or reuse either predecessor database.
`install-units.sh` installs units but does not start them.
`init-postgresql.sh` targets only the dedicated `127.0.0.1:28432` cluster.

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
image digests separately; equality is required for the claimed release.
