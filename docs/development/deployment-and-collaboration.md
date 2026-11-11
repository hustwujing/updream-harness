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

## Release sequence

1. Inspect `git status`; preserve concurrent edits.
2. Run focused tests, then the release-level suite required by the change.
3. Build `web/dist` when frontend or API static serving changed.
4. Build/push Runtime images when Runtime, adapters, tools or image dependencies
   changed; deploy immutable digests.
5. Confirm no active Turn would be interrupted.
6. Run `deploy/check-service-access.sh`.
7. Use `deploy/record-release.py` to pin Git revision and `PYTHONPATH`.
8. Restart only affected services and verify API/Gateway health plus Media
   Worker systemd state. API and Media Worker are independent failure domains.
9. Run a focused deployed case and confirm state, logs and cleanup.

`record-release.py` refuses staged or unstaged changes to tracked files in the
selected checkout; it does not detect untracked files. Inspect `git status`
yourself. When another window owns uncommitted work, create a clean detached
worktree under `.data/releases`, build its frontend, grant the service user read
access and deploy with `--code-root`. Do not commit, discard or deploy another
window's partial edits merely to make a release command pass.

## Database and service changes

`Store.initialize` currently performs `create_all` and explicit additive ALTERs;
there is no external migration runner. Follow [database migration
guidance](database-migration.md). `install-units.sh` installs units but does not
start them. `init-postgresql.sh` targets only the dedicated cluster.

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
the public entry works, and a targeted real case proves the changed boundary.
