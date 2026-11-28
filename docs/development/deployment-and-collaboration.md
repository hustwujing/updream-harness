# Deployment and Collaboration

## Applies to

Read this for releases, service restarts, provider switching, database
initialization, Runtime images, environment changes and Git publication.

## Isolation

Work only in `/data/wujing02/sandbox-harness-platform-parallel-ali` on branch
`sandbox-harness-platform-parallel-ali`. All generated data, dependencies,
caches, logs, databases and temporary files stay below this checkout's `.data`
tree or another explicit `/data/wujing02` path.

The deployment owns four PostgreSQL databases and roles. `run-service.sh`
removes the cluster administrator credential and exposes only the databases
required by the selected service. Do not add a shared application database or
cross-domain SQL session.

## Releases

1. Inspect the working tree and preserve concurrent edits.
2. Run focused tests for the changed contract.
3. Build an immutable service release with `tools/service_release.py`, or an
   external image-inference release with `tools/image_inference_release.py`.
4. Atomically select the manifest under `.data/service-current/`.
5. Check active Turns, leases and reconciliations.
6. SIGKILL the affected service and start it through `deploy/ali/start.sh` so
   recovery is exercised rather than assumed.
7. Verify service health and rerun the failed or affected real cases.
8. Run the full Python gate, frontend tests/build and the 199-case catalog gate
   before publication.

`deploy/ali/start.sh` starts missing services. Use `deploy/ali/stop.sh` before a
sandbox provider switch. Do not start both provider gateways together.

## Database Changes

Follow [Database migration](database-migration.md). The four domain databases
are the only application authorities. The `postgres` maintenance database is
used only by initialization/provisioning and its administrator URL must never
reach an application process.

Never delete production rows or reset a database without explicit user scope.
Test databases and stale predecessor databases may be removed only after their
processes and references are proven absent.

## Git

- Commit coherent changes with explicit paths.
- Do not reset, checkout or rewrite another process's work.
- Push only the tested branch and verify the remote head.
- Keep credentials and ignored `.data` artifacts out of Git.

## Cleanup

After verification, retain only selected service descriptors, the configured
Runtime/Code Bundle, current Controlled Runtime, four production databases,
current object data and the final case report. Remove unselected releases,
historical test databases, caches, logs and build metadata after confirming no
active process references them.
