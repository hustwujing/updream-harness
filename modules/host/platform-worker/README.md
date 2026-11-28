# Platform Worker

Host control-plane execution service. Every replica may claim durable Turn,
continuation and interaction work. A database-fenced leader lease owns singleton
coordination loops such as warm-pool convergence, lifecycle relay, model catalog
refresh, expiry and orphan cleanup.

- Entry: `sandbox_harness.services.platform_worker.platform_service:run`
- Databases: accounts, core, media and sites through independent repositories
- Runtime state: `.data`; immutable code/resources: selected service release
- Verification: leader fencing, API-independent restart, Turn recovery, Outbox and real Ali cases

The API persists requests but does not run PlatformWorker loops.

Primary Runtime streams use the shared event buffer. Public text is available
through bounded Redis live deltas while PostgreSQL receives aggregated batches
on the configured interval; Runtime ACK follows the lease-fenced commit.

Workspace restore must complete before the Worker may replace the Thread's
checkpoint. A failed restore releases the fresh sandbox while preserving the
last good object reference. Checkpoints above 8 MiB use the Runtime's 1 MiB
idempotent chunk protocol.
