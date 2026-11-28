# Sandbox Runtime

In-sandbox HTTP control plane for native Harness processes, event replay, workspace,
inputs, interactions, background jobs, checkpointing and output collection.

- Entry: `sandbox_runtime.main:run`
- Credentials: short-lived Turn capability only
- Verification: Runtime bundle, client, recovery, upload and terminal tests

Normalized events are appended to a private bounded segmented journal before
streaming. The Host resumes by `execution_id` and committed sequence, then ACKs
only database-committed ranges. ACK compacts old segments; terminal journals are
removed after a short recovery retention period.

Large checkpoint restore stages fixed 1 MiB chunks under the private Runtime
state root. Chunk offsets and bytes are idempotent; commit verifies total length
and SHA-256, extracts once, and retains a replayable receipt. Runtime startup
removes abandoned staging, and sandboxes receive no object-store credential.
