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
