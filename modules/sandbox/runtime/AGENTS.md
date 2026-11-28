# Sandbox Runtime Module Rules

- Keep the Runtime portable and free of host database/Redis/object-store credentials.
- Preserve ordered idempotent event receipts and bounded replay journals.
- Append normalized events to the private journal before publication. Compact
  only through monotonic Host ACK received after durable commit.
- Preserve a stable Runner error across EOF. Classify missing native sessions
  explicitly and never expose raw RPC messages or silently replay input.
- Verify Codex native-session durability with a fresh app-server before
  completion. Checkpoint transport retries must replay immutable bytes and
  refresh a failed provider endpoint only before Harness execution.
- Fence initialization, reset, uploads and execution identity across replacement.
- Files leave only through declared outputs, checkpoints or validated transfers.
- Complete with bundle-closure and real sandbox restore/command tests.
