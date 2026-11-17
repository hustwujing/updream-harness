# Durable Thread Pause Design

## Intent

The visible pause action must stop active Harness computation without losing the Thread objective, durable inputs, completed Artifacts, or safely resumable Agent state. A later resume must not duplicate Provider submissions or rerun completed Jobs. Hard cancellation remains a separate terminal operation.

## Chosen Approach

Use a PostgreSQL-authoritative Thread pause saga. The Thread control row records the desired state and monotonic generation before any process is interrupted. The Primary Harness and every running child Harness become pause participants with independent checkpoints. Provider and other durable external Jobs continue to be reconciled while continuation delivery is held behind the Thread pause fence.

The rejected alternatives are saving only the Primary Harness checkpoint, which loses child Agents, and reconstructing exclusively from chat history, which can duplicate side effects.

## State Model

Thread control states are `RUNNING`, `PAUSING`, `PAUSED`, and `RESUMING`. A pause request increments the generation, closes scheduling and continuation delivery, and creates participant records. Participant states are `REQUESTED`, `QUIESCED`, `CHECKPOINTED`, `PAUSED`, `RESUMED`, and `FAILED`.

The Primary Turn finishes as `PAUSED` after its Harness process is quiesced and its workspace is checkpointed. Queued child Harness Jobs become `PAUSED` without starting. Running child Harness Jobs are quiesced, checkpointed independently, and become `PAUSED`. Provider and Operation Jobs are not blindly cancelled; their durable receipts continue to be reconciled while the Thread is paused.

## Pause And Restart

The API acknowledges pause only after the control row and participant set are committed. Process-local tasks, Redis notifications, and SSE connections are not authoritative. A watchdog may claim any `PAUSING` saga after restart and repeat each idempotent step. Fencing uses the pause generation and existing Turn/Job generations.

Checkpoint object upload is content addressed. The database reference is committed only after the complete object exists. A crash before that commit falls back to the previous checkpoint and reports degraded recovery; a crash after it is safe to repeat sandbox cleanup.

## Resume

Resume changes the control row through `RESUMING` to `RUNNING`, requeues paused child Harness Jobs, and re-enables continuation claims. A new Primary Turn restores the Thread checkpoint and native Harness session. Completed Jobs remain terminal. External Jobs are queried from their durable receipt and are never resubmitted merely because the Thread resumed.

For a paused Thread, the UI maps the explicit resume action to the control endpoint. A user message such as `继续` is submitted only after the pause fence is removed, so it reaches the restored native Harness session rather than an unrelated fresh session.

## User Experience

The square destructive stop action is replaced by a pause action. The interface displays `正在暂停`, `已暂停`, or `正在恢复`, and exposes a resume command while paused. Hard cancellation remains available through the explicit stop-all API and must not be described as resumable.

## Failure Semantics

- A failed participant checkpoint is visible as degraded pause and is never described as exact recovery.
- A missing sandbox with an older checkpoint resumes from the last durable boundary.
- A Provider outcome that cannot be determined remains `UNKNOWN`/reconciling and is not retried automatically.
- New Job claims and continuation Turns are fenced while the Thread is not `RUNNING`.
- Service restart during `PAUSING` resumes the saga; it never resumes business execution by itself.

## Verification

Unit and API tests cover Primary Harness checkpointing, queued and running child Harness participants, continuation suppression, idempotent pause/resume, and restart takeover. A real-machine case pauses a Thread with parallel Harness Jobs and a durable external Job, restarts the API/worker boundary, resumes it, and proves that completed work and Provider identities are not duplicated.
