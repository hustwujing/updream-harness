# Durable Pause Verification Report

Date: 2026-11-17

## Scope

The release adds a PostgreSQL-authoritative Thread pause saga, independent Primary/Child Harness pause participants, native-session and workspace checkpoints, Provider reconciliation fencing, restart takeover, and pause/resume UI behavior. Hard cancellation remains separate.

## Automated Evidence

- Pause Store, Broker restart, Job Worker, Harness adapter, API cancellation/continuation, and execution-graph regression: 58 tests passed.
- Focused pause and restart tests: 8 tests passed.
- Browser pause/resume ordering: passed; the page displayed `已暂停`, then sent `resume` before creating the continuation Turn.
- Existing continuation browser flow: passed after replacing the destructive top-level stop action with pause.
- Frontend production build: passed.

## Real-Machine P21

Evidence: `.data/evidence/durable-pause/p21-r2.json`

- Thread: `dbb4948ce89e434d9d30418a2eb54423`
- Primary Harness: checkpointed and resumed.
- Child Harness participants: 2.
- Job Worker: stopped before pause processing and started after API restart.
- API: restarted while the pause saga was active.
- Child checkpoints: 2 independent content-addressed objects.
- Resumed child terminal states: `SUCCEEDED`, `SUCCEEDED`.
- Primary continuation state: `COMPLETED`.
- Pause generation: 1.

The first P21 attempt exposed a shutdown race: systemd shutdown cancelled a Broker task that was already handling pause cancellation, interrupting the checkpoint request and leaving the participant pending. Commit `82939f7` prevents the second cancellation and adds a focused regression test. P21 then passed without relaxing the case.

## Recovery Boundaries

Provider Jobs with an accepted external identity may continue reconciliation while paused. Unsubmitted Provider and Operation Jobs cannot start. Continuation Outboxes remain unclaimed until resume. If a sandbox is unavailable, the participant is marked degraded rather than falsely reported as exactly resumable; the next user Turn receives the prior objective and durable ProjectFile context.
