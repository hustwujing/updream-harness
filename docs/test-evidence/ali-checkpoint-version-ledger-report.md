# Ali Checkpoint Version Ledger Report

## Scope

This supplemental gate verifies the Core-owned checkpoint version ledger and
pre-input rollback boundary on real Ali sandboxes. It does not change the
frozen 200-case catalog count.

- Source revision: `83d5bd3`
- Checkpoint ledger revisions: `645eb8c`, `002a7d0`, `88706f3`
- Runtime Code Bundle: `cc8feaf1a0e07e7a0eb4452d9f3c7441c6a87490529d7069b49624eeee314f66`
- Sandbox backend: `aliyun`
- Runner: `tests/run_real_checkpoint_versions.py`
- Machine evidence: `.data/evidence/checkpoint-versions-real-cc8feaf1.json`

## Results

| Case | Result | Real boundary |
| --- | --- | --- |
| CV01 | passed | Two real Codex Turns produced one ACTIVE and at least one SUPERSEDED version. |
| CV02 | passed | A corrupt ACTIVE archive failed in a fresh sandbox, became INVALID, rolled back, and the next Turn completed from the previous version. |
| CV03 | passed | A VERIFIED, non-ACTIVE version survived PlatformWorker SIGKILL without changing the Thread pointer. |
| CV04 | passed | A missing native session failed before input acceptance, rolled back, and the next Turn completed from the previous version. |
| CV05 | passed | Retention converged to exactly one ACTIVE and no more than two SUPERSEDED versions. |

CV02 replaced PlatformWorker PID `2930086` with `2930913`; CV04 replaced
`2930913` with `2931882`; CV03 replaced `2931882` with `2933586`. The negative
Turns failed with the intended `checkpoint_version_rolled_back` public code,
not a replayed Harness execution.

## Native Input Acceptance Fix

The first CV04 attempt exposed a Runtime protocol defect: a Codex runner input
written to the runner pipe, but rejected before `thread/resume`, was reported as
accepted. Runtime now consumes an explicit, bounded list of native-accepted
input IDs from the runner and filters it against the execution's received IDs.
An initial native-session failure therefore permits checkpoint rollback; a
post-Turn persistence verification failure retains the accepted input ID and
cannot authorize replay.

Focused verification at `83d5bd3`:

```text
87 passed, 57 warnings in 51.92s
```

This includes Runtime boundaries, Codex runner behavior, Runtime Code Bundle
closure and Broker checkpoint rollback tests.

The release gate at `079e878` completed with:

```text
1663 passed, 23 skipped, 189 warnings in 350.14s
```

There were no Python test failures. The skips are the repository's existing
environment-conditional tests.

## Migration And Cleanup

The explicit production migration backfilled 43 ACTIVE versions and updated the
Core schema digest. It refused to run while a Turn was active. A final pg_dump
backup exists under the isolated deployment data root with SHA-256
`0e3eb44f2c43efdb1bbbd2fbac11c9327a43495e6eef72269dac90fc1231818d`.

After the real gate, the test Thread and its sandbox were removed. Database
inspection showed zero active Turns, zero active Jobs, zero test Threads and 43
production ACTIVE versions, with no test INVALID or SUPERSEDED rows retained.
The target production Thread `42772a1eb3784210a527e67864215789`
still referenced checkpoint `sha256:f458ba35ee1a561e59d4ed2de8154b9df720993f7d725e4a22cc8a7c6d01dc48`
and native session `01a22066-71e9-79e3-ab4f-e3f52d338476`.
