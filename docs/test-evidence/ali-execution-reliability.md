# Ali execution reliability verification

## Implemented design

The provider-neutral Job state machine owns submission, acceptance and recovery.
The configured SandboxBackend owns provider lifecycle calls. None of the fixes
below branches on a case ID, sandbox provider or Operation name, and none relies
on mutable provider metadata.

1. **Capacity waits:** `JobWorker` keeps one attempt while capacity is unavailable.
   `Store.mark_job_submission_started` restores `INFRASTRUCTURE_WAIT` to
   `SUBMITTING` before retry. Allocation still requires submission intent, and
   owner/generation/acceptance checks still reject stale writers.
2. **Accepted execution polling:** transport failures and HTTP
   408/429/500/502/503/504 stay nonterminal until the frozen deadline. The same
   external handle is reused. Permanent query failures remain reconciliation
   failures; polling never grants permission to resubmit.
3. **Child submission boundary:** configuration and input staging happen before
   execution. Their failure retires the sandbox and yields
   `child_preparation_failed`, without consuming acceptance. Once execution
   submission is attempted, response loss retains the sandbox, reservation and
   execution ID. `SubmissionOutcomeUnknown` carries the complete receipt for
   status/recovery rather than deleting possibly running work. Cancellation
   retains that identity across reservation heartbeat loss and the subsequent
   mark-running step. A fenced terminal cleanup can finish revocation/deletion
   against an exactly matching released reservation without reactivating it.
4. **Campaign integrity:** Primary success does not override a program failure
   in an internal Durable Job. Recovered model/Provider failures remain in the
   evidence. The SQL continuation fixture now provides its exact query and
   inline data instead of asking a Child to infer absent inputs. An independent
   Primary program error or invalid final artifact takes precedence over an
   unrelated model/Provider error.
5. **Authorized continuations:** Runtime instructions let the Harness choose
   routine reversible details for bounded edits and incremental deliverables.
   Skill planning defaults do not introduce another approval for authorized
   work. Missing material facts and required Provider/deployment cards retain
   their existing interaction boundaries.

## Incident audit

- **K08:** two Operations failed with `stale_job_generation` after continuous
  capacity waits. The strengthened test reproduced that error for both
  Operation and Provider Jobs before the fix. Ali real regression at `503be28`
  passed all four Operations and their artifact contracts in 17.489 seconds.
- **C05:** the prior top-level success contained a Child `UNKNOWN`. Durable
  receipt and scoped telemetry identify `HTTPStatusError` in Runtime
  configuration before execution. Historical telemetry does not retain its
  exact HTTP response code. Tests inject a preparation HTTP failure and a
  distinct execution-response loss to verify opposite side-effect boundaries.
- **R12:** the prior top-level success contained a Reviewer `UNKNOWN`. Actual
  scoped telemetry shows `HTTPStatusError` from execution-status polling; it
  does not establish the exact HTTP response code. Regression tests cover
  transient transport/HTTP failures, permanent HTTP rejection and reuse of the
  accepted handle without a second submission. A later real regression had no
  internal Job failure but asked for redundant design approval on the requested
  incremental contact sheet; this motivated the generic authorization instruction.
- **K12:** the first historical failure was a model 429. The next run reached a
  blocked Child because the query-review objective supplied no query. The
  fixture now includes PostgreSQL VALUES data, the exact aggregation, explicit
  ordering and a requirement to distinguish static findings from measured plans.

## Validation layers

The focused SQLite/fake tests cover the state transitions, stale lease fencing,
acceptance accounting, reservation preservation, transport errors and cleanup.
`tests/test_job_worker.py`, `tests/test_job_store.py`, `tests/test_harness_job.py`,
`tests/test_job_errors.py` and campaign-runner tests are the primary regressions.
PostgreSQL race tests use a newly created temporary database on the isolated Ali
instance and remove it afterward; they do not run against application data.

Pre-gate validation at `239f242` recorded:

- Python: 1,128 passed, five optional PostgreSQL tests skipped in the generic
  invocation; those five passed separately against the isolated test database.
- Frontend: 23 tests passed; production build passed with existing chunk-size
  notices. No UI implementation changes were made for these fixes.
- Real Ali Provider protocol: AP01–AP05 passed (allocation identity, accepted
  response loss, external deletion, endpoint refresh and repeated deletion).
- Browser fixtures: execution graph desktop/mobile checks passed. Separate live
  page checks used actual Ali API health, both viewport sizes and zero page
  errors. Browser fixtures alone do not prove sandbox recovery.

## Release evidence contract

Generated receipts stay below the Ali checkout's `.data/evidence/`; raw logs and
temporary files stay below `.data/tmp`. The per-case status file is
`real-campaign/ali-handoff-status.md`. Historical reports labelled
`composite_verified` are not a clean full gate. Final acceptance requires all
169 cases in a fresh report without `--resume-report`, with the committed
release revision, per-case classifications and inspected internal failures.

The release manifest records source revision, frontend digest, Runtime Code and
Content Bundle digests and the signed profile catalog. These host-state-machine
fixes do not change image dependencies, Bootstrap or Harness binaries, so they
use the existing immutable images; a needless image rebuild is not a release
requirement. Preserve deployed and rollback image/profile dependencies during
cleanup, and verify owned sandbox absence before reclaiming associated data.

Final reporting must distinguish deterministic tests, real Ali Provider
protocol, real Harness/Provider campaign, browser evidence and deployment
health. Original OpenSandbox deployment state is outside this release's
mutation scope.
