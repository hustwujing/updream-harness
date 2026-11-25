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
6. **Optional output formats:** multi-format adapters can select candidates
   through `ArtifactGateway.emit_matching` using frozen MIME patterns. Excluded
   formats create no stored object; selected items retain stable identity and
   dense positions. Strict emission and Store authorization/cardinality checks
   remain unchanged. See the [format-selection design](../superpowers/specs/2026-11-25-artifact-format-selection-design.md).

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
- **C09:** the first clean full gate on `91189f2` discovered optional PSD/TIFF
  outputs outside a frozen PNG/ORA/ZIP/JSON collection. The source execution
  succeeded; repeated collection hit `artifact_mime_not_allowed`. The generic
  selector preserves the requested formats without widening authorization.
  That interrupted gate drained to 50 successes, one failure and 118 unstarted
  cases; it is not final release acceptance.
- **S29:** the next fresh gate on `2bb6b54` hit a local Gateway 413 while the
  Primary inspected multiple video contact sheets. The Provider video was
  already valid. Model image history exceeded the shared 16 MiB limit, before
  model activity creation, so the public result only said `other`. The new
  model-specific bounded ingress and durable error observation are described
  in the [model ingress design](../superpowers/specs/2026-11-25-model-ingress-budget-design.md).
  That gate drained to 38 successes, one failure and 130 unstarted cases.
- **C09 allocation:** the `289455c` gate exposed a distinct Child allocation
  failure. Sandbox creation became uncertain before Runtime submission; the
  old generic exception path recorded an empty accepted handle and dropped
  capacity ownership. Durable allocation phases now retain the original
  provider/profile/reservation through delayed visibility, cancellation,
  Worker replacement and cleanup. Reviewer wrappers use the same lifecycle.
  That gate drained to 58 successes, one failure and 110 unstarted cases.
  See the [allocation recovery design](../superpowers/specs/2026-11-25-child-allocation-recovery-design.md).

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
`composite_verified` are not a clean full gate. The user's later instruction
on 2026-11-25 changes this campaign's acceptance workflow: after a fix, rerun
only its triggering/affected cases and finish never-run cases. Preserve
unaffected successes with their original revisions and an explicit impact
assessment. Final coverage still requires all 169 cases and inspected internal
failures. Reports spanning revisions must remain `composite_verified`; do not
claim that every success ran on the final revision.

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

### Deterministic collection count validation

The resumed `3006bd2` run exposed a different C09 boundary: a frozen one-file
collection received ten files. Store blocked publication, but the generic
collector retried the same immutable result. The typed cardinality correction
returns a nonretryable validation failure with requested/actual counts, retains
unpublished candidates and normal cleanup, and applies to Child and other
adapters without provider/case branches. Missing/excess-output regressions first
failed with `WAITING_EXTERNAL`; the focused suite now passes 97 tests. Campaign
classification remains strict. Real C09 retest and final release evidence are
pending; see the cardinality validation plan for scope and checks.

### Live input Artifact availability

The resumed run also exposed R06: a newly materialized attachment was absent
from the delegation catalog until input completion. A Broker regression reads
the catalog from inside Runtime append and first failed with zero inputs.
Prebinding now makes the authorized immutable identity available before the
Harness consumes its message, with lease/execution/cancel fencing and unchanged
input delivery semantics. Review additionally caught old Child grants losing
same-name versions; all immutable IDs are now retained and aliases follow input
sequence. This is a platform failure, correcting the runner's initial
`optimizable_prompt` label in a separate failure audit. The drained campaign has
68 successes, C09/R06 failures and99 unrun cases; those68 successes are preserved
under the user's scoped-retest policy. See the live-input binding plan and raw
`.data/evidence/real-campaign/resume-3006bd2/` receipts.

### Primary release revision recovery

An API restart from D08 exposed D06/R17 Primary reattachment rejecting the
existing sandbox: recovery reconstructed revision from provider_ref instead of
the durable Handle revision used by the original allocation. The generic fix
uses the recorded revision and still checks the full compatibility key. The
existing recovery regression was strengthened to change both current release
and Bundle configuration; its red run failed in adoption and the focused suite
passes111. D06's Child SIGKILL protocol had already passed with one external
execution across two attempts. Real scope is D06/R17 plus the D08 restart
trigger; unaffected successes are preserved. See the Primary release recovery
plan and `.data/tmp/profile-revision-*.log`.

### Runtime bootstrap status read retry

D40's business Turn failed in the initial Code Bundle status GET with
ConnectError after its protocol phase passed. The common RuntimeClient now
applies the existing bounded startup retry to Code/Content status reads as well
as idempotent uploads. Execution submission is unchanged. Three regression
variants first failed; the permanent-rejection negative test already passed.
The corrected focused suite passes144 tests and review is clear. Real scope is
D40 plus unfinished cases, preserving unaffected successes. See the startup-read
plan and `.data/tmp/startup-read-*.log`; release and real regression are pending.
