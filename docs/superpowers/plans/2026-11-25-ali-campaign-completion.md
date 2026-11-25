# Ali campaign completion implementation plan

**Goal:** Resolve generic Job execution defects, audit all reported outcomes, run the complete 169-case gate and release the isolated Ali deployment.

**Architecture:** Keep durable submission intent and recovery in Store/JobWorker. Provider adapters retain their common protocol. Reuse accepted execution handles and the same capacity-wait attempt; never retry an unknown submission without reconciliation.

**Tech stack:** Python 3.13, asyncio, SQLAlchemy/PostgreSQL, pytest, configured SandboxBackend, systemd and the existing real-case runner.

**Spec:** User handoff dated 2026-11-25; existing provider design in `docs/superpowers/specs/2026-11-23-aliyun-sandbox-backend-design.md`.

## Constraints

- Work only in the `sandbox-harness-platform-parallel-ali` worktree and branch. All generated state stays under `/data2/wujing03`.
- Preserve original deployment isolation and existing UI. No case/provider/operation-specific fix branches.
- No metadata-update dependency, duplicate external execution, or false successful publication.
- Independent fixes receive focused tests and separate commits. Final evidence identifies the committed revision.

## Review focus

- Repeated capacity waits must preserve generation and restore submission intent before allocation.
- Lease takeover and accepted/terminal attempts must reject stale submission writes.
- Transport errors after acceptance must reconcile the original execution rather than resubmit.
- Internal Job failures must remain visible even when a Primary case completes.
- Cancellation, deadline and stop during waits must preserve cleanup ownership.

## Tasks

1. K08 state machine (`tests/test_job_worker.py`, Store tests, `job_worker.py`, `store.py`).
   - [x] Make the continuous capacity test fail after two infrastructure failures when the successful adapter records allocation.
   - [x] Restore `INFRASTRUCTURE_WAIT` to `SUBMITTING` under the existing lease/generation fence immediately before retry.
   - [x] Verify same attempt, one accepted execution, fencing, cancellation/stop/deadline tests; run relevant Store/Job suites and commit.
   - [x] Release affected service code and run K08; affected Job regressions continue below.
2. Audit K12, C05 and R12 from durable Jobs, attempts and scoped telemetry.
   - [ ] Reproduce each remaining program defect with a focused regression; implement generic fixes and commit separately.
   - [ ] Verify K06, K09–K14 final outcomes and rerun affected cases only during iteration.
3. Final release gate.
   - [ ] Commit updated design/development/test reporting before the gate.
   - [ ] Run full Python tests, frontend build, relevant provider protocol and browser checks.
   - [ ] Run all 169 real cases on one revision without inheriting old successes; inspect failures as results arrive.
   - [ ] Pin/release Ali deployment and verify health, page, takeover/recovery and command execution.
   - [ ] Inventory local/remote images and owned test data; remove only verified obsolete resources after release verification.
   - [ ] Publish a final per-case result report with evidence and any limits.

The user explicitly authorized continuous native execution; no additional plan approval is required.

## Current execution policy and additional resource gate

The later user instruction supersedes the clean-run reset above: preserve
unaffected successful receipts, rerun failed/affected cases, finish unrun cases,
and report mixed revisions as `composite_verified`. Restart affected isolated
Ali services while tests are active to verify recovery; do not wait for drain.

- [ ] Recover reservations and sandbox allocations across worker loss for all
  sandbox-backed Job adapters. Reproduce loss after reserve and before Runtime
  submission; preserve unknown creations and original cleanup generation.
- [ ] Restrict inherited protocol locking to the existing explicit shared-service
  mutator set. Verify ordinary handlers bypass the lock and mutators retain it.
- [ ] Add a separate real resource-exhaustion gate: fill an isolated configured
  sandbox/Job capacity, verify waiting without duplicate creation, release and
  automatic progress, cancellation/deadline and Worker replacement. The user
  clarified the additional boundary as provider inability to create a sandbox;
  exercise an isolated real provider instance quota.
  Record configured-capacity exhaustion separately from provider quota failures
  and internal sandbox limits; never claim one proves another.
- [ ] Include added case counts and receipts separately from the original 169
  until the final report explicitly accounts for both.
