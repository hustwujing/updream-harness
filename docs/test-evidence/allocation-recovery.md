# Bounded allocation recovery

This change keeps sandbox creation and reconciliation independent and bounds
recovery of an unstarted Job without releasing an uncertain old allocation.
The previous completed release remains the [180-case MediaWorker baseline](ali-media-68d0274-cases.md)
until the affected deployed gate below finishes. No new Runtime image is required.

## Implemented behavior

- Allocation lease defaults to 30 seconds, renewed every 10 seconds. Creation
  and observation have separate bounded task sets. Durable observation tokens,
  due times and backoff prevent starvation and stale-result publication.
- New allocation policies freeze a 90-second issued-create recovery window and
  a 300-second provisioning TTL. Ordinary local/remote capacity waiting does not
  consume this uncertain recovery window. Successful adoption renews normal TTL.
- Before any Runtime submission intent, one replacement may be queued atomically.
  The original reservation, identity and cleanup stay owned. A replacement needs
  additional Pool, Scheduler and provider capacity; it cannot overcommit.
- Source generation and attempt outcome fence late Workers. Accepted/possibly
  started Runtime work uses existing same-execution recovery. Exhaustion and
  capacity failure decisions survive process loss before terminal publication.
- JobWorker reconstructs retained Pool capacity from the allocation ledger and
  legacy receipts before admission. Reducing the configured limit preserves
  existing ownership until enough allocations are retired. Late cleanup cannot
  stop a replacement collector or discard its session.
- Provider adapters reconstruct explicit inventory handles. The generic Sandbox
  object does not interpret provider metadata. OpenSandbox and Ali tests check
  original allocation identity with process caches cleared; database policy and
  frozen release identity remain authoritative.

No shared state-machine branch depends on a provider name, case or Operation.
Legacy records retain their existing policy; the seven historical unknown main
allocations are not declared absent by this migration.

## Verification before deployment

Two scheduling regressions and two provider identity regressions failed before
their fixes. Focused worker/store/provider/cleanup tests passed, including an
independent review's restart-capacity regression. The final functional code at
`0caaeb6` passed 1305 Python tests with 14 skipped; 14 isolated PostgreSQL tests
passed separately, including populated upgrades and Runtime/replacement races.
The frontend build passed. These are component/database/build checks, not
evidence for real model behavior.

The real runner [run_real_allocation_recovery.py](../../tests/run_real_allocation_recovery.py)
kills owned AllocationWorker processes at actual provider boundaries. It uses
separate databases, namespaces, credentials and a two-instance test Team.

| Case | Fault and required outcome |
| --- | --- |
| AR01 | Kill after durable claim, before provider RPC; one replacement executes, old unknown intent remains charged until authoritative fixture-scope retirement |
| AR02 | Kill after provider create returns, before handle commit; recover the same sandbox, one create and one Runtime request |
| AR03 | Same kill plus delayed inventory visibility; one capacity-accounted replacement, late original cleanup before the replacement Runtime call, one business execution |
| AR04 | Same kill plus JobWorker restart and one-slot Pool; restored old ownership blocks replacement, no Runtime request, late original is cleaned |

Development verification passed all four cases. Its source changed while the
adapter-boundary correction was being validated, so it is not the frozen release
gate. The fixed-commit gate at `426479e` is tracked separately under
`.data/evidence/allocation-recovery/recovery-final.json`. Reports preserve the
frozen profile, template/image identity, Runtime bundle digests, source hashes,
fault PIDs, request counts, real 8×8 output verification and cleanup state.
AR01 requires a separate test-Team retirement receipt before cleanup is complete.

An initial AR02 exposed the inventory-handle defect; another attempt completed
business but exposed the fixture's incorrect cleanup Gateway address. Both failed
receipts are preserved. Their owned resources and databases were cleaned with
separate evidence; neither failure is relabeled successful.

## Affected deployment gate

Read-only inspection of prior case receipts and their durable Job rows, plus
the inherited protocol mapping, selected 74 of the 169 product cases for rerun.
The remaining 95 preserve their original revisions. Child/Reviewer and sandbox
Operations, cancellation, pause, takeover and inventory adoption are affected;
pure Provider/Media workflows and preflight/artifact-only protocols retain their
previous results. C05 is included as an explicit prior unknown-submission audit.
The exact assessment is `.data/evidence/allocation-recovery/impact-assessment.json`.

Also rerun RC01–RC07 and preserve the four unaffected real MediaWorker cases.
With AR01–AR04 the eventual combined matrix contains 184 cases. Its results must
be labeled `composite_verified`, not a clean rerun of every earlier case.
Deployment, affected product results and final cleanup are still pending here;
the final report will retain each case's actual tested revision.

### Project-file transfer defect found by C29

The deployed `9c2c93d` campaign completed C05, C16, C22, R01 and R02. C29's
Child succeeded and all five media artifacts validated, but the Primary
continuation failed with `ReadError` while materializing a project file. That
byte-upload path lacked the existing bounded transport retry. Its Runtime
endpoint also shared a fixed temporary filename between requests, making an
overlapping retry unsafe. Both failure receipts remain retained.

The generic correction retries the immutable PUT body and gives each Runtime
upload its own temporary file. A lost-reply regression failed with `ReadError`
before the client fix; the overlapping-disconnect regression failed with
`FileNotFoundError` before the Runtime fix. The initial focused suite passed 137
tests; independent review passed eight checks including exhaustion and
cancellation. After adding the retry-budget and cancellation regressions, the
complete Python suite passed 1310 tests with 14 skipped. Deployed C29/C22 reruns
remain pending.
The remaining 68 product cases will continue afterward. Other successful cases
retain their evidence: healthy byte transfers and execution submission behavior
are unchanged, with transport failure and overlap covered at both API boundaries.
