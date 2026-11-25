# Ali release baseline — 2026-11-25

This record describes the tested and published code baseline
`90f51d5864c58ba9b307f158a393a78226c766f3`. Documentation committed afterward
does not change these receipts or claim another code deployment.

## Verification

| Evidence | Result |
| --- | --- |
| Product matrix | 169/169 cases met expectations |
| Real resource cases | RC01–RC07 passed on `90f51d5` with a clean source tree |
| Combined coverage | 176 passed; zero failed, running or unrun; mean 370.0 seconds |
| Coverage scope | `composite_verified`: 152 unchanged historical successes, 17 product cases rerun on `90f51d5` |
| Python | 1253 passed; seven PostgreSQL tests skipped in this invocation |
| Isolated PostgreSQL | All seven skipped tests passed separately, with allocation, ownership and live-input checks; temporary database removed |
| Internal audit | C05: one Job; R12: four Jobs; R16: six Jobs; all succeeded with completed cleanup and no attempt error codes |
| Deployed checks | Health, desktop/mobile pages, takeover, checkpoint restore, terminal command/file verification and reconnect replay passed |

RC02 expects cancellation and RC03 expects failure before submission; both
require zero Runtime executions. RC06 permits two sequential sandbox creations
after confirmed retirement, with one business execution. RC07 proves accepted
recovery and cleanup progress while the replacement Worker's only admission
slot waits on a real provider quota. See the
[resource gate contract](sandbox-capacity-gate.md).

The final incremental selection was R16, D06, D07, D08, D18, D45, P05, P27 and
K06–K14. The other 152 product successes retain their original revision and
evidence. This is complete coverage assembled under the user's scoped-retest
instruction, not a same-revision rerun of all 169 cases.

## Design and release

The [reliability record](ali-execution-reliability.md) describes the original K08,
C05/R12, output-contract and live-input fixes. The completed baseline also
includes durable allocation dispatch independent of JobWorker lifetime,
replacement allocation identity fencing, adapter-scoped cleanup claims,
cancellation-safe Broker database work, and bounded accepted recovery under
admission saturation. These are shared lifecycle fixes with no case-specific
or provider-specific state-machine branches.

The checkout, namespace, database and application state are isolated from the
original project. API uses port 24767, PostgreSQL uses 28433, and the verified
ingress is `/sandbox-harness-platform-parallel-ali/` with `Host: updream-lab`.

| Component | Loaded release at verification |
| --- | --- |
| API / JobWorker / ControlledWorker | `90f51d5` |
| AllocationWorker | `10b2c5e` |
| MediaWorker / SiteWorker / Ali Gateway | `6adddb1` |

Unchanged components kept their validated processes. The local release and
remote branch `sandbox-harness-platform-parallel-ali` were verified at
`90f51d5`. Runtime Code Bundle is
`79846858d9159cc135f93c761b85dee925b903fa30c1ef521736de2817e3551a`, Content Bundle is
`d7391229092a59483601be9a5750e88d8d538fe5da2fa325534dd49590f9dd70`, and Web digest is
`a11c76f10eba402c5a23d49fff31a81f0469cadc420c28fb7bb51dc5ad018753`.
No image dependency changed; the existing immutable images were reused.

## Cleanup and remaining reconciliation

Owned test directories reclaimed 10,651,570,176 bytes (about 9.92 GiB).
The exclusive capacity-test Team, template, quota and API key were deleted;
`TeamNotFound` and an empty matching Team list confirmed retirement. Its retained
diagnostic database and revoked local fixture credentials were then removed.
The 18 inspected remote image tags remained referenced by the current release;
unknown-owner or referenced local images were retained.

Seven historical allocations in the main Ali namespace remain unresolved and
charged. They have no sandbox handle; empty inventory cannot prove absence.
Provider audit queries returned `NoPermission`. Successful current cases do not
resolve these historical obligations, and they are not included in reclaimed
resources. Follow the [recovery rules](../development/recovery-and-observability.md)
before releasing their ownership.

## Local evidence

Raw reports remain in the ignored `.data` tree of the Ali checkout, not in Git.
They contain the complete per-case table and original verification revisions:

- `.data/evidence/release/ali-final-90f51d5.md` and `.json`
- `.data/evidence/real-campaign/resume-90f51d5/report.json`
- `.data/evidence/real-campaign/maintenance-impact-90f51d5.json`
- `.data/evidence/resource-exhaustion/job-capacity-maintenance-final.json`
- `.data/evidence/unit-gate-90f51d5.json`
- `.data/evidence/real-campaign/final-internal-audit-90f51d5.json`
- `.data/evidence/release-full-90f51d5.json`
- `.data/evidence/release/service-versions-90f51d5.json`
- `.data/evidence/release/health-90f51d5.json` and `push-90f51d5.json`
- `.data/evidence/browser/live-90f51d5/report.json`
- `.data/evidence/release/terminal-90f51d5.json`
- `.data/evidence/cleanup/capacity-provider-team.json`
- `.data/evidence/provider-audit/unresolved-cleanup-90f51d5.json`

The old capacity fixture and credentials have been retired. A future RC run
must provision a new exclusive provider scope and fixture as described in the
[capacity gate](sandbox-capacity-gate.md); the archived fixture is not runnable.
