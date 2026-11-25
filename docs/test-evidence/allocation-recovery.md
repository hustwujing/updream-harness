# Allocation recovery and affected-case release

The isolated Ali deployment runs `8b79ea5`. The current 184-case gate is an
**incomplete composite**: it retains actual revisions for unaffected successes
and reruns failures and affected cases. **173 cases meet expectations**; K02
passed on the new images. D02 and ten unrun cases are blocked by a closed
upstream image endpoint.
The [full current matrix](ali-allocation-8b79ea5-cases.md) lists every case and
its actual revision. The earlier [180-case baseline](ali-media-68d0274-cases.md) remains historical
completed evidence, not evidence that the current upstream endpoint works.

## Implemented recovery design

AllocationWorker uses a 30-second lease with a 10-second heartbeat. Creation and
reconciliation have independent bounded task sets, durable observation tokens,
due times and backoff. A stalled create does not stop reconciliation or cleanup.

New allocations freeze a 90-second uncertainty window and 300-second provisioning
TTL. Ordinary capacity waiting does not consume that uncertainty window. Before
Runtime submission intent, one replacement can be queued atomically and must
acquire additional capacity. The old allocation remains owned and charged until
authoritative reconciliation. Accepted or possibly started Runtime executions
reattach their existing identity; they are never blindly resubmitted.

JobWorker reconstructs retained Pool capacity from PostgreSQL, including legacy
receipts and reduced-limit debt. Late cleanup cannot discard a replacement
session. Provider adapters recover explicit identity from inventory; mutable
provider metadata is not the platform's lifecycle database. Shared state-machine
branches do not depend on a provider, case or Operation.

The seven historical unknown allocations remain charged. Lease expiry, TTL
passage and an empty inventory response are not evidence that they disappeared.

## Real resource and allocation gates

The real [allocation runner](../../tests/run_real_allocation_recovery.py) kills
owned AllocationWorker processes at actual Ali provider boundaries, with isolated
databases and a bounded test Team. Its deterministic Runtime fixture verifies
execution identity and artifacts; these four cases are not model-quality tests.

| Case | Verified fault and recovery |
| --- | --- |
| AR01 | Kill after claim, before RPC; one replacement executes, unknown intent remains charged until authoritative test-scope retirement |
| AR02 | Kill after create returns, before handle commit; recover the same sandbox with one create and one Runtime request |
| AR03 | Delayed inventory; one capacity-accounted replacement, original cleaned before replacement Runtime starts, one business execution |
| AR04 | Restart JobWorker with one Pool slot; restored ownership causes terminal `allocation_replacement_capacity_unavailable`, zero Runtime calls, followed by original cleanup |

AR01–AR04 passed in `recovery-final.json`; RC01–RC07 passed separately in
`capacity-final.json`, under `.data/evidence/allocation-recovery/`. The test Team
and isolated databases were removed with separate receipts. Initial failing
attempts remain preserved. OpenSandbox lifecycle checks are separate evidence;
the AR matrix is not claimed to have run on both providers.

The allocation release passed 14 isolated PostgreSQL tests, including populated
upgrades and Runtime/replacement races. Later transfer, observer and image fixes
changed no schema. At `8b79ea5`, the complete Python suite passed **1377 tests,
14 skipped**. Skips are not counted as passes.

## Defects found and verified during the affected gate

| Boundary | Generic correction | Deployed evidence |
| --- | --- | --- |
| Reviewer inline JSON | Parse inline JSON before filesystem probing, avoiding `ENAMETOOLONG` | R03/R04 and R12 internal audit passed |
| Artifact detection/collection | Bounded Matroska/WebM DocType detection; typed deterministic validation failures; transient I/O remains retryable | C29 fresh workflow and original Matroska replay passed |
| Child grant deadline | Inherit the parent deadline and sample issuance once; confirmed pre-execution retirement uses pre-acceptance failure semantics | R09 passed |
| Appended input after restart | Initialize the recovered execution identity before completing input receipts | R05/R15 and delivered-input API SIGKILL proof passed; same execution, counter remains one |
| Large managed-file upload | 4 MiB idempotent chunks, bounded staging/receipts, hash-checked atomic commit and reset/new-writer fencing | Real Ali and OpenSandbox byte/stream upload with lost chunk/commit replies passed |
| Explicit Provider rejection | Preserve `provider_endpoint_unavailable`, submission phase, side-effect state and retryability through MediaWorker/JobWorker | D02 proves correct rejection semantics; business remains externally blocked |
| Observer restore | Activate signed Runtime before restoring checkpoint and managed files, without granting model execution | 35 MiB restore/hash proof, D18/D19/P18/D42 passed |
| Packaged presentation CLI | Add the declared `editppt` image entrypoint for login shells | Three offline images and six real provider/Harness image gates passed; deployed K02 passed |

Every correction has focused red/green regression evidence and a separate small
commit. A top-level successful case is not accepted when its internal execution
shows a platform exception. K02's earlier successful top-level result was
explicitly invalidated when the missing CLI caused a model installation workaround.
The `8b79ea5` rerun finished all six Child Jobs in 1376.57 seconds, including
local disk-capacity waiting. The image-to-editable-ppt Child used the packaged
CLI without installation; all six Job reservations were released.
Its two intermediate nonzero calls were audited independently. Sequence 314 was
a redirected compound shell invocation with no recorded output, so its exact
cause remains undetermined. Sequence 364 was a diagnostic-file read of a missing
file and did not call `editppt`. Neither establishes a platform/Skill exception;
subsequent success does not erase this attribution limit. The sanitized receipt
is `runtime-cli-images/k02-nonzero-call-audit.json` under `.data/evidence`.

C29's fresh workflow completed in 902 seconds. Its new video was smaller than the
original trigger, so the exact original 64,782,809-byte Matroska was also replayed
through both byte and uneven-stream transfer with deliberately lost replies.
The independent larger upload gate used 67 MiB. These receipts are distinct.

D42's long wait was finite resource contention, not a leaked completed Job:
seven historical reservations and active Child Jobs left insufficient CPU for
its four-CPU render. It eventually acquired one reservation, completed, and
released it. The final observer-restore rerun also passed.

## Deployment and image verification

Only the three `primary-full-cpu` Harness images changed. The complete signed
catalog retains every other profile, with three new OpenSandbox digest references
and three verified Ali template revisions. Code Bundle and Content Bundle are
unchanged by the image-only fix. Registry credentials stayed on the host.

The six real image gates cover Ali/OpenSandbox × Codex/Claude/DeepSeek: native
client availability, login-shell CLI discovery, doctor, preparation, editable
PPTX generation, preview and host-side artifact verification. OpenSandbox Codex
also restored the same PPTX into another sandbox and verified its hash. All owned
probe sandboxes were deleted. Initial probe-driver environment/header failures
remain separate. One initial Ali Codex preparation failed with `HTTPStatusError`
before the probe; its response status was not captured, so its root cause is
unresolved. A fresh owned sandbox passed the full probe without a product change.
The initial failed receipt is retained and is not reclassified as a pass.

The seven application services loaded `8b79ea5`; API health, existing ingress,
desktop/mobile pages, checkpoint restore, takeover, terminal command and replay
are checked independently. The original project and shared infrastructure were
not restarted. The named paused Job still uses unchanged `general-cpu`; its
pre-existing expired deadline is not claimed to have been repaired or resumed.
The cutover audit found no nonterminal or unconfirmed old `primary-full-cpu`
Jobs. This is a specific safe cutover, not generic support for rescheduling
multiple historical profile revisions; retaining old templates alone does not
provide that capability.

Evidence paths:

- `.data/evidence/release-full-8b79ea5.json`: frozen source, catalog and bundles.
- `.data/evidence/runtime-cli-real-v3/matrix.json`: six real image checks.
- `.data/evidence/runtime-cli-images/`: registry/template publication, catalog
  diff, frozen-recovery audit, rollout, terminal and cleanup receipts.
- `.data/evidence/browser/live-8b79ea5/report.json`: real browser checks.
- `.data/evidence/allocation-recovery/live-status.json`: all 184 current rows.
- `.data/evidence/real-campaign/`: original, rerun and internal-failure receipts.

## Remaining external gate

`seedream-5.0` returns `InvalidEndpoint.ClosedEndpoint`. D02 is a non-program
failure. D17, D22, D31, D40, P01, P09, P20, P25, P26 and P27 remain unrun in the
current affected gate. A restored endpoint or a verified configured replacement
is required; old successful receipts do not prove that it currently works.
The campaign must not be described as 184/184 passing until those cases finish.

Cleanup removes only completed owned test fixtures after process-reference
checks. Failed receipts and live logs remain. Closed predeployment telemetry was gzip
archived with decompressed SHA-256 verification; archived historical logs require
offline decompression. The cleanup summary records each removed fixture and
archive receipt under `.data/evidence/runtime-cli-images/cleanup-summary.json`.
The registry currently contains 18
current profile images plus three previous primary images retained for rollback
and historical frozen allocation dependencies. These 21 tags are not obsolete
merely because a newer primary image exists.
