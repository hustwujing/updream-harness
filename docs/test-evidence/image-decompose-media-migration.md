# Image decomposition delegated to MediaWorker

## Change and evidence boundary

`image.decompose` now hands its authorized Durable Job to MediaWorker through
one fenced database transaction. The independently deployed image-layering
HTTP service retains inference/GPU ownership. The source Job retains its
Artifact contract, Review and continuation. No sandbox-provider switch or UI
redesign is included.

The old direct HTTP submission, active-job guessing and temporary output cache
were removed after their replacement passed real inference tests. Exact legacy
receipt import and matching already-bound Artifact identities remain supported.
MediaWorker resumes accepted native IDs and persisted output manifests; an
unknown submit or cancellation never authorizes another create.

The isolated service-write fix is commit `5b747d4`. Its deployment receipt is
`.data/evidence/image-service-isolation-5b747d4.json`: Ali service restart,
HTTP 200, explicit cache paths and patched dependency hashes. Shared GPU lock
inodes/selection remain unchanged and locks are used read-only.

## Focused real cases

Runner: `tests/run_real_image_decompose_media.py`. The four cases used real HTTP
and Qwen inference, isolated platform SQLite databases and actual Worker
subprocesses. They did not run a real Primary Harness; deployed product cases
below cover that separate boundary. Fault injection forwarded real requests
and dropped selected real responses.

| Case | Behavior | Result | Seconds |
| --- | --- | --- | ---: |
| DM01 | Filter PNG/ORA; drop one artifact response; resume staged collection | Passed, one create, six selected files | 155.05 |
| DM02 | SIGKILL MediaWorker after native ID commit; replacement polls same ID | Passed, generation 1→7, one create, six files | 139.04 |
| DM03 | Cancel an accepted native execution | Passed, CANCELLED, no published files, quiescence confirmed | 17.58 |
| DM04 | Lose accepted create response; cancel still preserves ambiguity | Passed, UNKNOWN, one create, test-owned native job quiesced separately | 2.41 |

Evidence: `.data/evidence/image-decompose-media/run-20261125-1307/report.json`.
Mean completed duration was 78.52 seconds. The receipt records base HEAD
`5b747d4` with a working-tree source digest, because the migration had not yet
been committed. That source digest must match the release's implementation;
do not silently relabel the original receipt to a later commit.

## Regression impact assessment

| Existing case | Why rerun |
| --- | --- |
| C09 | Primary Harness invokes real decomposition and delivers reconstruction/ORA |
| K07 | Decomposition participates in a parallel visual Operation group |
| P20 | Shared MediaWorker/Provider collection and one mixed-group continuation |
| K14 | Two-stage immutable Provider Artifact handoff and publication |

Retain the other 172 successes from the 176-case `90f51d5` composite baseline
with their actual revisions. Native Ark/MiniMax/Tripo request formats, sandbox
allocation, ControlledWorker execution and frontend rendering did not change.
Their existing component suites remain part of the full Python gate. D46's
inherited driver uses a controlled lost-response adapter, so it is not relabeled
as real MediaWorker crash evidence; DM02 is the new real inference crash proof.
The combined case inventory becomes 180 after adding DM01–DM04.

## Component and database verification

- First regression failed because JobWorker still sent the provider POST.
- A second execution regression failed before MediaWorker transport dispatch
  was implemented.
- Independent review reproduced unknown cancellation, legacy Artifact identity
  and optional-format size bugs; the fixes passed separate regression checks.
- Focused suite: 131 passed, followed by the additional owner/thread scope test.
- Isolated PostgreSQL suite: 10 passed, including populated old-schema upgrade,
  concurrent handoff across Store instances and cancellation/handoff ordering.
  The throwaway database was removed.
- Service/deployment isolation checks: 12 passed, with no GPU used by the tests.
- Full Python suite: 1275 passed, 9 skipped in 316.98 seconds. The subsequently
  added owner-scope check passed separately; the added PostgreSQL migration
  check is included in the ten-test isolated PostgreSQL gate above.
- Frontend tests: 27 passed. UI implementation was unchanged.

## Deployed verification and cleanup

Implementation commit `68d0274` is deployed to the isolated Ali API, JobWorker,
MediaWorker and Gateway. The additive source-Job schema upgrade is present.
Health, desktop/mobile pages and the existing `/sandbox-harness-platform-parallel-ali/`
ingress passed. Real sandbox takeover, checkpoint restore, terminal execution,
command-file bytes and WebSocket replay passed on that release.

C09, K07, P20 and K14 all passed. The four product cases contain 13 successful
Durable Jobs and seven successful MediaJobs. K07's one infrastructure-capacity
wait completed within the same attempt; no unresolved internal program error
was hidden by a successful top-level case. C09's final PNG is 512×512, and its
ORA was independently opened and verified to contain three decodable layers
on a 512×512 canvas. Every final file matched its persisted size and SHA-256.

The eight current regressions passed with a mean duration of 160.27 seconds.
The [complete 180-case result table](ali-media-68d0274-cases.md) retains the actual
revision for every historical success. This is composite coverage, not a new
180-case run on one revision.

Release receipts are `.data/evidence/release-full-68d0274.json`,
`.data/evidence/decompose-rollout-68d0274.json`,
`.data/evidence/decompose-product-internal-audit-68d0274.json` and
`.data/evidence/release/ali-media-68d0274.json`. Runtime Code Bundle is
`454e70b86df6856057a69b043e049716837d3a8e2a55c2ef9c01d186611d8b6f`;
Content Bundle is `d7391229092a59483601be9a5750e88d8d538fe5da2fa325534dd49590f9dd70`.
The existing verified runtime images were reused; this host-code release did
not require new images.

After verification, four owned terminal inference jobs and eight named pytest
temporary/cache directories were removed, totaling 995,082,240 allocated bytes.
Selected artifacts, isolated database receipts and sanitized reports remain
available. The seven historical unknown sandbox allocations were not modified
or reported as reclaimed. Cleanup evidence is
`.data/evidence/decompose-cleanup-68d0274.json`.

The repository is public at
[hustwujing/updream-harness](https://github.com/hustwujing/updream-harness).
Code and subsequent documentation commits use branch
`sandbox-harness-platform-parallel-ali`; documentation-only commits do not
change the deployed `68d0274` receipts.
