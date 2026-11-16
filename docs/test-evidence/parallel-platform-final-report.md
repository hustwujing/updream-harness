# Sandbox Harness Platform Parallel Final Report

## Outcome

The isolated Parallel environment completed its runnable real-case catalog with
98/98 success: 20 durable parallel protocol cases and 78 business cases. The
final merged report contains zero program failures, zero non-program failures
and zero optimizable prompt failures.

The deployment is independent from both predecessor projects. PostgreSQL,
objects, logs, caches, evidence and Runtime Bundles remain below
`/data2/wujing03/sandbox-harness-platform-parallel/.data`; Docker storage is
below `/data2/wujing03/opensandbox-stack/data/docker`. PostgreSQL uses port
28432 and database/user `sandbox_harness_parallel`; API uses 24766; Redis and
object prefixes are isolated.

## Real Cases

| Family | Cases | Latest mean time | Result |
|---|---:|---:|---|
| Parallel protocol P01-P20 | 20 | not recorded by protocol runner | 20 success |
| Basic S01-S31 | 31 | 122.80 s | 31 success |
| Complex C01-C30 | 30 | 379.20 s | 30 success |
| Intervention R01-R17 | 17 | 552.56 s | 17 success |
| Total | 98 | 315.08 s for 78 timed business cases | 98 success |

The longest business case was R11 at 1016.847 seconds because it performed four
independent review iterations before publishing the accepted edge repair. The
final simple `hi` browser case completed in about five seconds, produced four
activity rows and two execution-graph nodes, and created no background Job or
media task.

Primary result:
`.data/evidence/real-campaign/r15-round14.json`

Supporting evidence includes:

- `protocol-complete.json`: P01-P20 receipts.
- `prior-success-validation.json`: independent validation of earlier outputs.
- `targeted-round2-validation.json`: ASR and external media validation.
- `c16-round7-internal.json`: two first-attempt `audio.process` Jobs.
- `c22-round9.json`: registered 24-segment concat and final media checks.
- `r10-r17-round13-regraded.json`: complete intervention matrix after pagination correction.

## General Fixes

- Real-case Runner now confirms real media/JobGroup cards, waits durable
  continuations and quiescence, paginates all transcript Items, and validates
  image/audio/video/text contracts independently.
- Offline `sandbox-transcribe` reuses the predecessor's verified Whisper model
  through a digest-checked Profile overlay. No model download was required.
- Signed Profile Catalog exposes 18 Profile x Harness images. ASR is layered
  only on `primary-full-cpu` and `media-cpu`; other profiles reused existing
  layers.
- Added registered OpenSandbox Operations `audio.process` and
  `media.concat_sequence`, including deterministic retry, frozen Artifact
  inputs, role-addressed multi-output collection and validation receipts.
- Execution Catalog now returns exact parameter and JobGroup schemas plus
  frozen Turn input Artifact identities.
- Job delivery Artifacts become user-visible atomically when their group is
  terminal; unchanged `(path, sha256)` publications are idempotent across
  Turns.
- Reviewer cancellation preserves `CANCELLED`, findings use a fixed object
  schema, and the Runtime Profile is resolved from candidate MIME.
- Upload registration uses database upsert and remains idempotent under eight
  concurrent cases.
- Broker lease release completes its database transaction before cancellation,
  eliminating shutdown write-lock races.
- Dynamic interventions attach only new inputs and post-delivery hooks wait for
  actual Artifact events instead of fixed timing.

## Verification

- Runtime image matrix: 18/18 pushed; signed Profile Catalog digest
  `668e6d6f9fb0a1208eb439b8da69fa0bff45ce3329b9e35505df308902bc1076`.
- Python full gate: 566 passed, 5 skipped, 0 failed.
- Frontend: production build succeeded; Vitest 3/3 passed.
- Broker slow-cleanup stress: 10/10 repeated passes.
- Seven isolated systemd units active; API health reported warm pool ready,
  recovery scanner and orphan reaper active, and no unexpected active leases.
- Nginx prefix `/sandbox-harness-platform-parallel/` returned 200 with the
  expected cookie path; prefixed health returned the deployed release.
- Real Chromium desktop/mobile checks had zero page errors, failed requests or
  HTTP errors and no horizontal overflow. Screenshots:
  `.data/final-browser-desktop.png`, `.data/final-browser-mobile.png`, and
  `.data/final-browser-hi.png`.

## Residual Warnings

The full gate emits one Starlette TestClient deprecation warning, and Vite warns
that several preview/tool chunks exceed 500 kB. Neither is a functional failure;
large preview modules are already loaded as separate chunks. Local DNS does not
resolve `updream-lab.bilibili.co`, so external-path verification used the real
Nginx server with an explicit Host mapping as required by the environment.
