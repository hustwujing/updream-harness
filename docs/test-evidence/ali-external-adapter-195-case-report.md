# Ali External-Adapter 195-Case Report

## Outcome

The catalog contains 195 runnable cases. The composite report has 195 successful
latest results, zero program failures, zero non-program failures and zero
optimizable-prompt failures. It is `composite_verified`: unaffected cases retain
their actual historical revision instead of being rerun solely to create a
single-revision report.

Evidence:

- `.data/evidence/real-campaign/final-195-composite-fd76e5a.json`
- `.data/evidence/real-campaign/representative-async-5cases-fd76e5a.json`
- `.data/evidence/real-campaign/K15-provider-contract-reference-prefix-3f12d79.json`
- `.data/evidence/real-campaign/K16-video-reference-prefix-3f12d79.json`

| Scope | Total | Success | Program failure | Non-program failure |
| --- | ---: | ---: | ---: | ---: |
| Retained unaffected evidence | 188 | 188 | 0 | 0 |
| Current representative reruns | 5 | 5 | 0 | 0 |
| New focused Provider cases | 2 | 2 | 0 | 0 |
| **Composite total** | **195** | **195** | **0** | **0** |

## Representative Reruns

| Case | External asynchronous boundary | Result |
| --- | --- | --- |
| S01 | Primary model route and real Harness | Success; validated 256x256 PNG |
| P13 | Child Harness, sandbox allocation and JobWorker SIGKILL takeover | Success; two attempts, one external execution, no duplicate submission |
| K08 | Controlled Operations and Media Render | Success; four operations overlapped and all frozen output roles were collected |
| XDB05 | Site confirmation, cross-database command and immutable publication | Success; two-file public version activated |
| EVO12 | Real Harness public/internal error separation | Success; no diagnostic detail leaked and troubleshooting identity stayed stable |
| K15 | Ark image contract and isolated reference prefix | Success; `3K` rejected with zero Provider submissions; text and reference images collected |
| K16 | Happyhorse and Wan isolated reference prefix | Success; both Provider IDs persisted, polled and collected |

## Implemented Contracts

- `src/sandbox_harness/common/execution/job_adapters.py` defines mandatory
  side-effect-free `preflight()` before submit. JobWorker retains only generic
  leases, generation fences, retry, reconciliation, cancellation, collection
  and cleanup behavior.
- `src/sandbox_harness/common/providers/` owns Ark, MiniMax, Tripo and DashScope
  model schemas, reference limits and request normalization. Missing Adapter
  registration fails closed; there is no kind-based compatibility fallback.
- Child Harness, Reviewer, Controlled Operations, Media Render, Image
  Decompose, Site publication, model routes and every Sandbox Backend have
  explicit preflight boundaries. `tests/test_async_adapter_contracts.py`
  prevents a production Adapter from inheriting an empty implementation.
- `src/sandbox_harness/common/contracts/public_errors.py` preserves stable
  orchestration codes while projecting a bounded `error_reason` only from the
  trusted redacted envelope. Job/Media continuation, Site views, execution
  graph and browser details consume the same projection.

## Defects Found And Fixed

1. Seedream requests converted explicit dimensions into unsupported `2K/3K`
   aliases. Ark now preserves `WIDTHxHEIGHT` and enforces the real 4,624,220
   pixel ceiling before confirmation.
2. A Published model inventory entry did not prove endpoint capability.
   Provider contracts and real probes are now the release authority.
3. MiniMax voice clone code `2039` represented a duplicate deterministic voice
   identity. The Worker confirms absence, rotates the owner/reference revision
   once and retries the same frozen audio; a second collision is terminal.
4. Media reference URL validation hard-coded the predecessor path
   `/updream/aniforge/{kind}` and rejected this deployment's configured
   `aniforge/wujing02` prefix. Validation now derives the exact HTTPS origin,
   bucket, configured media prefix and kind while continuing to reject
   signed/internal/cross-bucket URLs.
5. Runtime Code Bundle closure omitted the new Provider contract package. The
   bundle builder and activation test now import the packaged MediaCatalog from
   the materialized bundle.
6. API startup correctly rejected a stale frontend digest after the new build.
   Deployment configuration was updated to the exact served digest before
   health verification.

## Release And Verification

- Full Python gate: `1613 passed, 23 skipped` in 331.08 seconds.
- Focused post-fix media/reference gate: `85 passed`.
- Catalog and campaign gate after K15/K16: `43 passed`.
- Frontend unit gate: `37/37`; production Vite build passed.
- Desktop/mobile Playwright Media, Site and execution-graph flows passed with
  zero page errors.
- Runtime Code Bundle:
  `613241c007473bb5a5451f35a9222fd4ffe2b9f55d554be5eab049016d921960`.
- Every affected service was restarted with SIGKILL. PIDs changed, systemd
  recovered each unit and health reported zero observability drops, disk errors,
  export failures and schema violations.

An exploratory K09 run before the reference-prefix fix produced three
deterministic platform failures (`image-reference`, `happyhorse-edit`,
`wan-edit`). Its accepted long-running MiniMax text task retained the same
Provider ID across MediaWorker restart. K15 and K16 reran exactly the three
affected reference paths after the fix; unrelated Provider successes are
retained from their tested revisions.
