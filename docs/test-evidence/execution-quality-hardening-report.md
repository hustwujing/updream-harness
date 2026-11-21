# Execution Quality Hardening Evidence

## Scope

Branch: `sandbox-harness-platform-parallel-ali`

This release addresses the execution defects observed in Thread
`2e8637bbafef48449810f0f8fe816096`: ambiguous Child publication intent,
unnecessary successful-Job re-execution, missing Child runtime tooling,
Primary timeline pollution, cumulative reused-Sandbox diagnostics, and noisy
public-proxy connect errors. The OpenSandbox implementation remains the active
provider; Ali Sandbox provider integration is intentionally deferred.

## Revision Chain

| Commit | Evidence |
| --- | --- |
| `e17990c` | Explicit delegated artifact disposition and Primary inspect preflight |
| `3465704` | Reviewed artifact fetch with ownership, length, digest and atomic-write checks |
| `5a06f97` | Signed/scoped Child catalog and bounded PPT montage helper |
| `2c6410a` | Child model activity excluded from Primary Items while observability remains |
| `ae2f17c` | Instance-bound diagnostics and safe proxy failure classification |

## Deterministic Verification

| Scope | Result |
| --- | --- |
| Task 3 Harness/Reviewer/Gateway/Skill/Bundle/CLI | 72 passed |
| Task 4 ModelActivity/Gateway/GatewaySecurity/Store | 85 passed |
| Task 5 Broker/Proxy/Observability/Config | 88 passed |
| Combined Tasks 1-5 and provider contracts | 267 passed |
| Complete Python suite | 1032 passed, 5 skipped, 1 dependency warning |
| Frontend tests/build | 19 passed; Vite production build passed |
| Release gate excluding separately evidenced Python stage | Passed, 32 non-live browser/system stages |

## Revision-Bound External Evidence

The deployable code revision was
`50326b34d13ad08ffab73e6014bf594a46f302e7`. The final release revision is the
documentation-only descendant containing this report; no `src`, `runtime`,
`deploy`, or `web` file changed between those revisions.

| Case | Result | Key evidence |
| --- | --- | --- |
| SP01 | Success | Accepted create response was dropped; idempotency reconciliation found exactly one Sandbox |
| SP02 | Success | External deletion converged; replacement capacity was acquired; managed inventory returned to zero |
| SP03 | Success | Endpoint cache expired and refreshed on the same Sandbox; Runtime health responded |
| SP04 | Success | Repeated deletion was idempotent; owned inventory was empty |
| K01 | Success | Seven Web/visual Skill Child Harnesses overlapped and produced host-validated product archives |
| K02 | Success | Six brand/DrawIO/PPT Skill Child Harnesses passed; native PPTX product contained four files and 560581 bytes |
| K12 | Success | Two Child Harnesses produced one durable continuation and final `k12-summary.md` delivery |
| Candidate fetch | Success | One reviewed source Artifact was fetched by CLI and copied byte-for-byte; source/delivery SHA-256 `41e332e509568591541e878fce188167b4097ae374a88ab4447b45cbbd693717`; Job count stayed 1; one continuation |

Supporting reports:

- `.data/evidence/sandbox-provider/execution-quality-50326b3.json`
- `.data/evidence/real-campaign/execution-quality-K01-50326b3.json`
- `.data/evidence/real-campaign/execution-quality-K02-50326b3.json`
- `.data/evidence/real-campaign/execution-quality-K12-50326b3.json`
- `.data/evidence/real-campaign/candidate-fetch-50326b3.json`
- `.data/evidence/gate/20261121T021606Z.json`

## Ali Deployment Verification

The active release manifest pinned Runtime Bundle
`9bd5b7851d008d49aa57ee3fc8361a581177642612a1247fbc3be9b21ec9e60b`
and Runtime Code Bundle
`8f9691311fbf1a12d64d82a702a604207930400f9b3216c3782b10dcd689e9da`.
Before restart, both nonterminal Turn and Job counts were zero. Only the Ali
Gateway, Job Worker and API were restarted. Each reported `NRestarts=0`, the API
reported the pinned service version, database and execution backend available,
all supervisors running, and warm pool ready.

Direct Nginx ingress and the shared FRPC PROXY listener both returned HTTP 200
with `text/html` at `/sandbox-harness-platform-parallel-ali/`. Nginx syntax,
service-account source/asset access, LibreOffice execution and independent
OpenSandbox mutable-state ownership checks passed. All release state, caches,
temporary files, evidence and bundles remained below
`/data2/wujing03/sandbox-harness-platform-parallel-ali`.
