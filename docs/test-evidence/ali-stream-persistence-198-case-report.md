# Ali Stream Persistence 198-Case Report

The impact-assessed composite contains 198 successful latest results, zero
program failures and zero non-program failures. Unaffected results retain their
actual earlier revision; only contracts changed by stream batching and recovery
were rerun.

Evidence: `.data/evidence/real-campaign/final-198-composite-b111a08.json`.

## Change

- Primary, Child and Reviewer public text and tool output use one shared buffer.
- Redis publishes bounded live text at most every 300 ms; PostgreSQL commits
  aggregated stream batches every five seconds by default.
- Control and terminal events flush preceding data immediately.
- Runtime writes a private segmented journal before publication. Database commit
  precedes monotonic ACK and journal compaction.
- Child JobActivity display order is independent from each Runtime execution's
  replay sequence, so nested continuation execution can restart at sequence one.
- Main transcript and execution graph show received public character progress;
  hidden reasoning and tool arguments are excluded.

## Deterministic Gates

- Full Python gate before release: `1633 passed, 23 skipped` at `1956821`.
- Focused Child collection fix gate: `109 passed` at `b111a08`.
- Frontend: `39/39` Vitest and production Vite build.
- Real-case catalog generation and contract tests: `43 passed`, 198 cases.
- Runtime/module closure gate: `49 passed`.

## Real Ali Evidence

| Case | Result | Evidence |
| --- | --- | --- |
| P28 Primary stream SIGKILL | success | Runtime sequence 3 vs committed 1; PID 498396 to 503051; two durable text batches; markers exactly once |
| P29 Child stream SIGKILL | success | Runtime sequence 9 vs committed 1; PID 538387 to 542643; three durable text batches; original execution retained |
| P13 Child accepted takeover | success | two Attempts, one accepted external execution, no duplicate submission |
| P24 nested Child continuation | success | real `image.segment`, second Child execution resumed, `segmented-person.png` delivered |
| D43 Reviewer takeover | success | two Reviewer Attempts, same external execution, valid 64x64 `final.png` |

The first P29 run exposed a collector/collection race: Runtime and persisted
Activity contained the full result, but Job collection read before the final
collector flush and stored only `CH`. `SandboxChildBackend.status()` now waits
for the collector to settle before terminal collection, and `child_summary`
reads the complete Activity payload instead of its 1000-character display
summary. The focused gate and the repeated real P29 passed after this fix.

## Browser And Deployment

- Live transcript character samples increased monotonically from 35 to 159.
- Execution graph model-round character progress was visible and updating.
- Desktop 1440x960 and mobile 390x844 screenshots showed no overlap.
- API health and nginx Host/path ingress returned 200. The development host's
  resolver could not resolve the public hostname directly, so ingress was also
  checked through local nginx with `Host: updream-lab.bilibili.co`.

Selected release digests:

- PlatformWorker: `4ac38f4f3e7826c1cd4c8c9f35c3467c5aa04c8edb59917565c163ac4a7e4ade`
- JobWorker: `93e8d206866c97b27abd49e2aa0f501be26f206754b4f684c2cc1e298d6ea8ba`
- sandbox-gateway: `8b2839f0f3ddf043374326b486c717f330b8fd83656be57537f5b0fb9901b6c1`
- API: `628333f18c9842b2e3b08b8cd58e3758cda7b39e61b9fbc1d10248fb1d52f836`
- Runtime Code Bundle: `0a82a8402d494253f7c785123fe5aae4e550aa55ae66fe71717a20be31c4d5a5`
- Web distribution: `a1b14d43283734484e0dd39f66c4ba7d3a3f0dc3f3f7757f863a60d394a034d9`

All affected services were selected from immutable manifests and restarted by
SIGKILL. Health, leader ownership, model catalog, queue writer and observability
counters were healthy after restart.
