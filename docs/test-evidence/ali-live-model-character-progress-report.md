# Ali Live Model Character Progress Report

## Result

Live model character progress is deployed for Primary, Child and Reviewer model
rounds. The main transcript keeps one cumulative Turn counter across tool
boundaries; Execution Graph round details receive content-free live counts.
Both surfaces animate between observed values instead of mounting at the final
number.

The frozen real catalog remains the previously verified 200/200 composite at
`.data/evidence/real-campaign/final-200-composite-83f021e.json`. This change does
not alter Sandbox, Runtime execution, Provider request, Job submission or
artifact contracts, so unaffected catalog results were not rerun. Supplemental
MC01 covers the changed real Gateway/API/browser path.

## Root Causes

- Transcript progress was mounted inside one `agent_message` and keyed by that
  Item. Tool boundaries completed the Item, removed the counter and mounted a
  new identity at its final value, so the animation had no visible transition.
- Gateway already counted public output, but Execution Graph received only the
  first update, five-second PostgreSQL batches and the terminal value. A round
  first rendered with a positive count mounted directly at that value.
- One historical terminal Job retained an `in_progress` model Item. Projection
  trusted that stale row and could describe a completed parent as still generating.

## Implementation

- `web/src/main.jsx` renders one active Turn-scoped counter after all visible
  Agent messages and sums Unicode code points across response segments.
- `web/src/animated-number.jsx` animates observed totals over 500 ms while the
  stable Turn/request identity remains mounted.
- `common/models/model_activity.py` publishes bounded count-only progress at the
  configured live interval. It never publishes model text, prompts, reasoning or
  tool arguments and never waits for Redis on the Provider terminal path.
- `common/events/events.py` isolates progress in `model-turn` and `model-job`
  streams, separate from transcript and Child Runtime text replay.
- `services/api/execution_graph.py` overlays only matching active request IDs,
  never regresses the durable count, and projects stale active rows as
  interrupted when their authoritative Turn or Job is terminal.
- The database write interval remains five seconds. Redis loss degrades display
  freshness to the durable baseline without changing execution correctness.

MC01 also exposed a cleanup race: an immediate DELETE between Turn completion
and `media.turn.finished` acknowledgement durably refused the deterministic
lifecycle operation. `common/db/lifecycle.py` now rechecks the same rejected
local barrier after the blocker clears, but only when no participant command was
ever issued. Remote/committed/aborted operations remain final and effects stay
exactly once.

## Evidence

- Current final Python gate: `1647 passed, 23 skipped, 0 failed` in 354.55 seconds.
- Focused Gateway/API/security tests: `115 passed`.
- Lifecycle/cross-domain focused tests: `58 passed, 9 skipped`; direct retry
  regressions: `14 passed`.
- Frontend Vitest: `41 passed`; production build completed.
- Deployed Playwright character test observed chat `2 -> intermediate -> 36`
  and graph `0 -> intermediate -> 160`, with zero page errors. The original
  Execution Graph browser suite also passed.
- Final MC01 evidence:
  `.data/evidence/model-character-progress-final-30fbf7b.json`. A real GPT-5.6
  Turn completed in 53.788 seconds with 90 strictly increasing Redis samples
  and 89 increasing Execution Graph samples, ending at 2474 and 2471 visible
  characters respectively. Its Thread was deleted; Ali absence and capacity
  release are both confirmed.
- Browser screenshots:
  `.data/evidence/browser/deployed/character-progress-chat-mobile.png`,
  `character-progress-chat-desktop.png`, `character-progress-mobile.png` and
  `character-progress-desktop.png`.

## Deployment

- Sandbox Gateway release:
  `cba7183d05bdf5af104eb8f5066a822ad58075cba453dcd46405a916b4e78233`.
- API release:
  `51f8d7fbd57ba5f2a096ca72681bc7ba42a3e0eedf495187a3c97e81ff46e686`.
- Web distribution digest:
  `ab69d2609fdda8ee94a0c897658af9f640ebf0236d42d969c09490d566199db6`.
- PlatformWorker, JobWorker and Runtime releases were not changed or restarted.
- API and Gateway health are ready, database and Redis are available, model
  catalog inventory is fresh, and observability reports zero dropped records,
  disk errors, export failures and schema violations.
