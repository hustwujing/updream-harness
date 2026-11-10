# Full Product Verification Report

Date: 2026-11-10

## Frozen System

- API/UI source: `3556831` (`fix: reserve sandbox capacity atomically`).
- Interaction/background Runtime source: `b476d95`.
- Public entry: `http://updream-lab.bilibili.co/sandbox-harness/`.
- Codex image: `sha256:9a6b2484a2f082a1b6675db32995187ff33e27d39efbd678c22bed8b76aba814`.
- Claude image: `sha256:7c47d03b74c9afd529c7750ff2ef5e7ce6d743927323d2ba6c2831e894f2b0aa`.
- DeepSeek image: `sha256:576332b0a69c1cc0cf98bb419c021afea38e9d4f2bc8dc071ed26d50a9543cc8`.
- Local gate: 58 passed, 0 failed, one Starlette/httpx deprecation warning.
- Web gate: Vite production build passed.

The matrix follows the risk-first and first-failure retention discipline used by `distributed-agent-sandbox-backend`. System terminal state alone was not treated as success: artifacts, browser state, database leases, Runtime events, Sandbox identity and cleanup behavior were checked independently where applicable.

## Final Matrix

| ID | Scenario | Final result | Evidence |
| --- | --- | --- | --- |
| H01 | Codex code/artifact | passed | `gpt-6-sol`, real artifact and checkpoint |
| H02 | Claude code/artifact | passed | `claude-sonnet-5-5`, real artifact and checkpoint |
| H03 | DeepSeek code/artifact | passed | `deepseek-v4-pro`, real artifact and checkpoint |
| H04 | casual conversation, three Harnesses | passed | three completed Turns, zero artifacts |
| I01 | Claude question and browser response | passed | `AWAITING_USER`, desktop answer, same Turn continued, mobile re-entry |
| I02 | Codex approval rejection | passed | rejected JSON, protected file absent |
| I03 | duplicate, foreign and expired response guards | passed | owner/idempotency unit and API tests |
| W01 | upload, rename, history and delete | passed | desktop/mobile browser plus tombstone/API tests |
| W02 | assets and artifacts restored in new Sandbox | passed | different Sandbox IDs, independently downloaded stage-2 JSON |
| W03 | deleted asset absent in attached Sandbox | passed | same Sandbox reused, `deleted.json={"absent":true}` |
| B01 | successful supervised background job | passed | delayed artifact, bounded log and completed Turn |
| B02 | background process exits 7 | passed | deterministic `background_job_failed` |
| B03 | user cancels background process group | passed | UI/API cancellation, deterministic `background_job_failed` |
| R01 | API main process `SIGKILL` | passed | PID changed, same Sandbox, generation 1 to 2, one execution |
| R02 | missing Runtime execution | passed | no replay, failed Turn and Sandbox deletion |
| R03 | Gateway restart during Turn | passed | Turn completed, recovery scanner stayed alive |
| R04 | Sandbox deletion during active Turn | passed | safe failed state, lease released, no replay |
| R05 | cleanup interrupted after intended result | passed | intended terminal state recovered, no pending cleanup |
| R06 | warm/attached Sandbox expires in OpenSandbox before Pool claim | passed | stale IDs replaced; concurrent deletion skipped during reconciliation |
| U01 | desktop interaction and Workspace | passed | pending card, file commands and background cancellation |
| U02 | mobile interaction and Workspace | passed | resolved state after re-entry, panel within viewport |
| U03 | refresh/re-entry while waiting | passed | durable Interaction and transcript, no duplicate response |

H04 contains separate Codex, Claude and DeepSeek executions. The stable browser-render round additionally passed Canvas 2D, Three.js WebGL through SwiftShader, and deliberate browser-error repair with independent frame, codec, duration and pixel-change checks.

## First Failures And Fixes

| First failure | Classification | General fix | Final verification |
| --- | --- | --- | --- |
| Runtime event and API state event selected the same sequence | program error | per-Turn async lock plus database Turn row lock | concurrent interaction continuation passed |
| Seven completed attached sessions starved new Turns until 900-second TTL | program error | evict the least recently used idle attached Sandbox | B03 and later batches started immediately |
| Two concurrent claims raced after the last semaphore slot check | program error | reserve an immediately available slot while holding the Pool lock | concurrent-capacity regression and stable six-case batch |
| Codex B03 failed upstream before creating a job | model/upstream failure; boundary not triggered | retained original result; driver now exits immediately on terminal state | Claude B03 triggered and passed cancellation protocol |
| Workspace history browser assertion ran before async response | test program error | explicit DOM wait | desktop/mobile Workspace test passed |
| Parallel smoke drivers lost HTTP while W02 intentionally restarted API | test orchestration error | mark drivers as conflicting; rerun in stable window | all three model smokes and browser cases passed |
| A user received “sandbox environment unavailable” after an overnight idle period | program error | normalize bind/detail 404 as `SandboxNotFound`, replace stale warm/attached entries, tolerate deletion between list and detail | 14 Pool/namespace tests; real deletion of two warm IDs produced two new Sandbox IDs and no metadata 404 |

The real R06 allocation test deliberately deleted both Claude warm Sandboxes before submitting two new Turns. Both Turns received newly created Sandbox IDs and entered `RUNNING`, proving the original metadata-bind 404 was removed. Their later Claude requests did not reach a prompt terminal state during the bounded observation window and were explicitly cancelled; that external/model tail is not presented as a conversational-success case.

No case-specific production branch was added. Prompt changes are limited to a short Runtime contract describing Workspace paths, output publication, `sandbox-background`, `sandbox-interact`, casual-chat behavior and the rule against redundant confirmation. Programmatic schema, ownership, fencing and lifecycle checks remain authoritative.

## Evidence Files

- `.data/final-model-stable.json`
- `.data/final-browser-stable.json`
- `.data/final-full-project-files.json`
- `.data/native-features-report.json`
- `.data/background-cancel-report.json`
- `.data/fault-matrix-report.json`
- `.data/final-broker-recovery-report.json`
- `.data/live-interaction-pending-desktop.png`
- `.data/live-interaction-resolved-mobile.png`
- `.data/background-job-running.png`
- `.data/project-files-desktop.png`
- `.data/project-files-mobile.png`

## Final Internal State

The API, Gateway and network units are active. Health reports the warm pool ready with recovery scanner and orphan reaper running. Final closure requires zero active Turn leases and zero pending cleanup states; those values are checked again after this report is committed.
