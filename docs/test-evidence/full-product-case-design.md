# Full Product Case Design

This matrix adapts the risk-first campaign discipline from `distributed-agent-sandbox-backend` without importing its DAG, Planner, Reviewer or media-operation assumptions. Every real case binds source commit, Runtime image digests and driver hash; first failures remain in evidence and retries do not overwrite them.

| ID | Group | Scenario | Required evidence |
| --- | --- | --- | --- |
| H01 | Harness | Codex code and artifact smoke | transcript, tool events, artifact bytes, checkpoint, cleanup |
| H02 | Harness | Claude code and artifact smoke | same |
| H03 | Harness | DeepSeek code and artifact smoke | same |
| H04 | Harness | casual conversation creates no files | response, empty artifact delta |
| I01 | Interaction | Claude question through `sandbox-interact` | `AWAITING_USER`, browser answer, same execution continues |
| I02 | Interaction | Codex approval reject path | durable rejection and no protected action |
| I03 | Interaction | duplicate, foreign and expired responses | HTTP status and unchanged Runtime state |
| W01 | Workspace | direct upload, rename, history and delete | owner-scoped API, tombstone, no object reference leak |
| W02 | Workspace | prior upload and output restored in a new Sandbox | different Sandbox IDs and independent output validation |
| W03 | Workspace | deleted file absent from attached Sandbox next Turn | reset/materialization evidence |
| B01 | Background | successful job finishes after Harness delegates it | job events, bounded log, artifact and Turn completion |
| B02 | Background | nonzero job blocks successful Turn | failed job and `background_job_failed` |
| B03 | Background | user cancellation terminates process group | cancelling/cancelled states and no live child |
| R01 | Recovery | API main process `SIGKILL` during active Turn | same Sandbox, generation advance, no duplicate execution |
| R02 | Recovery | Runtime execution missing after lease expiry | deterministic safe failure and Sandbox cleanup |
| R03 | Recovery | Gateway restart during model activity | bounded Harness behavior, scanner remains alive |
| R04 | Recovery | Sandbox deletion during active Turn | safe terminal state, no replay, no leaked lease |
| R05 | Recovery | cleanup interruption after intended final state | final-state recovery and capability revocation attempt |
| U01 | Browser | desktop interaction, jobs and Workspace | screenshot, no page errors or overlap |
| U02 | Browser | mobile interaction, jobs and Workspace | screenshot, controls inside viewport |
| U03 | Browser | refresh/re-entry while waiting | pending decision and transcript preserved |

Classification is `passed`, `program_failure`, `prompt_failure`, `model_failure`, `external_dependency` or `pending_attribution`. A non-program failure counts as acceptable only when the program preserves evidence, applies the documented bounded response and cleans up resources.
