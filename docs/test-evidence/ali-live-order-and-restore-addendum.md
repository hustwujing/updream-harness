# Ali Live Order And Restore Addendum

This addendum covers the post-198-case fixes for explicit native-session errors
and exact live transcript Item routing. It does not change the frozen 198-case
catalog.

## Findings And Fixes

- A missing Codex `thread/resume` target previously emitted
  `harness_runner_failed`, then Runtime overwrote it with
  `harness_result_missing`. Runner now emits `native_session_unavailable` and
  Runtime preserves that stable error across EOF. The public message explains
  that a new conversation is required and that the old message is not replayed.
- Redis live text previously keyed overlays by Runtime execution only. One
  execution can contain multiple Agent messages separated by tools, so later
  text could be projected onto an earlier message. The first durable text batch
  now returns its exact Item ID; every live envelope carries that ID and browser
  reconciliation uses Item ID plus Runtime sequence.
- The configured 300 ms Redis publication and five-second PostgreSQL persistence
  intervals are unchanged.

## Verification

- Focused Python: `83 passed` for Runtime buffer, Broker, Runtime, Codex Runner,
  Redis boundaries and public errors.
- Frontend: `40/40` Vitest and production build.
- Runtime/module closure: `45 passed`.
- Real missing-session projection: Thread `b198109893344c7e86fcce3ccb803836`
  returned `native_session_unavailable`, RPC method `thread/resume`, code
  `-32600`, with no raw RPC message exposed.
- Real tool boundary: Thread `4e7a088fbf9c4b39aa01c754a034a9a3`
  persisted `BEFORE_TOOL_MESSAGE` at Item sequence 4, the tool at sequence 6 and
  `AFTER_TOOL_MESSAGE` at sequence 7. Redis bound the two text segments to
  different exact Item IDs.
- Real long stream: 659 characters arrived in 27 monotonic text updates; all 60
  numbered lines were present once and ordered 001 through 060. Median update
  interval was 309 ms, matching configuration; the largest retained batch was
  52 characters.
- P28 Primary SIGKILL recovery passed with nine uncommitted Runtime events.
- P29 Child SIGKILL recovery passed with nine uncommitted Runtime events.
- P18 real checkpoint release/new-sandbox restore passed three consecutive
  runs with byte-exact `project/p18.txt`. The successful restore log contained
  no `ReadError`.

The separate `b198...` queued Turn that reported
`sandbox_workspace_restore_unavailable` made four checkpoint upload attempts,
all ending in transport `ReadError` after 100-135 ms. Its preceding active Turn
was successfully recovered at generation two. This is retained as a transient
Ali Runtime endpoint failure, not evidence that Worker takeover or checkpoint
format is broken.

## Deployed Digests

- PlatformWorker: `40df4d03e33423ca01f34cbd01a1b0d6fb4089ea46ecaebee1c0584671ed4713`
- JobWorker: `d2ae6f2065cf5fd0b8c460e10ec78e7bfe8994f0444eebf9be0cd32f0d9e49a6`
- API: `cc0df4bd258780edcee0e9ec97a0e5eb7b79ed1f247b6a628bfebfbae58bb3ad`
- Runtime Code Bundle: `b03a8f2a46eadd7a546f091b6d599823953203e4f4b46639691729fc249f37a5`
- Web distribution: `906c0813186c990eb775186e02b64cf9be5a1adcbb3dbe3cdda9e5217fb35d30`
