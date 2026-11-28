# Ali Cold Restore 199-Case Report

The impact-assessed composite contains 199 successful latest results, zero
program failures and zero non-program failures. Evidence is retained at
`.data/evidence/real-campaign/final-199-composite-7772ca8.json`.

## Root Causes

Two independent cold-resume defects appeared after a Thread outlived the
15-minute attached-sandbox keepalive:

1. Codex native thread IDs were stored by the platform, but the corresponding
   app-server session was absent from checkpoint. The old Runner terminated the
   app-server without first closing stdin and waiting for durable shutdown.
2. Checkpoint transport retries reused one short-lived HTTP client's connection
   pool and one cached Ali endpoint/token. A transport-reset endpoint therefore
   remained in every retry even though immutable checkpoint bytes were replayable.

The failures were not caused by checkpoint age, size or format. Failed samples
were about 3.0 MB; successful P18/P30 checkpoints were 3.8-4.4 MB and passed
SQLite/tar integrity checks.

## Fix

- Codex app-server now receives stdin EOF and exits before completion is
  acknowledged. A fresh app-server must successfully `thread/resume` the same
  ID before Runtime emits `harness.input.completed`.
- A missing session remains `native_session_unavailable`; no old message is
  automatically replayed and no replacement thread is silently created.
- Every checkpoint transport attempt creates a new HTTP client. After any
  `TransportError`, the next attempt calls Ali `connect(sandbox_id)` again and
  refreshes its traffic token before replaying the complete immutable object.

## Evidence

- Fault injection proved the first endpoint could fail and the refreshed
  endpoint received byte-identical checkpoint content.
- P30 cold restore passed four real runs. Each run completed a Codex Turn,
  checkpointed and released its sandbox, created a new sandbox, restored the
  workspace and resumed the same native thread.
- Verification overhead between `Codex response` and `Input completed` was
  84 ms and 96 ms in the measured run; it made no model request and consumed no
  tokens.
- The resulting 4.4 MB checkpoint had SQLite integrity `ok`, one native thread
  row and the exact platform native thread ID.
- P18 real checkpoint release/new-sandbox restore passed four consecutive runs;
  the latest successful restore logged no `ReadError`.
- P28 and P29 stream recovery remained successful after the related deployment.

Selected releases:

- PlatformWorker: `454b74ad79def887cf83ab45756e878e10c714a4b7f04d3d274d3822a852c2d9`
- JobWorker: `f845a58c08e2e9a824290fc069bee47b6459413bae67b588f73d17d57650f308`
- Runtime Code Bundle: `3e9ee491e58f52ab36ee3235014827cedacfff5f696440e747bb88dc1cfc18a8`
