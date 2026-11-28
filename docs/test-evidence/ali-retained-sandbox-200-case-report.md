# Ali Retained Sandbox 200-Case Report

The impact-assessed composite contains 200 successful latest results, zero
program failures and zero non-program failures. Evidence is retained at
`.data/evidence/real-campaign/final-200-composite-83f021e.json`.

## Incident

Thread `42772a1eb3784210a527e67864215789` completed its Primary Turn and retained
Ali sandbox `sbx-792e1b24-e1a6-48a5-953b-ce8a9b462d96` until 04:41:26. A
PlatformWorker restart at 04:28:32 incorrectly classified it as an orphan and
deleted it, thirteen minutes before the durable deadline. Four later user Turns
read the unchanged `RETAINED` ownership and failed with `sandbox_not_found`.

The cause was not TTL or Ali deletion. Startup orphan protection included live
Turns, Jobs and local Pool state but omitted non-RELEASED `sandbox_ownership`
handles restored from PostgreSQL.

## Fix

- Every non-RELEASED durable ownership handle and allocation is included in
  startup orphan protection, including `RETAINED` sessions.
- A provider-authoritative not-found result commits direct release of the stale
  ownership before a normal Primary Turn creates a fresh allocation.
- Explicit allocation owners remain conservative; they do not silently replace
  an accepted logical allocation.

## Evidence

- Focused ownership, Pool, Broker and namespace regressions: `163 passed`.
- P31 created a retained sandbox, selected the new PlatformWorker release,
  SIGKILLed PID 2321297, started PID 2355522, confirmed the provider sandbox
  remained present and completed a second Turn using the same sandbox ID.
- The affected user Thread checkpoint is 4.28 MB, has SQLite integrity `ok`,
  contains exactly one Codex thread row and includes native thread
  `01a22066-71e9-79e3-ab4f-e3f52d338476`. Once stale ownership converges, it is
  eligible for fresh-sandbox checkpoint restore and native-session resume.

Selected PlatformWorker release:
`683ff22701735fa8383717d2bf3b4d036a876ac6413b39f31fd4edb264a26e96`.
