# Broker Restart Recovery Report

Date: 2026-11-09

Scope: PostgreSQL Turn leases, fencing generations, Sandbox Runtime event replay, OpenSandbox reattachment, checkpoint and artifact publication, capability revocation, terminal cleanup recovery and namespace orphan collection.

## Deployment

The recovery implementation was validated at commit `c833d2a`; the final deployed source also includes the later project-file work. Runtime images containing the background Harness reader and replay endpoints are:

| Harness | Immutable image digest |
| --- | --- |
| Codex | `sha256:279cc36892c82953f97d82270073a070c55f1b23de646d6f03a7584370ee47b8` |
| Claude | `sha256:f751cee7a8defc18449e0378ac0d8329e5856234a9dd8110e8488ef27675b89a` |
| DeepSeek | `sha256:c9c474d5436f3853bef7b0e8683b7a1e0c4efbd93bb7dafe4619da8ef9cb62c6` |

The public route `http://updream-lab.bilibili.co/sandbox-harness/` and the internal health endpoint both returned HTTP 200 after deployment. API, Gateway and network units were active.

## Verification

The local suite passed `45` tests. It includes stale-owner fencing, expired running-Turn reattachment, Runtime event replay, cleanup completion after Sandbox deletion, input/drain concurrency and execution without any HTTP subscriber.

Real model smokes passed for Codex `gpt-6-sol`, Claude `claude-sonnet-5-5` and DeepSeek `deepseek-v4-pro`. Every Turn completed through a real OpenSandbox Runtime and exported a validated JSON artifact.

The non-graceful recovery case started a Claude Turn whose sandbox program exclusively created a marker, slept for 35 seconds and produced a JSON artifact. After Runtime events were committed, the API main process received `SIGKILL`.

| Check | Result |
| --- | --- |
| API PID | `3121523` to `3146169` |
| Turn | `5947d0d17a8046a9a09afb1d51634b69` |
| Sandbox | `58b0cb9f-3a40-4d2c-b2f9-bcb7e2b77f6d` before and after restart |
| Recovery generation | `1` to `2` |
| Runtime sequence | `3` to `40` |
| Terminal state | `COMPLETED` |
| Recovery observation | `turn_runtime_reattached` persisted |
| Duplicate execution | none; exclusive marker succeeded and artifact reported `executions: 1` |
| Elapsed after SIGKILL | `65.403` seconds, including the 35-second sandbox program |

The first deployment attempt exposed two issues that were fixed before the final run: OpenSandbox rejects `pageSize=1000`, and an existing PostgreSQL database had received `lease_expires_at` as a timestamp without time zone. The list size is now within the real API limit, orphan scans cannot stop API startup, recovery loops survive scan errors, and initialization converts the legacy lease column to `TIMESTAMPTZ` using the database time zone.

The smokes also exposed a terminal-status contract issue: Codex could report a failed native Turn while the Runtime marked the input completed. Codex Runner now forwards the native status and error, and Runtime maps native Codex and Claude error markers to a failed Turn. The final three-Harness smoke passed after this correction.

## Final State

After verification, PostgreSQL had zero owned Turn leases and zero pending cleanup states. OpenSandbox contained five pristine warm sandboxes and one completed conversation sandbox retained by the configured session TTL. The periodic orphan reaper and expired-lease scanner were both running.
