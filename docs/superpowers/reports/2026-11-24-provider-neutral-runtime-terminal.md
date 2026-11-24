# Provider-neutral Runtime terminal report

## Design

The browser keeps using an owner-scoped, short-lived terminal ticket issued by
`SandboxInspector`. Terminal lifecycle and WebSocket transport now use the common
Runtime endpoint on port 8080 instead of a provider management endpoint. The
provider backend only resolves the Runtime endpoint; it does not implement PTY
semantics.

The Runtime contract is:

- `POST /v1/pty` creates a shell inside `/workspace/project`.
- `GET /v1/pty/{id}` reports running state, exit code and replay offsets.
- `DELETE /v1/pty/{id}` terminates the process group and closes the PTY.
- `WS /v1/pty/{id}/ws?since=N` carries input, resize, output and exit frames.

Output replay is bounded to 4 MiB per session. Frames carry their absolute byte
offset, so reconnect does not duplicate output. Paths are resolved below
`/workspace`; commands are restricted to `/bin/bash` and `/bin/sh`. Terminal
contents are not stored in PostgreSQL, Redis or host logs.

## Development

- Added the Runtime PTY manager, process-group cleanup, resize and bounded replay.
- Added RuntimeClient terminal lifecycle and endpoint resolution.
- Migrated Inspector create/status/delete/WebSocket from provider port 44772 to
  the Runtime contract.
- Recreates an exited shell instead of returning a stale session.
- Retries nonblocking PTY writes after the descriptor becomes writable.
- Preserved 44772 only as a blocked preview port; it is no longer an execution
  dependency.

## Test status

| Case | Result | Evidence |
| --- | --- | --- |
| Runtime create/input/output/resize/delete | PASS | `tests/test_runtime_terminal.py` |
| Runtime reconnect and offset replay | PASS | `tests/test_runtime_terminal.py` |
| Exited-session replacement | PASS | `tests/test_sandbox_inspector.py` |
| Runtime endpoint/credential propagation | PASS | `tests/test_runtime_client.py`, `tests/test_sandbox_inspector.py` |
| Focused regression | PASS | 31 passed |
| Full regression | PASS | 1103 passed, 5 skipped |
| Ali real PTY | PENDING | Requires new Runtime Code Bundle deployment |
| P18 restore/takeover with terminal | PENDING | Runs after Ali real PTY probe |

## Existing real-case baseline

| Case | Current result |
| --- | --- |
| AP01-AP05 provider lifecycle | PASS on Ali revision `40b758e` |
| K01 seven child cases | PASS on Ali revision `0c5a64e` |
| P18 checkpoint restore | PASS on Ali revision `0c5a64e` |
| P18 terminal before this change | FAIL: provider Execd `/pty` returned HTTP 500 |

The old failure is the reason for this protocol change, not an Ali-specific
branch. OpenSandbox, Ali and future providers will all use the same Runtime PTY.
