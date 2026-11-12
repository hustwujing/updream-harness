# Sandbox Inspector Development

## Applies to

Read this before changing live sandbox overview, files, processes, metrics,
terminal, port preview, takeover, release or inspector recovery.

## Ownership and data flow

The browser never receives an OpenSandbox endpoint, API key or Execd token.
`SandboxInspector` owner-checks the Thread and relays to the Runtime or Execd:

```text
Browser -> owner-scoped API/WebSocket -> SandboxInspector
        -> Runtime /v1/inspect/*       -> safe live inspection
        -> Execd PTY WebSocket         -> terminal relay
        -> OpenSandbox port endpoint   -> private preview relay
```

The logical Thread workspace remains authoritative. A live Sandbox is an
attachment that can be deleted and restored from `threads.checkpoint_ref` plus
managed ProjectFiles.

## Protected boundaries

- File inspection exposes only `project`, `inbox` and `outputs`. It rejects
  absolute paths, `..` and symlink traversal.
- Inline editing requires `user_controlled` state and excludes managed
  `project/assets` and `project/artifacts` paths.
- Process results omit argv and environment. Telemetry and audit records never
  include terminal input/output or file content.
- Ports below 1024 and Runtime, Execd, Gateway and proxy ports are never
  previewable. A grant is owner-, Thread-, Sandbox- and expiry-bound.
- A terminal ticket is HMAC-signed, short lived and bound to owner, Thread,
  Sandbox and PTY session. The browser connects only to the host WebSocket.
- A user-controlled capability permits public proxy access according to account
  policy but has no model or Provider route.

## Takeover lifecycle

```text
observer -> user_controlled -> releasing -> observer
```

Takeover cancels an active Turn when explicitly requested and waits for normal
cleanup before acquiring the Sandbox. `Broker.submit`, archive and delete reject
while control is active. Release deletes the PTY, writes a checkpoint, revokes
the capability and schedules normal attached-Sandbox expiry. If Runtime was
killed, release deletes the broken Sandbox, revokes access and converges using
the previous checkpoint with `checkpointed=false`.

`sandbox_controls` survives API failure. Startup orphan reconciliation protects
all unfinished controls, including expired rows; the Inspector expiry loop then
checkpoints/releases them. Execd owns bounded PTY replay by byte offset, so
terminal output is not duplicated into PostgreSQL or Redis.

## Required checks

- Owner isolation and schema lifecycle for controls, grants and audit events.
- Runtime path/symlink boundaries and managed-file write rejection.
- Terminal ticket tamper/expiry and Execd replay after browser/API disconnect.
- Active-Turn cancellation before takeover; Turn submission blocked during
  control; release checkpoint and broken-Runtime convergence.
- Private port ownership, blocked ports, HTTP assets and WebSocket forwarding.
- Desktop/mobile browser layout with no overflow or page errors.
- Real OpenSandbox restore, PTY, process, metrics, port preview and API SIGKILL.
