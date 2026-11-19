# Background Execution Control Design

## Goal

Give native Harnesses one predictable execution protocol without confusing a
Sandbox-local process with a durable distributed Job. Work that must survive a
Turn, Runtime, Sandbox or machine boundary uses the existing host Durable Job
and Continuation system. Only work whose lifetime is bounded by the current
Sandbox may use `sandbox-background`.

## Execution Boundary

| Need | Executor |
| --- | --- |
| Short command whose result is immediately needed | synchronous Harness tool call |
| Independent or long command that may run concurrently in the same Sandbox | Sandbox-local supervised background Job |
| External Provider operation | host Provider Worker and Continuation |
| Work crossing a Turn, Runtime, Sandbox or machine | host Durable Job and Continuation |
| Child Harness | host Durable Harness Job; child Harness cannot delegate another Harness |

The Sandbox supervisor is not durable infrastructure. It may continue while the
API or Broker restarts, but Sandbox loss terminalizes its work as `interrupted`.
The host lease, Watchdog and Durable Job state remain the outer authority.

## Sandbox Background Protocol

`sandbox-background` exposes `start`, `wait`, `status`, `list` and `cancel`.

`start` accepts `--timeout` and `--terminate-grace`. The Runtime computes an
absolute deadline no later than the configured Sandbox background ceiling. The
supervisor enforces that deadline inside the Sandbox so it remains effective if
the Runtime process becomes temporarily unavailable.

`wait JOB_ID` is the normal consumption path. It keeps one tool invocation open,
receives Runtime heartbeats, and returns only when the Job reaches a terminal
state. `wait --timeout N` is an observation timeout: it returns a non-terminal
snapshot without cancelling or failing the Job.

The Job state machine is:

```
starting -> running -> completed | failed
                    -> cancelling -> cancelled
                    -> timing_out -> timed_out
                    -> interrupted
```

On timeout or cancellation, the supervisor sends `SIGTERM` to the complete
process group, waits the bounded grace period, sends `SIGKILL`, reaps descendants,
and atomically persists the terminal receipt. Runtime maps the receipt state
before considering the supervisor exit code; a cancelled Job can therefore
never appear completed merely because cleanup returned zero.

Only `failed`, `cancelled`, `interrupted` and `timed_out` Jobs may be replacement
targets. Errors are structured JSON with a stable code, human-readable message,
retryability and allowed actions; the CLI never exposes a Python traceback for
an expected API rejection.

## Lifecycle And Recovery

Background records, logs and supervisor receipts live in `/workspace/state`.
Runtime restart validates PID, boot ID and process start identity. Any surviving
local process group is quiesced and the Job becomes `interrupted`; it is never
silently replayed. A replacement must be explicit.

Host Durable Jobs preserve their existing lease, idempotency, reconciliation,
Continuation and cleanup behavior. This change does not create a second host
queue and does not promote Sandbox-local work to cross-Sandbox durability.

## User And Harness Experience

Runtime emits one background activity from start through terminal state. Waiting
is represented as progress on that activity rather than repeated `list` commands.
Harness guidance requires synchronous execution by default, `sandbox-background`
only for genuine same-Sandbox concurrency, and host Job tools for any boundary
crossing. Child mode keeps the existing no-recursive-Harness rule.

## Verification

Unit and component tests cover API validation, CLI error rendering, wait behavior,
deadline enforcement, cancellation mapping, replacement rules and Runtime restart.
At least three real OpenSandbox cases cover successful wait, observation timeout
without cancellation, and forced timeout of a process that ignores `SIGTERM`.
The final release requires the complete gate, deployed smoke verification and a
remote Git commit.
