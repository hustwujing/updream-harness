# External Execution Error Contract

This standard applies to every Durable Operation, Provider, Child Harness and
future external executor. An Adapter-specific exception or upstream response is
not a public error contract.

## Required Result Fields

Every `FAILED`, `BLOCKED` or `UNKNOWN` Job result must expose:

- `error_code`: stable machine-readable identity registered in
  `src/sandbox_harness/job_errors.py`;
- `error_message`: safe user-facing reason;
- `error_guidance`: one concrete next action;
- `failed_phase`: one of `input_validation`, `authorization`, `capacity`,
  `submission`, `status`, `execution`, `collection`, `validation`, `review`,
  `cancellation`, `reconciliation`, `cleanup`, or `internal`;
- `retryable` and `retry_class`: whether and how the same immutable execution
  may continue;
- `side_effect_state`: `none`, `possible`, `accepted`, `completed`, or
  `unknown`.

`error_detail` is diagnostic evidence. UI surfaces must use `error_message` and
`error_guidance`, never an upstream body, Python stack, credential, path or
opaque Provider message.

## Phase And Retry Matrix

| Failure boundary | Side effect | Permitted action |
|---|---|---|
| Input or authorization rejected | `none` | Fail immediately. Change input or authorization; do not automatically retry. |
| Capacity unavailable | `none` | Wait for a compatible node. Capacity waits do not consume business retry attempts. |
| Submission rejected before send | `none` | Use the Descriptor's bounded pre-accept policy. |
| Submission response lost | `unknown` | Persist the idempotency key and any handle. Reconcile only; never blind-submit. |
| Accepted task temporarily cannot be polled | `accepted` | Keep querying the same handle with backoff. |
| Accepted task reports failure | `accepted` | Preserve the failure and let the parent choose a replacement. |
| Result download or collection fails | `completed` | Resume collection from the same execution. Do not rerun generation or Operation work. |
| Deterministic validation fails | `completed` | Block publication; create an explicit repair or replacement. |
| Cancellation result is unknown | `unknown` | Reconcile the original handle before any replacement. |
| Deadline before submission | `none` | A replacement is safe after checking budget and scope. |
| Deadline during or after submission | `unknown` | Reconcile or cancel the original execution; never assume failure. |

## Adapter Rules

1. Validate frozen inputs before external submission. Raise `PreAcceptFailure`
   with a registered code, canonical phase and explicit retryability.
2. Do not throw a raw `ValueError`, HTTP exception or SDK exception across the
   Adapter boundary for an expected external failure.
3. A Provider transport error after bytes may have been sent is
   `provider_submission_outcome_unknown`, not `provider_service_unavailable`.
4. Once an external handle exists, retries operate on that handle. A new submit
   requires an explicit replacement Job with a new idempotency key.
5. `status()` maps transient query failures to a nonterminal status when the
   Provider can still be queried. A terminal `UNKNOWN` must retain the handle and
   use a reconciliation error code.
6. `collect()` failures are collection failures. The Durable Worker records
   `artifact_collection_failed` and preserves the accepted execution boundary.
7. Deterministic Operations may retry only before acceptance and only up to the
   frozen Descriptor limit. Input failures are never infrastructure failures.
8. GPU node failures use `retry_class=node_health_change`; invalid media,
   missing audio, no target and policy rejection do not.
9. New stable error codes must be added to `JOB_ERROR_CONTRACTS` with tests
   before the Adapter can ship.

## User Experience

- The execution graph shows the safe reason, failed phase, retry policy,
  external side-effect state and next action.
- `UNKNOWN` means “must reconcile”, not ordinary failure.
- A waiting/retrying Job retains an active visual state and says which phase is
  waiting.
- Query IDs and internal logs retain diagnostic detail; the UI does not expose
  raw upstream content.

## Required Tests

Each Adapter must cover input rejection, authentication/permission failure,
rate or capacity wait, response loss at submit, status outage after acceptance,
remote execution failure, collection failure, invalid output, cancellation
ambiguity and deadline behavior where those states are supported. Tests assert
the stable code, phase, retryability, side-effect state, public message and
guidance, plus the absence of duplicate external submissions.
