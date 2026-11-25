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

Continuous capacity waits keep one Job attempt and generation. Before retrying
submission, JobWorker restores the same attempt from `INFRASTRUCTURE_WAIT` to
`SUBMITTING` through `Store.mark_job_submission_started`; provisional allocation
must never be recorded while the attempt is still waiting. The existing owner,
generation and acceptance fences remain mandatory. Accepted executions and
unknown submission outcomes are reconciled, not reopened by this transition.

Status transport failures and HTTP 408/429/500/502/503/504 remain nonterminal
until the frozen Job deadline. JobWorker polls the same accepted handle; it must
not turn a temporary status outage into an immediate terminal `UNKNOWN` or
submit a replacement. Permanent HTTP failures still follow the reconciliation
error contract.

Child Runtime configuration and input staging precede execution submission. A
failure there is `child_preparation_failed` with no execution side effects after
the allocated sandbox is retired. This includes the earlier grant issuance,
receipt persistence and pause-state lookup block. A failed retirement keeps the
original allocation/reservation for durable cleanup; cancellation retains its
existing phase-specific behavior. Once `start_execution` has been attempted,
response loss preserves the sandbox, reservation and execution ID in
`SubmissionOutcomeUnknown`; status/recovery uses that handle instead of deleting
the sandbox or submitting a second execution.

Cancellation after an execution request propagates as cancellation and retains
the provisional reservation for durable cleanup or worker takeover. Terminal
cleanup may revisit an already released reservation only after validating its
Job, attempt generation, node and node generation. It still performs capability
revocation and sandbox deletion; it does not renew or reactivate that reservation.
Nonterminal cleanup keeps the normal reservation hold.

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
6. Unexpected transfer/storage faults during `collect()` are collection failures. The Durable Worker records
   `artifact_collection_failed` and preserves the accepted execution boundary.
   If the Descriptor permits collection retry, the Store closes the current
   Attempt as `COLLECTION_RETRY`, releases its lease and creates a later
   generation against the same external execution ID. It must not call
   `submit()` again. `max_collection_attempts` is frozen with the Job.
7. Deterministic Operations may retry only before acceptance and only up to the
   frozen Descriptor limit. Input failures are never infrastructure failures.
8. GPU node failures use `retry_class=node_health_change`; invalid media,
   missing audio, no target and policy rejection do not.
9. New stable error codes must be added to `JOB_ERROR_CONTRACTS` with tests
   before the Adapter can ship.
10. Reviewer verdict/schema validation is deterministic. It never uses generic
    artifact collection retry: the Reviewer Job and required/optional Review
    Gate reach their terminal outcome atomically on the first such failure.
11. Frozen collection cardinality violations use the typed Store exception and
    `artifact_collection_cardinality_invalid`: validation phase, completed side
    effects, and no collection retry. Results retain the requested role/count
    bounds and actual count; `min_items`/`max_items` count files within a
    collection. Child collection preserves this boundary and its summary.
    Candidates remain unpublished and normal durable cleanup still runs.
    Cardinality evidence alone does not prove a non-program cause: the campaign
    continues to require source/output investigation and retains strict failure
    classification.
12. MIME contract rejection, declared/content mismatch, per-file limits and
    collection-total limits raise `ArtifactValidationError`. The corresponding
    `artifact_mime_not_allowed`, `artifact_mime_mismatch`,
    `artifact_item_too_large` and `artifact_collection_too_large` results use
    validation phase, completed side effects and no collection retry. Preserve
    preceding ownership, generation, role and disposition checks. MIME probing
    must propagate storage `OSError`; only content parsing failures may become
    an unknown format. Gateway/Store/Worker regressions exercise all four codes
    and replacement-worker recovery after text/ZIP reads fail once.

## Readiness And Preflight

Dynamic service availability belongs in the execution catalog, not in prompt
text. A Descriptor readiness probe publishes `available`, stable reason,
checked time and generation. Prepare, edit and confirm refresh this state and
reject an unavailable execution before a confirmation card, budget reservation
or Job exists.

Provider adapters expose a side-effect-free preflight for Capability scope,
mode/model parameters, Artifact bindings, bytes and media metadata. Preflight
must share validation logic with submission but may not publish assets, consume
Provider budget or create an external task. A successful preflight is not a
promise that capacity remains available after confirmation; that later failure
still follows the normal structured error contract.

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

Adapters with accepted-handle collection cover worker death between collection
attempts and exhaustion. Reviewer adapters instead cover malformed verdict and
schema failures as immediate terminal outcomes. Readiness-enabled Descriptors
cover unhealthy rejection and healthy re-enablement without a process restart.

## Explicitly unavailable Provider endpoints

An explicit closed endpoint rejection is `provider_endpoint_unavailable`:
submission phase, no automatic retry, and administrator recovery/configuration
advice. It is neither invalid user input nor an uncertain submission. Provider
wire codes are translated by `media_errors.classify_provider_error`; shared
workers consume only the canonical error contract. A real upstream execution
handle remains accepted even when a later lookup fails. A local MediaJob ID
alone does not prove that the upstream accepted execution.

`tests/test_provider_endpoint_failure.py` covers wire translation, exactly one
rejected submission and the canonical side-effect state through the Media
adapter and generic Job error normalization. Endpoint recovery itself requires
separate live Provider evidence; static model preflight does not establish it.
