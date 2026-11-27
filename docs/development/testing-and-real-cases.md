# Testing and Real Cases

## Applies to

Read this for every change and before reporting completion.

## Test layers

1. **Pure/unit tests** validate parsers, contracts and reducers without services.
2. **Component tests** use SQLite, fake Runtime/SandboxBackend or mock transports to
   exercise transactions and boundaries.
3. **Real sandbox tests** identify OpenSandbox or Ali explicitly and verify
   image, process, network, checkpoint and recovery with isolated namespaces/data.
4. **Real Provider tests** verify the selected native Harness and upstream route.
5. **Browser tests** verify the deployed product from the user surface.

Never report a deterministic fixture as a real model test. A retained success
from an earlier revision must keep that revision and an impact assessment; it
is not evidence that a newly changed boundary passed on current code.

## Source-to-test map

| Change | Minimum focused tests |
| --- | --- |
| Store, idempotency, schema | `test_store.py`, relevant owner/cancel tests |
| Broker, lease, recovery | `test_broker.py`, `test_event_bus_boundaries.py`, `test_cancel_boundaries.py` |
| Runtime/adapters | `test_runtime_boundaries.py`, `test_runtime_configuration.py`, adapter/runner tests |
| Sandbox background control | CLI structured errors, wait observation/terminal behavior, deadline escalation, cancellation mapping and real OpenSandbox B04-B06 |
| Sandbox/pool | `test_sandbox_namespace.py`, `test_runtime_client.py` |
| Durable sandbox allocation / replacement | `test_allocation_dispatch.py`, `test_allocation_dispatch_postgres.py`, Child/Operation tests and RC04/RC06 |
| Worker saturation / cleanup routing | `test_job_worker.py`, `test_job_store.py`, `test_job_postgres_concurrency.py`, `test_thread_pause.py` and RC07 |
| Broker cancellation during database work | `test_cancel_boundaries.py`, including repeated cancellation and startup claim/release barriers; `test_broker.py` |
| Gateway/capability/network | `test_gateway.py`, `test_capability.py`, `test_public_proxy.py` |
| Models/catalog | `test_catalog.py`, `test_model_health.py`, `test_model_activity.py` |
| Objects/files | `test_s3_objects.py`, `test_runtime_paths.py`, owner tests |
| Observability | `test_observability.py` plus affected lifecycle tests |
| Frontend | production build plus the affected `tests/browser_*.cjs` scripts |
| Runtime image | `test_runtime_image_contract.py`, image build and real sandbox smoke |
| Durable Jobs / delegation | coordinator, worker, scheduler, continuation, Reviewer and PostgreSQL race tests |
| External collection retry | accepted-handle reuse, worker takeover, bounded exhaustion and zero duplicate submissions |
| Reviewer | malformed verdict, required/optional Gate atomics and immediate terminalization of deterministic contract errors |
| Catalog readiness / Provider preflight | unhealthy-to-healthy generation, prepare/edit/confirm rejection and zero side effects |
| Deployment | `test_deployment_isolation.py`, `test_ingress_config.py`, service-kill recovery and browser ingress |

## Iteration and release

Run focused tests after each coherent fix. Do not rerun the complete suite after
every small edit. Before a release, run the full Python suite and frontend build
when shared contracts, lifecycle, Gateway, Runtime or deployment changed.

After a failure, reproduce it with the smallest relevant case, fix the shared
boundary, rerun that case and the affected previously-passing cases, then resume
the remaining matrix. Do not add case-name branches or prompt keywords to make a
real case green. For the Ali campaign, the user-authorized workflow preserves
unaffected successes and reruns only failed/affected cases plus unrun cases.
Record that assessment before resuming. Complete all 175 catalog cases and any
separately requested resource cases, with each result's actual revision. This is
`composite_verified`; run the whole matrix on one revision only when a
`clean_run` claim or a newly agreed release requirement needs it.

Supplemental `AS01` injects deletion of host artifact staging roots after
Worker startup and validates real Child Harness plus Controlled Operation
collection without resubmission. Run it whenever `ArtifactGateway`, writable
directory recovery, Child collection or Controlled collection changes. It does
not change the frozen 175-case catalog count.

Documentation-only updates to a verified baseline need content, source-reference
and link checks, not another real-provider campaign or deployment. Preserve the
tested/deployed revision and commit the documentation separately; do not rewrite
existing receipts to the new documentation revision.

PostgreSQL tests use `PARALLEL_TEST_POSTGRES_URL` pointing to a unique throwaway
database, never an application database. Use the focused PostgreSQL modules,
then remove only the test database they created.

Real protocol cases are concurrent by default. Serialize only cases that mutate
shared services or global configuration, and list those IDs explicitly in the
runner. A global protocol lock hides concurrency bugs and unnecessarily delays
unrelated K cases.

Real tests must use unique namespaces, data directories and idempotency keys.
They must clean only resources they created and must not restart shared Redis,
Docker, OpenSandbox, registry or object storage. Before service restart, query
nonterminal Turns and identify ownership. Authorized Ali test restarts may run
with cases in flight; capture generations and external execution IDs before and
after, verify recovery and zero duplicate execution, and do not wait for drain
solely to avoid exercising that boundary. Protect unrelated user work.

## Evidence

Record revision, Runtime image digests, configuration mode, case trigger,
elapsed time, outcome and cleanup state. For a failure, separate program defect,
model behavior and external dependency. A non-program failure passes only when
the program response, state, cleanup and user-visible error are correct.

Completion means no known program error in affected cases and no clear,
bounded prompt/UI improvement left unaddressed. It does not mean every model
response is semantically successful.

During a requested campaign, report progress every two minutes: total, started,
running, successful, program failures, non-program failures, unrun and average
elapsed time of completed cases. Explain what each running case is doing and
whether its current state matches expectations. Negative cases count as passing
only when their intended cancellation/failure and side-effect assertions pass.

The campaign combines Primary outcome with internal Durable Job outcomes.
A completed Primary with valid Artifacts cannot hide a program failure in a
Child, Operation or Reviewer. A successfully recovered model/Provider failure
may remain a successful case, with its internal failure retained in evidence.
If an operator aborts a stalled case, retain that intervention and audit the
whole case: an existing Artifact must not turn cancelled required work into a
successful result. Retest with distinct test identities and retain the old
unknown allocation's reconciliation obligation.

Evidence for external execution must distinguish submission, polling,
collection, deterministic validation/Review and cleanup. A successful upstream
task followed by a collection failure is not evidence for a second submission;
the report must retain the original external execution ID and show the bounded
collection attempt generations.

## Delegated artifact publication regression

Artifact-producing delegated tasks must choose `candidate`, `workspace`, or
`delivery` explicitly. A Primary continuation that accepts an already reviewed
Child artifact fetches it with `sandbox-artifact fetch`, verifies the advertised
length and SHA-256, and publishes the intended bytes; it must not create a
replacement Child Job merely to change publication disposition. Real evidence
records the source Job/Artifact IDs, the pre/post Job count, fetched digest, and
the final delivery Artifact digest.

Child model calls retain spans, timing and token accounting but do not create
Primary Thread timeline Items. Timeline assertions distinguish Primary model
activity from Job activity rather than accepting a polluted Item count.

## K campaign evidence

K01-K11 are exact capability matrices. K12-K14 are user-path E2E workflows and
must exercise durable Continuation plus final delivery. Skill coverage requires
both structured evidence and a host-validated product archive; a Harness-authored
list of commands or checks is not proof that a product existed.

Campaign reports use `verification_scope=clean_run` only when every reported
success was executed on the report revision. Retaining unaffected successes
from older revisions produces `composite_verified`; summaries must preserve
each result's revision and impact assessment. After a generic fix, rerun the
failed member and every case that crosses the changed contract. Do not repeat
unrelated real cases unless a separately agreed release claim explicitly
requires a clean single-revision matrix.

## Resource exhaustion evidence

Use `tests/run_real_sandbox_capacity.py` and the
[capacity gate contract](../test-evidence/sandbox-capacity-gate.md). RC01-RC04
exercise actual provider creation refusal, waiting, cancellation, deadline and
Worker replacement. RC05 covers local scheduling exhaustion; RC06 covers a
held real create response; RC07 covers accepted recovery and cleanup while all
admission slots wait for quota. Keep fake regressions and real fault-injection
evidence distinct, and never exhaust a shared production provider account.

## Sandbox provider protocol evidence

Provider lifecycle changes run `tests/run_real_sandbox_provider_protocol.py`
before the K campaign. SP01 drops a real accepted create response and requires
unique idempotency reconciliation; SP02 externally deletes an attached sandbox
and proves capacity can be reacquired; SP03 expires the endpoint cache and
requires a real refresh plus Runtime health; SP04 repeats deletion and requires
empty owned inventory. Evidence is written below
`.data/evidence/sandbox-provider/` with revision, unique run ID, sandbox IDs,
elapsed time and cleanup assertions.

These are real-provider cases even though SP01 uses a fault transport: the
transport forwards the request to OpenSandbox and drops only the successful
response. No case may delete an ID that was not recorded by its own ownership
tracker.
