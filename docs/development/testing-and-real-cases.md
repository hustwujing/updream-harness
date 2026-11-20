# Testing and Real Cases

## Applies to

Read this for every change and before reporting completion.

## Test layers

1. **Pure/unit tests** validate parsers, contracts and reducers without services.
2. **Component tests** use SQLite, fake Runtime/OpenSandbox or mock transports to
   exercise transactions and boundaries.
3. **Real OpenSandbox tests** verify image, process, network, checkpoint and
   recovery behavior with isolated namespaces/data.
4. **Real Provider tests** verify the selected native Harness and upstream route.
5. **Browser tests** verify the deployed product from the user surface.

Never report a deterministic fixture as a real model test. Never use a previous
revision's report as evidence for current code.

## Source-to-test map

| Change | Minimum focused tests |
| --- | --- |
| Store, idempotency, schema | `test_store.py`, relevant owner/cancel tests |
| Broker, lease, recovery | `test_broker.py`, `test_event_bus_boundaries.py`, `test_cancel_boundaries.py` |
| Runtime/adapters | `test_runtime_boundaries.py`, `test_runtime_configuration.py`, adapter/runner tests |
| Sandbox background control | CLI structured errors, wait observation/terminal behavior, deadline escalation, cancellation mapping and real OpenSandbox B04-B06 |
| Sandbox/pool | `test_sandbox_namespace.py`, `test_runtime_client.py` |
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
real case green. Run the complete gate once after all fixes and documentation
are committed; its receipt must identify the exact release revision.

Real protocol cases are concurrent by default. Serialize only cases that mutate
shared services or global configuration, and list those IDs explicitly in the
runner. A global protocol lock hides concurrency bugs and unnecessarily delays
unrelated K cases.

Real tests must use unique namespaces, data directories and idempotency keys.
They must clean only resources they created and must not restart shared Redis,
Docker, OpenSandbox, registry or object storage. Before service restart, query
nonterminal Turns and avoid interrupting active user work.

## Evidence

Record revision, Runtime image digests, configuration mode, case trigger,
elapsed time, outcome and cleanup state. For a failure, separate program defect,
model behavior and external dependency. A non-program failure passes only when
the program response, state, cleanup and user-visible error are correct.

Completion means no known program error in affected cases and no clear,
bounded prompt/UI improvement left unaddressed. It does not mean every model
response is semantically successful.

Evidence for external execution must distinguish submission, polling,
collection, deterministic validation/Review and cleanup. A successful upstream
task followed by a collection failure is not evidence for a second submission;
the report must retain the original external execution ID and show the bounded
collection attempt generations.

## K campaign evidence

K01-K11 are exact capability matrices. K12-K14 are user-path E2E workflows and
must exercise durable Continuation plus final delivery. Skill coverage requires
both structured evidence and a host-validated product archive; a Harness-authored
list of commands or checks is not proof that a product existed.

Campaign reports use `verification_scope=clean_run` only when every reported
success was executed on the report revision. Resuming passed cases from an older
revision produces `composite_verified`; summaries must not describe that as one
complete green matrix. After a generic fix, rerun the failed member and affected
passed members first, then run the complete matrix once before release when the
claim requires a clean full pass.
