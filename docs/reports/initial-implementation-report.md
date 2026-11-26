# Initial Design Implementation Report

Spec: `docs/superpowers/specs/2026-11-16-primary-harness-durable-job-platform-design.md`

Mapped sections: **27/27**

| Design section | Implementation evidence | Test evidence | Current evidence |
| --- | --- | --- | --- |
| Intent | `README.md`<br>`AGENTS.md` | `tests/test_project_isolation.py` | unit |
| Goals | `src/sandbox_harness/services/api/app.py`<br>`src/sandbox_harness/services/job_worker/job_service.py` | `tests/test_job_api.py`<br>`tests/test_job_service.py` | unit |
| Non-Goals | `AGENTS.md`<br>`docs/development/product-boundaries.md` | `tests/test_project_isolation.py` | review |
| Original-Project Baseline | `src/sandbox_harness/services/api/broker.py`<br>`src/sandbox_runtime/app.py` | `tests/test_broker.py`<br>`tests/test_runtime_configuration.py` | unit |
| System Shape | `src/sandbox_harness/services/api/app.py`<br>`src/sandbox_harness/common/execution/job_worker.py`<br>`src/sandbox_harness/services/sandbox_gateway/gateway.py` | `tests/test_job_worker.py`<br>`tests/test_gateway.py` | unit |
| Modules | `docs/development/architecture-and-ownership.md`<br>`src/sandbox_harness/services/job_worker/job_service.py` | `tests/test_job_service.py` | unit |
| Primary Harness And Turns | `src/sandbox_harness/services/api/broker.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_broker.py`<br>`tests/browser_input_status.cjs` | unit |
| Durable Job Protocol | `src/sandbox_harness/common/contracts/job_contracts.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_job_contracts.py`<br>`tests/test_job_store.py` | unit-postgres |
| Job Types | `src/sandbox_harness/common/catalog/job_catalog.py`<br>`src/sandbox_harness/common/execution/job_adapters.py` | `tests/test_job_catalog.py`<br>`tests/test_job_worker.py` | unit |
| Job Attempts And Acceptance | `src/sandbox_harness/common/execution/job_worker.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_job_acceptance.py`<br>`tests/test_job_worker.py` | unit-postgres |
| Mixed JobGroups | `src/sandbox_harness/common/jobs/job_coordinator.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_job_coordinator.py`<br>`tests/test_job_api.py`<br>`tests/browser_job_confirmation.cjs` | unit-browser |
| Harness Delegation | `src/sandbox_harness/services/job_worker/harness_job.py`<br>`src/sandbox_harness/common/scheduling/scheduler.py`<br>`src/sandbox_harness/services/job_worker/job_service.py` | `tests/test_harness_job.py`<br>`tests/test_scheduler.py`<br>`tests/test_job_service.py` | unit;real-pending |
| Continuation Turns | `src/sandbox_harness/services/api/job_continuation.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_job_continuation.py`<br>`tests/test_continuation_postgres_concurrency.py`<br>`tests/browser_job_continuation.cjs` | unit-postgres-browser |
| User Input While Jobs Run | `src/sandbox_harness/services/api/broker.py`<br>`src/sandbox_harness/services/api/job_continuation.py` | `tests/test_broker.py`<br>`tests/test_job_continuation.py`<br>`tests/browser_input_status.cjs` | unit;real-pending |
| Cancellation Scope | `src/sandbox_harness/services/api/app.py`<br>`src/sandbox_harness/common/jobs/job_coordinator.py` | `tests/test_job_cancellation.py`<br>`tests/browser_job_continuation.cjs` | unit-browser |
| Immutable JobGroups And Replacement | `src/sandbox_harness/common/contracts/job_contracts.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_job_contracts.py`<br>`tests/test_job_coordinator.py` | unit |
| Interactions And Decisions | `src/sandbox_runtime/app.py`<br>`src/sandbox_harness/services/api/app.py` | `tests/test_runtime_configuration.py`<br>`tests/browser_live_interaction.cjs` | unit-browser |
| Authorization And Budgets | `src/sandbox_harness/common/security/grants.py`<br>`src/sandbox_harness/services/sandbox_gateway/gateway.py`<br>`src/sandbox_harness/common/db/store.py` | `tests/test_grants.py`<br>`tests/test_budget_postgres_concurrency.py` | unit-postgres |
| Artifacts, Validation, Review, And User-Visible Outputs | `src/sandbox_harness/common/artifacts/artifact_gateway.py`<br>`src/sandbox_harness/services/job_worker/reviewer.py`<br>`src/sandbox_harness/services/api/document_preview.py` | `tests/test_job_artifacts.py`<br>`tests/test_reviewer.py`<br>`tests/test_document_preview.py` | unit;real-pending |
| Actual Execution Graph | `src/sandbox_harness/services/api/execution_graph.py`<br>`web/src/ExecutionGraph.jsx` | `tests/test_execution_graph.py`<br>`tests/browser_execution_graph.cjs` | unit-browser |
| Sandbox Images, Profiles, And Dynamic Bundles | `runtime/bootstrap.Dockerfile`<br>`runtime/common.Dockerfile`<br>`src/sandbox_harness/common/catalog/profiles.py` | `tests/test_runtime_image_contract.py`<br>`tests/test_runtime_bundle.py`<br>`tests/test_runtime_skills.py` | unit-image-smoke |
| Recovery And Failure Semantics | `src/sandbox_harness/common/execution/job_worker.py`<br>`src/sandbox_harness/services/job_worker/harness_job.py`<br>`src/sandbox_harness/services/api/broker.py` | `tests/test_job_worker.py`<br>`tests/test_broker.py`<br>`tests/test_job_cancellation.py` | unit;real-pending |
| Authoritative Data Model | `src/sandbox_harness/common/db/store.py`<br>`src/sandbox_harness/common/contracts/domain.py` | `tests/test_job_store.py`<br>`tests/test_job_postgres_concurrency.py` | unit-postgres |
| Database And Release Baseline | `deploy/init-postgresql.sh`<br>`deploy/record-release.py` | `tests/test_deployment_isolation.py`<br>`tests/test_release_catalog.py` | unit-deploy |
| Distributed Deployment | `deploy/install-units.sh`<br>`deploy/nginx.conf`<br>`src/sandbox_harness/services/job_worker/job_service.py` | `tests/test_deployment_isolation.py`<br>`tests/test_ingress_config.py`<br>`tests/test_job_service.py` | unit;deployed-pending |
| Implementation Sequence | `docs/superpowers/plans/2026-11-16-sandbox-harness-platform-parallel.md` | `tests/test_gate_contract.py` | commits |
| Required Test Families | `tests/gate.py`<br>`tests/run_real_campaign.py`<br>`tests/real_cases/cases.json` | `tests/test_gate_contract.py`<br>`tests/test_real_campaign_runner.py`<br>`tests/test_real_case_catalog.py` | gate-and-campaign-pending |

Pending live evidence is explicit and must be replaced by Task 15 receipts before final release.
