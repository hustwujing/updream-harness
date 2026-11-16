# Sandbox Harness Platform Parallel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build, verify, exercise, and independently deploy `sandbox-harness-platform-parallel` from the original stable Primary Harness platform with strict grants, mixed parallel Durable JobGroups, child-Harness delegation, actual execution graph, and complete design-to-code evidence.

**Architecture:** Preserve the original Thread/Turn/native-session Broker as the only task brain. Add immutable, dependency-free JobGroups whose Provider, Operation, and Harness Jobs run concurrently through registered adapters, produce immutable Artifacts, and resume the Primary Harness through transactional continuation Outboxes. PostgreSQL is authoritative; Gateway grants, Worker leases, Runtime receipts, object identity, and exact cleanup fence every external effect.

**Tech Stack:** Python 3.13, FastAPI, Pydantic v2, SQLAlchemy async, PostgreSQL, Redis Streams, OpenSandbox, systemd, Nginx, React/Vite, Playwright, pytest.

**Spec:** `docs/superpowers/specs/2026-11-16-primary-harness-durable-job-platform-design.md`

## Global Constraints

- New repository: `/data2/wujing03/sandbox-harness-platform-parallel`, cloned from `/data2/wujing03/sandbox-harness-platform` with no shared Git remote, runtime state, database, cache, object store, ports, service units, namespace, or Nginx prefix.
- Preserve every original-project capability and test unless the spec explicitly replaces its boundary.
- No mandatory LLM Manager, predicted business DAG, WorkUnit, Run, Task, plan table, or independent Delivery system.
- A committed JobGroup is immutable, contains only independently and immediately schedulable Jobs, and has fixed all-terminal completion.
- No backward-compatible API/schema/state requirement and no database migration framework; initialize one authoritative fresh schema.
- Use strict per-execution Artifact/Skill/Capability/Operation/network/budget grants and host-owned credentials.
- Use TDD, focused verification, and one coherent commit per task. During real-machine campaigns, stop dispatch on the first program defect or clearly optimizable prompt defect, fix generically, rerun the failed case, then rerun only affected passed cases before resuming.
- Do not run a complete gate after each fix. Run the complete gate once after campaign convergence and before final release.
- Generated state, downloads, images, caches, logs, PostgreSQL data, Redis prefix data, test evidence, and temporary files stay under the new repository `.data` tree or its explicitly isolated database/prefix.
- Default isolated deployment identities: API `127.0.0.1:24766`, PostgreSQL `127.0.0.1:28432` database/user `sandbox_harness_parallel`, shared OpenSandbox bridge Gateway `172.31.0.1:29880` with authenticated proxy `:29881`, Redis prefix `sandbox-harness-parallel:`, OpenSandbox namespace `sandbox-harness-parallel`, Nginx prefix `/sandbox-harness-platform-parallel/`, and service prefix `sandbox-harness-parallel-`. The bridge executable/network may be shared, but ports, grants, namespace, state, and firewall identity are isolated. Preflight must reject conflicts rather than reuse another project's state.
- Storage iron rule: every persistent service write, including PostgreSQL, object data, cache, logs, runtime bundles, workspaces and service temporary state, must remain under `/data2/wujing03/sandbox-harness-platform-parallel/.data`. Shared immutable caches or images may be reused only from `/data2/wujing03`; the root filesystem may contain system unit/configuration files and tmpfs only, never project storage. Deployment preflight must reject a different data root.
- Resource reuse rule: reuse digest-pinned base images, immutable dependency layers and verified local caches before downloading or rebuilding. When a download is unavoidable, prefer configured domestic mirrors and retain checksum/lockfile verification; never reuse another environment's mutable database, Redis keys, object prefix, credentials or runtime state.

## Review Focus

- A Provider accepts a side effect but the response is lost: preserve UNKNOWN or reconcile the exact external handle; never submit twice.
- Job completion, user input, stop-all, and Outbox dispatch race: create zero or one correctly ordered continuation without waking a stopped Thread.
- Concurrent confirmations contend for one billable/GPU/model budget: only reservations that fit commit, with no overspend.
- A resumed native Session receives a changed Skill/Profile grant: remove revoked paths, inject the authoritative change, or create an explicit handoff Session.
- Required review, collection cardinality, and Artifact streaming race: never expose partial or unreviewed delivery outputs and never lose committed object identity.

---

### Task 1: Independent Repository Baseline

**Files:**
- Create: `/data2/wujing03/sandbox-harness-platform-parallel/**` from the original repository
- Modify: `pyproject.toml`, `README.md`, `AGENTS.md`, `.gitignore`
- Create: `docs/superpowers/specs/2026-11-16-primary-harness-durable-job-platform-design.md`
- Create: `docs/superpowers/plans/2026-11-16-sandbox-harness-platform-parallel.md`
- Test: `tests/test_project_isolation.py`

**Interfaces:**
- Consumes: original repository HEAD and the approved spec/plan.
- Produces: clean independent Git repository with distribution identity `sandbox-harness-platform-parallel` while retaining Python import package `sandbox_harness`.

- [ ] Write `test_project_isolation.py` asserting project/state paths, service prefix, ports, Redis prefix, database identity, namespace, and Nginx prefix are distinct from both predecessors.
- [ ] Run the test against the untouched clone and verify it fails on predecessor defaults.
- [ ] Clone with `git clone --no-hardlinks`, remove inherited remotes, copy the approved spec/plan, and change product/deployment defaults without deleting original product behavior.
- [ ] Run original unit tests plus `tests/test_project_isolation.py`; require all pass.
- [ ] Commit: `chore(project): create isolated parallel platform`.

### Task 2: Job, Group, Attempt, Outbox, And Ledger Schema

**Files:**
- Modify: `src/sandbox_harness/domain.py`
- Modify: `src/sandbox_harness/store.py`
- Create: `src/sandbox_harness/job_contracts.py`
- Test: `tests/test_job_contracts.py`
- Test: `tests/test_job_store.py`
- Test: `tests/test_job_postgres_concurrency.py`

**Interfaces:**
- Produces: `JobState`, `JobGroupDraftState`, `JobGroupState`, `JobKind`, `JobRequest`, `JobResult`, `JobGroupDraftCreate`, `JobGroupCreate`, `JobAttemptReceipt`; Store CRUD/conditional-transition methods and fresh-schema rows named in the spec.

- [ ] Write contract tests for complete state enums, terminal classification, 32 default/100 hard Group member bounds, forbidden dependencies, immutable request digests, replacement/reconciliation lineage, and semantic `artifact_role` versus `output_disposition`.
- [ ] Write SQLite Store tests for Draft confirm/reject/expire, immutable Group creation, all-terminal projection, one Outbox per Group, Job attempts/leases, and conditional stale-generation rejection.
- [ ] Write PostgreSQL tests for concurrent Draft confirmation, Job terminalization, and one-winner Outbox insertion.
- [ ] Run the new tests and verify failures for missing entities.
- [ ] Add fresh-schema ORM rows, uniqueness constraints, indexes, state-transition methods, and the documented global lock order without compatibility ALTERs.
- [ ] Run new tests and original Store/Turn tests; require all pass.
- [ ] Commit: `feat(jobs): add authoritative durable job schema`.

### Task 3: Descriptor, Skill, Execution Profile, And Runtime Profile Catalogs

**Files:**
- Create: `src/sandbox_harness/job_catalog.py`
- Create: `src/sandbox_harness/profiles.py`
- Modify: `src/sandbox_harness/catalog.py`
- Modify: `src/sandbox_runtime/skills.py`
- Create: `deploy/job-catalog.example.json`
- Create: `deploy/profile-catalog.example.json`
- Test: `tests/test_job_catalog.py`
- Test: `tests/test_profile_resolver.py`
- Test: `tests/test_runtime_skills.py`

**Interfaces:**
- Produces: `OperationDescriptor`, `CapabilityDescriptor`, `ChildExecutionProfile`, `RuntimeDependencyProfile`, `ProfileResolver.resolve(harness, child_profile_id, skill_ids, requirements)`, and signed catalog digest verification.

- [ ] Write tests for inline/durable Descriptor restrictions, unknown IDs/versions/digests, Skill dependency union, minimal registered RuntimeDependencyProfile selection, unsupported combinations, profile fallback authorization, and reserved infrastructure fields.
- [ ] Write Runtime tests proving only Grant Skill IDs materialize, revoked paths disappear, and a capability-change frame precedes resumed native input.
- [ ] Run tests and verify red failures.
- [ ] Implement signed catalogs/resolver, `primary-full-cpu`, registered child profiles, exact dependency probes, and handoff-required results for incompatible native Session resumes.
- [ ] Run catalog/profile/Skill and original bundle tests; require all pass.
- [ ] Commit: `feat(runtime): resolve signed execution profiles and skills`.

### Task 4: Strict Grants And Atomic Budget Reservations

**Files:**
- Modify: `src/sandbox_harness/capability.py`
- Create: `src/sandbox_harness/grants.py`
- Modify: `src/sandbox_harness/gateway.py`
- Modify: `src/sandbox_harness/store.py`
- Test: `tests/test_grants.py`
- Test: `tests/test_gateway.py`
- Test: `tests/test_budget_postgres_concurrency.py`

**Interfaces:**
- Produces: `GrantAuthority.issue_turn`, `issue_job`, `verify_call`; Store authorization snapshots and immutable `reserve/consume/release/adjust` ledger APIs.

- [ ] Write tests for exact Artifact/Skill/Descriptor/Profile/network/output scope, source identity, Thread/Turn/Job/Sandbox/generation fences, TTL/nonce revocation, and host-only secrets.
- [ ] Write concurrent PostgreSQL confirmation tests proving one-shot budget reservations cannot overspend and unused reservations release exactly once.
- [ ] Run tests and verify failures.
- [ ] Implement four-scope grants, one-shot versus persistent authorization, continuous Gateway validation, atomic ledger reservation/consumption, and replacement policy accounting.
- [ ] Run grant/gateway/budget tests and original auth/proxy tests; require all pass.
- [ ] Commit: `feat(auth): enforce minimal job grants and budgets`.

### Task 5: Atomic JobGroup Draft Confirmation And Coordination

**Files:**
- Create: `src/sandbox_harness/job_coordinator.py`
- Modify: `src/sandbox_harness/app.py`
- Modify: `src/sandbox_harness/store.py`
- Create: `src/sandbox_runtime/job_cli.py`
- Test: `tests/test_job_coordinator.py`
- Test: `tests/test_job_api.py`
- Test: `tests/browser_job_confirmation.cjs`

**Interfaces:**
- Produces: `JobCoordinator.prepare_group`, `confirm_group`, `reject_group`, `cancel_job`, `cancel_group`; `/api/job-group-drafts`, `/api/job-groups`, `/api/jobs`; `sandbox-job` CLI.

- [ ] Write tests for free auto-commit, complete mixed-batch hold, editable Draft digest/idempotency, whole-batch confirm/reject/expiry, no early free Job start, immutable committed Group, and no dependency fields.
- [ ] Write a browser case that edits/removes a paid item, confirms once, and observes exactly the accepted immutable Jobs.
- [ ] Run tests and verify failures.
- [ ] Implement API/coordinator/CLI and one-transaction Draft confirmation with budget reservation and Job creation.
- [ ] Run focused API/browser/original media-confirmation tests; require all pass.
- [ ] Commit: `feat(jobs): commit confirmed parallel job groups`.

### Task 6: Generic Worker Claims And Executor Acceptance Receipts

**Files:**
- Create: `src/sandbox_harness/job_worker.py`
- Create: `src/sandbox_harness/job_adapters.py`
- Modify: `src/sandbox_harness/media_worker.py`
- Modify: `src/sandbox_harness/site_worker.py`
- Modify: `src/sandbox_harness/store.py`
- Test: `tests/test_job_worker.py`
- Test: `tests/test_job_acceptance.py`
- Test: `tests/test_media_worker.py`

**Interfaces:**
- Produces: `JobAdapter` protocol (`submit/status/cancel/collect/reconcile/cleanup`), `JobWorker.run_once`, and Provider/Operation acceptance receipts.

- [ ] Write tests for pre-accept retry, accepted lost response, external-handle resume, UNKNOWN, explicit reconciliation Job, deterministic Operation retry, non-idempotent Operation UNKNOWN, stale lease fencing, and budget consumption at acceptance.
- [ ] Run tests and verify failures.
- [ ] Implement generic claim/result consumer and adapt existing Media/Site workers without changing provider-specific proven semantics.
- [ ] Run focused workers plus original media/site suites; require all pass.
- [ ] Commit: `feat(workers): execute durable jobs with acceptance receipts`.

### Task 7: Artifact Stage, Emit, Collections, Validation, And Disposition

**Files:**
- Create: `src/sandbox_harness/artifact_gateway.py`
- Modify: `src/sandbox_harness/objects.py`
- Modify: `src/sandbox_harness/store.py`
- Modify: `src/sandbox_runtime/app.py`
- Create: `src/sandbox_runtime/artifact_cli.py`
- Modify: `src/sandbox_harness/document_preview.py`
- Test: `tests/test_job_artifacts.py`
- Test: `tests/test_artifact_streaming.py`
- Test: `tests/test_document_preview.py`

**Interfaces:**
- Produces: Runtime endpoints and `artifact.stage/emit`; Store `bind_job_artifact` and collection promotion; canonical MIME registry.

- [ ] Write tests for stable regular-file streaming, symlink/special-file rejection, crash before/after object binding, exact Artifact Grant scope, collection min/max/order/item-key, archive manifest, MIME sniff/alias/unknown download, and aggregate limits.
- [ ] Write tests proving semantic `artifact_role` differs from `output_disposition`, required review remains candidate, and complete collection promotion to delivery is atomic.
- [ ] Run tests and verify failures.
- [ ] Implement authenticated Runtime-to-Gateway streaming, object-first/idempotent binding, collection contracts, validators, disposition promotion, previews, and bounded orphan-object cleanup.
- [ ] Run focused Artifact/object/preview/original file tests; require all pass.
- [ ] Commit: `feat(artifacts): stream contracted job collections`.

### Task 8: Continuation Outbox, User Ordering, And Layered Cancellation

**Files:**
- Create: `src/sandbox_harness/job_continuation.py`
- Modify: `src/sandbox_harness/broker.py`
- Modify: `src/sandbox_harness/app.py`
- Modify: `src/sandbox_harness/store.py`
- Test: `tests/test_job_continuation.py`
- Test: `tests/test_job_cancellation.py`
- Test: `tests/test_continuation_postgres_concurrency.py`
- Test: `tests/browser_job_continuation.cjs`

**Interfaces:**
- Produces: `ContinuationDispatcher.run_once`, atomic Outbox batch Turn insertion, current-Turn/Job/Group/Thread cancellation endpoints and UI commands.

- [ ] Write crash-point and PostgreSQL race tests for last-Job terminalization, Outbox creation, 32-Group/100-Job/1-MiB batching, digest idempotency, concurrent user Turn ordering, active origin Turn wait, and exactly-once replay.
- [ ] Write tests for current-response stop, Job/Group cancel, Thread stop-all, all-cancelled no-wake, `SUPPRESSED` Outbox context on later user Turn, Provider cancellation refusal, and UNKNOWN.
- [ ] Run tests and verify failures.
- [ ] Implement dispatcher, Broker wake context, suppression, cancellation, truthful system Items, and browser controls.
- [ ] Run focused continuation/cancel/input/recovery tests; require all pass.
- [ ] Commit: `feat(continuation): resume primary from factual job results`.

### Task 9: Distributed Scheduler And Harness Delegation

**Files:**
- Create: `src/sandbox_harness/scheduler.py`
- Create: `src/sandbox_harness/node_agent.py`
- Create: `src/sandbox_harness/harness_job.py`
- Modify: `src/sandbox_harness/runtime.py`
- Modify: `src/sandbox_runtime/app.py`
- Modify: `src/sandbox_harness/config.py`
- Test: `tests/test_scheduler.py`
- Test: `tests/test_harness_job.py`
- Test: `tests/test_scheduler_postgres_concurrency.py`
- Test: `tests/run_real_harness_delegation.py`

**Interfaces:**
- Produces: node/profile registration, fenced resource reservations, `HarnessJobAdapter`, child Runtime assignment/result receipt, checkpoint/reattach, and exact cleanup.

- [ ] Write tests for profile/image/resource hard filtering, two-worker claim races, GPU identity/count/mode, reservation release, recursion rejection, no Child Interaction, BLOCKED, cancellation+replacement, and no Child steer endpoint.
- [ ] Write real OpenSandbox test submitting N child Jobs, killing Broker/Executor, reattaching exact executions, collecting role-scoped Artifacts, and proving cleanup/token revocation.
- [ ] Run tests and verify failures.
- [ ] Implement scheduler/node agent/Harness adapter with strict grants and native acceptance receipts.
- [ ] Run focused scheduler/delegation/recovery tests; require all pass.
- [ ] Commit: `feat(delegation): schedule isolated child harness jobs`.

### Task 10: Optional Reviewer Jobs

**Files:**
- Create: `src/sandbox_harness/reviewer.py`
- Modify: `src/sandbox_harness/job_catalog.py`
- Modify: `src/sandbox_harness/store.py`
- Test: `tests/test_reviewer.py`
- Test: `tests/run_real_optional_reviewer.py`

**Interfaces:**
- Produces: `ReviewerJobAdapter`, frozen review context, digest-bound PASS receipts, and candidate promotion gates.

- [ ] Write tests for default-off behavior, explicit user/Primary/policy triggers, deterministic Validators first, grouped candidates, read-only grants, PASS/REVISE/EVIDENCE_REQUIRED/BLOCKED, optional reviewer failure warning, and required review publication blocking.
- [ ] Run tests and verify failures.
- [ ] Implement Reviewer as a restricted Harness Job returned through normal continuation; never mutate candidates or ask users directly.
- [ ] Run focused and real Reviewer tests; require all pass.
- [ ] Commit: `feat(review): add explicit read-only reviewer jobs`.

### Task 11: Actual Execution Graph And UI

**Files:**
- Create: `src/sandbox_harness/execution_graph.py`
- Modify: `src/sandbox_harness/app.py`
- Modify: `web/src/main.jsx`
- Create: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/styles.css`
- Test: `tests/test_execution_graph.py`
- Test: `tests/browser_execution_graph.cjs`

**Interfaces:**
- Produces: owner-scoped Thread graph snapshot/SSE cursor and serpentine graph UI with nested live activities.

- [ ] Write projection tests for factual Turn/Group/Job/Interaction nodes, allowed edges, execution location, state, duration, retries, cursor/reset, historical expansion, and absence of predicted or command-level top nodes.
- [ ] Write desktop/mobile Playwright tests for serpentine rows, breathing states, node details, nested activities, file cards, reconnect, and no overlap.
- [ ] Run tests and verify failures.
- [ ] Implement projection and UI using original transcript events plus Job/attempt/resource facts.
- [ ] Run focused API/browser tests and inspect screenshots; require all pass.
- [ ] Commit: `feat(web): show factual parallel execution graph`.

### Task 12: Four-Layer Images And Signed Dynamic Bundles

**Files:**
- Modify/Create: `runtime/bootstrap.Dockerfile`, `runtime/common.Dockerfile`
- Create: `runtime/profiles/{primary-full-cpu,general-cpu,media-cpu,office,web-browser,data-analysis}.Dockerfile`
- Modify/Create: `runtime/{codex,claude,deepseek}.Dockerfile`
- Modify: `runtime/build-images.sh`, `runtime/build_runtime_code_bundle.py`, `runtime/skill_bundle.py`
- Create: `runtime/build_profile_catalog.py`
- Test: `tests/test_runtime_image_contract.py`
- Test: `tests/test_runtime_bundle.py`
- Test: `tests/test_runtime_skills.py`

**Interfaces:**
- Produces: immutable `RuntimeDependencyProfile x Harness` image digests, signed Profile/Runtime/Skill catalogs, and rebuild-boundary verification.

- [ ] Write tests/probes for L1-L4 contents, non-root identity, exact CLI/toolchain versions, profile command/module capability, layer reuse metadata, signed catalogs, selected Skill materialization, and recovery digest compatibility.
- [ ] Run tests and verify failures against copied images.
- [ ] Build four conceptual layers, final image matrix, catalogs, and bundle probes; do not include credentials or dynamic Skills in images.
- [ ] Build only representative changed combinations during development and record immutable digests.
- [ ] Run all image/bundle tests and one real Sandbox smoke per Profile/Harness family; require all pass.
- [ ] Commit: `feat(runtime): build layered profile harness images`.

### Task 13: Isolated Deployment And Operations

**Files:**
- Modify: `deploy/configure-local.py`, `deploy/install-units.sh`, `deploy/init-postgresql.sh`
- Modify: `deploy/nginx.conf`, `deploy/nginx-frpc.conf`
- Modify/Create: `deploy/systemd/sandbox-harness-parallel-*.service`
- Create: `docs/deployment.md`, `docs/development.md`
- Test: `tests/test_deployment_isolation.py`
- Test: `tests/test_ingress_config.py`
- Test: `tests/run_real_deployment_recovery.py`

**Interfaces:**
- Produces: completely isolated systemd/PostgreSQL/Redis-prefix/OpenSandbox/Nginx deployment and reproducible local release procedure.

- [ ] Write tests that resolve every path/port/database/prefix/unit/namespace and reject predecessor collisions, root-filesystem generated storage, shared database names, or unsafe FRPC registration.
- [ ] Run tests and verify failures.
- [ ] Implement fresh-schema initialization, isolated services/storage, health/readiness, Nginx HTTP/WebSocket/preview routes, and deployment docs. Do not add database migration machinery.
- [ ] Run deployment tests, service-access checks, and forced service-kill recovery against the new environment only.
- [ ] Commit: `feat(deploy): isolate parallel platform environment`.

### Task 14: Gate, Real-Case Catalog, And Design Traceability

**Files:**
- Create: `tests/gate.py`
- Create: `tests/real_cases/cases.json`
- Create: `tests/run_real_campaign.py`
- Create: `docs/design-traceability.yaml`
- Create: `scripts/render_design_report.py`
- Create: `docs/reports/initial-implementation-report.md`
- Migrate: applicable original `tests/run_real_*.py` and Plus case assets/definitions

**Interfaces:**
- Produces: one command full gate, resumable concurrency-8 real-machine campaign, per-attempt evidence, and requirement-to-code/test/evidence report.

- [ ] Inventory every original real-machine/browser case and every applicable Plus interaction, capability, recovery, output, graph, Reviewer, GPU, and delegation case; record provenance and expected classification.
- [ ] Add new cases for mixed Groups, whole-batch approval/rejection, concurrent Provider+Operation+Harness Jobs, multiple output-role items, continuation coalescing, user input ordering, replacements, stop scopes, grants, Skill/Profile resolution, node loss, and required review.
- [ ] Implement a gate covering formatting/static checks, all unit/integration/PostgreSQL races, frontend build/tests, browser matrix, image/bundle contracts, and deployment isolation.
- [ ] Implement campaign stop-on-program/prompt-defect behavior, targeted rerun, affected-pass regression selection, concurrency 8, elapsed/mean reporting, and evidence retention.
- [ ] Build traceability mapping for every design section to code symbols, tests, and live evidence; renderer must fail on an unmapped requirement or missing path/test.
- [ ] Run the gate without deployment mutation and require pass.
- [ ] Commit: `test(system): add gate campaign and design traceability`.

### Task 15: Real-Machine Convergence, Final Gate, Release, And Report

**Files:**
- Modify as defects require: only owning generic modules/tests/docs
- Create: `docs/reports/final-real-machine-report.md`
- Create: `.data/evidence/**` (untracked evidence)

**Interfaces:**
- Consumes: all prior tasks, isolated release artifacts, complete case catalog.
- Produces: converged real-machine results, final gate receipt, deployed environment, browser evidence, and complete design implementation report.

- [ ] Start the isolated deployment and run cases at concurrency 8, reporting aggregate and per-running-case state/elapsed/expectation every two minutes.
- [ ] On each program or clearly optimizable prompt failure, stop new dispatch, preserve evidence, diagnose, add a generic red test, fix, run the failed case, assess blast radius, rerun only affected passed cases, commit, and resume remaining cases.
- [ ] Continue complete rounds until one full round has no program defect or clearly optimizable prompt defect; classify provider/model/user/environment outcomes without forcing all cases to pass.
- [ ] Run the complete gate once after convergence; fix any gate defect with focused verification, then rerun the complete gate.
- [ ] Build immutable final images/bundles, deploy to the isolated environment, verify health, storage/port/prefix isolation, Nginx HTTP/WebSocket, desktop/mobile UI, previews, terminal/takeover, restart recovery, cleanup, and no predecessor impact.
- [ ] Generate the final traceability report listing every design requirement, implementation file/symbol, unit/integration/real/browser evidence, release/image/bundle/catalog digests, and any explicitly deferred item. The report must contain no unmapped or falsely completed requirement.
- [ ] Commit final documentation/evidence manifests: `docs(release): report parallel platform verification`.
