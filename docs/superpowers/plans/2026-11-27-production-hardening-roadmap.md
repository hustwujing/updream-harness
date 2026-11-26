# Production Hardening Implementation Plan

> **For agentic workers:** Execute tasks in order with TDD, focused verification and one reviewable commit per independent change.

**Goal:** Implement the approved production hardening design and produce a clean 188-case Ali release gate.

**Architecture:** Extract one PlatformWorker from API, strengthen shared error and durable command contracts, then add exact observability, multi-replica scheduling, storage/usage controls and user stages. Preserve provider-neutral behavior and split database ownership.

**Tech Stack:** Python 3.13, FastAPI, SQLAlchemy/PostgreSQL, Redis notification aid, OpenTelemetry, React/Vitest/Playwright, Ali Sandbox/OpenSandbox.

**Spec:** `docs/superpowers/specs/2026-11-27-production-hardening.md`

## Global Constraints

- No case-, Provider- or operation-specific production branches.
- PostgreSQL remains authoritative; notifications never replace durable state.
- Unknown external side effects are never blindly replayed.
- Generated state remains under `/data2/wujing03/sandbox-harness-platform-parallel-ali/.data`.
- Existing user work and unrelated changes are preserved.
- No project-owned Helm, DBA workflow, SLO/alert policy or production capacity sizing.

## Phase 1: Shared Errors And Durable Commands

- [ ] Add `ErrorEnvelope` and public error mapping with red/green tests across Runtime, Broker, Job, Media, Site and Gateway boundaries.
- [ ] Inventory every Domain command kind; define exact typed payload registry and pre-enqueue validation.
- [ ] Add contract version/digest to command/release compatibility without changing provider semantics.
- [ ] Replace unbounded Domain delivery retries with deterministic fail-fast and bounded transient dependency handling.
- [ ] Add first-failure diagnostics, dependency recovery wakeup and no-retry regression tests.
- [ ] Run SQLite and isolated PostgreSQL handoff/error suites; commit each independent contract change.

## Phase 2: PlatformWorker

- [ ] Create `modules/host/platform-worker` with README, AGENTS, manifest and entry point.
- [ ] Move Primary Broker execution/recovery and API background supervisors behind explicit service owners.
- [ ] Add database-fenced leader lease for singleton loops; keep durable claims multi-active.
- [ ] Remove background mutation loops from API and retain read/SSE projections.
- [ ] Add graceful shutdown, lease handoff and API/PlatformWorker independent restart tests.
- [ ] Rebuild module ownership/deployment checks and commit extraction in small steps.

## Phase 3: Metrics And Trace Completeness

- [ ] Define exact low-cardinality metric catalog and authoritative transition instrumentation.
- [ ] Add Turn, Sandbox, Job, Outbox, Provider and Artifact metrics with duplicate/recovery tests.
- [ ] Persist or derive producer trace context for Job, Allocation, Media, Site and Cleanup claims.
- [ ] Start new linked operations on every claim/reclaim and verify restart continuity.
- [ ] Add a deterministic OTLP collector fixture that reconciles metrics with database counts.

## Phase 4: Multi-Replica Admission And Provider Health

- [ ] Add persisted owner admission/fair selection without blocking recovery or cleanup.
- [ ] Add per-owner Turn/Job/Sandbox limits and starvation tests.
- [ ] Add provider/profile circuit state, bounded samples and half-open probe fencing.
- [ ] Ensure create saturation never blocks accepted execution observation or deletion.
- [ ] Add multi-process claim/kill PostgreSQL tests for every Worker role.

## Phase 5: Security And Release Compatibility

- [ ] Add capability `kid`, current/previous keyring verification and rolling rotation tests.
- [ ] Document/enforce HTTPS Gateway URLs for non-fake production configuration.
- [ ] Add protocol/contract compatibility manifests and refuse incompatible release selection.
- [ ] Add repository-owned canary health contract and rollback artifact verification; leave orchestration to SRE.

## Phase 6: Object GC, Usage And Public Stages

- [ ] Add cross-authority object reference snapshot/mark contract and protected in-flight references.
- [ ] Implement dry-run, grace-period, second-check sweep and deletion audit.
- [ ] Add idempotent Usage Ledger events and aggregates for token/Sandbox/Provider/object usage.
- [ ] Add authoritative public execution-stage projection and frontend rendering/actions.
- [ ] Add focused object-store, replay, owner-scope, browser and accessibility tests.

## Phase 7: Real Cases And Release

- [ ] Add `EVO01`-`EVO13` definitions, runners, assertions, cleanup and evidence schema.
- [ ] Run new cases first; investigate each failure immediately and rerun only affected cases while iterating.
- [ ] Run historical 175 cases on the final revision.
- [ ] Produce and validate one 188-case clean report.
- [ ] Run full Python, frontend, module, release and documentation gates.
- [ ] Refresh README, development guides, module docs and test evidence.
- [ ] Push small commits to `sandbox-harness-platform-parallel-ali`.
- [ ] Build/select immutable releases and Runtime bundle, restart without waiting for drain solely to avoid recovery, verify health/page/real Sandbox and cleanup unselected generated resources.
