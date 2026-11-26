# OTel Metrics And Durable Trace Links Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export standard OTel metrics and preserve trace links across durable database handoffs and continuations without adding application-owned collection infrastructure.

**Architecture:** Extend the existing observability runtime with an OTel `MeterProvider` sharing the current resource and optional OTLP/HTTP endpoint. Persist validated producer context in the existing digest-bound cross-domain Envelope and link delayed continuation work to trace context already stored on the origin Turn.

**Tech Stack:** Python 3.13, OpenTelemetry Python SDK/OTLP HTTP exporter, SQLAlchemy async repositories, pytest.

**Spec:** `docs/superpowers/specs/2026-11-26-otel-metrics-durable-trace-links.md`

## Global Constraints

- Collection, disk buffering, storage and visualization remain company-platform responsibilities.
- Telemetry must never change business outcomes or carry prompts, generated content, credentials, arbitrary headers, raw URLs or raw exception messages.
- Metric attributes must remain bounded; user, Thread, Turn, Job and sandbox identifiers are excluded from metric labels.
- Delayed asynchronous processing starts a new trace linked to the producer context.
- Generated data, caches and test temporaries remain under `.data` on `/data2`.

## Review Focus

- OTLP endpoint absent: metric recording and shutdown remain local and non-blocking.
- Exporter failure: business code proceeds and the failure counter increases without recursive telemetry.
- Invalid or missing persisted `traceparent`: delivery proceeds with no invalid Link.
- Handoff redelivery: the same trace context and digest remain stable.
- Continuation after process restart: the new dispatch trace links to the durable origin context.

---

### Task 1: OTel Metrics Runtime

**Files:**
- Modify: `src/sandbox_harness/common/observability/observability.py`
- Modify: `tests/test_observability.py`
- Modify: `.env.example`

**Interfaces:**
- Produces: `Telemetry.counter(name)` and `Telemetry.histogram(name, unit)` helpers backed by one shared `MeterProvider`.
- Produces: `Telemetry.record_operation(name, duration_ms, outcome)` with bounded attributes.

- [x] **Step 1: Write failing tests for metric instruments, absent exporter behavior and clean shutdown.**
- [x] **Step 2: Run the focused observability tests and confirm the new assertions fail.**
- [x] **Step 3: Add the shared `MeterProvider`, bounded instruments and optional OTLP metric reader.**
- [x] **Step 4: Record generic operation duration/outcome and observability health counters without high-cardinality labels.**
- [x] **Step 5: Run `tests/test_observability.py`.**
- [x] **Step 6: Commit as `feat: export bounded otel metrics`.**

### Task 2: Durable Cross-Domain Trace Context

**Files:**
- Modify: `src/sandbox_harness/common/db/handoff.py`
- Modify: `tests/test_domain_handoff.py`

**Interfaces:**
- Produces: `Envelope.traceparent: str = ""`, included in canonical serialization and digest.
- Consumes: observability `carrier`, `link_to`, and `operation` helpers.

- [x] **Step 1: Write a failing test that enqueues inside a trace, reloads the serialized Envelope and verifies the receive span Link.**
- [x] **Step 2: Run the focused handoff test and confirm it fails.**
- [x] **Step 3: Capture and validate the current W3C context in `enqueue`, then link `receive` processing.**
- [x] **Step 4: Add missing/invalid context and idempotent redelivery assertions.**
- [x] **Step 5: Run domain handoff SQLite and PostgreSQL tests.**
- [x] **Step 6: Commit as `feat: persist cross-domain trace links`.**

### Task 3: Job And Media Continuation Trace Links

**Files:**
- Modify: `src/sandbox_harness/common/db/job_groups.py`
- Modify: `src/sandbox_harness/services/api/job_continuation.py`
- Modify: `src/sandbox_harness/services/api/app.py`
- Modify: `tests/test_job_continuation.py`
- Modify: `tests/test_media_jobs.py`

**Interfaces:**
- Produces: Job continuation claim payloads with a validated origin `traceparent`; Media continuation reads the authoritative Core Turn.
- Consumes: `operation(..., new_trace=True)` and `add_link(traceparent)`.

- [x] **Step 1: Write failing tests proving Job and Media continuation dispatch spans link to the origin Turn after the current request context is gone.**
- [x] **Step 2: Run the focused continuation tests and confirm they fail.**
- [x] **Step 3: Return bounded origin trace context from claim/read repository methods.**
- [x] **Step 4: Wrap dispatch attempts in new linked operations without changing delivery idempotency.**
- [x] **Step 5: Run Job continuation, Child continuation and Media tests.**
- [x] **Step 6: Commit as `feat: link durable continuation traces`.**

### Task 4: Configuration, Documentation And Verification

**Files:**
- Modify: `docs/observability.md`
- Modify: `docs/development/logging-architecture.md`
- Modify: relevant module `README.md` files if their deployment contract changes.

**Interfaces:**
- Documents: stdout/OTLP application boundary, metric export settings, durable Link semantics and lack of an application-owned disk queue.

- [x] **Step 1: Update the observability contract and environment reference.**
- [x] **Step 2: Run observability, domain handoff, continuation, media, deployment isolation and configuration tests.**
- [x] **Step 3: Run the full Python suite because shared observability and cross-database protocols changed.**
- [x] **Step 4: Review `git diff`, verify no generated data or credentials are tracked, and commit documentation separately.**

### Task 5: Authenticated Sandbox Runtime Metrics

**Files:**
- Modify: `src/sandbox_harness/common/observability/telemetry_ingest.py`
- Modify: `src/sandbox_harness/services/sandbox_gateway/gateway.py`
- Modify: `src/sandbox_harness/common/sandbox/runtime.py`
- Modify: `src/sandbox_runtime/app.py`
- Modify: Runtime injection call sites and configuration examples
- Modify: `tests/test_observability.py`
- Modify: `tests/test_runtime_client.py`

**Interfaces:**
- Produces: `validate_otlp_metrics(data, claims) -> int` for the fixed Runtime metric contract.
- Produces: `forward_sandbox_metrics(settings, data) -> bytes` using only SRE OTLP configuration.
- Produces: `telemetry_metrics_enabled` in the Runtime configure protocol, default `false`.

- [x] **Step 1: Write failing tests for metric whitelist validation, Runtime enablement and Gateway relay identity.**
- [x] **Step 2: Reject arbitrary metric names, Resource/Scope values, labels and exemplar attributes.**
- [x] **Step 3: Relay validated protobuf with Gateway credentials and no application disk queue.**
- [x] **Step 4: Pass the deployment setting through Primary, Child, Operation and takeover Runtime configuration.**
- [x] **Step 5: Run observability, Runtime, Gateway, Broker, worker and Inspector impact tests.**

### Task 6: Bounded Harness Stderr Logs

**Files:**
- Modify: `src/sandbox_runtime/app.py`
- Modify: `src/sandbox_runtime/codex_runner.py`
- Modify: Runtime configuration and injection call sites
- Modify: `src/sandbox_harness/common/observability/schema.py`
- Modify: `tests/test_runtime_configuration.py`
- Modify: `tests/test_codex_runner.py`

**Interfaces:**
- Produces: `SANDBOX_HARNESS_RUNTIME_STDERR_LOG_MAX_BYTES`, default `1024`, with zero disabling text and negative values selecting the 1 MiB hard maximum.
- Produces: one `runtime.harness.stderr` event with tail, total bytes, line count, truncation state and full-stream digest.

- [x] **Step 1: Reproduce stderr leaking into the Harness stdout business stream.**
- [x] **Step 2: Separate and continuously drain the stderr pipe for every Harness.**
- [x] **Step 3: Bound and redact the retained tail while preserving safe failure metadata.**
- [x] **Step 4: Pass the setting through Primary, Child, Operation and takeover Runtime configuration.**
- [x] **Step 5: Run Runtime, Runner, configuration and observability impact tests.**
