# Full-chain Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make core platform execution reconstructable from structured telemetry and add a bounded, redacted DEBUG diagnostic channel for ingress and egress content.

**Architecture:** Preserve the existing OpenTelemetry operational pipeline, split its field contract and diagnostic capture into focused modules, and instrument shared HTTP boundaries. Normal telemetry remains payload-free; explicit DEBUG payload capture writes to a separate TTL-bound store.

**Tech Stack:** Python 3.13, OpenTelemetry, ASGI, HTTPX, pytest

**Spec:** `docs/development/logging-architecture.md`

## Global Constraints

- All state, logs and test data remain below this checkout's `.data` tree on `/data2`.
- Operational logs never contain prompts, generated content, commands, file contents, credentials, arbitrary headers, raw exception messages or unfiltered URLs.
- Payload capture requires both DEBUG level and `SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE=payload`.
- Logging failure never blocks or changes business behavior.
- Event fields and lifecycle checkpoints remain provider-neutral.

## Review Focus

- Chunked request and streaming response bodies remain byte-accurate and do not alter ASGI delivery.
- Secret-bearing headers, URLs and JSON fields remain absent from both telemetry planes.
- Binary and oversized bodies produce metadata and digests without unbounded memory or disk use.
- Concurrent processes cannot exceed diagnostic retention and normal telemetry remains nonblocking.
- Unknown event fields are observable in production and fail focused tests in strict mode.
- Invalid log directories and filename templates cannot escape the configured data root.

---

### Task 1: Canonical event field contract

**Files:**
- Create: `src/sandbox_harness/common/observability/schema.py`
- Modify: `src/sandbox_harness/common/observability/observability.py`
- Modify: affected event call sites under `src/sandbox_harness/`
- Test: `tests/test_observability.py`

**Interfaces:**
- Produces: `sanitize_fields(fields, *, strict=False)` and schema violation reporting used by every operational signal.

- [x] Add failing tests for canonical current fields, strict unknown-field rejection and production violation counters.
- [x] Implement the field registry and replace silent filtering.
- [x] Normalize all existing static event calls to canonical fields.
- [x] Run `pytest -q tests/test_observability.py` and the AST event-contract audit.
- [x] Commit as `feat: enforce observability event contracts`.

### Task 2: Bounded diagnostic capture

**Files:**
- Create: `src/sandbox_harness/common/observability/diagnostics.py`
- Modify: `src/sandbox_harness/common/observability/observability.py`
- Test: `tests/test_observability.py`

**Interfaces:**
- Consumes: canonical operational events from Task 1.
- Produces: `DiagnosticCapture`, `capture_http_exchange(...)`, and ASGI body tee integration.

- [x] Add failing tests for disabled, metadata and payload modes, redaction, binary digests, caps and TTL pruning.
- [x] Implement a separate JSONL capture store with restrictive permissions and bounded retention.
- [x] Tee ASGI ingress/egress without changing delivered chunks and emit capture audit events.
- [x] Run the focused observability tests.
- [x] Commit as `feat: add bounded debug diagnostic capture`.

### Task 3: Outbound HTTP and lifecycle coverage

**Files:**
- Create: `src/sandbox_harness/common/observability/http.py`
- Modify: provider, Runtime and gateway HTTP client construction under `src/sandbox_harness/`
- Modify: core Broker, allocation, Job and media lifecycle event sites
- Test: `tests/test_observability.py`
- Test: affected gateway, allocation, JobWorker and media tests

**Interfaces:**
- Consumes: `DiagnosticCapture` and trace carrier.
- Produces: `httpx_event_hooks(peer_service)` with outbound span/log correlation and optional diagnostic capture.

- [x] Add failing tests for trace injection, redacted egress capture and request/response timing.
- [x] Implement reusable HTTPX hooks and attach them at shared external boundaries.
- [x] Add missing provider-neutral state and generation checkpoints at core lifecycle boundaries.
- [x] Run observability plus affected service tests.
- [x] Commit as `feat: trace outbound calls and lifecycle transitions`.

### Task 4: Configuration, documentation and release verification

**Files:**
- Modify: `.env.example`
- Modify: `docs/observability.md`
- Modify: `docs/development/README.md`
- Modify: module README files where observability ownership applies
- Test: `tests/test_deployment_isolation.py`
- Test: `tests/test_ali_deployment.py`

**Interfaces:**
- Consumes: all previous tasks.
- Produces: documented environment contract and deployment-safe defaults.

- [x] Document log level, directory, filename template, segment/retention limits, diagnostic modes, OTLP Collector routing and trace queries.
- [x] Add deployment tests proving operational and diagnostic logs stay under the selected `/data2` data root and reject unsafe filenames.
- [x] Run the full Python suite and relevant frontend/deployment checks required by the shared boundary.
- [ ] Restart the affected independent services, verify recovery and inspect a real trace timeline.
- [ ] Commit as `docs: define full-chain logging operations`.
