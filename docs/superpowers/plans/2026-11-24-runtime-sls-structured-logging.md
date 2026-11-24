# Runtime SLS Structured Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit safe provider-neutral Runtime lifecycle JSON to stdout and enable automatic SLS logging on all 18 active Ali templates.

**Architecture:** Mirror the existing sanitized event stream through a Runtime-only stdout handler, propagate durable Job identity through the common Runtime configuration protocol, and configure Ali templates through a separately verified control-plane operation.

**Tech Stack:** Python 3.13, logging, FastAPI, pytest, Alibaba Cloud Agent Sandbox OpenAPI SDK.

**Spec:** `docs/superpowers/specs/2026-11-24-runtime-sls-structured-logging-design.md`

## Global Constraints

- No provider-specific fields in shared Runtime or Sandbox Protocol code.
- No secrets or user/model payloads in stdout logs.
- Preserve all template settings except `runtimeConfig.logConfig`.
- Do not use the root partition; evidence and generated state remain under `/data2/wujing03`.

## Review Focus

- A malformed or secret-bearing event cannot escape the existing sanitizer.
- Missing identity at bootstrap produces stable empty fields, not fabricated IDs.
- Child jobs carry a real durable `job_id`; primary turns remain valid without one.
- Configuring Runtime stdout does not add duplicate console logs to API/Gateway.
- A partial template update does not clear VPC, image, commands, or resource settings.

---

### Task 1: Runtime stdout JSON contract

**Files:**
- Modify: `src/sandbox_harness/observability.py`
- Modify: `src/sandbox_bootstrap/main.py`
- Test: `tests/test_observability.py`
- Test: `tests/test_runtime_code_bundle.py`

**Interfaces:**
- Produces: `ConsoleJsonHandler(service: str, stream=None)` and Runtime-only installation through `configure()`.

- [ ] Write failing tests for fixed JSON fields, sanitization, and bootstrap setup.
- [ ] Run the focused tests and verify the expected failures.
- [ ] Implement the handler and bootstrap configuration.
- [ ] Run focused tests and commit.

### Task 2: Durable Job identity propagation

**Files:**
- Modify: `src/sandbox_harness/runtime.py`
- Modify: `src/sandbox_runtime/app.py`
- Modify: `src/sandbox_harness/harness_job.py`
- Modify: `src/sandbox_harness/media_render_adapter.py`
- Test: `tests/test_runtime_client.py`
- Test: `tests/test_runtime_configuration.py`

**Interfaces:**
- Consumes: the JSON handler from Task 1.
- Produces: optional `job_id` in the provider-neutral Runtime configure payload and log context.

- [ ] Write failing payload and Runtime context tests.
- [ ] Run the focused tests and verify the expected failures.
- [ ] Add optional Job identity end to end.
- [ ] Run focused tests and commit.

### Task 3: Ali template logging rollout and canary

**Files:**
- Create: `.data/evidence/aliyun-sandbox/template-logging-auto-ede82d6.json`

**Interfaces:**
- Consumes: the active 18-template release map and control-plane credentials.
- Produces: 18 ready templates with automatic SLS configuration plus canary evidence.

- [ ] Read all 18 templates and save a pre-change snapshot.
- [ ] Prove automatic configuration semantics on one template and verify concrete log destination.
- [ ] Update the remaining templates, preserving unrelated fields, and poll all to `ready`.
- [ ] Create a fresh canary, verify health and structured SLS output, then delete only that canary.
- [ ] Save evidence without credentials and run the full regression suite.
