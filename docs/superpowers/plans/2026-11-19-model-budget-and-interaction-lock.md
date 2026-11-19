# Model Budget And Interaction Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve native Harness context management, prevent false local rate-limit failures from visual payloads, stop repeated DeepSeek instruction injection, and make submitted user decisions immediately non-editable.

**Architecture:** Codex, Claude and DeepSeek retain ownership of conversation compaction and image/Skill history. Gateway exposes a strict per-Harness endpoint policy, performs bounded protocol-aware input estimation before durable token reservation, and passes native state through unchanged. Runtime injects stable DeepSeek instructions once, while React interaction cards own an optimistic submitted receipt until durable state refreshes.

**Tech Stack:** Python, FastAPI, PostgreSQL, React, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-19-model-budget-and-interaction-lock-design.md`

## Global Constraints

- Do not increase or bypass Capability budgets.
- Do not expose Provider bodies or credentials.
- Do not allow a second interaction submission while delivery is ambiguous.
- Fall back closed when a payload cannot be estimated safely.
- Do not create a platform summarizer or translate native compaction records.
- Reject every model-route suffix not explicitly assigned to that Harness.

## Review Focus

- Large compressed and uncompressed image data URIs.
- Invalid image headers and non-image Base64.
- Local budget 429 versus upstream Provider 429.
- Interaction API success followed by refresh failure.
- Double click and responding state after page refresh.
- Native compact/count endpoints with large bodies, malformed payloads and missing usage.
- DeepSeek Session continuation after the first input without repeated developer instructions.

---

### Task 1: Native Model Endpoint Policy

**Files:**
- Modify: `src/sandbox_harness/gateway.py`
- Test: `tests/test_gateway_security.py`

**Interfaces:**
- Produces: a strict endpoint classifier used by the proxy to distinguish generation, compaction and token-count calls.

- [ ] Add failing tests for `/responses/compact`, `/messages/count_tokens`, native model replacement and unrelated suffix rejection.
- [ ] Implement the strict per-Harness endpoint policy and preserve query strings and native payload fields.
- [ ] Keep token counting outside visible model activity and generation reservations.
- [ ] Reserve and settle standalone compaction without adding `max_output_tokens` to its request.
- [ ] Run the focused Gateway route tests.

### Task 2: Protocol-aware Input Estimate

**Files:**
- Modify: `src/sandbox_harness/gateway.py`
- Test: `tests/test_gateway_security.py`

- [ ] Add failing tests for image and text estimates.
- [ ] Implement bounded recursive estimator and image dimension probes.
- [ ] Use the estimate in output-budget enforcement.
- [ ] Run focused Gateway tests.

### Task 3: Stable Local Budget Error

**Files:**
- Modify: `src/sandbox_runtime/adapters.py`
- Modify: `src/sandbox_harness/errors.py`
- Test: `tests/test_adapters.py`

- [ ] Add failing distinction tests.
- [ ] Preserve `model_token_budget_exhausted` before generic 429 mapping.
- [ ] Add actionable public copy.

### Task 4: DeepSeek Native Context Ownership

**Files:**
- Modify: `src/sandbox_runtime/deepseek_runner.py`
- Test: `tests/test_deepseek_runner.py`
- Test: `tests/test_runtime_configuration.py`

**Interfaces:**
- Consumes: the existing persistent `DSH_HOME` Session.
- Produces: first-input-only platform instructions and unmodified later user messages.

- [ ] Add a failing multi-input test that counts platform instruction occurrences.
- [ ] Inject platform instructions only on a new Session's first input; resumed Sessions and later inputs receive only the current message.
- [ ] Keep automatic DSH compaction, image offload and JSONL state under the checkpointed `DSH_HOME`.
- [ ] Run focused DeepSeek and checkpoint tests.

### Task 5: Optimistic Interaction Lock

**Files:**
- Modify: `web/src/main.jsx`
- Test: interaction browser test selected by inspection.

- [ ] Add delayed-response and failure browser cases.
- [ ] Replace editable controls with an optimistic receipt on submit.
- [ ] Restore controls only on definite failure.
- [ ] Run browser tests and inspect the resulting states.

### Task 6: Release Verification

- [ ] Run focused tests, the full Python gate, frontend build and browser regressions.
- [ ] Build only changed Runtime images; Codex/Claude image rebuild is unnecessary when their bundled binaries do not change.
- [ ] Commit, deploy Gateway/API/static frontend and verify health.
- [ ] Run deployed probes for large image input, native endpoint forwarding, DeepSeek continuation and immediate interaction locking.
