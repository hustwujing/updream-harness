# Cross-Harness Context And Budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Codex, Claude and DeepSeek context budgeting cache-aware and preserve local budget failure identity.

**Architecture:** Normalize native usage in `OutputProbe`, settle one weighted durable budget, explicitly configure Codex compaction, and reconcile generic native 429 errors from the durable model activity.

**Tech Stack:** Python, FastAPI, SQLAlchemy, native Codex/Claude/DeepSeek Harnesses

**Spec:** `docs/superpowers/specs/2026-11-20-cross-harness-context-budget-design.md`

## Global Constraints

- Preserve native Harness sessions and reasoning state.
- Do not add case-specific prompt or error matching.
- Do not change the database schema.

## Review Focus

- Partial SSE usage frames must merge without double counting.
- Missing usage must still fail closed.
- Concurrent model requests must remain serialized by the reservation.
- Genuine upstream 429 must remain distinguishable from local exhaustion.
- Child Harness parent budgets must retain their shared boundary.

---

### Task 1: Normalize cache-aware usage

**Files:**
- Modify: `src/sandbox_harness/model_activity.py`
- Modify: `src/sandbox_harness/config.py`
- Modify: `.env.example`
- Test: `tests/test_model_activity.py`

- [x] Add failing OpenAI, Claude and DeepSeek usage-shape tests.
- [x] Implement non-overlapping usage dimensions and weighted charge.
- [x] Add bounded read/write budget percentage settings.

### Task 2: Correct reservation and failure identity

**Files:**
- Modify: `src/sandbox_harness/gateway.py`
- Modify: `src/sandbox_harness/store.py`
- Modify: `src/sandbox_harness/broker.py`
- Test: `tests/test_gateway_security.py`
- Test: `tests/test_broker.py`

- [x] Reserve output before unknown cache outcome and settle weighted usage.
- [x] Persist local reservation failures as model activities.
- [x] Reconcile only generic native 429 with the durable local error.

### Task 3: Align native compaction

**Files:**
- Modify: `src/sandbox_runtime/adapters.py`
- Test: `tests/test_adapters.py`

- [x] Configure Codex auto-compaction and explicitly select local compaction.
- [x] Prove Claude and DeepSeek commands do not receive unsupported overrides.

### Task 4: Dismissible failed-Turn tip

**Files:**
- Modify: `web/src/main.jsx`
- Modify: `web/src/enhancements.css`
- Test: `tests/browser_input_status.cjs`

- [x] Show the sanitized latest Error Item for a failed latest Turn.
- [x] Persist dismissal by Turn ID and show a later failure independently.
- [x] Prove no raw `turn.error_detail` reaches the banner.

### Task 5: Verify and release

**Files:**
- Modify: `README.md`
- Modify: `docs/development/recovery-and-observability.md`
- Modify: `docs/development/gateway-security-providers.md`

- [x] Run focused unit/component tests for all three Harnesses.
- [x] Run real Codex, Claude and DeepSeek model probes including cache usage.
- [ ] Run the full gate, deploy, verify and push.
