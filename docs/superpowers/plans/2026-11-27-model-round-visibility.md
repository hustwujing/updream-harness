# Model Round Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show durable GPT conversation rounds and live phase/duration in every execution-graph Harness node.

**Architecture:** Persist payload-free model activity for Primary and Child Harness requests in existing Core Items. Project allowlisted per-request rows with aggregate usage, then render a compact summary and collapsed details in the existing Harness model-usage panel.

**Tech Stack:** Python 3.13, SQLAlchemy, FastAPI projection, React, Vitest.

**Spec:** `docs/superpowers/specs/2026-11-27-model-round-visibility-design.md`

## Global Constraints

- No database migration or new table.
- Do not expose prompts, output content, commands, URLs, credentials or raw exceptions.
- One unique `model_request_id` is one round; replay is counted once.
- Primary and Child Harness use the same contract.
- Live elapsed time updates in presentation only; terminal duration stays fixed.

## Review Focus

- A Child Turn already terminal while its Job remains live must retain in-progress model activity.
- Replayed request IDs must not duplicate round or token totals.
- Missing token and first-output values must render as zero/omitted without `NaN`.
- Failed rounds must remain visible and stop their elapsed timer.
- More than 5,000 persisted rows must set the existing partial-data marker.

---

### Task 1: Persist Child Model Activity

**Files:**
- Modify: `src/sandbox_harness/common/models/model_activity.py`
- Test: `tests/test_model_activity.py`

**Interfaces:**
- Produces: payload-free `ItemRow` model activity containing `model_request_id`, `model`, `phase` and `job_id` for every Harness request.

- [ ] Add a failing test that starts and finishes a `ModelActivity` with a non-empty `claims.job_id` and asserts persistence/update calls retain that job ID.
- [ ] Run the focused test and confirm it fails because Child activity receives no item identity.
- [ ] Make `ModelActivity.start()` persist the same `STEP_STARTED` record for Primary and Child claims, using the actual allowlisted `job_id`.
- [ ] Run `tests/test_model_activity.py` and commit the persistence change.

### Task 2: Project Model Rounds

**Files:**
- Modify: `src/sandbox_harness/services/api/execution_graph.py`
- Test: `tests/test_execution_graph.py`

**Interfaces:**
- Consumes: model activity Item payloads from Task 1.
- Produces: `aggregate_model_activity(rows)` keyed by `(turn_id, job_id)` with `token_usage` and ordered `model_rounds`.

- [ ] Add failing graph assertions for two Primary rounds and one Child round, including active, completed, failed, timing, token and deduplication fields.
- [ ] Run the focused execution-graph test and confirm `model_rounds` is absent.
- [ ] Replace usage-only aggregation with the shared activity projection and attach `model_rounds` plus `model_rounds_truncated` to Primary and Child nodes.
- [ ] Add the >5,000-row truncation assertion and run `tests/test_execution_graph.py`.
- [ ] Commit the API contract change.

### Task 3: Render Round Progress

**Files:**
- Modify: `web/src/HarnessRunDetails.jsx`
- Modify: `web/src/ExecutionGraph.jsx`
- Modify: `web/src/harness-token-usage.css`
- Create: `web/src/harness-model-rounds.js`
- Create: `web/src/harness-model-rounds.test.js`

**Interfaces:**
- Consumes: `node.model_rounds` and `node.model_rounds_truncated` from Task 2.
- Produces: `modelRoundSummary(rounds, now)` and compact/expanded model-round UI.

- [ ] Add failing helper tests for no rounds, completed rounds, an active streaming round, a failed round and missing metrics.
- [ ] Implement the pure summary/format helpers and run the new Vitest file.
- [ ] Render the summary in the model-usage header and a collapsed `<details>` list with phase, duration, first output and per-round tokens.
- [ ] Append only the round count to the graph node status line and add responsive styles that keep labels within their container.
- [ ] Run the full frontend tests and `npm --prefix web run build`.
- [ ] Commit the frontend change.

### Task 4: Affected Real Recovery Verification

**Files:**
- Modify if needed: `tests/run_real_evolution.py`
- Evidence: `.data/evidence/real-campaign/`

**Interfaces:**
- Consumes: deployed API/frontend contract from Tasks 1-3.
- Produces: real Ali evidence that a live model round survives service restart and continues to terminal state without duplicate allocation.

- [ ] Deploy the new releases while an affected long-running Harness Job is active, using the repository stop/start scripts.
- [ ] Assert the same `model_request_id` remains one round, the Job recovery generation advances, and the existing allocation/external execution is reused.
- [ ] Verify the public execution graph shows the active round and then freezes its terminal duration.
- [ ] Run focused Python and frontend checks again, commit any real-case addition, and push the branch.
