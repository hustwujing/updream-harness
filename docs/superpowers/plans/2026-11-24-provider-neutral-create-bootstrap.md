# Provider-Neutral Create And Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove unnecessary fresh-create reconciliation latency and make Runtime bootstrap independent of provider UID and `/run` mount behavior.

**Architecture:** Keep allocation identity and ambiguous-outcome reconciliation in the unified Sandbox Protocol. Resolve bootstrap state through one provider-neutral runtime function used by the supervisor and compatibility CLI.

**Tech Stack:** Python 3.13, asyncio, FastAPI, pytest, shell Runtime entrypoints.

**Spec:** `docs/superpowers/specs/2026-11-24-provider-neutral-create-bootstrap-design.md`

## Global Constraints

- No case-specific or Ali-specific behavior in shared scheduling and Runtime code.
- Provider errors remain typed protocol outcomes.
- Preserve OpenSandbox and Aliyun switchability through configuration.
- Generated state stays inside the sandbox or `/data2/wujing03` on the host.

## Review Focus

- Successful fresh create must issue zero inventory reads before provider create.
- Lost accepted response must reconcile once and never replay provider create.
- Repeated in-process allocation must return the same fenced handle.
- Runtime must start when the legacy state path is absent or unwritable.
- CLI dispatch and supervisor must resolve the identical active code release.

---

### Task 1: Create Outcome Semantics

**Files:**
- Modify: `tests/test_sandbox_aliyun.py`
- Modify: `src/sandbox_harness/sandbox_aliyun.py`

**Interfaces:**
- Consumes: `SandboxCreateRequest.allocation_id`, `SandboxCreateOutcomeUnknown`.
- Produces: `AliyunSandboxBackend.create()` with create-first, reconcile-on-ambiguity behavior.

- [ ] Add a failing test asserting a successful fresh create has one create call and no inventory calls before it.
- [ ] Run the focused test and verify the existing implementation fails on inventory calls.
- [ ] Remove repeated pre-create provider reconciliation while retaining cache reuse and exception reconciliation.
- [ ] Run all Ali adapter tests and verify accepted-response-loss, repeated create and fencing remain green.
- [ ] Commit the task independently.

### Task 2: UID-Neutral Runtime State

**Files:**
- Modify: `tests/test_runtime_code_bundle.py`
- Modify: `src/sandbox_bootstrap/supervisor.py`
- Modify: `runtime/runtime_cli_dispatch.sh`

**Interfaces:**
- Produces: one `runtime_state_dir()` contract honoring `SANDBOX_RUNTIME_STATE_DIR` and otherwise using a private current-UID temporary path.
- Consumes: supervisor code activation and preinstalled CLI dispatch.

- [ ] Add failing tests for absent legacy state path and a configured writable state directory.
- [ ] Run the focused tests and verify failure against the hardcoded `/run/harness` behavior.
- [ ] Implement the resolver and make supervisor and CLI dispatch use the same environment contract.
- [ ] Run Runtime bootstrap/client/CLI tests and verify signed activation and restart behavior.
- [ ] Commit the task independently.

### Task 3: Cross-Provider Verification And Release Evidence

**Files:**
- Update: `docs/superpowers/reports/2026-11-24-provider-selected-gateway-listener.md`
- Create: `.data/evidence/` outputs only (not committed).

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: fake/unit, real Ali protocol, OpenSandbox regression and K-series evidence.

- [ ] Run focused provider and Runtime suites.
- [ ] Run the full pytest suite.
- [ ] Rebuild/publish immutable Runtime images/templates required by the bootstrap change.
- [ ] Run real Ali protocol cases and verify no leaked owned sandboxes.
- [ ] Run the K-series canary, then the remaining campaign only if the canary passes.
- [ ] Record exact revisions, image/template releases, timings and residual provider SLO risk.

