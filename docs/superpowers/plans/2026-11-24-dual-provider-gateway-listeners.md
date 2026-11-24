# Dual-provider Gateway Listeners Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run explicit, provider-specific Gateway and proxy listeners for OpenSandbox and Aliyun.

**Architecture:** Launch two instances of the existing Gateway entry point with bind-host overrides. Keep application protocol, persistence, authorization and proxy policy shared.

**Tech Stack:** Bash, systemd transient units, FastAPI/uvicorn, asyncio proxy, pytest.

**Spec:** `docs/superpowers/specs/2026-11-24-dual-provider-gateway-listeners-design.md`

## Global Constraints

- Use only `172.31.0.1` and `10.194.49.8`; never a wildcard bind.
- Preserve the existing OpenSandbox network rule.
- Keep state and logs under `/data2/wujing03/sandbox-harness-platform-parallel-ali/.data`.
- Do not switch the application backend during listener verification.

## Review Focus

- One unit failing must not prevent the other listener from being stopped or diagnosed.
- Re-running start must remain idempotent.
- Status must distinguish provider and host, not merely report a port found anywhere.
- Stop must remove both transient units without affecting the original deployment.
- Aliyun verification must not rely on dynamic VPC metadata.

---

### Task 1: Provider-specific Gateway process entry points

**Files:**
- Modify: `deploy/ali/run-service.sh`
- Test: `tests/test_ali_deployment.py`

- [ ] Add failing tests for `gateway-opensandbox` and `gateway-aliyun` host overrides and wildcard rejection.
- [ ] Run the focused tests and confirm failure.
- [ ] Implement the two component cases using the existing Gateway executable.
- [ ] Run the focused tests and commit.

### Task 2: Dual unit lifecycle and exact status

**Files:**
- Modify: `deploy/ali/start.sh`
- Modify: `deploy/ali/stop.sh`
- Modify: `deploy/ali/status.sh`
- Test: `tests/test_ali_deployment.py`

- [ ] Add failing lifecycle tests for both unit names and exact host/port checks.
- [ ] Implement idempotent start, stop and status handling for both listeners.
- [ ] Preserve the existing OpenSandbox network rule and startup ordering.
- [ ] Run deployment tests and commit.

### Task 3: Runtime verification and evidence

**Files:**
- Create: `docs/superpowers/reports/2026-11-24-dual-provider-gateway-listeners.md`

- [ ] Start both Gateway units and verify exact host sockets.
- [ ] Verify host-local `/health` and proxy TCP sockets for both providers.
- [ ] Create an Aliyun sandbox from the READY template without dynamic VPC metadata and verify Gateway `/health` plus proxy reachability.
- [ ] Verify the OpenSandbox listener remains reachable.
- [ ] Run focused and full regression tests, request review and record evidence.
