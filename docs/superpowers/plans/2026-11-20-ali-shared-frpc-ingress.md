# Ali Shared FRPC Ingress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the independent Ali deployment through the existing public FRPC port 8090 under `/sandbox-harness-platform-parallel-ali/`.

**Architecture:** Add a version-controlled location-only snippet to the existing PROXY v2 Nginx server and proxy only to the Ali API on `127.0.0.1:24767`. Keep all mutable state under `/data2` and preserve the existing ingress routes.

**Tech Stack:** Nginx, FRPC PROXY Protocol v2, pytest, curl

**Spec:** `docs/superpowers/specs/2026-11-20-ali-shared-frpc-ingress-design.md`

## Global Constraints

- Do not change `/sandbox-harness/` or `/sandbox-harness-platform-parallel/`.
- Do not expose PostgreSQL, Redis or Gateway ports.
- Store Ali configuration and mutable state under `/data2/wujing03/sandbox-harness-platform-parallel-ali`.
- Validate Nginx before reload.

## Review Focus

- Prefix redirect and trailing-slash routing.
- Trusted client address comes only from PROXY Protocol.
- FRPC authentication marker cannot be supplied by the caller.
- Streaming, uploads and WebSockets remain enabled.
- Existing routes remain unchanged after installation.

---

### Task 1: Version and test the Ali route

**Files:**
- Create: `deploy/nginx-frpc-ali-shared-location.conf`
- Modify: `tests/test_ingress_config.py`
- Modify: `docs/deployment.md`

**Interfaces:**
- Consumes: shared PROXY v2 listener at `127.0.0.1:18090`.
- Produces: location-only Nginx snippet for the Ali prefix and upstream.

- [ ] **Step 1: Add a failing ingress contract test**

Assert that the Ali snippet contains the exact prefix, `127.0.0.1:24767`
upstream, PROXY-derived client headers, forced `frpc` ingress marker,
streaming/upload settings, and no database, Redis or Gateway ports.

- [ ] **Step 2: Run the focused test and confirm it fails because the snippet is missing**

Run: `.venv/bin/pytest -q tests/test_ingress_config.py`
Expected: FAIL for the missing Ali snippet.

- [ ] **Step 3: Add the minimal Ali Nginx snippet and deployment documentation**

- [ ] **Step 4: Run the focused ingress tests**

Run: `.venv/bin/pytest -q tests/test_ingress_config.py`
Expected: all tests pass.

- [ ] **Step 5: Commit the versioned route contract**

### Task 2: Install and verify the shared route

**Files:**
- Runtime include: `/etc/nginx/conf.d/sandbox-harness-frpc.conf`
- Versioned snippet: `deploy/nginx-frpc-ali-shared-location.conf`

**Interfaces:**
- Consumes: Task 1 snippet and the Ali API on `127.0.0.1:24767`.
- Produces: public `:8090/sandbox-harness-platform-parallel-ali/` ingress.

- [ ] **Step 1: Add one include line for the versioned `/data2` snippet**

- [ ] **Step 2: Run `nginx -t` before reload**

Expected: syntax is ok and test is successful.
