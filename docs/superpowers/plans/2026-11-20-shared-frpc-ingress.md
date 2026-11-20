# Shared FRPC Ingress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve Parallel through the existing public FRPC port 8090 under its own path prefix.

**Architecture:** Install a location-only, version-controlled snippet into the existing PROXY v2 Nginx server. Preserve the independent Parallel API and FRPC authentication boundary.

**Tech Stack:** Nginx, FRPC PROXY Protocol v2, pytest, curl

**Spec:** `docs/superpowers/specs/2026-11-20-shared-frpc-ingress-design.md`

## Global Constraints

- Do not change the original `/sandbox-harness/` route.
- Do not expose API, Gateway, Redis or PostgreSQL directly.
- Validate Nginx before reload.

## Review Focus

- Prefix redirect and trailing-slash routing.
- Trusted client address comes only from PROXY Protocol.
- FRPC authentication marker cannot be supplied by the caller.
- Streaming, uploads and WebSockets remain enabled.
- The public URL serves static assets and prefixed APIs.

---

### Task 1: Version and verify the shared route

**Files:**
- Create: `deploy/nginx-frpc-shared-location.conf`
- Modify: `tests/test_ingress_config.py`
- Modify: `docs/deployment.md`

- [x] Add a failing contract test for the shared location snippet.
- [x] Add the prefix route with PROXY-derived headers and `frpc` marker.
- [x] Run the focused ingress tests.

### Task 2: Install and verify the public route

- [x] Install the snippet and include it in the existing `18090` server.
- [x] Run `nginx -t`, reload Nginx and probe local PROXY v2 routing.
- [x] Probe the public page, API session endpoint and static asset.
- [ ] Commit and push the configuration contract.
