# OpenSandbox State Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild OpenSandbox with state owned by `opensandbox-stack`, independent of stopped products.

**Architecture:** Retain the dedicated Docker daemon and immutable images. Relocate only mutable server state/config, discard sandbox instances, and let current clients rebuild their warm pools.

**Tech Stack:** systemd, OpenSandbox Docker runtime, SQLite, shell health probes.

**Spec:** `docs/superpowers/specs/2026-11-19-opensandbox-state-ownership-design.md`

## Global Constraints

- Store all mutable state under `/data2/wujing03`.
- Do not restart or delete the Docker daemon or image cache.
- Do not re-enable stopped Plus or distributed-agent services.

## Review Focus

- Metadata patch must prove the former 500 path is fixed.
- Existing Docker sandboxes must not become unmanaged leftovers.
- Parallel warm-pool convergence must be observed after restart.
- Unit restart must work after the predecessor runtime directory is absent.
- Logs and SQLite files must be created under the new owner path.

---

### Task 1: Relocate and rebuild OpenSandbox state

**Files:**
- Create: `/data2/wujing03/opensandbox-stack/config/sandbox-opensandbox.toml`
- Modify: `/etc/systemd/system/sandbox-agent-opensandbox.service`
- Test: deployed OpenSandbox lifecycle probes

**Interfaces:**
- Consumes: the existing API key and dedicated Docker daemon.
- Produces: an independent OpenSandbox lifecycle service on `127.0.0.1:28090`.

- [ ] Snapshot current container inventory and confirm no active client Turns.
- [ ] Stop OpenSandbox clients and the lifecycle API without stopping Docker.
- [ ] Remove old sandbox containers and initialize the new state directories.
- [ ] Install the independent config/unit paths and restart OpenSandbox.
- [ ] Restart current clients and verify create/patch/runtime/delete plus warm-pool recovery.
- [ ] Record the incident boundary in development documentation and commit.
