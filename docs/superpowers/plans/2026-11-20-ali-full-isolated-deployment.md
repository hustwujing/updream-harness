# Ali Full Isolated Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the temporary Ali API-only baseline with a complete, reproducible and storage-isolated deployment under `/data2/wujing03/sandbox-harness-platform-parallel-ali`.

**Architecture:** Generate an Ali-owned release snapshot and configuration, run a dedicated PostgreSQL cluster, isolate all shared-service identities, and supervise the full process graph with transient systemd units defined in `/data2`. Use the shared OpenSandbox service only through an Ali namespace until `AliyunSandboxBackend` is implemented.

**Tech Stack:** Python 3.13, FastAPI, PostgreSQL 16, Redis, OpenSandbox, systemd transient units, Nginx, pytest, shell

**Spec:** `docs/superpowers/specs/2026-11-20-ali-full-isolated-deployment-design.md`

## Global Constraints

- All Ali code and persistent state stay under `/data2/wujing03/sandbox-harness-platform-parallel-ali`.
- Do not modify the original Parallel checkout, branch, ports, database or namespaces.
- Do not install persistent systemd units or application state on the root partition.
- Shared credentials are copied only through an explicit allowlist; deployment secrets are regenerated.
- Every live mutation has an Ali-specific target and a read-only preflight.

## Review Focus

- A stale or partially written environment must never start services.
- Port, prefix, namespace and database collisions must fail closed.
- Transient unit restart/stop operations must never match original Parallel units.
- Image/catalog rewrites must preserve manifest digest integrity.
- Cutover failure must leave the previous SQLite evidence and existing Parallel service intact.

---

### Task 1: Ali identity and configuration generator

**Files:**
- Create: `deploy/ali/configure.py`
- Create: `deploy/ali/identity.py`
- Test: `tests/test_ali_deployment.py`

**Interfaces:**
- Produces: `.data/config/platform.env`, isolated directories and copied immutable assets.
- Consumes: source `platform.env`, signing key and controlled runtime source.

- [ ] Write tests for exact path/branch, identity matrix, allowlisted credentials, regenerated secrets, port collision, digest verification and root-partition rejection.
- [ ] Run tests and confirm RED because Ali deployment modules are missing.
- [ ] Implement `identity.py` constants and `configure.py --source-env --runtime-signing-key --controlled-runtime-source [--check-only|--force]`.
- [ ] Run focused tests GREEN and commit.

### Task 2: Independent PostgreSQL lifecycle

**Files:**
- Create: `deploy/ali/init-postgresql.sh`
- Create: `deploy/ali/postgresql-start.sh`
- Test: `tests/test_ali_deployment.py`

**Interfaces:**
- Consumes: Task 1 database URL and `.data/postgresql` paths.
- Produces: PostgreSQL cluster on 28433 with Ali role/database.

- [ ] Add RED tests for PGDATA, port, role/database, socket/log paths and prohibition of 28432/root paths.
- [ ] Implement initialization and foreground start scripts using shared PostgreSQL binaries.
- [ ] Run focused tests GREEN and commit.

### Task 3: Runtime assets, catalogs and registry namespace

**Files:**
- Create: `deploy/ali/materialize-assets.py`
- Modify: `deploy/ali/configure.py`
- Test: `tests/test_ali_deployment.py`

**Interfaces:**
- Consumes: source release digests and immutable image references.
- Produces: Ali-owned bundles, catalogs, controlled runtime snapshot and Ali registry references.

- [ ] Add RED tests for independent paths, digest checks, no source `.data` references and Ali registry prefix.
- [ ] Implement selective bundle/catalog/runtime copying and catalog image-reference rewriting.
- [ ] Add a registry promotion command that preserves manifest digests and fails on mismatch.
- [ ] Run focused tests GREEN and commit.

### Task 4: Network rule and transient service graph

**Files:**
- Create: `deploy/ali/network-rule.sh`
- Create: `deploy/ali/run-service.sh`
- Create: `deploy/ali/start.sh`
- Create: `deploy/ali/stop.sh`
- Create: `deploy/ali/status.sh`
- Test: `tests/test_ali_deployment.py`

**Interfaces:**
- Consumes: Tasks 1-3 environment, PostgreSQL and assets.
- Produces: Ali transient units for PostgreSQL, Network, Gateway, Workers and API.

- [ ] Add RED tests for Ali-only unit names, ports, rootless persistent state and dependency order.
- [ ] Implement idempotent systemd-run lifecycle and Ali-specific firewall rule.
- [ ] Run focused tests GREEN and commit.

### Task 5: Configure, initialize and cut over live Ali deployment

**Files:**
- Runtime state only under `.data`.

**Interfaces:**
- Consumes: Tasks 1-4 scripts and current source release inputs.
- Produces: complete live deployment using PostgreSQL and `backend=opensandbox`.

- [ ] Run configuration preflight and verify all candidate ports/identities.
- [ ] Materialize independent assets and initialize PostgreSQL.
- [ ] Stop only the temporary Ali nohup API.
- [ ] Start the transient service graph and verify all units/ports.
- [ ] Verify environment contains no source checkout mutable paths.

### Task 6: End-to-end verification and handoff

**Files:**
- Modify: `docs/deployment.md`
- Create: `docs/test-evidence/ali-isolated-deployment-report.md`

**Interfaces:**
- Consumes: live deployment from Task 5.
- Produces: reproducible deployment documentation and current-revision evidence.

- [ ] Run full Python tests and frontend build.
- [ ] Run Nginx, storage-isolation, database, Redis, namespace and unit checks.
- [ ] Run a real Codex Turn through direct ingress and verify sandbox cleanup.
- [ ] Verify existing Parallel direct/FRPC routes and services remain healthy.
- [ ] Document exact revisions, identities, digests, evidence and residual Ali-backend work.
- [ ] Commit documentation and evidence.

