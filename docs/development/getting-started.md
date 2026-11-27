# Getting Started

## Applies to

Read this before the first code change or when setting up a new checkout.

## Toolchain and storage

- Python requirement: CPython 3.13 (`pyproject.toml` and Runtime ABI contract).
- Frontend: Node.js with the lockfile in `web/package-lock.json`.
- Python package and console entry points are defined in `pyproject.toml`.
- Dependencies and build output stay inside the project checkout on `/data/wujing02`;
  runtime state, caches, logs, databases and test temporaries belong under
  the selected checkout's `.data` directory. The isolated Ali checkout is
  `/data/wujing02/sandbox-harness-platform-parallel-ali`; its state must never
  use the original checkout's `.data` tree. Other explicit writable paths for
  this work must also stay under `/data/wujing02`.
- Reuse shared binaries and existing infrastructure services. The deployment
  owns its PostgreSQL cluster on port 28433 and never shares an application
  database or runtime directory.

## Initial setup

```bash
mkdir -p .data/tmp .data/cache .data/uv-cache .data/pip-cache .data/npm-cache
export TMPDIR="$PWD/.data/tmp" XDG_CACHE_HOME="$PWD/.data/cache"
export UV_CACHE_DIR="$PWD/.data/uv-cache" PIP_CACHE_DIR="$PWD/.data/pip-cache"
export npm_config_cache="$PWD/.data/npm-cache" PYTHONDONTWRITEBYTECODE=1
export PLAYWRIGHT_BROWSERS_PATH="$PWD/.data/playwright-browsers"
uv sync --extra test
npm --prefix web ci
```

Before starting a local service, configure its ignored `.env` with its own
database, namespace, object/Redis prefixes and writable paths. Root
`.env.example` contains original-deployment paths and ports; use it as a field
reference, not as an unchanged Ali configuration. The configured Ali services
load `.data/config/platform.env` through `deploy/ali/run-service.sh`.

Use `SANDBOX_HARNESS_BACKEND=fake` only for local API/UI development. Production
must use an explicitly configured real Sandbox backend; the code deliberately
has no host-execution fallback. The verified Ali baseline uses
`SANDBOX_HARNESS_BACKEND=aliyun` and the durable allocation dispatcher. OpenSandbox
remains configurable; provider switching requires checking outstanding work and
the frozen release/route configuration before restarting the selected services.
Local `.env` setup does not replace the deployed `.data/config/platform.env`.
Keep credentials in ignored environment files, never in source or test fixtures.

## Common commands

```bash
.venv/bin/pytest -q tests/test_store.py tests/test_broker.py
npm --prefix web run build
.venv/bin/uvicorn sandbox_harness.services.api.app:create_app --factory --reload --port 22766
```

The full Python suite is `.venv/bin/pytest`. `pyproject.toml` redirects pytest
temporary and cache data into `.data`. Browser scripts use the built frontend
and a running service; invoke them with `NODE_PATH=web/node_modules` when Node
module resolution needs the project dependency directory.

## Before editing

1. Check `pwd`, `git worktree list`, `git branch --show-current` and
   `git status --short`; preserve unrelated work. Ali edits belong only in
   `/data/wujing02/sandbox-harness-platform-parallel-ali` on branch
   `sandbox-harness-platform-parallel-ali`. The sibling original project is
   read-only for this work.
2. Prefer CodeGraph when `.codegraph/` exists; otherwise locate the current
   implementation with `rg`. Do not infer it from an old report.
3. Read the relevant development guide from the index.
4. Identify the smallest focused tests that exercise the affected contract.

## Completion

- Source, tests and documentation agree.
- Focused tests pass; broader checks match the blast radius.
- No generated data, credentials or local environment files are tracked.
- The handoff distinguishes code written, tests run and code actually deployed.
