# Getting Started

## Applies to

Read this before the first code change or when setting up a new checkout.

## Toolchain and storage

- Python requirement: CPython 3.13 (`pyproject.toml` and Runtime ABI contract).
- Frontend: Node.js with the lockfile in `web/package-lock.json`.
- Python package and console entry points are defined in `pyproject.toml`.
- Dependencies and build output stay inside the project checkout on `/data2`;
  runtime state, caches, logs, databases and test temporaries belong under
  `/data2/wujing03/sandbox-harness-platform-parallel/.data`.
- Do not install a second PostgreSQL, Redis, Docker, registry, object store or
  OpenSandbox instance for this product.

## Initial setup

```bash
UV_CACHE_DIR=.data/uv-cache uv sync --extra test
npm --prefix web ci --cache .data/npm-cache
cp .env.example .env
```

Use `SANDBOX_HARNESS_BACKEND=fake` only for local API/UI development. Production
must use `opensandbox`; the code deliberately has no host-execution fallback.
Keep credentials in ignored environment files, never in source or test fixtures.

## Common commands

```bash
.venv/bin/pytest -q tests/test_store.py tests/test_broker.py
npm --prefix web run build
.venv/bin/uvicorn sandbox_harness.app:create_app --factory --reload --port 22766
```

The full Python suite is `.venv/bin/pytest`. `pyproject.toml` redirects pytest
temporary and cache data into `.data`. Browser scripts use the built frontend
and a running service; invoke them with `NODE_PATH=web/node_modules` when Node
module resolution needs the project dependency directory.

## Before editing

1. Run `git status --short` and preserve unrelated work.
2. Locate the current implementation with `rg`; do not infer it from an old
   report.
3. Read the relevant development guide from the index.
4. Identify the smallest focused tests that exercise the affected contract.

## Completion

- Source, tests and documentation agree.
- Focused tests pass; broader checks match the blast radius.
- No generated data, credentials or local environment files are tracked.
- The handoff distinguishes code written, tests run and code actually deployed.
