# Development Rules

This repository is an independent product. Before changing code, inspect the
working tree and read the task-specific guidance in
[docs/development/README.md](docs/development/README.md). Preserve concurrent
work and use the implementation, tests and deployed configuration to verify
claims made by documentation.

## Product Boundary

- Every user message is executed by the selected Codex, Claude or DeepSeek
  Harness inside the configured SandboxBackend. The host does not plan, review
  or reinterpret the task.
- The host owns browser identity, Thread and Turn serialization, leases,
  sandbox lifecycle, short-lived capabilities, event transport, checkpoints,
  artifacts and hard resource limits.
- Sandbox provider failure never falls back to a host model or local Agent.
- Sandboxes receive no long-lived model, Provider, database, Redis or object
  storage credential.
- Do not import source, database models, deployment scripts, runtime bundles or
  frontend assets from the predecessor repositories.

## Correctness Invariants

- A Thread has at most one active Turn. Queued followers may run only after the
  active pointer advances.
- Turn state, lease owner and recovery generation fence every authoritative
  write made by a worker.
- Once Harness execution starts, recovery reattaches the existing Runtime
  execution; it never blindly resends a message with unknown side effects.
- Persist sandbox allocation intent separately from Runtime acceptance. Unknown
  creation outcomes retain ownership until authoritative reconciliation.
- Used sandboxes may be retained only for the same Thread and are never returned
  to the pristine warm pool.
- A non-RELEASED durable sandbox ownership handle protects its provider object
  from startup orphan deletion. Provider-confirmed absence must converge that
  ownership to RELEASED before a replacement allocation is created.
- Persist the intended final state before cleanup. Revoke capability access,
  quiesce Runtime work and confirm sandbox deletion before releasing capacity.
- Accepted external work keeps its original generation identity across Worker
  takeover; cancellation and cleanup target that accepted identity while the
  current lease generation fences the caller.
- Failed durable cleanup persists bounded retry eligibility and retained
  ownership. A Worker restart must not turn an unresolved dependency into a
  hot retry loop or release its capacity.
- PostgreSQL is authoritative. Redis is a bounded notification/replay aid.
- Checkpoint commit precedes successful or waiting publication. Artifact and
  checkpoint writes remain lease-fenced and content-addressed.
- Stream events are ordered by `(turn_id, sequence)` and Runtime receipts are
  idempotent by `(execution_id, source_sequence)`.
- Public text and tool-output fragments use the shared Runtime stream buffer.
  Redis may carry bounded live deltas, but PostgreSQL advances the committed
  Runtime cursor only with the aggregated transcript/activity transaction.
  ACK Runtime only after that transaction succeeds; control and terminal
  boundaries flush pending fragments first. Primary live deltas bind to the
  exact durable transcript Item; execution identity alone is insufficient.
- Files leave a sandbox only through validated attachments, managed project
  files, checkpoints or `/workspace/outputs` artifacts.

## Module Boundaries

- `modules/**/module.json` is the machine-readable ownership, entry-point,
  dependency, resource and external-input contract for each release module.
- Every module directory has its own `README.md` and `AGENTS.md`. Read that
  module guidance before changing owned source, packaging or deployment.
- Service implementations may import declared common libraries, never another
  service implementation. Common code may not import a service.
- Cross-domain state changes use the durable handoff/lifecycle protocols. A
  service may open only the database authorities declared for it.
- Changes to shared contracts require consumer tests in every affected module;
  changes to one module release must not silently change another descriptor.

## Development Discipline

- Treat `src/sandbox_harness/common/contracts/`, the Accounts/Core/Media/Sites
  repositories, API routes, module manifests and tests as the source of truth.
  If documentation disagrees, verify behavior and fix it in the same change.
- Keep generated data, dependencies, caches, logs, databases and temporary
  files under `.data` or another explicit `/data/wujing02` path.
- Provision every long-lived writable directory for the service account and
  revalidate its boundary, type, owner and write access whenever work writes
  through it. Disposable staging may be recreated; ownership mismatch fails
  closed.
- Reuse the existing ownership, isolation, cancellation, recovery, event and
  observability mechanisms. A feature is incomplete if it bypasses them.
- Never persist native Harness text or tool-output fragments one transaction per
  chunk. Primary, Child, Reviewer and future Harness consumers must reuse
  `common/events/runtime_stream.py`, with configurable live and durable flush
  intervals and Runtime journal replay.
- Fix shared contracts without case-, operation- or sandbox-provider-specific
  state-machine branches. Admission saturation must not block cleanup or
  observation of already accepted executions.
- Every asynchronous Adapter implements the shared preflight, submit, status,
  reconcile, cancel, collect and cleanup lifecycle. Provider/model validation
  belongs to its registered contract under `common/providers/`; never add
  Provider rules or an unregistered fallback to `MediaCatalog` or JobWorker.
- Program decisions use stable error codes. A user- or model-visible external
  reason must be created by the bounded public error envelope and projected
  identically through API, execution graph and continuation. Raw diagnostic
  detail remains internal.
- Never log prompts, generated content, commands, file contents, credentials,
  arbitrary headers, raw exception messages or unfiltered URLs. New telemetry
  attributes require explicit allowlisting.
- Use focused tests while iterating. Before release, run the broader checks
  required by the change's blast radius. Report fake, real OpenSandbox, real
  Provider and browser evidence separately.
- After a defect, rerun the failed case and cases that cross the changed
  contract. Preserve unaffected successes with their actual revision; do not
  repeat unrelated real cases solely to manufacture a single-revision report.
- A local edit is not a deployment. A previous test report is not evidence for
  the current revision.

## Domestic Mirrors And Caches

- On this host, use an available domestic mirror for package, browser and other
  large dependency downloads. Keep every download cache under this checkout's
  `.data` directory; never fall back to a root-partition cache.
- Install Playwright browsers with
  `PLAYWRIGHT_DOWNLOAD_HOST=https://npmmirror.com/mirrors/playwright` and
  `PLAYWRIGHT_BROWSERS_PATH=$PWD/.data/playwright-browsers`. Do not download
  Playwright browsers from the default international CDN when the domestic
  mirror is available.
- Configure npm and Python package tools with a domestic registry or index for
  downloads while preserving the repository lockfile and hash checks.

## Documentation

Development instructions live under `docs/development/`. Keep this file as the
short routing and invariant entry point. New instructions must identify when
they apply, authoritative code entry points, protected contracts and completion
checks.
