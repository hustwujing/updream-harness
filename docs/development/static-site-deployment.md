# Static Site Deployment

## Applies to

Read this when changing `sandbox-site`, deployment drafts/jobs, Site Worker,
published-site routing, versions, rollback or the browser deployment cards.

## Boundary

Harness builds a complete static directory under `/workspace/outputs` and calls
`sandbox-site prepare`. The CLI validates and uploads a deterministic tar
through the Turn-scoped `/v1/sites` capability. It has no database, Redis or
object-store credential. Preparation creates a confirmation draft and performs
no public side effect.

As a deterministic fallback, Broker detects each completed output directory
containing `index.html` and creates the same confirmation draft after Artifact
publication. The bundle digest deduplicates this with an earlier CLI draft.
This keeps deployment discoverable when a Harness produces a valid site but
omits `sandbox-site prepare`; it still never publishes without user confirmation.
Failed/partial workspace preservation does not create a deployment draft.

The browser confirms a draft. API creates a durable `site_deploy_job`; the
independent Site Worker validates the archive again, writes every file to the
content-addressed store and atomically activates a new immutable version. A
Sandbox process or private port is a preview, never a deployment.

Gateway request bodies, Worker archive reads and tar members are staged as
bounded files under `data_root/tmp/site-stages`; neither service may materialize
the whole archive plus expanded site in memory. Stages are removed on success,
validation failure and cancellation, and dead-process stage directories are
reaped on the next validation.

## Protected contracts

- Input must contain `index.html`, use only regular files and stay within file,
  count, archive and expanded-size limits. Reject links, duplicate paths,
  absolute paths, traversal and special files at CLI and Worker boundaries.
- Site Job ownership, confirmation and management are owner-scoped. Runtime
  status is additionally Thread-scoped by its capability claim.
- Every claimed Site Job increments a generation and heartbeats its lease over
  archive read, validation, object verification/repair and activation. Every
  Worker mutation requires owner plus generation; heartbeat rejection or error
  cancels the stale business task.
- Deterministic archive/manifest defects are terminal and actionable. Storage,
  database and service failures use bounded exponential retry on the same
  immutable draft. Exhaustion exposes the error and an owner-scoped continue
  action; it never asks the user to rebuild the draft.
- Object writes are content-addressed. Before activation, the Worker reads and
  identity-checks every public file and repairs a missing/corrupt object from
  the validated staged archive. Only the database activation transaction makes
  a version public, so success means every file is immediately readable.
- Draft idempotency stores a digest of the complete frozen request. Database
  locks serialize the idempotency key and globally unique slug allocation;
  identical concurrent calls replay one draft while different content is a
  semantic conflict.
- Public HTML uses a CSP sandbox without `allow-same-origin`. Generated scripts
  cannot inherit the main application's authenticated origin. Published pages
  receive no application Cookie.
- The serving layer prepends an isolated in-memory `localStorage` and
  `sessionStorage` compatibility shim before generated scripts. This prevents
  a direct Storage API read from aborting all page initialization while keeping
  the page on an opaque origin. It intentionally does not promise persistence
  across reloads; durable browser storage requires a separate site origin or a
  bounded trusted-shell bridge and must never be implemented by adding
  `allow-same-origin` on the application origin.
- Versions are immutable. Updating creates a version; rollback changes only the
  active version pointer. Thread deletion is rejected while a published site is
  active.
- Static sites need no server process or credential. Dynamic backends are a
  separate product capability and must not be smuggled through this protocol.
- An immutable `index.html` Artifact opens through its owner-scoped directory
  preview route in a CSP-sandboxed iframe. Do not render it as source text or
  let its script inherit the main application origin.
- Runtime instructions must state that `apply_patch` is rooted at its current
  working directory: build under `/workspace/project/work`, verify there, then
  copy regular deliverables to `/workspace/outputs`. Do not weaken the helper's
  path boundary to accommodate generated output.

## Completion

Run `tests/test_site_deployment.py`, `tests/test_site_service.py`,
`tests/test_gateway.py`, `tests/test_site_durability_review12.py`, the frontend build and
`tests/browser_site_deployment.cjs`. A release also requires an immutable
Runtime image containing `sandbox-site`, the independent systemd unit, a real
OpenSandbox prepare call, user confirmation, Worker completion, browser
interaction, restart recovery, rollback, downline and cleanup evidence.
