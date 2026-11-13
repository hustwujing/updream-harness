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

## Protected contracts

- Input must contain `index.html`, use only regular files and stay within file,
  count, archive and expanded-size limits. Reject links, duplicate paths,
  absolute paths, traversal and special files at CLI and Worker boundaries.
- Site Job ownership, confirmation and management are owner-scoped. Runtime
  status is additionally Thread-scoped by its capability claim.
- Job leases make API, Site Worker and browser restarts independent. Object
  writes are content-addressed; only the database activation transaction makes
  a version public.
- Public HTML uses a CSP sandbox without `allow-same-origin`. Generated scripts
  cannot inherit the main application's authenticated origin. Published pages
  receive no application Cookie.
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
`tests/test_gateway.py`, the frontend build and
`tests/browser_site_deployment.cjs`. A release also requires an immutable
Runtime image containing `sandbox-site`, the independent systemd unit, a real
OpenSandbox prepare call, user confirmation, Worker completion, browser
interaction, restart recovery, rollback, downline and cleanup evidence.
