# Gateway, Security and Providers

## Applies to

Read this for capabilities, upstream routing, model discovery, Provider routes,
public networking, budgets or credentials.

## Capability boundary

`CapabilityCodec` signs versioned claims containing Thread, Turn, sandbox,
Harness, model, route prefixes, expiry, request limits, public-request limit and
token budget. Gateway verifies the route prefix on every request. The token is
written under `/run/harness`, excluded from checkpoints and revoked during
cleanup. Active model and public-proxy connections are closed on revocation.

Job grants remain subsets of their Turn/Group parent. `GrantAuthority.issue_job`
samples time once for remaining-lifetime validation and signing. An explicit
positive TTL must fit within that remaining lifetime; `ttl_seconds=None`
inherits the parent's deadline. `JobGrantFactory` uses inheritance so crossing
a clock second between parent and child issuance cannot reject an otherwise
valid grant. Expired parents and every scope/budget expansion remain rejected.
See `tests/test_grants.py` and the Child/Reviewer factory clock regressions in
`tests/test_harness_job.py`.

The current Gateway authenticates possession of that signed token. It records
the signed `sandbox_id` but does not independently attest the caller's network
identity against OpenSandbox. Do not describe the field as source-sandbox
verification. Protect the token file, keep its lifetime scoped to the Turn and
revoke it before sandbox reuse/deletion.

Never send upstream credentials into a sandbox. `ModelRoutes` stores only a base
URL and the name of a host environment variable. Gateway resolves that variable
when forwarding the request.

Use [Gateway capability guidance](add-gateway-capability.md) when the new route
is not an existing model protocol. A generic route prefix is not a substitute
for operation-specific ownership, budget and idempotency rules.

## Model usage and compaction

Do not treat model protocols as sharing one usage shape. Normalize every model
response into ordinary input, cache-read input, cache-write input and output:

- Responses/OpenAI-style usage reports total `input_tokens` and nested cached
  details;
- Claude reports ordinary `input_tokens` separately from
  `cache_read_input_tokens` and `cache_creation_input_tokens`;
- DeepSeek may use either Responses cached details or prompt cache hit/miss.

Logical input is the sum of all input dimensions. The durable Turn budget uses
configured cache-read/write policy percentages; these are not a pricing claim.
Missing usage consumes the full reservation. Preflight reserves requested
output only because cache outcome is unknowable before the response, while the
full remaining reservation still fences concurrent calls.

Compaction remains Harness-native. Codex freezes an explicit local auto-compact
threshold and marks remote compaction unsupported because the current upstream
returns 502 for `/responses/compact`. Claude Code owns its automatic
compaction. DeepSeek owns its Session state and must not receive unsupported
Responses `context_management` fields.

Model ingress uses `gateway_model_request_max_bytes` (64 MiB default/maximum)
independently from general capability and public HTTP request limits (16 MiB
defaults). Both declared size and streamed bytes are bounded; authentication,
idle/total timeouts and token budgets still apply. Do not rewrite image content
to fit the transport budget.

After capability and model-route validation, generation/compaction ingress
starts and terminalizes the same durable model activity as an upstream request,
including malformed bodies, size rejection, timeout and cancellation. Oversize
ingress uses `model_request_too_large` with no upstream call/token reservation.
Broker restores local budget/size errors only from the latest Gateway-origin
model activity and a corresponding ambiguous native failure. A later pending,
successful or upstream-failed request fences older local errors; native steps
and Child timeline data cannot supply this authority. Never rewrite a genuine
upstream 429/413. Tests: `test_gateway_model_ingress.py`,
`test_model_activity.py`, `test_broker.py`.

## Model routes and discovery

- Defaults come from `upstream_{harness}` plus the corresponding
  `SANDBOX_HARNESS_UPSTREAM_*_KEY`.
- Every non-empty default and override is canonicalized and validated at
  startup: HTTP(S) origin/path only, no userinfo, query or fragment. Empty
  defaults are explicitly unconfigured, not temporarily busy.
- `model_routes_json` overrides an exact `harness/model` pair.
- Discovery fetches each default `/models` endpoint and filters IDs by native
  protocol family.
- Explicit route/default models are protected during a successful inventory
  replacement. The previous full inventory is retained only as stale fallback
  when discovery fails, so removed models leave and new models can enter a full
  capped catalog.
- Inventory is capped per Harness before probes. Exact configured routes and
  current/default models have deterministic priority; truncation is visible in
  inventory status.
- Health probes use the same resolved route and protocol shape as execution.
  Busy/transient states remain selectable; explicit unsupported/auth failures
  become unavailable through the health reducer.
- Catalog and health snapshots are versioned under `data_root` and never freeze
  a different choice into an already created Turn.
- Snapshot/refresh parses and probes a complete candidate before one atomic
  catalog swap. Invalid data, probe exceptions or snapshot-write failure leave
  the live catalog unchanged. Snapshot metadata restores counts, last success,
  last attempt and per-Harness stale/error state after restart.
- HTTP 404/405 and stable endpoint/protocol rejection are
  `unavailable/model_route_missing`; 429, 5xx and timeouts remain selectable
  transient busy evidence. The browser retains choices while showing stale
  inventory/API state and clears it after recovery.

## Public HTTP/S proxy

Runtime injects the Turn token into `HTTP_PROXY` and `HTTPS_PROXY`. The public
proxy accepts only ports 80/443, resolves destinations itself, rejects every
non-public answer, then applies domain/IP/CIDR allow and deny rules. Deny rules
take precedence. Restricted Turns use the configured allowlist. Public Turns
are issued only for authenticated-account submissions and skip the allowlist while
retaining deny rules, public-address validation, port limits, budgets and revocation.

The same path is used by curl, Requests, the Chromium wrapper, arbitrary
Playwright scripts and `sandbox-browser-screenshot`. Browser loopback bridges
only supply proxy authorization. Raw sandbox egress remains blocked by the host
firewall.

HTTPS CONNECT exposes only host and port; path filtering is not implemented and
must not be claimed. A domain rule alone does not authorize private resolved
addresses. Redirects and subresources create new proxy checks.

## Security review

- Route scope matches the active Harness and model.
- Budgets are consumed atomically in Redis and fail closed when revoked or
  exhausted.
- User-controlled URLs, headers and exceptions are not copied into logs.
- Direct IP, mixed DNS answers, wildcard boundaries and private-address results
  are tested.
- Model calls use `trust_env=False`; they do not accidentally inherit the
  sandbox public proxy.

## Required checks

`test_capability.py`, `test_gateway.py`, `test_public_proxy.py`,
`test_model_activity.py`, `test_model_health.py`, `test_catalog.py` and
`test_config.py`. Provider changes also require a real minimal request through
the deployed Gateway, reported separately from protocol fixtures.
