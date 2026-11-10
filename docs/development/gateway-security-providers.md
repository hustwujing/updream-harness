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

## Model routes and discovery

- Defaults come from `upstream_{harness}` plus the corresponding
  `SANDBOX_HARNESS_UPSTREAM_*_KEY`.
- `model_routes_json` overrides an exact `harness/model` pair.
- Discovery fetches each default `/models` endpoint and filters IDs by native
  protocol family.
- Explicit route models remain candidates when discovery fails.
- Health probes use the same resolved route and protocol shape as execution.
  Busy/transient states remain selectable; explicit unsupported/auth failures
  become unavailable through the health reducer.
- Catalog and health snapshots are versioned under `data_root` and never freeze
  a different choice into an already created Turn.

## Public HTTP/S proxy

Runtime injects the Turn token into `HTTP_PROXY` and `HTTPS_PROXY`. The public
proxy accepts only ports 80/443, resolves destinations itself, rejects every
non-public answer, then applies domain/IP/CIDR allow and deny rules. Deny rules
take precedence. A non-empty allowlist changes the default to deny.

The same path is used by Requests and `sandbox-browser-screenshot`. The helper's
loopback bridge only supplies Chromium's proxy authorization. Raw sandbox
egress remains blocked by the host firewall.

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
