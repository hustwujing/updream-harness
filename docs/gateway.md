# Capability Gateway

Each sandbox receives a signed, short-lived token bound to the Session, Turn, sandbox ID, Harness, model, network access, allowed route prefixes, request limit, token budget and expiration. The token is written to `/run/harness/capability-token`, excluded from checkpoints and revoked before cleanup.

The Gateway exposes model and Provider namespaces and streams upstream responses without placing long-lived credentials in the sandbox. It enforces route scope, request count, byte limits and expiration.

The same service hosts an HTTP/HTTPS proxy on the sandbox bridge. Runtime derives standard proxy authentication from the same dynamic Turn capability token and exposes it through `HTTP_PROXY` and `HTTPS_PROXY`. The proxy verifies expiry and Redis revocation and charges a separate public-request budget, so browsing does not consume model-call budget. It resolves every destination itself, permits ports 80 and 443, and rejects loopback, private, link-local, multicast, reserved and cloud-metadata addresses. Raw sandbox egress remains deny-by-default, so direct connections cannot bypass destination validation.

`SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST` and `SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST` are JSON arrays of exact domains, wildcard subdomains such as `*.example.com`, individual IPs, or CIDRs. The allowlist applies to `restricted` Turns. Authenticated FRPC submissions receive `public` access and may reach every otherwise-valid public destination. The denylist takes precedence in both modes. Every HTTP request and HTTPS CONNECT is checked again, so redirects cannot bypass the policy. HTTPS paths are encrypted and cannot be filtered without terminating TLS; policy therefore applies to hosts and resolved addresses, not URL paths.

curl, Python HTTP clients, the `chromium` wrapper, Playwright scripts and
`sandbox-browser-screenshot` all use the same authenticated proxy. Browser
bridges only add the current Turn's proxy authorization; every page, redirect
and subresource still traverses this Gateway. Raw sandbox egress has no public
route and cannot bypass the frozen Turn policy.

The Gateway records transport and usage facts. It does not approve tool calls, interpret prompts or judge output quality.

Model routes may override the default Harness upstream per exact `harness/model`. `SANDBOX_HARNESS_MODEL_ROUTES_JSON` stores only `base_url` and `credential_env`; the referenced environment variable stores the actual credential. Discovery, availability probes and execution all use the same resolver. Route configuration and persisted Turn records never contain the credential value.
