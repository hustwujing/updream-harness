# Capability Gateway

Each sandbox receives a signed, short-lived token bound to the Session, Turn, sandbox ID, Harness, model, network access, allowed route prefixes, request limit, token budget and expiration. The token is written to `/run/harness/capability-token`, excluded from checkpoints and revoked before cleanup.

The Gateway exposes model and Provider namespaces and streams upstream responses without placing long-lived credentials in the sandbox. It enforces route scope, request count, byte limits and expiration.

The same service hosts an HTTP/HTTPS proxy on the sandbox bridge. Runtime derives standard proxy authentication from the same dynamic Turn capability token and exposes it through `HTTP_PROXY` and `HTTPS_PROXY`. The proxy verifies expiry and Redis revocation and charges a separate public-request budget, so browsing does not consume model-call budget. It resolves every destination itself, permits ports 80 and 443, and rejects loopback, private, link-local, multicast, reserved and cloud-metadata addresses. Raw sandbox egress remains deny-by-default, so direct connections cannot bypass destination validation.

`SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST` and `SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST` are JSON arrays of exact domains, wildcard subdomains such as `*.example.com`, individual IPs, or CIDRs. The allowlist applies to `restricted` Turns. Any submission with a valid account Session receives `public` access and may reach every otherwise-valid public destination; anonymous direct submissions remain restricted. The denylist takes precedence in both modes. Every HTTP request and HTTPS CONNECT is checked again, so redirects cannot bypass the policy. HTTPS paths are encrypted and cannot be filtered without terminating TLS; policy therefore applies to hosts and resolved addresses, not URL paths.

curl, Python HTTP clients, the `chromium` wrapper, Playwright scripts and
`sandbox-browser-screenshot` all use the same authenticated proxy. Browser
bridges only add the current Turn's proxy authorization; every page, redirect
and subresource still traverses this Gateway. Raw sandbox egress has no public
route and cannot bypass the frozen Turn policy.

The Gateway records transport and usage facts. It does not approve tool calls,
interpret prompts or judge output quality.

Capability security state is authoritative in PostgreSQL. The first use
registers the signed claim digest, expiry, request/public-request limits and
token budget. Every request atomically consumes the durable counter; revocation
creates or updates a durable nonce tombstone. Redis may cache revocation and is
used to wake and close live connections, but clearing Redis cannot restore a
revoked grant or reset either counter. If PostgreSQL freshness cannot be
established, authorization fails closed with
`capability_authority_unavailable`.

Model calls use a durable reservation ledger. Codex Responses uses
`max_output_tokens`; Claude Messages and DeepSeek/OpenAI-compatible Chat use
`max_tokens`. A request above the remaining signed budget is rejected before
submission. Preflight walks the bounded native request structure: ordinary
text uses a conservative UTF-8 estimate, while inline images use decoded image
dimensions instead of Base64 transport size. Malformed or unknown binary
structures fall back to the raw request byte ceiling. Provider-reported total
usage remains authoritative at settlement. An accepted request reserves the remaining grant budget so
concurrent calls cannot spend it twice. Provider-reported input plus output
usage settles the reservation idempotently; missing, malformed, or
out-of-range usage conservatively consumes the full reservation. Job grants
carry their parent nonce and share the parent's aggregate token ceiling.

Native Harnesses own conversation compaction. The strict model route surface is
Codex `/responses` and `/responses/compact`, Claude `/messages` and
`/messages/count_tokens`, and DeepSeek `/chat/completions`. Claude token counting
is authenticated and request-counted but creates no generation reservation or
user-visible model activity. Codex standalone compaction is reserved and settled
like generation without injecting a generation-only output limit. The Gateway
does not summarize, prune, reinterpret or discard native compaction records.

All non-streaming Gateway request bodies are bounded by Content-Length and
chunk accounting, with independent idle and total deadlines. Artifact emit is
the explicit streaming exception and has its own bounded staging contract.
The public proxy derives a canonical authority from the absolute URL, rejects
ambiguous/duplicate framing headers, replaces Host with that authority, and
streams bounded request bodies. Stable failures include `request_too_large`,
`request_body_timeout`, and `model_response_idle_timeout`. A model upstream
idle before the first byte returns 504; after streaming begins it terminates as
an explicit transport failure unless a native terminal frame was already
observed.

Model routes may override the default Harness upstream per exact `harness/model`. `SANDBOX_HARNESS_MODEL_ROUTES_JSON` stores only `base_url` and `credential_env`; the referenced environment variable stores the actual credential. Discovery, availability probes and execution all use the same resolver. Route configuration and persisted Turn records never contain the credential value.
