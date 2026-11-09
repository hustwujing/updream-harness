# Capability Gateway

Each sandbox receives a signed, short-lived token bound to the Session, Turn, sandbox ID, Harness, model, allowed route prefixes, request limit, token budget and expiration. The token is written to `/run/harness/capability-token`, excluded from checkpoints and revoked before cleanup.

The Gateway exposes model and Provider namespaces and streams upstream responses without placing long-lived credentials in the sandbox. It enforces route scope, request count, byte limits and expiration.

The same service hosts an HTTP/HTTPS proxy on the sandbox bridge. Runtime derives standard proxy authentication from the same dynamic Turn capability token and exposes it through `HTTP_PROXY` and `HTTPS_PROXY`. The proxy verifies expiry and Redis revocation and charges a separate public-request budget, so browsing does not consume model-call budget. It resolves every destination itself, permits ports 80 and 443, and rejects loopback, private, link-local, multicast, reserved and cloud-metadata addresses. Raw sandbox egress remains deny-by-default, so direct connections cannot bypass destination validation.

The Gateway records transport and usage facts. It does not approve tool calls, interpret prompts or judge output quality.
