# Changing Public Network Policy

## Applies to

Required for proxy allow/deny rules, destination resolution, allowed ports,
browser proxy behavior or sandbox firewall access.

## Current policy path

Runtime configures authenticated `HTTP_PROXY`/`HTTPS_PROXY` values using the
current Turn capability. Requests and the supported browser helper enter
Gateway port 28601. Host firewall rules allow the internal sandbox network to
reach only the Gateway/Runtime ranges and drop other traffic.

Gateway normalizes the requested host, resolves it itself, rejects the request
if any answer is non-public, then evaluates the Turn's frozen network access:

1. exact/wildcard domain deny;
2. resolved IP/CIDR deny;
3. for `restricted`, require an exact/wildcard domain allow or all resolved
   addresses allowed by IP/CIDR when the allowlist is non-empty;
4. for `public`, skip the allowlist;
5. connect to the already validated numeric address.

Deny takes precedence. `*.example.com` does not match the apex `example.com`;
configure both when both are intended. `*` matches all destinations. Only ports
80 and 443 are supported. HTTPS paths are not visible.

## Safety requirements

- Normalize case, trailing dot, IDNA and IP spelling before matching.
- Reject schemes, credentials, paths and malformed wildcard/IP rules at startup.
- Check every DNS answer to prevent mixed public/private resolution bypass.
- Apply CIDR deny even when the domain is allowed.
- Recheck redirects and browser subresources as separate proxy requests.
- Direct IP requests must not bypass IP/CIDR policy.
- Blocked requests return `destination_denied`; invalid/private destinations
  retain `destination_rejected`.
- Do not log the requested full URL or capability token.

## Configuration and rollout

Lists are JSON environment values:

```dotenv
SANDBOX_HARNESS_PUBLIC_PROXY_ALLOWLIST='["example.com","*.example.com"]'
SANDBOX_HARNESS_PUBLIC_PROXY_DENYLIST='["blocked.example.com","203.0.113.0/24"]'
```

Validate configuration before restart, confirm no active model/public streams
will be interrupted, restart Gateway only, inspect `/health` policy counts and
test an FRPC public destination plus a direct restricted rejection with
short-lived capabilities.

Run `test_public_proxy.py`, `test_gateway.py`, `test_config.py`, capability tests
and the Runtime/browser proxy contract tests.
