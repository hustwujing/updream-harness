# Shared FRPC Ingress Design

## Intent

Expose Parallel at `/sandbox-harness-platform-parallel/` through the existing
public `120.48.75.168:8090` FRPC endpoint without merging application services,
authentication state or storage.

## Design

FRPC continues to map public TCP port `8090` to the shared loopback PROXY v2
listener `127.0.0.1:18090`. A versioned location-only Nginx snippet adds the
Parallel prefix to that existing server and proxies it to the independent API
at `127.0.0.1:24766`.

The route overwrites client forwarding headers with PROXY Protocol facts, sets
`X-Updream-Ingress: frpc`, retains the external prefix, supports uploads,
streaming and WebSockets, and does not expose Gateway or database ports. The
existing `/sandbox-harness/` route remains unchanged. Nginx is reloaded only
after syntax validation.

## Alternatives

- A new public port mapped to the existing dedicated listener `18091` preserves
  stronger ingress configuration isolation but does not satisfy the requested
  `:8090/sandbox-harness-platform-parallel/` URL.
- Replacing the shared `18090` server with the Parallel standalone config would
  break the original application and is rejected.

