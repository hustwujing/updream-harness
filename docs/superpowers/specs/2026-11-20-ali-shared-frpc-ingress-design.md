# Ali Shared FRPC Ingress Design

## Intent

Expose the independent Ali deployment at
`/sandbox-harness-platform-parallel-ali/` through the existing public
`120.48.75.168:8090` FRPC endpoint without changing the existing
`/sandbox-harness/` or `/sandbox-harness-platform-parallel/` applications.

## Design

FRPC continues to map public TCP port `8090` to the shared loopback PROXY v2
listener `127.0.0.1:18090`. A versioned location-only Nginx snippet adds the
Ali prefix to that server and proxies it only to the independent Ali API at
`127.0.0.1:24767`.

The versioned snippet and all Nginx logs, caches and deployment state stay under
`/data2/wujing03/sandbox-harness-platform-parallel-ali`. The existing root
Nginx configuration receives only one include line for the versioned snippet.
Nginx is syntax-checked before reload.

The route derives client addresses from PROXY Protocol, overwrites the ingress
marker with `frpc`, preserves the external prefix, and supports uploads,
streaming and WebSockets. It never exposes the Ali PostgreSQL, Redis or Gateway
ports.

## Isolation

- Public prefix: `/sandbox-harness-platform-parallel-ali/`.
- Ali API: `127.0.0.1:24767`.
- Existing public prefixes and upstreams remain byte-for-byte unchanged.
- Mutable deployment state remains below the Ali checkout `.data` directory.
- The root partition stores no Ali logs, caches, databases or build artifacts.

## Alternatives

- A dedicated public port avoids changing the shared listener but was rejected
