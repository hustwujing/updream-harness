# Deployment

The product has its own API, Gateway, PostgreSQL cluster and data namespace. It reuses host infrastructure programs where they do not create project data on the root filesystem.

| Resource | Process or endpoint | This product's isolated space |
| --- | --- | --- |
| PostgreSQL | `sandbox-harness-postgresql.service`, `127.0.0.1:25432` | `.data/postgresql`, database and role `sandbox_harness` |
| Media Worker | `sandbox-harness-media-worker.service` | Provider submission, polling, lease recovery and media artifact commit |
| Site Worker | `sandbox-harness-site-worker.service` | static bundle validation, immutable upload, version activation and recovery |
| Redis | `127.0.0.1:6379` | key prefix `sandbox-harness:` |
| Docker/runsc | `opensandbox-docker.service` | image repository `sandbox-harness/*` |
| image registry | `127.0.0.1:25001` | repository prefix `sandbox-harness/` |
| OpenSandbox | `sandbox-agent-opensandbox.service`, `127.0.0.1:28090` | metadata `sandbox-harness.io/namespace=sandbox-harness` |
| BOSS/S3 internal | `http://shjd-boss.bilibili.co`, bucket `updream` | object prefix `sandbox-harness/` |
| BOSS reference media | `https://boss-shjd.biliapi.net`, bucket `updream` | immutable prefix `aniforge/image|video|audio/` |
| local state | host filesystem | `/data2/wujing03/sandbox-harness-platform/.data` only |
| web/API | this product | `127.0.0.1:22766`, route `/sandbox-harness/` at the reverse proxy |
| capability Gateway | this product | sandbox bridge `172.31.0.1:28600` |
| public HTTP/S proxy | this product | sandbox bridge `172.31.0.1:28601` |
| FRPC web ingress | existing FRPS/FRPC | PROXY v2 to nginx `127.0.0.1:18090`, marker `frpc` |

The deployment must not start another Redis, Docker daemon, registry, object store or OpenSandbox server. It must not change the configuration, storage directory or lifecycle of those shared dependencies. The PostgreSQL process is project-owned so its data and logs never use the root filesystem.

## Storage Policy

Project dependencies, caches, database files, logs, temporary files and build output must remain under `/data2/wujing03/sandbox-harness-platform`. The service units mount an in-memory private `/tmp` and set `TMPDIR`, `XDG_CACHE_HOME`, Python, uv, npm and Playwright cache variables to `.data`. The environment file is `.data/config/platform.env`; do not create a second copy under `/etc` or `/root`. Read-only operating-system binaries and libraries under `/usr` are host programs, not project-generated state.

## Isolation Rules

- Every OpenSandbox create request includes the product namespace. Reconciliation and cleanup query both namespace and Turn identity.
- Runtime images use immutable digests under `127.0.0.1:25001/sandbox-harness/*`.
- Every Turn freezes its Runtime image, Runtime Code Bundle and Content Bundle digests.
  Queued Turns keep those versions across later releases; recovery reinjects
  the recorded Bundle and never silently upgrades an active execution.
- The read-only Runtime Skill bundle is digest-verified at build time and materialized into native Harness roots after fresh configuration or checkpoint restore.
- PostgreSQL connects only to the dedicated cluster as the `sandbox_harness` role and database.
- Every Redis key starts with the configured prefix. Lua scripts and scans must include that prefix.
- Any authenticated-account Turn submission freezes `network_access=public`; anonymous submissions remain restricted. Both modes still reject private destinations and raw egress.
- Every checkpoint, attachment and artifact key starts with the configured BOSS/S3 prefix. Sandboxes never receive object-store credentials.
- Model reference media uses the stable HTTPS BOSS URL only. The internal BOSS endpoint and presigned PUT URL must never enter a Provider request. Seedance receives only an Active Ark `asset://` URI.
- API and Gateway secrets live in `.data/config/platform.env` with restricted permissions.
- The shared OpenSandbox Docker network is internal and has no public NAT. Its host firewall permits `172.31.0.0/24` to reach only the existing runtime range plus this product's `172.31.0.1:28600-28601`; port 28601 proxies public HTTP/S only and rejects private destinations after DNS resolution. OpenSandbox rejects per-sandbox `networkPolicy` on this user-defined network, so create requests do not claim unsupported enforcement.

## Installation Order

1. Create `.data/config/platform.env` from `.env.example`; keep every cache and temporary path under `.data`.
2. Verify the shared services are healthy; do not restart them.
3. Run `deploy/install-units.sh`, then `deploy/init-postgresql.sh` to initialize the fresh project database on `/data2`.
4. Place the ignored, checksum-pinned draw.io package under `runtime/vendor/`, verify the Skill manifest, then build the three Runtime images through the shared Docker socket and push them under the product registry namespace.
5. Record immutable image digests in the project environment file and start API/Gateway.
6. Run a real OpenSandbox smoke Turn and verify its metadata namespace, image digest, event stream, checkpoint and artifact prefix.

The API uses 30-second Turn leases, 10-second heartbeats, a 15-second expired-lease scan and a 60-second orphan scan by default. Normal API shutdown preserves currently executing sandboxes so the replacement process can reattach them. Do not add an `ExecStop` command that deletes namespace sandboxes.

Local UI tests may use SQLite, local content-addressed objects and the explicit fake lifecycle backend. Those results do not count as a deployment or real OpenSandbox validation.

Required reference-media settings are:

```dotenv
SANDBOX_HARNESS_OBJECT_ENDPOINT=http://shjd-boss.bilibili.co
SANDBOX_HARNESS_OBJECT_BUCKET=updream
SANDBOX_HARNESS_OBJECT_REGION=updream
SANDBOX_HARNESS_OBJECT_PUBLIC_ENDPOINT=https://boss-shjd.biliapi.net
SANDBOX_HARNESS_MEDIA_REFERENCE_PREFIX=aniforge
SANDBOX_HARNESS_ARK_ACCESS_KEY=<Ark control-plane AK>
SANDBOX_HARNESS_ARK_SECRET_KEY=<Ark control-plane SK>
SANDBOX_HARNESS_ARK_PROJECT_NAME=<matching Ark project>
SANDBOX_HARNESS_ARK_ASSET_GROUP_ID=<AIGC asset group>
```

Keep secrets only in `.data/config/platform.env`. The Ark Bearer API key,
BOSS AK/SK and Ark control-plane AK/SK are distinct credential roles.

Sandbox Inspector limits are host configuration, not Sandbox credentials:

```dotenv
SANDBOX_HARNESS_SANDBOX_CONTROL_TTL_SECONDS=3600
SANDBOX_HARNESS_SANDBOX_PORT_GRANT_TTL_SECONDS=3600
SANDBOX_HARNESS_SANDBOX_TERMINAL_TICKET_SECONDS=60
SANDBOX_HARNESS_SANDBOX_INSPECT_MAX_FILE_BYTES=2097152
SANDBOX_HARNESS_SANDBOX_USERNAME=updream
```

The username is a Runtime image build argument. Rebuild all Harness images after
changing it. UID/GID remain `10001`; do not use this setting to change ownership.

Terminal and port preview require OpenSandbox/Execd 1.2 PTY and server-proxy
WebSocket support. Nginx must preserve WebSocket upgrade headers for `/api/`;
the application remains the owner/authentication boundary.

The foundation image tag includes the SHA-256 prefix of `runtime/foundation.Dockerfile`. `runtime/toolchain.lock.json` is verified while building the common runtime base, and the three final Harness images must expose the same locked toolchain before their immutable registry digests are deployed.

`deploy/record-release.py` publishes both signed Bundles and sets their directory
and digest variables. Retain old files while a queued or running Turn references
them. Code activation accepts only declared Runtime/shared Python modules and
CLI packages compatible with the frozen dependency lock. Content activation
accepts ordinary files under `skills/` plus the two instruction files. Both
reject links, devices, path traversal, unknown files, oversize and hash or
signature mismatches.

Before restarting API/Gateway after adding source files or building the frontend,
run `bash deploy/check-service-access.sh`. It grants the service user read/traverse
access only to `src` and `web/dist`, then imports both services and reads every web
asset as that user. Root-only unit tests do not detect unreadable new modules.

Install `deploy/nginx.conf` for the direct route and
`deploy/nginx-frpc.conf` as a separate loopback PROXY Protocol listener. FRPC
must target port 18090 with `transport.proxyProtocolVersion = "v2"`; do not send
PROXY frames to the shared port 80 listener.
