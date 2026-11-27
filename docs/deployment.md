# Deployment

The only supported deployment root is
`/data/wujing02/sandbox-harness-platform-parallel-ali`. All mutable state is
under its `.data` directory.

| Resource | Current identity |
| --- | --- |
| API | `127.0.0.1:24767` |
| PostgreSQL | `127.0.0.1:28433` |
| Databases | `sandbox_harness_ali_accounts`, `sandbox_harness_ali_core`, `sandbox_harness_ali_media`, `sandbox_harness_ali_sites` |
| Namespace | `sandbox-harness-platform-parallel-ali` |
| Gateway / proxy | `29882` / `29883` on the selected provider host |
| Public path | `/sandbox-harness-platform-parallel-ali/` |

## Services

`deploy/ali/start.sh` manages eight independent application services plus the
project PostgreSQL and network units:

- API
- sandbox-gateway
- JobWorker
- AllocationWorker
- ControlledWorker
- MediaWorker
- SiteWorker
- image-inference

Seven Python services resolve one immutable descriptor each from
`.data/service-current/`. image-inference resolves its external descriptor from
the same directory while referencing model files by verified digest without
copying them. Each service receives only the database credentials declared for
its role in `DatabaseSettings.SERVICE_DOMAINS`.

## Commands

```bash
cd /data/wujing02/sandbox-harness-platform-parallel-ali
deploy/ali/start.sh
deploy/ali/status.sh
deploy/ali/stop.sh
```

Select `SANDBOX_HARNESS_BACKEND=aliyun` or `opensandbox` in
`.data/config/platform.env`. A provider switch requires a full stop so only one
sandbox-gateway owns the provider ports.

`deploy/ali/init-postgresql.sh` initializes the cluster administrator against
the `postgres` maintenance database. `deploy/ali/provision-domain-databases.py`
creates the four non-privileged domain roles and databases. Application
services never receive the administrator URL.

## Ingress

- `deploy/nginx-ali.conf` is the direct ingress location.
- `deploy/nginx-frpc-ali-shared-location.conf` is the PROXY v2/FRPC location.

Both forward only to port 24767 and expose no database, Redis or Gateway port.

## Release

Build a service with `tools/service_release.py`, atomically select its manifest,
then restart only that service. Build image-inference with
`tools/image_inference_release.py`. Do not run application services from the
working tree.

Before release, verify focused tests, the full Python gate, frontend tests and
build, deployed health, the affected real cases and the 175-case catalog report.
After verification, remove unselected releases, old test data and unreferenced
bundles. Keep current PostgreSQL data, object data, selected descriptors and the
final report.
