# Provider-selected Gateway Listener Report

## Result

The independent Ali checkout can deploy the existing Gateway protocol for
either OpenSandbox or Aliyun. Exactly one provider-specific Gateway process is
active at a time:

- OpenSandbox: `172.31.0.1:29882/29883`
- Aliyun: `10.194.49.8:29882/29883`

No wildcard listener is used. Production remains configured for OpenSandbox.

Implementation commits:

- `168a97b` introduced provider-specific service entry points.
- `e619ad7` corrected the lifecycle to select one listener by backend after
  review found that two simultaneous processes would break connection revoke.

## Design And Development

- `run-service.sh` maps explicit provider components to the existing Gateway
  executable and overrides only the bind host and sandbox-visible URL.
- The same runner selects `SANDBOX_HARNESS_GATEWAY_URL` for every API, Job,
  Inspector and worker process, so capability revoke and cleanup target the
  selected provider listener rather than a stale OpenSandbox address.
- `start.sh` reads the frozen deployment environment and starts only the unit
  selected by `SANDBOX_HARNESS_BACKEND`.
- An active unit for the other provider causes a fail-closed error requiring a
  full stop before provider switch.
- Aliyun startup additionally requires exact configured Gateway and proxy
  routes; an omitted or unexpected route is rejected.
- `stop.sh` handles both provider units and the legacy unit name.
- `status.sh` reports selected backend, exact unit states and exact host/port
  sockets rather than accepting any listener on the same port.
- The existing OpenSandbox bridge firewall rule is unchanged. Aliyun is not
  exposed unless Aliyun is the selected backend; its VPC security group remains
  the ingress boundary.

## Template And Network Evidence

Template `tf6ud6ouc1ynsmivnjkq` was corrected and reached `ready` with:

- runtime VPC `vpc-uf6vyypwp2gwxeyz9zg8q`;
- vSwitch `vsw-uf61eit3xy63ge7rzpmdn`;
- security group `sg-uf68z63kpqcjike1dx9f`;
- matching ACR EE registry-pull network;
- start command `/usr/local/bin/sandbox-harness-runtime`;
- readiness command `/usr/bin/curl -fsS http://127.0.0.1:8080/health >/dev/null`;
- 8 vCPU, 8192 MB memory, 30720 MB disk, `linux-amd64`, Internet access enabled.

An Aliyun sandbox created without dynamic VPC metadata proved that the template
configuration itself works:

- Runtime `/health`: `bootstrap`;
- host-private Gateway `/health`: `ready=true`;
- host-private proxy TCP: connected;
- temporary sandbox: deleted.

The temporary Ali listener was stopped after verification. OpenSandbox remained
healthy and is the only active production listener.

## Deployment Asset Recovery

The first Gateway migration exposed a stale schema-v1 Profile Catalog that the
long-running old process had not reloaded. The new process correctly failed
before binding. The deployment Catalog was converted atomically to schema v2,
validated with `ProfileCatalogPayload`, installed content-addressed under
`.data/profile-catalogs`, and the environment digest/path was updated with a
recoverable backup. OpenSandbox Gateway was restored before further work.

## Tests

- Ali deployment contract tests: `15 passed`.
- Shell syntax checks: passed for `run-service.sh`, `start.sh`, `stop.sh`, and
  `status.sh`.
- Full Python suite: `1073 passed, 5 skipped, 1 existing deprecation warning`.
- OpenSandbox deployment health: ready, execution backend available, warm pool
  ready, no active Turns or reconciliations.
- Aliyun template, Runtime, Gateway and proxy canary: passed.

## Review

The initial two-process design was rejected during review because active stream
and CONNECT state are process-local and could not be revoked atomically, and an
always-on Ali listener would violate the fail-closed route boundary. The final
provider-selected single-process lifecycle closes both issues. A follow-up
review also found and closed a stale host-side revoke URL in Ali mode by moving
provider URL selection into the shared service runner.

## Rollback

For OpenSandbox, stop the provider-specific unit and start the legacy Gateway
entry point using the same environment. The schema-v2 Catalog is additive to
the deployment flow and has a timestamped configuration backup under
`.data/config`. No original project service, port or root-filesystem path was
modified.
