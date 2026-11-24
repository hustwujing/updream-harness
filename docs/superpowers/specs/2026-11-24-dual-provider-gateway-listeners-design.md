# Dual-provider Gateway Listeners Design

## Intent

Expose the same signed Gateway and public-proxy protocol to OpenSandbox and
Aliyun sandboxes without wildcard listeners or provider-specific application
behavior. A deployment may use either backend, while both network entrances
remain independently observable during migration and cleanup.

## Constraints

- OpenSandbox keeps `172.31.0.1:29882/29883`.
- Aliyun uses `10.194.49.8:29882/29883`.
- Never bind `0.0.0.0` or an unspecified address.
- Both instances use the same database, Redis, signing/control keys, catalogs,
  capability rules and proxy policy.
- The existing OpenSandbox bridge firewall rule remains unchanged.
- Aliyun ingress is constrained by the configured VPC security group; the host
  listener binds only the verified private address.
- All runtime and service state remains under `/data2/wujing03`.

## Approaches

### Selected: two instances of the existing Gateway service

Run the same entry point twice with provider-specific bind-host overrides.
Each process owns its matching API and proxy sockets. This preserves the
existing Gateway implementation and isolates listener failures.

### Rejected: one process with multiple pre-bound uvicorn sockets

This saves one process but requires custom uvicorn lifecycle and multi-server
proxy management. It increases the regression surface without changing the
protocol or authorization model.

## Service Model

- `sandbox-harness-parallel-ali-gateway-opensandbox.service`
  binds `172.31.0.1:29882/29883`.
- `sandbox-harness-parallel-ali-gateway-aliyun.service`
  binds `10.194.49.8:29882/29883`.
- `run-service.sh` accepts `gateway-opensandbox` and `gateway-aliyun`, setting
  only the bind host before launching the existing Gateway entry point.
- Start, stop and status scripts manage both units explicitly.

## Verification

Static tests assert disjoint explicit hosts, unit lifecycle coverage and no
wildcard bind. Runtime verification checks both `/health` endpoints, both proxy
ports, OpenSandbox bridge access, and Aliyun access from a sandbox created from
template `tf6ud6ouc1ynsmivnjkq` without dynamic VPC metadata.

## Rollback

Stop the Aliyun Gateway unit and restore the single OpenSandbox Gateway unit.
No schema, capability token or provider protocol rollback is required.
