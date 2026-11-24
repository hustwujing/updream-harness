# Dual-provider Gateway Listeners Design

## Intent

Expose the same signed Gateway and public-proxy protocol to OpenSandbox and
Aliyun sandboxes without wildcard listeners or provider-specific application
behavior. A deployment selects exactly one provider listener from configuration.

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

### Selected: one provider-selected instance of the existing Gateway service

The deployment owns two explicit unit definitions but starts exactly one,
selected by `SANDBOX_HARNESS_BACKEND`. This preserves process-local connection
revocation, keeps the inactive VPC address closed, and reuses the same Gateway
implementation for both providers.

### Rejected: two simultaneous Gateway processes

Process-local active stream and CONNECT state cannot be revoked atomically
across two listeners without a new distributed revocation channel. A permanent
Ali listener would also expand the VPC attack surface while OpenSandbox is active.

## Service Model

- `sandbox-harness-parallel-ali-gateway-opensandbox.service`
  binds `172.31.0.1:29882/29883`.
- `sandbox-harness-parallel-ali-gateway-aliyun.service`
  binds `10.194.49.8:29882/29883`.
- `run-service.sh` accepts `gateway-opensandbox` and `gateway-aliyun`, setting
  only the bind host before launching the existing Gateway entry point.
- Start chooses one unit and refuses a provider mismatch. Stop manages both
  possible unit names. Status reports the selected and inactive providers.

## Verification

Static tests assert provider selection, disjoint explicit hosts, unit lifecycle
coverage and no wildcard bind. Runtime verification tests each listener in
isolation, including Aliyun access from a sandbox created from template
`tf6ud6ouc1ynsmivnjkq` without dynamic VPC metadata, then restores OpenSandbox.

## Rollback

Stop the Aliyun Gateway unit and restore the single OpenSandbox Gateway unit.
No schema, capability token or provider protocol rollback is required.
