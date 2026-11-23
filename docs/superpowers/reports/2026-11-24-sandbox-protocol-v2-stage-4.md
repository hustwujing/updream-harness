# Sandbox Protocol v2 - Stage 4 Report

## Scope And Design

Stage 4 introduces a backend registry that separates the provider used for new allocations from the provider used to recover or clean an existing Handle.

Implementation commit: `a0e57d3e2f33b34d6882f6e4f81732523187d2d6`

## Development

- Added `SandboxBackendRegistry` with one frozen active provider and any number of registered cleanup providers.
- New create/reconcile operations use only the active provider.
- Get, renew, endpoint, route, and delete dispatch by `SandboxHandle.provider`.
- Registry lookup rejects raw sandbox IDs and unregistered providers.
- Provider switching is rejected while active Handles exist; no automatic execution failover is implemented.
- Application and Job Service assembly now construct a registry even when only one provider is configured.
- Pool health checks use complete Handles.

## Migration And Rollback

- The current deployment registers only its configured provider, so runtime behavior is unchanged.
- Additional providers may be registered later for cleanup without changing the active creation provider.
- Rollback is code-only and requires no schema or cloud rollback.

## Tests

- Registry unit and Pool tests: `8 passed`.
- Job Service, Job API, Broker, and Inspector assembly/regression tests: `65 passed`.
- Explicit two-provider coverage proves provider dispatch, raw-ID rejection, delete routing, and the active-work switch fence.
- `git diff --check`: passed.

## Review Findings

- Closed the Stage 1 finding that production dispatch could bypass Handle provider fences through raw IDs.
- The registry intentionally does not retry an execution on another provider.
- No deployment or cloud mutation was performed.

## Residual Risks

- Catalog resolution still provides provider references through the schema v1 image fields. Stage 5 replaces this with provider releases.
- Runtime route injection still reads global Settings in execution paths. Stage 6 removes that dependency.
