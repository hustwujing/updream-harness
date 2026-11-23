# Sandbox Protocol v2 - Stage 6 Report

## Scope And Design

Stage 6 makes Runtime network injection provider-owned. The application requests a validated `SandboxRuntimeRoute` for the allocated Handle and never branches on provider names.

Implementation commit: `943de5cfcefa4022c87011c0e291960c4b9243b6`

## Development

- Main Harness, child Harness, media operation, and Inspector injection now consume `backend.runtime_route(sandbox)`.
- Removed direct global Settings Gateway/proxy injection from execution paths.
- OpenSandbox continues to use its explicitly configured bridge origin.
- Added explicit Aliyun sandbox-visible Gateway/proxy Settings and isolated deployment environment values.
- Registry route lookup dispatches by Handle provider, so cleanup or recovery cannot receive the active provider's route accidentally.
- Gateway listeners remain explicit host bindings; no wildcard bind was introduced.

## Migration And Rollback

- Current OpenSandbox route values are unchanged.
- Aliyun route defaults are explicit private origins and remain unused until Ali is deliberately selected.
- Rollback is code/config-only; no network listener or cloud resource was changed during this stage.

## Tests

- Factory, Broker, child Job, media operation, Inspector, Settings, and isolated deployment suites: `115 passed`.
- Tests prove provider-specific route dispatch and verify Runtime injection receives the backend route rather than global Settings.
- `git diff --check`: passed.

## Review Findings

- Provider route selection is now part of the backend protocol and follows durable Handle dispatch.
- Provider credentials remain in endpoint headers and are not injected as Runtime environment variables.
- No deployment, restart, listener change, backend switch, or cloud mutation was performed.

## Residual Risks

- The Ali private origin must be verified from a real Ali sandbox after template creation succeeds.
- Multi-listener host networking is deployment work and remains gated behind real connectivity validation.
