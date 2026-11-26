# API

Browser-facing FastAPI request plane. It owns identity middleware, Thread/Turn
request APIs, inspector proxy routes, SSE, uploads, public projections and web assets.
It does not run Turn execution, recovery or singleton supervisors.

- Entry: `sandbox_harness.services.api.main:run`
- Databases: accounts, core, media and sites through `ApiRepositories`
- Runtime state: `.data`; immutable code/resources: selected service release
- Verification: API/auth/inspector tests, browser tests, request-only health and EVO01

Cross-domain writes must use `CrossDomainCommands` or the lifecycle coordinator.
