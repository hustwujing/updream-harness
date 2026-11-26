# API

Browser-facing FastAPI service. It owns identity middleware, Thread/Turn APIs,
Broker recovery, inspector routes, SSE, uploads, lifecycle polling and web assets.

- Entry: `sandbox_harness.services.api.main:run`
- Databases: accounts, core, media and sites through `ApiRepositories`
- Runtime state: `.data`; immutable code/resources: selected service release
- Verification: API/Broker/auth/inspector tests, browser tests, health and real recovery cases

Cross-domain writes must use `CrossDomainCommands` or the lifecycle coordinator.
