# Sandbox Gateway

Host capability gateway used by sandbox Runtime processes. It validates scoped
capabilities, routes model/provider traffic, serves the public proxy and ingests telemetry.

- Entry: `sandbox_harness.services.sandbox_gateway.gateway:run`
- Databases: core, media and sites through `GatewayRepositories`
- Units: `sandbox-gateway-aliyun` or `sandbox-gateway-opensandbox`
- Verification: gateway/security/proxy/model ingress tests and real capability probes
