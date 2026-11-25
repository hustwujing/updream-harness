# Development Guide Index

Use this index before editing. Read only the shared guidance and the rows that
match the task, then inspect the referenced source. Documentation is a map; the
current implementation and tests are the authority.

## Shared reading

Every code change requires:

1. [Getting started](getting-started.md)
2. [Product boundaries](product-boundaries.md)
3. [Architecture and ownership](architecture-and-ownership.md)
4. [Testing and real cases](testing-and-real-cases.md)

For the isolated Ali checkout, first verify the directory and branch described
in [Getting started](getting-started.md). The
[90f51d5 release record](../test-evidence/ali-release-90f51d5.md) is the completed
baseline; subsequent documentation edits do not relabel that release evidence.

## Task routing

| Task | Required guidance |
| --- | --- |
| Thread, Turn, input, event, lease or database change | [State and data contracts](state-and-data-contracts.md), [Lifecycle changes](change-lifecycle.md), [Database migrations](database-migration.md) |
| Recovery, cancellation, cleanup, pool or checkpoint change | [State and data contracts](state-and-data-contracts.md), [Recovery and observability](recovery-and-observability.md), [Lifecycle changes](change-lifecycle.md) |
| Sandbox allocation dispatcher, admission saturation or provider quota exhaustion | [Recovery and observability](recovery-and-observability.md), [Testing and real cases](testing-and-real-cases.md), [Real capacity gate](../test-evidence/sandbox-capacity-gate.md) |
| Add or change a Harness | [Sandbox and Harness](sandbox-and-harness.md), [Adding a Harness](add-harness.md), [Skill and image lifecycle](skill-and-image-lifecycle.md) |
| Add a model route or Provider | [Gateway, security and Providers](gateway-security-providers.md), [Adding a model Provider](add-model-provider.md) |
| Add image/video/audio/3D generation | [Media Generation Design and Provider Integration Standard](media-generation.md) |
| Add or change an Operation, Provider, Child Harness or external worker error | [External execution error contract](external-error-contracts.md), [Recovery and observability](recovery-and-observability.md) |
| Change execution catalog readiness, Provider preflight, collection retry or Reviewer terminalization | [External execution error contract](external-error-contracts.md), [State and data contracts](state-and-data-contracts.md), [Testing and real cases](testing-and-real-cases.md) |
| Add a Runtime CLI or image dependency | [Sandbox and Harness](sandbox-and-harness.md), [Adding a Runtime tool](add-runtime-tool.md) |
| Add an OS, Python, Node or media dependency to Runtime images | [Adding an image dependency](add-image-dependency.md), [Skill and image lifecycle](skill-and-image-lifecycle.md) |
| Add, upgrade or retire an Agent Skill; change Skill runtime requirements | [Adding an Agent Skill](add-agent-skill.md), [Skill and image lifecycle](skill-and-image-lifecycle.md), [Sandbox and Harness](sandbox-and-harness.md) |
| Add a non-model Gateway capability | [Adding a Gateway capability](add-gateway-capability.md), [Gateway, security and Providers](gateway-security-providers.md) |
| Public network or proxy policy | [Gateway, security and Providers](gateway-security-providers.md), [Changing public network policy](change-public-network-policy.md) |
| Account, Cookie, ingress or FRPC authentication | [Authentication and ingress](authentication-and-ingress.md), [Database migrations](database-migration.md) |
| API, SSE or browser behavior | [API and frontend](api-and-frontend.md), [State and data contracts](state-and-data-contracts.md) when persistence changes |
| Live sandbox files, terminal, processes, ports or takeover | [Sandbox Inspector](sandbox-inspector.md), [Sandbox and Harness](sandbox-and-harness.md), [Recovery and observability](recovery-and-observability.md) |
| Upload, file card or online document preview | [File upload and preview](file-upload-and-preview.md), [API and frontend](api-and-frontend.md) |
| Static interactive site preparation, deployment, rollback or serving | [Static site deployment](static-site-deployment.md), [Adding a Runtime tool](add-runtime-tool.md), [Adding a Gateway capability](add-gateway-capability.md), [API and frontend](api-and-frontend.md) |
| Logging, tracing or error reporting | [Recovery and observability](recovery-and-observability.md) and [observability contract](../observability.md) |
| Deployment, service unit, image or environment change | [Deployment and collaboration](deployment-and-collaboration.md) |

## Existing system references

- [Architecture](../architecture.md)
- [Actual data model overview](../data-model.md)
- [Client API](../api.md)
- [Product interface](../product-interface.md)
- [Capability Gateway](../gateway.md)
- [Deployment](../deployment.md)
- [Logging and tracing](../observability.md)

## Rule for new guidance

Do not add a rule based only on intended design. Cite a current source entry
point or test. If the behavior is planned but absent, label it as proposed and
keep it out of mandatory completion criteria until it is implemented.
