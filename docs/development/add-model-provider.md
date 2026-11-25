# Adding a Model Provider or Route

## Applies to

Required when adding an upstream, changing model protocol, introducing an exact
Harness/model route or changing discovery/probe behavior.

Sandbox infrastructure providers use [Adding a Sandbox Provider](add-sandbox-provider.md).

## Existing route model

`ModelRoutes` resolves an exact `(Harness, model)` override from
`SANDBOX_HARNESS_MODEL_ROUTES_JSON`, otherwise it uses the Harness default
upstream. An override contains only:

```json
{"base_url":"https://provider.example/v1","credential_env":"PROVIDER_KEY"}
```

Credentials live in the named host environment variable. They must not appear
in route JSON, database rows, API responses, logs or sandbox environment.

Gateway currently exposes model namespaces for Codex, Claude and DeepSeek and
Provider namespaces for image, video, audio and 3D. Route scope comes from the
Turn's capability; adding an upstream URL without scope and budget enforcement
is incomplete.

## Implementation sequence

1. Decide whether this is an exact model override using an existing protocol or
   a new protocol family. Prefer the override when wire behavior is compatible.
2. Validate URL and credential environment names at startup.
3. Make discovery, health probe and execution resolve through the same
   `ModelRoutes` result.
4. Preserve streaming response bytes while enforcing start/idle timeout, byte
   and token budgets.
5. Record model request start, headers, first effective output and finish
   without payload content.
6. Classify explicit unsupported/auth responses separately from transient busy,
   timeout and transport failures.

## Tests

- Exact route selection and missing credential behavior.
- Discovery failure retains the previous inventory and explicit route models.
- Probe body/protocol matches the Harness family.
- Start timeout, stream idle timeout, cancellation and revocation.
- Token values preserve provider-reported zero and leave absent values absent.
- A real minimal request using the deployed route and selected Harness.

Do not declare a model available based only on `/models`; a real Turn is the
final integration proof.
