# Runtime SLS Structured Logging Design

## Intent

Make sandbox startup and Runtime events queryable as structured JSON in any
provider log collector, while enabling automatic SLS collection for every
active Ali template.

## Constraints

- Keep the Sandbox Protocol provider-neutral; no Ali metadata or environment
  variable is part of the Runtime contract.
- Reuse the existing observability field allowlist and secret redaction.
- Emit no credentials, prompts, model output, arbitrary exception messages, or
  other user payloads.
- Identity fields unavailable before `/v1/configure` are emitted as empty
  strings rather than guessed values.
- Existing sandboxes are not mutated; template logging applies to newly created
  sandboxes after each template returns to `ready`.

## Approaches

1. Add ad-hoc `print(json.dumps(...))` calls in Runtime code. Rejected because
   it duplicates redaction and misses existing events.
2. Add an Ali-specific logging adapter. Rejected because it breaks provider
   parity and cannot be reused by OpenSandbox or future providers.
3. Mirror the existing safe structured event stream to stdout only inside
   sandbox Runtime processes. Selected because SLS, container logs, and future
   provider collectors all consume stdout without changing the protocol.

## Design

- `sandbox_harness.observability` owns a `ConsoleJsonHandler` that serializes
  safe event attributes to one JSON object per line with fixed
  `schema_version`, `timestamp`, `level`, `event`, `service`, `sandbox_id`,
  `thread_id`, `turn_id`, `job_id`, and `execution_id` fields.
- `configure()` installs the handler only for `runtime` and
  `runtime-bootstrap`; API, Gateway, and workers retain their current logging.
- Bootstrap emits lifecycle events before the Runtime Code Bundle is active.
- `/v1/configure` carries optional `job_id` alongside existing logical
  identities. Child/operation jobs pass their durable Job ID; ordinary turns
  leave it empty.
- Ali template updates use the control-plane `UpdateTemplate` API with automatic
  logging semantics, preserve every unrelated field, poll each template back to
  `ready`, and verify the returned concrete Project/Logstore before proceeding.
- One fresh canary sandbox must produce structured startup and configured
  Runtime events before the rollout is accepted.

## Verification

- Unit tests prove schema stability, redaction, provider neutrality, bootstrap
  handler installation, and `job_id` propagation.
- Focused Runtime and provider contract tests pass.
- All 18 active Ali templates report non-empty log configuration and `ready`.
- A newly created Ali canary reaches health and its SLS Logstore contains the
  expected JSON event.
