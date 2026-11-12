# Adding a Gateway Capability

## Applies to

Required for a new non-model service exposed from a sandbox through the
Capability Gateway, including generation, search, storage-adjacent or internal
service adapters.

## Existing mechanism

Gateway is a generic authenticated reverse proxy for configured route prefixes.
`Broker.ROUTES` grants prefixes to a Harness. Capability claims bind those
prefixes to one Thread, Turn, sandbox, Harness, model, expiry and budgets.
Gateway removes caller credentials/hop headers, injects the host credential,
uses `trust_env=False`, streams the upstream response and closes active responses
on revocation.

Configured non-model prefixes currently include image, video, audio and 3D Provider
routes under `/v1/providers/...`. Adding a Settings URL alone does not make a
new capability complete.

## Required design

1. Define a narrow route prefix and the Harnesses allowed to receive it.
2. Add the upstream URL and credential environment reference. Keep the secret
   out of route JSON, claims, database, logs and sandbox environment.
3. Define request and response schema, body/stream byte limits, timeout,
   concurrency and usage budget. The existing shared request count may be
   insufficient for an expensive capability.
4. Decide whether the operation has external side effects. If submission can
   outlive the HTTP receipt, add an idempotency/provider-request identity and a
   recovery contract before exposing it. Never automatically resubmit an
   unknown outcome.
5. Strip untrusted forwarding headers and force any server-owned identity,
   model or project fields from verified claims rather than request JSON.
6. Define cancellation/revocation behavior for active and asynchronous work.
7. Add structural telemetry without request/response content.

## Authorization rules

Route-prefix authorization is necessary but may not be sufficient. Validate
operation-specific resource ownership, allowed action, parameter ranges and
budget at Gateway. A Skill or Harness prompt is not an authorization control.

Do not expose database, Redis, object-store or arbitrary internal HTTP access as
a generic Gateway capability. Prefer a narrow adapter with typed operations.

## Completion

- Capability tests prove wrong prefix, expired/revoked token and exhausted
  budget fail closed. `sandbox_id` is signed context, not independent source
  network attestation in the current Gateway.
- Gateway tests cover header stripping, credential injection, body/stream
  limits, timeout, disconnect and revocation.
- Unknown submission receipt and duplicate idempotency cases are tested for
  side-effecting providers.
- A real sandbox case invokes the capability without receiving the host secret,
  validates the result and confirms cleanup/revocation.
- Documentation identifies whether failure is program, Provider or external
  dependency and what the user sees.
