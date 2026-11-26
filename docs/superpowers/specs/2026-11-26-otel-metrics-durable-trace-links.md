# OTel Metrics And Durable Trace Links Design

## Intent

The application must produce standard, safe observability signals while the
company platform owns collection, buffering, storage and visualization. Local
development keeps bounded JSONL telemetry for direct diagnosis. Kubernetes
deployments write structured JSON logs to stdout and export OpenTelemetry
traces and metrics only when the platform injects an OTLP endpoint.

## Goals

- Export OTel metrics with the same service resource and OTLP endpoint used by
  logs and traces.
- Preserve the producer trace across persisted cross-database handoffs and link
  delayed consumers to that trace after process restart.
- Link continuation dispatch to the authoritative trace context already stored
  on the origin Turn.
- Keep all telemetry optional and non-blocking for business behavior.
- Keep telemetry labels bounded and free of prompts, generated content,
  credentials, arbitrary headers, raw URLs and raw exception messages.
- Keep Harness stdout as the business protocol while logging one configured,
  credential-redacted stderr tail per execution for developer diagnosis.

## Non-Goals

- Deploying an OTel Collector, sidecar, disk queue, dashboard or alerting stack.
- Providing application-owned replay for OTLP export failures.
- Adding observability-only columns to the four strict domain database schemas.
- Making delayed work a child span of a trace that may already be complete.

## Approaches

Three metrics approaches were considered: derive metrics from JSON logs, expose
a Prometheus endpoint, or use the OTel Metrics SDK. The selected approach is the
OTel Metrics SDK because the application already uses OTel resources and an
OTLP endpoint, and the company platform can collect all three signals through
one standard interface.

For durable cross-database context, explicit outbox/inbox columns, lookup from a
related Turn, and a digest-bound Envelope field were considered. The selected
approach stores a validated `traceparent` in the persisted Envelope JSON. It is
covered by the existing envelope digest and therefore survives retry without a
schema migration. Continuations that already have an origin Turn use its
persisted `last_traceparent`, falling back to `submit_traceparent`.

## Design

`Telemetry` owns one OTel `MeterProvider` with the same `Resource` as logs and
traces. It exposes bounded counters and histograms for observability health and
generic operation duration/outcome. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
the SDK uses an OTLP/HTTP periodic metric reader. Without an endpoint, metric
recording remains valid and no network exporter is installed. Export interval
and timeout are configurable through standard OTel environment variables.

External Sandbox Runtime metrics remain disabled by default. When the K8s
deployment enables them, Runtime sends OTLP protobuf with its current short-lived
capability to Sandbox Gateway. Gateway accepts only the fixed metric names,
units, descriptions, Resource/Scope fields and low-cardinality labels defined
by this repository, then forwards the original protobuf with SRE-provided OTLP
credentials. Sandbox authorization is never forwarded and the application does
not buffer the relay on disk.

`Envelope.traceparent` defaults to an empty string for protocol compatibility.
`DomainHandoff.enqueue` captures the current W3C carrier, validates it through
the existing propagator and persists only the canonical `traceparent` value.
`receive` starts a new processing operation and adds a Link to the persisted
producer context. Relay redelivery preserves the same envelope and digest.

Job and media continuation dispatchers obtain the origin Turn's durable trace
context from the repository response and link their new dispatch operation to
it. A missing or invalid context produces an unlinked trace and never prevents
delivery.

Production Kubernetes configuration enables structured stdout for platform
collection and disables the file sink, while bounded local JSONL remains the
default for development. No application disk-backed OTLP queue is added.

## Completion

- Focused metrics tests prove instruments, optional OTLP configuration and
  shutdown behavior, plus authenticated Sandbox metric validation and relay.
- Handoff tests prove trace context persists through serialization, retry and
  receive, with invalid context rejected or omitted safely.
- Continuation tests prove a new dispatch trace links to the origin Turn.
- Observability, domain handoff, continuation and media continuation suites
  pass; documentation and environment examples match the implementation.
