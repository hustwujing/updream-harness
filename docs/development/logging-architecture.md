# Full-chain Logging Architecture

Status: implemented and unit/component verified on the referenced source
revision; deployment and supplemental AS01 evidence remain release gates.

## Goals

Every API request, durable command, worker attempt, sandbox lifecycle action,
Runtime execution and external provider request must be reconstructable from a
shared trace and stable business identifiers. Operational telemetry remains
safe for normal collection. Explicitly enabled diagnostic capture may retain
redacted request and response details for a short, bounded period.

## Two telemetry planes

The operational plane emits OpenTelemetry logs, spans and bounded metrics. It contains event
names, resource identity, trace context, business identifiers, state
transitions, outcomes, error classes, durations, byte counts and content
digests. Except for the explicitly configured Harness stderr tail described
below, it never contains prompts, generated content, commands, file contents,
credentials, arbitrary headers, query strings or arbitrary exception messages.

Harness stdout is a business protocol and is never copied wholesale into
operational logs. Runtime drains stderr on a separate pipe to prevent process
blocking and emits one `runtime.harness.stderr` record when stderr is non-empty.
`SANDBOX_HARNESS_RUNTIME_STDERR_LOG_MAX_BYTES` controls the retained tail:
zero omits text, positive values retain that many bytes, and negative values
select the hard maximum of 1 MiB. The same credential redactor applies before
JSONL/stdout/OTLP export. Users receive stable error codes and a troubleshooting
identity, not `stderr_tail`.

The diagnostic plane is disabled by default and writes only beneath the
configured data root. `SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE` accepts `off`,
`metadata` or `payload`; payload capture also requires
`SANDBOX_HARNESS_LOG_LEVEL=DEBUG`. It records redacted ingress and egress HTTP
metadata and bounded bodies in a separate capture directory. Capture sessions
have a size limit and TTL. Binary bodies are represented by MIME type, length
and SHA-256 rather than embedded in JSON.

Authorization, proxy authorization, cookies, API keys, passwords, database
URLs and credential-bearing URL components are always removed. Query strings
are excluded. Payload capture is additionally bounded by content type and byte
limit. Enabling, disabling, pruning and write failures emit operational audit
events without payloads.

## Event contract

Event attributes use one canonical field vocabulary. Business attributes are
persisted under the `updream.` namespace and OpenTelemetry semantic attributes
retain their standard names. Unknown fields may never disappear silently:
development and test strict mode raises immediately, while production drops the
field, increments a schema-violation counter and emits a bounded
`telemetry.schema_violation` record.

Core lifecycle events carry the identifiers available at that boundary plus
`state_before`, `state_after`, `generation`, `attempt`, `outcome`,
`error_code`, `error.type` and `duration_ms` as applicable. External calls also
carry `peer_service`, `server.address`, `http.request.method`, `http.route`,
status, byte counts and retry attempt. Provider-neutral fields are used across
all sandbox and media providers.

## Correlation

W3C `traceparent` propagates across API, Broker, workers, Runtime, sandbox
gateway and external providers. Durable asynchronous records retain their
trace parent or link so a replacement worker can continue or link the existing
trace. The primary query key is `trace_id`; `thread_id`, `turn_id`, `job_id`,
`execution_id`, `sandbox_id`, `allocation_id`, `provider_job_id` and
`request_id` provide additional joins.

## Storage and export

Each process uses a bounded in-memory queue. Local development may enable the
bounded JSONL sink. Kubernetes deployments enable structured JSON stdout for
the SRE-managed company log agent and export application logs, traces and bounded metrics over
OTLP/HTTP when the platform injects an endpoint. Collection, disk buffering,
vendor routing, retry, storage and display belong to the company platform; this
repository does not deploy a Collector or application-owned disk queue. An
export outage must not block product work; queue depth, dropped records, disk
errors and export failures remain observable.

External Sandbox Runtime remains outside the SRE-managed Kubernetes log agent. It uses
the current Turn's short-lived capability to send logs, traces and optionally
bounded application metrics through the scope-checked Sandbox Gateway and keeps
a bounded diagnostic ring for cleanup fallback. Gateway validates the fixed
metric contract and relays protobuf with SRE credentials; Sandbox credentials
never reach the company Collector. Cross-DB Domain
Envelopes bind canonical `traceparent` to their digest, while Job and Media
continuations recover the authoritative origin Turn context and start a new
Trace with Span Links after restart or delayed delivery.

The operational log level, directory, filename template, queue capacity,
segment size, aggregate size and retention days are configurable. Diagnostic
capture has independent directory, filename template, per-record body limit,
aggregate size and TTL settings. Relative directories resolve beneath the
deployment data root; absolute directories must also remain inside that root.
Filename templates may use only the documented service, instance and capture
session variables and may not contain path separators or traversal segments.

Operational and diagnostic writers reassert disposable directories on every
write. Removal after process startup increments recovery telemetry or emits a
bounded recovery event. A directory symlink, non-directory or path outside the
configured data root fails closed without redirecting logs or payloads.

## Completion checks

- All static event calls use declared fields and no field is silently dropped.
- Ordinary logs contain no diagnostic bodies or secrets.
- Diagnostic capture is inert unless explicitly enabled and scope-limited.
- Ingress and instrumented egress records share trace context.
- Queue overload, disk failure and exporter failure do not block business work.
- A focused integration test reconstructs a request timeline across service
  boundaries.
