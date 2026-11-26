# Logging, metrics and distributed tracing

This is the implemented contract for updream-harness. Logs diagnose execution; PostgreSQL and persisted business events remain authoritative for task state and delivery. Logs, traces and bounded metrics share one OpenTelemetry resource. The design follows [W3C Trace Context](https://www.w3.org/TR/trace-context/), [OpenTelemetry Logs](https://opentelemetry.io/docs/specs/otel/logs/data-model/), [trace context in JSON logs](https://opentelemetry.io/docs/specs/otel/compatibility/logging_trace_context/), and [Span Links](https://opentelemetry.io/docs/specs/otel/overview/#links-between-spans). Python SDK/exporter versions are pinned together in `pyproject.toml`.

## Identity and lifetime

| Field | Meaning |
| --- | --- |
| `trace_id`, `span_id`, `trace_flags` | Standard SDK-generated technical trace context; business IDs are not reused as Trace IDs. |
| `resource.service.namespace` | `updream-harness`. |
| `resource.service.name` | `api`, `gateway`, or `runtime`. Broker and storage code run inside the API service. |
| `resource.service.instance.id` | Unique process-start identity, including after restarts. |
| `resource.service.version` | Deployment-pinned Git revision on the host, immutable build revision in Runtime images. |
| `attributes.updream.thread_id` | Conversation identity. |
| `attributes.updream.turn_id` | One business Turn, unchanged across recovery. |
| `attributes.updream.input_id` | A particular appended input, unchanged on replay. |
| `attributes.updream.request_id` | API ingress request identity; returned as `X-Request-ID` for troubleshooting. |
| `attributes.updream.execution_id`, `sandbox_id` | The execution and sandbox actually involved. |
| `attributes.updream.model_request_id`, `upstream_request_id` | Our individual model invocation and the upstream's receipt. |
| `attributes.updream.generation`, `broker_id` | Lease generation and Broker instance. |

HTTP propagates validated W3C `traceparent` / `tracestate`, without automatically propagating baggage. A background processing attempt starts a new Trace linked to the submission and previous attempt. The submission context and latest processing context are persisted on the Turn. Updating the latest context is lease-fenced. Resuming an expired lease creates a new Span/Trace; it never invents successful termination of a crashed process's old Span.

Cross-database commands persist a canonical `traceparent` inside the digest-bound Domain Envelope. The destination starts a new Trace and adds a Span Link after any process restart or redelivery. Job Outbox claims recover every origin Turn's durable context so batched and nested continuations retain all source Links. Media continuation reads the authoritative Core Turn and applies the same Link without copying observability state into the Media database. Missing or invalid legacy context leaves the new Trace unlinked and never blocks delivery.

Native Harnesses do not necessarily forward tracing headers. Gateway model calls therefore also link to the context in the signed capability, and always carry the verified Turn/thread/sandbox identities. Caller-supplied tracing metadata grants no authorization. HTTP resource IDs are recorded only in recognized ID formats; URLs, query strings and request bodies are not copied into logs.

## Events and timing

All records are JSON Lines with `schema_version=1`. Log records contain UTC timestamp, observed timestamp, severity, stable event name, resource, attributes and available trace context. Span records contain parent, links, start/end-derived duration and status. Event names and attribute types are stable API contracts. Custom fields use the `updream.` namespace.

Key event groups:

- HTTP ingress/finish/failure, request ID returned to the browser. API error UI includes the troubleshooting request ID.
- Turn submission/deduplication, lease claim/loss, state commits, recovery/reattachment and cancellation.
- Sandbox acquire/reuse/create/bind/adopt/release/delete and orphan cleanup.
- Input acceptance, replay, delivery and unknown receipt outcomes; interaction replies.
- Runtime process lifecycle, input acknowledgements, background jobs and cancellation. Harness stdout remains the business protocol; stderr is drained separately and produces one bounded `runtime.harness.stderr` operational record per execution. Native tool IDs produce source-side `harness.tool` spans and monotonic duration logs; replay does not recalculate execution time from Broker receipt time. Missing native IDs are explicit `runtime.tool.unidentified` events, and unfinished tool spans end as interrupted.
- Model request start, response headers, first effective output, terminal status and total duration. Header arrival and effective output are distinct. Provider-reported input/output token values preserve zero; absent values remain absent with `usage_source=unavailable`. Counts are observations, not budget reconciliation.
- Object reads/writes, checkpoint transfer and durable commit, artifact transfer/persistence.
- Redis fallback, model-catalog/probe failures, telemetry queue/drop/export/disk status.

Durations use monotonic clocks and are expressed in milliseconds. Wall timestamps use UTC. Logs are not a global distributed clock: use runtime sequence, lease generation and Span relationships when timestamps are close. A completed instrumentation operation means that function returned; the authoritative Turn outcome is separately recorded by `turn.state.changed`. HTTP 200 alone is not model completion.

OTel Metrics records `updream.operation.count`, `updream.operation.duration` and `updream.telemetry.condition`. Dimensions are limited to stable operation name, bounded outcome and a controlled telemetry condition; request, user, Thread, Turn, Job, sandbox and Provider identifiers are never metric labels. Metrics report process behavior and observability health, not authoritative business state.

INFO records lifecycle events; WARNING covers recoverable transport/dependency issues; ERROR covers failed operations. Expected cancellation is recorded separately. `failure_origin` distinguishes `program`, `model`, `provider`, `infrastructure`, `user`, `unknown` where there is evidence; it is an initial boundary classification, not a substitute for root-cause review. Uncertain causes remain unknown.

## Confidentiality and bounded impact

An allowlist controls operational attributes before persistence/export. Tokens, passwords, cookies, prompts, generated text, attachment contents, shell commands, SQL parameters, full URLs, arbitrary HTTP headers and exception messages are excluded from ordinary event fields. Structured exception records retain type and code-location stack frames without source lines or local variables. Harness stderr is the explicit bounded exception: Runtime stores only its configured tail in `stderr_tail`, applies the existing credential redactor, and never copies it into user-facing Runtime events. Set the stderr limit to zero where policy forbids saving any stderr text. Foreign-library diagnostics retain severity/location rather than formatting potentially sensitive messages. Sandbox telemetry is marked `origin=sandbox_reported`; it never drives business state.

Unknown event fields are no longer discarded silently. Strict schema mode raises at the call site; normal production mode increments `schema_violations` and emits `telemetry.schema_violation` with safe field names only. The static observability audit checks every declared event, operation, bind and enrich field.

Explicit diagnostic capture is a separate local JSONL plane. `metadata` records bounded request/response metadata and body digests. `payload` additionally records bounded text/JSON bodies and requires `DEBUG`. Credential headers, credential-bearing URLs and credential JSON fields remain redacted. Query strings are removed and binary bodies retain only size and SHA-256. Capture files use a separate directory, template, total budget and TTL; capture failure never changes a request result. Provider, Runtime, sandbox and media HTTPX clients propagate `traceparent` and emit request lifecycle events. Full payload capture is intended for a scoped session or `trace_id`, then disabled.

Logging is asynchronous with a bounded in-memory queue. Queue overload increments `dropped_records`; the writer emits a drop summary. Disk errors and exporter errors increment independent counters and do not fail the business operation. `/api/health` and Gateway `/health` expose local telemetry counters and writer status. When local JSONL is enabled, external OTLP failures leave those files intact. The final Runtime collection record also preserves its instance-cumulative drop/export/disk counters and last export HTTP status. The writer coalesces records for up to 200 ms or 128 records per batch. The application does not implement an OTLP disk queue or Collector; collection buffering, retry, storage and display belong to the deployment platform.

Ordinary asynchronous logs cannot guarantee zero loss under SIGKILL or host/storage failure. Critical business state and durable events stay in PostgreSQL. Shutdown flushes are bounded, and recovery explicitly records the new attempt. This distinction is intentional and tested.

## Runtime collection and retention

Runtime uses the standard OTLP/HTTP exporters to send logs, traces and optionally metrics to the existing Gateway's `/v1/telemetry/{signal}` route. Runtime Metrics is off by default. When enabled, Gateway accepts only the three documented metric names, fixed units/descriptions, bounded Resource/Scope fields and fixed low-cardinality labels, then relays the original protobuf using its SRE-provided OTLP endpoint and credentials. Sandbox Authorization is never forwarded. The Runtime route requires a valid, unrevoked capability. The receiver verifies business scope, limits batches to 4 MiB / 512 records, limits ingestion rate independently, and does not consume model/public-request budgets. Compressed input is rejected; the Runtime exporter uses uncompressed protobuf.

Before credential revocation and sandbox deletion, the Broker also fetches the Runtime's bounded in-memory diagnostics window. It validates records again, labels their origin, and preserves source event IDs. Queries deduplicate live export and fallback copies. Collection failure and an older window boundary are explicit events; a missing tail is never fabricated. The Runtime ring retains 8192 records, so a prolonged export outage can exceed the fallback window.

Optional local telemetry files default to `<data_root>/logs`, deployed under `/data2/wujing03/sandbox-harness-platform-parallel-ali/.data/logs`. The directory and basename template are configurable but must remain beneath `data_root`; templates cannot contain path separators or traversal. API/Gateway share a cross-process retention lock: default 7 days and 2 GiB total, oldest segments removed first. Files rotate at 8 MiB or daily. Production Kubernetes deployments normally disable this file sink and enable structured JSON stdout for the company log agent. Files remain useful for local development. Per-sandbox temporary files are bounded to 16 MiB under `/run/harness/observability`; the Runtime's temporary directory is not a durable archive.

| Setting | Default / behavior |
| --- | --- |
| `SANDBOX_HARNESS_LOG_LEVEL` | `INFO`; `payload` diagnostic capture requires `DEBUG`. |
| `SANDBOX_HARNESS_LOG_FILE_ENABLED` | `true`; set `false` in Kubernetes when platform stdout collection is authoritative. |
| `SANDBOX_HARNESS_LOG_STDOUT_ENABLED` | `false`; set `true` in Kubernetes for structured JSON stdout. Runtime stdout remains available for sandbox collection. |
| `SANDBOX_HARNESS_LOG_DIR` | `<data_root>/logs`; must remain beneath `data_root`. |
| `SANDBOX_HARNESS_LOG_FILE_TEMPLATE` | `{service}-{instance}.telemetry.jsonl`. |
| `SANDBOX_HARNESS_LOG_QUEUE_SIZE` | 8192, validated range 1–65536. |
| `SANDBOX_HARNESS_LOG_RING_CAPACITY` | 8192 records, validated range 128–65536. |
| `SANDBOX_HARNESS_LOG_MAX_BYTES` / `SANDBOX_HARNESS_RUNTIME_LOG_MAX_BYTES` | Application-service aggregate 2 GiB; Runtime temporary storage 16 MiB. |
| `SANDBOX_HARNESS_LOG_SEGMENT_BYTES` | 8 MiB per segment. |
| `SANDBOX_HARNESS_LOG_SEGMENT_MAX_AGE_SECONDS` | 86400 seconds. |
| `SANDBOX_HARNESS_LOG_RETENTION_DAYS` | 7, configurable 1–3650. |
| `SANDBOX_HARNESS_LOG_OTLP_TIMEOUT_SECONDS` | 2 seconds, configurable 0.1–60. |
| `SANDBOX_HARNESS_LOG_SCHEMA_STRICT` | `false`; use `true` in development/test to reject undeclared fields. |
| `SANDBOX_HARNESS_RUNTIME_METRICS_ENABLED` | `false`; enable only when Gateway has an SRE-provided OTLP Metrics endpoint. |
| `SANDBOX_HARNESS_RUNTIME_STDERR_LOG_MAX_BYTES` | `1024`; `0` omits stderr text, positive values retain that many tail bytes, negative values retain at most 1 MiB. Positive values above 1 MiB are rejected. |
| `SANDBOX_HARNESS_SERVICE_LOG_DIR` | Launcher stdout/stderr directory, default `<data_root>/service-logs`. |
| `SANDBOX_HARNESS_SERVICE_LOG_FILE_TEMPLATE` | `{component}.log`. |
| `SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE` | `off`, `metadata` or `payload`; default `off`. |
| `SANDBOX_HARNESS_DIAGNOSTIC_DIR` | `<data_root>/diagnostics`. |
| `SANDBOX_HARNESS_DIAGNOSTIC_FILE_TEMPLATE` | `{service}-{instance}-{session}.diagnostic.jsonl`. |
| `SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE_SESSION` | Optional stable session identifier for a bounded investigation. |
| `SANDBOX_HARNESS_DIAGNOSTIC_TRACE_ID` | Optional 32-character lowercase trace filter. |
| `SANDBOX_HARNESS_DIAGNOSTIC_MAX_BODY_BYTES` | 256 KiB captured per request/response body. |
| `SANDBOX_HARNESS_DIAGNOSTIC_MAX_BYTES` | 256 MiB aggregate capture budget. |
| `SANDBOX_HARNESS_DIAGNOSTIC_TTL_HOURS` | 24 hours, configurable 1–168. |
| `SANDBOX_HARNESS_ENVIRONMENT` | `local`, included as resource metadata. |
| `SANDBOX_HARNESS_RELEASE` | Immutable selected service or Runtime release digest. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional company OTLP/HTTP base URL. The repository's K8s application services send `/v1/logs`, `/v1/traces` and `/v1/metrics`. Without it, configured stdout/files and the Runtime-to-Gateway pipeline remain active. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Optional external exporter headers, never logged. Direct connection; ambient HTTP proxy credentials are not used. |
| `OTEL_METRIC_EXPORT_INTERVAL` | Metrics export interval in milliseconds; default `60000`. |
| `OTEL_METRIC_EXPORT_TIMEOUT` | Metrics export timeout in milliseconds; default follows the configured OTLP timeout (`2000` by default). |

Runtime export endpoint/credential is replaced on configure using the sandbox's current short-lived token. No long-lived observability credential is sent into the sandbox. External OTLP export is implemented and protocol-tested; an organization-specific collector address is deployment configuration, not a hardcoded dependency.

Kubernetes deployment of this repository's application services uses:

```bash
SANDBOX_HARNESS_LOG_FILE_ENABLED=false
SANDBOX_HARNESS_LOG_STDOUT_ENABLED=true
SANDBOX_HARNESS_RUNTIME_METRICS_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://platform-otel-collector.observability:4318
```

The SRE-managed observability platform injects the endpoint and any headers. This repository does not deploy a sidecar, DaemonSet, Collector, disk queue, dashboard or alerting backend. External cloud sandboxes are outside the SRE-managed Kubernetes log agent's reach, so their Runtime and native Harness lifecycle logs, traces and bounded application metrics continue through the scope-checked `sandbox-gateway`. Harness stdout business content and long-lived platform credentials do not cross that boundary; the configured, credential-redacted stderr tail is the documented exception. Sandbox CPU, memory, filesystem and network metrics remain the Sandbox Provider/infrastructure collector's responsibility.

Example scoped payload investigation:

```bash
SANDBOX_HARNESS_LOG_LEVEL=DEBUG
SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE=payload
SANDBOX_HARNESS_DIAGNOSTIC_CAPTURE_SESSION=incident-20261126
SANDBOX_HARNESS_DIAGNOSTIC_TRACE_ID=0123456789abcdef0123456789abcdef
```

Remove the capture mode and restart after collecting evidence. Operational logs can be queried with `sandbox-harness-logs --trace <trace_id> --related`; diagnostic files are intentionally excluded from that command and from OTLP export.

## Deploying a logging change

After tests, build and select the affected immutable service descriptor with
`tools/service_release.py`; image-inference uses
`tools/image_inference_release.py`. Restart through `deploy/ali/start.sh` and
verify the loaded digest. Runtime image revisions remain independently pinned
by the signed profile catalog. API creation recomputes the exact static
directory it mounts and refuses a configured `SANDBOX_HARNESS_WEB_DIST_DIGEST`
mismatch before accepting traffic; health exposes the verified digest.

API readiness requires PostgreSQL, the selected execution backend and every
required API-owned supervisor. Warm-pool target shortfall is optional only while
execution itself remains available. Gateway readiness requires PostgreSQL;
Redis is an explicitly degraded cache/notification dependency and is probed for
recovery without preventing startup. Its telemetry fallback expires old minute
windows, enforces a hard LRU capacity and clears after Redis recovery. Health payloads expose dependency and
supervisor status rather than reporting a process-only liveness result.

## Troubleshooting

From the project directory:

```bash
.venv/bin/sandbox-harness-logs --turn TURN_ID
.venv/bin/sandbox-harness-logs --thread THREAD_ID --json
.venv/bin/sandbox-harness-logs --request REQUEST_ID --json
.venv/bin/sandbox-harness-logs --trace TRACE_ID --related
.venv/bin/sandbox-harness-logs --turn TURN_ID --since 2026-11-10T00:00:00Z --limit 2000
```

`--request` automatically includes the related Turn once identified; `--related` also expands a trace to its business Turn. The default log directory is `<SANDBOX_HARNESS_DATA_ROOT or .data>/logs`; use `--log-dir` explicitly outside the project. The query works without database/API credentials, skips malformed lines, reports their count, and deduplicates by source event ID or Trace/Span ID. Output is bounded to at most 10,000 records.

Start from the user's request ID or Turn ID. Locate acceptance, lease generation, sandbox binding, Runtime acknowledgement, model timing, checkpoint/artifact persistence, credential revocation and cleanup. Compare service versions and instance IDs across recovery. A missing ordinary log record is not proof an operation did not happen; check the durable state and telemetry loss counters.

## Development and tests

Use `event()`, `operation()`, `observed()` or `observed_sync()` from `observability.py`. Add new metadata to the allowlist only after checking sensitivity and bounded size. Do not log payloads to diagnose them. Keep business mutations independent of log/export availability. Do not make task IDs metric labels or create a new Trace for every streamed token.

Relevant tests cover context isolation, W3C propagation, persisted/fenced recovery context, digest-bound cross-database Links, durable continuation Links, bounded Metrics, separated Harness stdout/stderr, stderr limits and credential redaction, payload exclusion, queue overflow, disk and collector failures, retention, scope-checked OTLP ingestion, request budget isolation, duplicate collection, HTTP request IDs, and the existing execution/recovery behavior. Real sandbox and real Provider evidence is reported separately.
