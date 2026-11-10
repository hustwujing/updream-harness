# Logging and distributed tracing

This is the implemented contract for updream-harness. Logs diagnose execution; PostgreSQL and persisted business events remain authoritative for task state and delivery. The design follows [W3C Trace Context](https://www.w3.org/TR/trace-context/), [OpenTelemetry Logs](https://opentelemetry.io/docs/specs/otel/logs/data-model/), [trace context in JSON logs](https://opentelemetry.io/docs/specs/otel/compatibility/logging_trace_context/), and [Span Links](https://opentelemetry.io/docs/specs/otel/overview/#links-between-spans). Python SDK/exporter versions are pinned together in `pyproject.toml`.

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

Native Harnesses do not necessarily forward tracing headers. Gateway model calls therefore also link to the context in the signed capability, and always carry the verified Turn/thread/sandbox identities. Caller-supplied tracing metadata grants no authorization. HTTP resource IDs are recorded only in recognized ID formats; URLs, query strings and request bodies are not copied into logs.

## Events and timing

All records are JSON Lines with `schema_version=1`. Log records contain UTC timestamp, observed timestamp, severity, stable event name, resource, attributes and available trace context. Span records contain parent, links, start/end-derived duration and status. Event names and attribute types are stable API contracts. Custom fields use the `updream.` namespace.

Key event groups:

- HTTP ingress/finish/failure, request ID returned to the browser. API error UI includes the troubleshooting request ID.
- Turn submission/deduplication, lease claim/loss, state commits, recovery/reattachment and cancellation.
- Sandbox acquire/reuse/create/bind/adopt/release/delete and orphan cleanup.
- Input acceptance, replay, delivery and unknown receipt outcomes; interaction replies.
- Runtime process lifecycle, input acknowledgements, background jobs and cancellation. Native tool IDs produce source-side `harness.tool` spans and monotonic duration logs; replay does not recalculate execution time from Broker receipt time. Missing native IDs are explicit `runtime.tool.unidentified` events, and unfinished tool spans end as interrupted.
- Model request start, response headers, first effective output, terminal status and total duration. Header arrival and effective output are distinct. Provider-reported input/output token values preserve zero; absent values remain absent with `usage_source=unavailable`. Counts are observations, not budget reconciliation.
- Object reads/writes, checkpoint transfer and durable commit, artifact transfer/persistence.
- Redis fallback, model-catalog/probe failures, telemetry queue/drop/export/disk status.

Durations use monotonic clocks and are expressed in milliseconds. Wall timestamps use UTC. Logs are not a global distributed clock: use runtime sequence, lease generation and Span relationships when timestamps are close. A completed instrumentation operation means that function returned; the authoritative Turn outcome is separately recorded by `turn.state.changed`. HTTP 200 alone is not model completion.

INFO records lifecycle events; WARNING covers recoverable transport/dependency issues; ERROR covers failed operations. Expected cancellation is recorded separately. `failure_origin` distinguishes `program`, `model`, `provider`, `infrastructure`, `user`, `unknown` where there is evidence; it is an initial boundary classification, not a substitute for root-cause review. Uncertain causes remain unknown.

## Confidentiality and bounded impact

An allowlist controls attributes before persistence/export. Tokens, passwords, cookies, prompts, generated text, attachment contents, shell commands, SQL parameters, full URLs, arbitrary HTTP headers and exception messages are excluded. Structured exception records retain type and code-location stack frames without source lines or local variables. Foreign-library diagnostics retain severity/location rather than formatting potentially sensitive messages. Credential-like values are redacted even in allowed string fields. Sandbox telemetry is marked `origin=sandbox_reported`; it never drives business state.

Logging is asynchronous with a bounded queue. Queue overload increments `dropped_records`; the writer emits a drop summary. Disk errors and exporter errors increment independent counters and do not fail the business operation. `/api/health` and Gateway `/health` expose local telemetry counters and writer status. External OTLP failures leave local files intact. The final Runtime collection record also preserves its instance-cumulative drop/export/disk counters and last export HTTP status. The writer coalesces records for up to 200 ms or 128 records per batch. The single worker gives bounded memory/ordering but an unavailable exporter can increase queue occupancy, visible in these counters.

Ordinary asynchronous logs cannot guarantee zero loss under SIGKILL or host/storage failure. Critical business state and durable events stay in PostgreSQL. Shutdown flushes are bounded, and recovery explicitly records the new attempt. This distinction is intentional and tested.

## Runtime collection and retention

Runtime uses the standard OTLP/HTTP exporters to send logs and traces to the existing Gateway's `/v1/telemetry/logs` and `/v1/telemetry/traces`. This requires a valid, unrevoked capability with the telemetry route. The receiver verifies business scope, limits batches to 4 MiB / 512 records, limits ingestion rate independently, and does not consume model/public-request budgets. Compressed input is rejected; the Runtime exporter uses uncompressed protobuf.

Before credential revocation and sandbox deletion, the Broker also fetches the Runtime's bounded in-memory diagnostics window. It validates records again, labels their origin, and preserves source event IDs. Queries deduplicate live export and fallback copies. Collection failure and an older window boundary are explicit events; a missing tail is never fabricated. The Runtime ring retains 8192 records, so a prolonged export outage can exceed the fallback window.

Authoritative local telemetry files live at `<data_root>/logs`, deployed under `/data2/wujing03/sandbox-harness-platform/.data/logs`. API/Gateway share a cross-process retention lock: default 7 days and 2 GiB total, oldest segments removed first. Files rotate at 8 MiB or daily. Retention uses segment modification time; idle writers also prune every 60 seconds. A segment can include up to one day of events, so event-age retention has that rotation granularity. Files are private to the service account. Per-sandbox temporary files are bounded to 16 MiB under `/run/harness/observability`; Docker storage is on `/data2`. The Runtime's temporary directory is not a durable archive.

| Setting | Default / behavior |
| --- | --- |
| `SANDBOX_HARNESS_LOG_LEVEL` | `INFO`; DEBUG enables high-frequency request diagnostics. |
| `SANDBOX_HARNESS_LOG_QUEUE_SIZE` | 8192, validated range 1–65536. |
| `SANDBOX_HARNESS_LOG_MAX_BYTES` | Host aggregate 2 GiB; Runtime temporary storage 16 MiB. |
| `SANDBOX_HARNESS_LOG_RETENTION_DAYS` | 7, configurable 1–3650. |
| `SANDBOX_HARNESS_ENVIRONMENT` | `local`, included as resource metadata. |
| `SANDBOX_HARNESS_RELEASE` | Build revision in Runtime; deploy/record-release.py pins the host revision. Root-run development processes can discover Git when unset. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional external OTLP/HTTP base URL. SDK sends `/v1/logs` and `/v1/traces`. Without it, host files and the Runtime-to-Gateway pipeline remain active. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Optional external exporter headers, never logged. Direct connection; ambient HTTP proxy credentials are not used. |

Runtime export endpoint/credential is replaced on configure using the sandbox's current short-lived token. No long-lived observability credential is sent into the sandbox. External OTLP export is implemented and protocol-tested; an organization-specific collector address is deployment configuration, not a hardcoded dependency.

## Deploying a logging change

After tests and committing tracked changes, run `bash deploy/check-service-access.sh` and `python3 deploy/record-release.py` before restarting API/Gateway. The latter atomically changes only the release field in the existing env file, preserves its permissions, and records `.data/release.json`. It refuses dirty tracked code. The service account does not need access to `.git`; deployment verification must reject an `unknown` version. Runtime image revisions remain independently pinned in their image environment and digest.

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

Relevant tests cover context isolation, W3C propagation, persisted/fenced recovery context, payload exclusion, queue overflow, disk and collector failures, retention, scope-checked OTLP ingestion, request budget isolation, duplicate collection, HTTP request IDs, and the existing execution/recovery behavior. Real sandbox and real Provider evidence is reported separately.
