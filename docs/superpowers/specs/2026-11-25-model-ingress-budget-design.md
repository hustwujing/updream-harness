# Bounded multimodal model ingress

## Evidence and boundary

The fresh 2bb6b54 real gate hit a local Gateway 413 during a video verification
continuation. The Provider had already produced the video. Native image history
included a 7.23 MiB PNG contact sheet plus earlier/later previews; the common
16 MiB request limit rejected the next model request before ModelActivity was
created. Native execution reported `other`, hiding the local resource limit.

The host must not rewrite image content, plan the task or resend execution.
Model protocols need their own bounded ingress budget. A configurable 64 MiB
model limit accommodates accumulated base64 image history. General capability
and public proxy limits remain 16 MiB. Every path retains declared-length and
streamed-byte enforcement, idle/total timeouts and capability authorization.

After capability/route validation, generation/compaction requests start one
ModelActivity before reading the body. Local over-limit rejection finishes it
with `model_request_too_large`, before any upstream call or token reservation.
The Broker can restore that stable code only from the latest durable model
activity and an ambiguous native error. A newer successful/pending request or a
real upstream error must prevent substitution. No raw body, image or arbitrary
exception text is added to telemetry or user messages.

## Implementation and verification

- Add failing Gateway regressions for realistic image history exceeding 16 MiB,
  all three native protocols, declared/chunked limits, unchanged non-model
  limits, observed rejection and zero upstream side effects.
- Add failing latest-activity and Broker/public-error tests to fence stale
  observations and preserve genuine upstream failures.
- Implement the independent config, bounded ingestion and error observation.
- Run Gateway/config/activity/Broker/error tests and broader required checks;
  obtain a fresh read-only review. Commit as a coherent generic fix.
- Drain the current gate, release only Ali services, run S29 and affected
  multimodal continuation cases, then run a fresh full169 gate on the final
  committed revision. The failed 2bb6b54 gate remains failure evidence.
