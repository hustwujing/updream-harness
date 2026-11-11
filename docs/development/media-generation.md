# Media Generation Design and Provider Integration Standard

## Boundary

All image, video and audio generation is an asynchronous platform Job, even
when a Provider exposes a synchronous HTTP request. Harness submits an intent
and returns; the independent `sandbox-harness-media-worker.service` owns
Provider execution, persistence, polling and result download. API owns Job
creation, query and cancel requests. Broker owns SSE and model continuation;
PostgreSQL is the authoritative state shared across services.

Provider credentials remain in the host environment; Sandboxes receive only a
short-lived capability token.

## Provider catalog

Each model is configured with `provider`, `adapter`, `route`, `execution`
(`sync|async`), `cancellation` (`supported|unsupported`), `progress`
(`provider|worker|none`) and a parameter schema. `sync + supported` is invalid.
The resolved provider, adapter, model, capabilities and config version are
snapshotted on Job creation. Only models that pass a real minimal request and
result validation may be enabled.

## Confirmation gate

Generation is two-phase: `prepare intent -> user confirms/edits -> create Job ->
Worker executes`. Before confirmation there is no Provider request or external
side effect. Draft APIs are owner-scoped and idempotent:

```text
POST/PATCH /api/media/drafts
POST       /api/media/drafts/{draft_id}/confirm
POST       /api/media/drafts/{draft_id}/cancel
```

The confirmation card has media-specific fields (image ratio/quality,
video duration/resolution/references, audio voice/format) and a Details modal.
The modal is dismissible, stays over the conversation, and never shows keys,
private URLs or raw upstream responses. The server revalidates every parameter
and creates exactly one Job on confirm.

## Job and recovery

```text
CREATED -> SUBMITTED -> RUNNING -> SUCCEEDED
                         |          |
                         v          v
                      FAILED     UNKNOWN
RUNNING -> CANCEL_REQUESTED -> CANCELLED
```

Sync execution persists `PREPARED`, `REQUESTING`, `RESPONSE_RECEIVED`,
`DOWNLOADING` and `COMMITTED`. A lease-expired `PREPARED` request may retry;
after `REQUESTING`, recovery must reconcile by idempotency key or mark
`UNKNOWN`, never blindly resubmit. Async jobs resume by `provider_job_id`.

API and Media Worker are separate processes. Every Worker instance has a
unique lease owner and persists an async Provider ID before polling. After a
restart it may resume only that ID. A `REQUESTING` Job without a Provider ID
has an ambiguous submission outcome and becomes `UNKNOWN`. Artifact
registration, terminal Job state and Outbox creation use one transaction.

Only `async + cancellation=supported` is cancellable. Other cancel requests
return `media_cancel_not_supported` and leave the Job unchanged. Ambiguous
Provider cancellation becomes `CANCEL_UNKNOWN`.

## Harness, UI and progress

The model-facing CLI is a thin client: `sandbox-media prepare`, `status` and
`cancel`. It does not persist state, confirm a draft or call Providers
directly. Draft editing and confirmation remain owner-scoped API/UI actions.
Status and cancel use the Turn Capability through Gateway and are restricted to
Jobs in that Capability's Thread. Progress travels
through durable events and SSE; the model is resumed only at terminal state.

In-progress cards use distinct animation: image skeleton/shimmer, video
film-frame/scan, audio waveform/level. Real percentages include their source;
otherwise show phase plus indeterminate animation. Details opens a modal with
type, task ID, safe Provider/model, phase, progress source, timestamps,
cancelability, timeline, artifact metadata and actionable errors.

## Outbox and continuation

Worker writes terminal `media_jobs` state and a `media_job_outbox` row in one
PostgreSQL transaction. Redis/Event Bus only wakes Broker. Broker startup and
periodic recovery scan pending/expired Outbox rows and terminal Jobs without a
continuation. A unique `(job_id, terminal_event_version)` makes at-least-once
delivery safe. Broker creates one internal continuation Turn with filtered
artifact references; it never replays the original prompt.

## Provider integration checklist

1. Add a typed adapter, route, credential environment reference and capability
   catalog.
2. Define canonical translation, limits, idempotency and reconciliation.
3. Run a real minimal request per enabled model; validate MIME, size and media
   metadata; record only sanitized evidence.
4. Add tests for schema, route, auth, timeout, unknown outcome, recovery,
   cancellation refusal and confirmation idempotency.
5. Add real OpenSandbox and browser cases for confirmation, progress, restart,
   artifact export, cleanup and continuation.

## Confirmed local probes

The local `ai-tool` catalog was inspected. These minimal probes succeeded and
are eligible for the first remote catalog:

| kind | model | adapter | capability |
| --- | --- | --- | --- |
| image | `seedream-5.0` | Ark image | platform async |
| video | `sed2` | Ark video v2 | async Provider |
| audio | `speech-2.8-turbo` | MiniMax T2A | async Provider |

Other local models remain disabled until individually probed. Reports include
revision, model, route/config mode, elapsed time, result validation and cleanup
state, never keys, signed URLs or raw prompts.
