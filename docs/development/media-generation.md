# Media Generation Design and Provider Integration Standard

## Boundary

All image, video, audio and 3D generation is an asynchronous platform Job, even
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

Limits apply to one Provider Job, not to the final production. Seedance 2.0
clips are 4-15 seconds. Longer videos are storyboarded into clips and assembled
locally. MiniMax text must be shorter than 10,000 characters; this product also
caps one audio draft at an estimated 180 seconds and requires longer narration
to be split at sentence boundaries.

Reference modes are `reference_image` for Seedream and `first_frame`,
`first_last_frame`, `multimodal_reference`, `edit_video` and `extend_video` for
Seedance. A reference is always a frozen ProjectFile version and object digest;
the Harness supplies a managed project path, role and usage, never a URL.

Tripo modes are `text_to_model`, `image_to_model`, `multiview_to_model`,
`texture_model`, `decimate_model` and `convert_model`. Image inputs use the
same immutable public BOSS publication as Seedream. Post-processing accepts
only an `input_job_id` for a successful Tripo Job owned by the same user and
Thread; the host resolves it to the Provider task ID. Harness code cannot pass
a Provider task ID, URL or local path through this field.

## Reference publication

The internal content store remains on `http://shjd-boss.bilibili.co` and is
never visible to a Provider. Confirmed Jobs publish a content-addressed copy
through a presigned PUT generated for `https://boss-shjd.biliapi.net`:

```text
aniforge/image/<sha-prefix>/<sha256>.<ext>
aniforge/video/<sha-prefix>/<sha256>.<ext>
aniforge/audio/<sha-prefix>/<sha256>.<ext>
```

The stable model-facing URL is
`https://boss-shjd.biliapi.net/updream/<object-key>`. It is HTTPS, has no Cookie
or query signature, and is verified by size, SHA-256, MIME and BOSS metadata.
The PUT URL is never persisted as a model input. `If-None-Match: *` makes a
content-addressed publication immutable.

Seedream receives the stable BOSS URL. Seedance never receives that URL: the
Media Worker calls Ark `CreateAsset`, resumes `GetAsset` until `Active`, and
submits `asset://<ark_asset_id>`. `media_reference_assets` caches the stable URL
to Asset ID mapping. A deterministic Asset name derived from SHA-256 lets a
replacement Worker reconcile a lost CreateAsset response with `ListAssets`
instead of creating a duplicate.

Direct clients may use:

```text
POST /api/threads/{thread_id}/media/reference-uploads
PUT  <returned presigned BOSS URL>
POST /api/media/reference-uploads/{upload_id}/complete
```

Completion validates ownership, expiry, metadata and full content digest,
copies the immutable object into the existing content-addressed namespace and
registers an `assets/...` ProjectFile.

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

One Agent Turn may prepare any number of image, video, audio and 3D drafts. It then
calls `sandbox-media commit` once to seal that Turn's media batch. A sealed
batch rejects new drafts. Turn finalization also seals an open batch as a
recovery fallback when the model omits `commit`.

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

Tripo create responses are persisted before polling. A lost create response
without a task ID becomes `UNKNOWN` and is never automatically resubmitted.
Unknown Provider states are polled for a bounded interval and then release the
lease for later reconciliation. Tripo has separate concurrency through
`SANDBOX_HARNESS_TRIPO_WORKER_CONCURRENCY` and advertises cancellation as
unsupported because its public API has no cancellation endpoint.

API and Media Worker are separate processes. Every Worker instance has a
unique lease owner and persists an async Provider ID before polling. After a
restart it may resume only that ID. A `REQUESTING` Job without a Provider ID
has an ambiguous submission outcome and becomes `UNKNOWN`. Artifact
registration, terminal Job state and Outbox creation use one transaction.

Reference preparation uses `PREPARING_REFERENCES`, `WAITING_REFERENCE_ASSET`
and `REGISTERING_ASSETS` before `REQUESTING`. These phases are safe to reclaim.
Only a crash after the Provider request boundary becomes an unknown submission.
Ark `Processing` resumes the same Asset ID; `Failed`, timeout, missing
credentials and invalid media terminate before Seedance submission.

Only `async + cancellation=supported` is cancellable. Other cancel requests
return `media_cancel_not_supported` and leave the Job unchanged. Ambiguous
Provider cancellation becomes `CANCEL_UNKNOWN`.

## Harness, UI and progress

The model-facing CLI is a thin client: `sandbox-media catalog`, `prepare`,
`commit`, `status`, `cancel`, `fetch` and `promote`. It does not persist state,
confirm a draft or call Providers
directly. Draft editing and confirmation remain owner-scoped API/UI actions.
Draft confirmation defaults to 24 hours and is configurable through
`SANDBOX_HARNESS_MEDIA_DRAFT_TTL_SECONDS`. Generic Harness approval cannot
confirm media drafts. When confirmation creates an asynchronous Job, the
conversation task panel opens automatically once for that batch; a user's
manual collapse remains respected during later refreshes.
Status and cancel use the Turn Capability through Gateway and are restricted to
Jobs in that Capability's Thread. Progress travels
through durable events and SSE; the model is resumed only at terminal state.

In-progress cards use distinct animation: image skeleton/shimmer, video
film-frame/scan, audio waveform/level. Real percentages include their source;
otherwise show phase plus indeterminate animation. Details opens a modal with
type, task ID, safe Provider/model, phase, progress source, timestamps,
cancelability, timeline, artifact metadata and actionable errors.

## Outbox and continuation

Worker writes terminal `media_jobs` state, Artifact and a `media_job_outbox`
row in one PostgreSQL transaction. Redis/Event Bus is advisory. Broker polls
durable sealed `media_batches`; an expired batch lease returns to `sealed`, and
the continuation uses `media-continuation-{origin_turn_id}` as its idempotency
key.

A batch may continue only after the origin Turn is terminal, every draft is
confirmed, cancelled or expired, and every confirmed Job is terminal. The
continuation contains one result per draft, so mixed success/failure and an
all-cancelled batch both converge. It never replays the original prompt.
An all-expired batch creates a visible, idempotent Agent continuation that
recreates confirmation drafts; it is not silently marked as delivered. An
all-cancelled batch closes without another Agent invocation.

Generated bytes always become an immutable per-Turn Artifact. A successful 3D
Job commits the model, optional rendered preview, terminal state, Artifact
items, ProjectFiles and Outbox in one transaction. GLB outputs are validated by
signature and open in the Three.js browser viewer; converted formats remain
downloadable. `workspace` and
`delivery` outputs are also registered in the persistent project manifest in
the same terminal transaction. A `candidate` stays out of that manifest. The
successor Agent can inspect it with `sandbox-media fetch` and retain it only by
calling `sandbox-media promote --output-role workspace|delivery`; promotion is
thread-scoped and idempotent.

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
| 3D | `v3.1-20260211` | Tripo v3 | async Provider |

Other local models remain disabled until individually probed. Reports include
revision, model, route/config mode, elapsed time, result validation and cleanup
state, never keys, signed URLs or raw prompts.

Reference probes additionally passed with a stable BOSS Seedream input, an
Active Ark image Asset, an Active two-second H.264 video Asset, and Seedance 2.0
image/video `asset://` generations. Provider limits are enforced before Draft
creation: role combinations, 14 Seedream images, 9 Seedance images, 3 videos,
3 audio files, 15-second aggregate video/audio duration, MIME, dimensions,
pixel count, FPS, codec and per-file size.

Tripo host configuration is:

```dotenv
SANDBOX_HARNESS_TRIPO_BASE_URL=https://openapi.tripo3d.com/v3
SANDBOX_HARNESS_TRIPO_API_KEY=<secret>
SANDBOX_HARNESS_TRIPO_WORKER_CONCURRENCY=2
SANDBOX_HARNESS_TRIPO_POLL_SECONDS=2
SANDBOX_HARNESS_TRIPO_TASK_TIMEOUT_SECONDS=1800
```

The key is absent from Runtime images, catalog responses, logs and raw error
details. Shared BOSS configuration remains: upload endpoint
`https://boss-shjd.biliapi.net`, model-facing public URL
`https://boss-shjd.biliapi.net/updream/aniforge/...`, server-only endpoint
`http://shjd-boss.bilibili.co`, region/bucket `updream`. Never pass the
server-only endpoint or a presigned PUT URL to a Provider.
