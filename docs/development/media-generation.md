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

DashScope edit models use a narrower platform contract that is generated and
validated from the same adapter limits. Happyhorse accepts one 3-60 second
MP4/MOV source plus up to five reference images and produces at most 15 seconds.
Wan 2.7 accepts one 2-10 second MP4/MOV source plus up to four reference images.
Both expose only 720p/1080p, preserve source aspect and request original audio;
unsupported Provider-native fields are rejected before Draft creation.

MiniMax speech exposes two discriminated modes: `text_to_audio` accepts no
reference and may use a system `voice_id`; `reference_voice` requires exactly
one `reference_audio` with `usage=audio` and forbids `voice_id`. Voice cloning
references are frozen ProjectFile versions in MP3,
M4A or WAV format, 10-300 seconds and at most 20 MB. The confirmation card
states that confirmation asserts the user's right to use the referenced voice.
Worker uploads the frozen bytes with `purpose=voice_clone`, creates a
deterministic owner-scoped voice ID, and then submits the normal async TTS Job.
The Sandbox never receives Provider file IDs, cloned voice IDs or credentials.

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

The public catalog gives every 3D mode its own `allowed_parameters`; the
top-level `optional` map is only a cross-mode type dictionary. CLI and server
validate the same mode whitelist. Current generation support intentionally
keeps the verified H3.1 subset: `image_seed`, `model_seed`, `texture_seed`, UV
export and export orientation are supported, while segmented parts, meshopt
compression and alternate texture-model versions remain unavailable until
their output and browser compatibility are tested. `quad=true` declares FBX
output. `texture=false` deterministically sets `pbr=false` and rejects texture
quality/alignment/seed fields instead of letting the Provider silently change
the request. Tripo references accept only PNG, JPEG and WebP up to 20 MB.

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

The Harness never performs this publication and never supplies a URL. Ark
content-policy rejection during Asset activation is classified as
`media_reference_policy_rejected`, distinct from transport or storage failure.
It is deterministic for the same bytes and must not be retried unchanged;
browser and continuation surfaces preserve this category so the next Agent can
choose a different authorized input or a local editing path.

## Stable failure contract

Provider response text is diagnostic data, not orchestration control. Worker
maps image, video and speech failures to stable categories and persists the
sanitized detail separately. Job API, browser details and Agent continuation
all expose the same `error_code` and `error_guidance`:

- platform retry: `provider_rate_limited`, `provider_service_unavailable`,
  `provider_output_unavailable`;
- change input or method: `provider_input_invalid`,
  `provider_content_policy_rejected`, `media_reference_policy_rejected`,
  `provider_processing_failed`, `voice_clone_input_invalid`;
- operator action: `provider_authentication_failed`,
  `provider_permission_denied`, `voice_clone_permission_denied`;
- reconcile the original request without resubmission:
  `provider_submission_outcome_unknown`, `provider_result_unresolved`;
- terminal protocol/result defects: `provider_job_expired`,
  `provider_result_invalid`, `voice_clone_failed`.

Adapters may retain a sanitized Provider code in `error_detail`, but prompts,
browser logic and recovery decisions use only the stable category. New Provider
codes require a classifier regression test before changing retry behavior.

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
POST       /api/turns/{turn_id}/media/drafts/confirm
POST       /api/turns/{turn_id}/media/drafts/cancel
```

The confirmation card has media-specific fields (image ratio/quality,
video duration/resolution/references, audio voice/format) and a Details modal.
The modal is dismissible, stays over the conversation, and never shows keys,
private URLs or raw upstream responses. The server revalidates every parameter
and creates exactly one Job on confirm.

Drafts prepared and sealed by one Agent Turn form one authorization batch. The
browser presents one confirm/cancel action for the complete batch; the server
revalidates every member before one transaction creates any Jobs. An expired,
invalid or foreign member rejects the entire batch. Replaying the same batch
confirmation returns the existing ordered Jobs and never duplicates Provider
work. Individual Draft endpoints remain available only for an explicitly
separate item decision.

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
HTTP 429 creation responses are safe to retry because no task ID was issued;
the Worker honors `Retry-After`/`X-RateLimit-Reset` with bounded backoff. A
transport timeout or 5xx without a task ID remains `UNKNOWN` because the
Provider may have accepted the request.

API and Media Worker are separate processes. Every Worker instance has a
unique lease owner and persists an async Provider ID before polling. After a
restart it may resume only that ID. A `REQUESTING` Job without a Provider ID
has an ambiguous submission outcome and becomes `UNKNOWN`. Artifact
registration, terminal Job state and Outbox creation use one transaction.

Long-running MiniMax audio Jobs use bounded polling slices. One Worker claim
performs one Provider query, then releases the lease with a short retry delay
while the Provider still reports processing. The number of submitted,
nonterminal MiniMax tasks is capped independently from total Worker
concurrency, so a large batch cannot flood the Provider queue. Once the
configured reconciliation window expires, a still-processing task becomes
`UNKNOWN`; its existing Provider ID is retained and the request is never
resubmitted.

Voice clones are cached by `(owner_id, reference_sha256)` and fenced by a
separate lease, so concurrent drafts do not purchase the same clone twice. The
clone request includes a short preview synthesis, which makes its deterministic
voice ID reconcilable through `get_voice` if the clone response is lost. Cached
voices are checked before reuse after six days because MiniMax deletes a clone
that remains unused for seven days. A missing voice is recreated from the
frozen ProjectFile; an unavailable lookup defers the Job without submitting
TTS or cloning a second voice.

`POST /api/media/jobs/{job_id}/refresh` is an owner-scoped scheduling hint, not
a Provider proxy. It may make the next poll immediately eligible only when no
Worker holds an active lease. For a recoverable `UNKNOWN` audio Job, and for
legacy false `audio_timeout` failures that retain a Provider ID, it permits one
more Worker query. Success and explicit Provider failure become terminal;
continued processing or a transient query failure returns to `UNKNOWN` after
that bounded check. The browser never calls the Provider directly.

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
confirm a draft or call Providers directly. Prose and `sandbox-interact` also
cannot authorize a Provider call that has a media card; semantic outline/style
questions remain separate from Provider authorization. Draft editing and
confirmation remain owner-scoped API/UI actions.
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

## Controlled segmentation selection

`image.segment` uses SAM3 and treats `no_target_found` as a semantic terminal
result for one frozen selector. It must not retry the same selector. A successor
may create a replacement Job using a broader model-native category or inspected
box/points. Compound marks and text wordmarks are not reliable prompt-only
targets. Use inspected geometry when semantic matting is required; use a scoped
Child Harness with `media-processing` for geometric crop or uniform-background
removal, then validate alpha bounds and excluded content. Never silently replace
an explicitly requested named segmentation method with another algorithm.

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
| video | `happyhorse-video-edit` | DashScope Happyhorse | async Provider; `edit_video` only |
| video | `wan-2.7-video-edit` | DashScope Wan 2.7 | async Provider; `edit_video` only |
| audio | `speech-2.8-turbo` | MiniMax T2A | async Provider |
| 3D | `v3.1-20260211` | Tripo v3 | async Provider |

The two DashScope edit adapters were probed on 2026-11-13 with the same 12.6
second 720p H.264/AAC source. Happyhorse succeeded in 217.5 seconds and
preserved the full source duration and audio. Wan 2.7 succeeded in 839.1
seconds and preserved an AAC track, but its output was limited to 10 seconds.
These measurements are probe evidence only; public model guidance describes
capabilities and documented limits rather than routing around another
Provider's policy decision.

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
SANDBOX_HARNESS_TRIPO_SUBMIT_MAX_RETRIES=5
SANDBOX_HARNESS_TRIPO_TASK_TIMEOUT_SECONDS=1800
```

The key is absent from Runtime images, catalog responses, logs and raw error
details. Shared BOSS configuration remains: upload endpoint
`https://boss-shjd.biliapi.net`, model-facing public URL
`https://boss-shjd.biliapi.net/updream/aniforge/...`, server-only endpoint
`http://shjd-boss.bilibili.co`, region/bucket `updream`. Never pass the
server-only endpoint or a presigned PUT URL to a Provider.

## Durable Provider output boundary

Media execution is generation-fenced and heartbeats its lease across Provider
submission, polling, download, validation, object storage and terminal commit.
Every Worker-owned phase, Provider ID, error, lease release and terminal write
must carry the claimed generation; owner identity alone is never a fence.
Heartbeat rejection or database failure cancels the business coroutine before
it can issue another external call or write. The same rule applies inside
reference publication and voice-clone preparation.
Synchronous and asynchronous success follows the persisted sequence
`requesting -> response_received -> downloading -> object_staged -> committed`.
The immutable Provider result receipt is committed before download. Each
content-addressed staged output is committed before the terminal transaction.
A replacement Worker therefore reuses the receipt/object and never resubmits
after response receipt, download failure or object-write crash.

Provider-controlled output URLs use HTTPS/443 only and must match
`provider_output_allowlist`. Every redirect is revalidated. DNS resolution
rejects any non-public answer and the validated numeric address is pinned while
preserving Host/TLS SNI, preventing DNS rebinding. Credentials, fragments,
loopback/private/link-local/reserved addresses and alternate ports are rejected
before a request is sent. Responses stream into process-owned bounded staging
files; Content-Length and chunk limits are enforced before object binding, and
SHA-256 is computed during transfer. Reference publication streams from the
object store under the same bounded identity contract.

Catalog entries may advertise cancellation only when a real Provider cancel
adapter exists. The current catalog has no such adapter, so `supported` entries
are rejected at startup and the API never exposes a false cancel action.
Archival/deletion is blocked by active Media Jobs, unexpired pending reference
uploads and sealed/delivering continuation batches. Expired upload tickets stay
auditable but are not active obligations. Reference creation/completion and
archive/delete share the Thread lock, so no pending ticket can appear behind an
archive decision. Archived batches are blocked rather than retried; unarchive
returns them to sealed for delivery. Voice preparation is owned by its active
Media Job; voice rows have no independent Thread identity.
