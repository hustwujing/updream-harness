# API and Frontend Development

## Applies to

Read this for FastAPI routes, ownership behavior, SSE, transcript Items, browser
navigation, composer controls, interactions or file preview.

## API rules

`app.py` is the route authority; `docs/api.md` is descriptive. Every Thread,
Turn, Item, interaction, artifact and project-file operation is owner-scoped by
the signed browser identity. Return 404 rather than revealing a foreign object.

Write endpoints require idempotency where retry ambiguity exists. Attachment
ownership and aggregate byte limits are checked before scheduling. Object reads
verify size and digest. Inline preview is limited to approved MIME families and
returns a sandboxing Content-Security-Policy.

Static-site Artifact preview resolves sibling files only within the immutable
owning Turn and directory selected by an owner-scoped `index.html` Artifact.
Path traversal and foreign Artifact IDs return 404. Generated HTML uses both a
response CSP sandbox and an iframe sandbox without `allow-same-origin`.

SSE reads authoritative transcript/event batches from PostgreSQL. During an
active Turn it may also project bounded `live_text_delta` entries from the
owner-scoped Redis Stream. Honor `Last-Event-ID`, preserve durable event
sequence, filter live Runtime parts at or below the committed cursor, and close
only after a terminal Turn has no remaining rows. Reconnect first reconciles
the PostgreSQL Item and then overlays only higher Runtime sequences; correctness
must not depend on Redis.

## Browser rules

- OpenRaster, PSD and TIFF remain downloadable files; do not route their MIME
  types into a browser `<img>` preview. A separately published PNG preview uses
  the normal image path (`web/src/file-preview.js`).

- `api.js` is the API/error mapping layer; do not scatter raw fetch contracts
  through components.
- The Thread ID lives in the query string and browser history.
- Thread navigation commits the selection and URL only after the target's
  primary workspace payload loads. Failure preserves the previous Thread and
  URL and presents a retryable sidebar error.
- Refresh uses last-known-good state per data slice. A failed or malformed
  optional response marks that slice stale; it never becomes an authoritative
  empty list. Primary message/interaction/file/pause failures gate dependent
  controls until retry converges. Background polling and SSE schedule the same
  refresh path and clear the stale banner only after success.
- The composer remains available during active work for appended input. A
  Harness is fixed per Thread; model and reasoning behavior follows the current
  product controls and frozen Turn record.
- Stream follow occurs only near the bottom. User scrolling must not be pulled
  back by new output.
- Live transcript overlays deduplicate by execution and Runtime sequence. A
  durable refresh may replace their base text but may not duplicate or regress
  higher uncommitted fragments. Overlays bind to the server-provided Item ID so
  text after a tool cannot be appended to an earlier message from the same
  execution. Character progress counts Unicode code points
  from received public text and animates only toward an observed value.
- Only active Items animate. Terminal Items retain a fixed elapsed duration.
- Domain-specific terminal sets drive elapsed timers. Site retry exhaustion is
  terminal even though the Job remains manually recoverable.
- Activity rows summarize model waits and tools; agent public output belongs in
  the transcript.
- Child Job activity APIs return the semantic display projection by default.
  `view=raw` is an explicit diagnostic surface. Display updates merge by stable
  activity `id`; polling advances with `cursor_sequence`, not the first raw
  sequence. The default UI groups projected tools into expandable phases using
  structured native tool metadata, never command-text or model-text matching.
  Failed tools, warnings, errors and user waits remain independent rows. The
  browser never requests or exposes the raw view. Clicking a Primary or Child
  Harness shows objective, current stage, elapsed time, child progress, semantic
  timeline and outputs in one continuous panel. Never render native JSON,
  internal absolute paths or transport field names directly into the product UI.
- Execution Graph nodes carry a stable `visual_role`; role icons are distinct
  from their status badge. Frontend code must not infer a role icon from a
  translated display label.
- Every Harness node appends a compact aggregate token count to its status line.
  Its selected details expose ordinary input, cache-read input, cache-write
  input and output separately. The same panel shows one ordered round per
  unique `model_request_id`, including waiting, streaming, completed, failed
  and restart-interrupted states. A Child round is owned by its persisted
  `job_id`; product state never reads logs or OTLP. Updates to the same selected Harness animate
  from the currently rendered values; switching Harness identity and reduced
  motion update immediately. Animation is presentation-only and never changes
  persisted accounting.
- A terminal Turn with pending media Drafts is displayed as `等待确认 N 项媒体生成`,
  not as completed. Drafts sharing the same origin Turn use one batch decision;
  pending confirmation does not animate the execution graph as active work.
  Expired confirmation returns `media_confirmation_expired`; a cancelled,
  already-handled or otherwise changed confirmation returns
  `media_confirmation_state_changed`. Neither code permits a Provider submit.
  Natural-language continuation may prepare a new Draft but never authorizes it.
- Markdown renders no raw HTML. Sandbox paths become links only after resolving
  to owner-checked artifact/project-file APIs.
- Preview and download use API URLs, never object-store URLs.
- `index.html` Artifacts are interactive sites, not source-text previews. A
  successful output directory creates one digest-deduplicated deployment draft
  even when the Harness omitted `sandbox-site prepare`; partial/failed Turn
  publication must not create one.

## Error behavior

Public errors are stable and actionable. The browser includes the API request ID
when available. Do not expose raw Provider messages, credentials, internal URLs
or stack traces. A failed input receipt remains visible as failed/unknown rather
than silently creating a duplicate Turn.

Every user action owns busy and error state. Archive/delete/navigation,
confirmation, media refresh/cancel, site retry/rollback/unpublish and Workspace
file actions must resolve to visible success or visible recoverable failure;
never leave a rejected Promise in an event handler. Attachment and Workspace
batch uploads track each file independently, retain committed successes, and
offer retry/removal for failures. Open detail panels store an identity and
derive current data from refreshed props rather than freezing an old object.
An interaction response locks synchronously before the request starts and shows
the submitted action/answers as a read-only receipt. Only a definite POST
failure restores the editable controls. A successful POST followed by a refresh
failure remains locked until durable interaction state is observed, preventing
an ambiguous duplicate decision.
Generic Job filenames use persisted `local_job_id` only for presentation, so
multiple `foreground.png` outputs are distinguishable without renaming their
immutable Artifact objects. Explicit deliverable filenames remain unchanged.

## Required checks

- Build: `npm --prefix web run build`.
- API ownership/state tests relevant to the route.
- Browser scripts for navigation, IME, input status, interactions, unread
  markers, Markdown, project files and responsive layout as applicable.
- Desktop and mobile screenshots when layout or preview behavior changes.
- Verify no page errors, duplicate SSE streams or terminal animation loops.
