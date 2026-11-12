# Client API

The routes below are implemented by `src/sandbox_harness/app.py`. Every resource
route is scoped to the signed browser owner; foreign resources return 404.

## Service and catalog

```text
GET /api/health
GET /api/session
GET /api/catalog/harnesses
```

`GET /api/session` reports ingress, whether account authentication is required,
and the current account when present. FRPC ingress requires login; direct
ingress may continue with its signed anonymous browser owner.

## Account authentication

```text
POST /api/auth/login
POST /api/auth/logout
POST /api/auth/change-username
POST /api/auth/change-password
```

There is no public registration endpoint. Account administration uses the host
CLI. Username/password changes require the current password; password change
revokes other account Sessions.

## Threads

```text
POST   /api/threads
GET    /api/threads?archived=false|true
GET    /api/threads/{thread_id}
POST   /api/threads/{thread_id}/read
POST   /api/threads/{thread_id}/archive
POST   /api/threads/{thread_id}/unarchive
DELETE /api/threads/{thread_id}
```

Creating a Thread freezes its Harness. A Turn may choose another configured
model/reasoning level for that Harness, but switching Harness requires a new
Thread. Deletion rejects nonterminal work and removes the retained Thread
sandbox before deleting durable records.

The read endpoint acknowledges only the exact latest `COMPLETED` Turn shown to
the browser; a stale request cannot consume a newer unread marker.

Archiving rejects a Thread with nonterminal work and removes any retained
sandbox before returning. Archived Threads are excluded from the default list,
remain owner-scoped and readable, and reject creation of a new Turn until they
are unarchived.

## Turns, inputs and transcript

```text
POST /api/threads/{thread_id}/turns
GET  /api/threads/{thread_id}/turns
GET  /api/threads/{thread_id}/items?after=&limit=
GET  /api/turns/{turn_id}
POST /api/turns/{turn_id}/cancel
POST /api/turns/{turn_id}/inputs
GET  /api/turns/{turn_id}/inputs
```

Turn creation accepts `start`, `queue` and `steer` in the request model, but an
actual steer is delivered through the active Turn's `/inputs` endpoint;
`steer` on the creation endpoint is rejected. Each create/input request has an
idempotency key. A transport loss after input submission records `unknown`
instead of automatically replaying it.

The API derives `network_access` from the authenticated principal rather than
the request body. A valid account Session freezes `public`; an anonymous direct
submission freezes `restricted`. The returned Turn exposes the frozen
value, and an idempotency-key replay through a different ingress is rejected.

## Events

```text
GET /api/turns/{turn_id}/events?after=       # SSE
GET /api/turns/{turn_id}/events/history?after=&limit=
```

SSE event names are the values of `domain.EventKind`, including text, step,
tool, artifact, warning/error, waiting, done and state events. The server honors
both `after` and `Last-Event-ID`, reads PostgreSQL first, uses Redis only to wait
for new data and closes after terminal state once rows are drained.

## Interactions and background jobs

```text
GET  /api/threads/{thread_id}/interactions
POST /api/interactions/{interaction_id}/respond
GET  /api/turns/{turn_id}/background-jobs
POST /api/turns/{turn_id}/background-jobs/{job_id}/cancel
```

Interaction responses are execution-bound and idempotent. Background jobs are
Runtime-supervised processes belonging to the active execution.

## Uploads, artifacts and project files

```text
POST   /api/uploads?name=&mime=
GET    /api/artifacts/{artifact_id}?inline=
GET    /api/threads/{thread_id}/project-files
POST   /api/threads/{thread_id}/project-files?path=&mime=
GET    /api/threads/{thread_id}/project-files/history?path=
GET    /api/project-files/{file_id}?inline=
POST   /api/project-files/{file_id}/rename
DELETE /api/project-files/{file_id}
```

Uploads and downloads enforce ownership, safe names/paths, byte limits and
content-addressed integrity. Inline rendering is limited to approved media/text
types and uses a sandboxing Content-Security-Policy.

## Media generation and reference uploads

```text
GET    /api/catalog/media
POST   /api/threads/{thread_id}/media/reference-uploads
POST   /api/media/reference-uploads/{upload_id}/complete
POST   /api/threads/{thread_id}/media/drafts
GET    /api/threads/{thread_id}/media/drafts
PATCH  /api/media/drafts/{draft_id}
POST   /api/media/drafts/{draft_id}/confirm
POST   /api/media/drafts/{draft_id}/cancel
GET    /api/threads/{thread_id}/media/jobs
GET    /api/media/jobs/{job_id}
POST   /api/media/jobs/{job_id}/cancel
```

Reference upload creation accepts `name`, `kind`, `mime`, `size` and `sha256`.
It returns a 15-minute `PUT` URL plus required headers, a stable `object_key`
and public URL. The caller uploads exactly once and then calls `complete`.
Completion checks the owner, ticket expiry, BOSS metadata, byte count, SHA-256,
actual media type and decodability before registering a ProjectFile. A repeated
PUT may return 412 because reference object keys are immutable; completion may
still succeed when the existing object matches the ticket.

Draft `generation_mode` and `references` are validated before confirmation.
References identify a managed ProjectFile by ID or path and provide only a
role/usage. Callers cannot provide Provider URLs or Ark Asset IDs.

## Runtime API

The in-sandbox Runtime API is private to `RuntimeClient`; it includes configure,
checkpoint restore/save, attachment/project materialization, Turn start/resume,
input, interaction, background job, cancel, diagnostics and artifact endpoints.
It is not a browser API and must not be exposed through nginx.
