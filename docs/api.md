# Client API

The routes below are implemented by `src/sandbox_harness/app.py`. Every resource
route is scoped to the signed browser owner; foreign resources return 404.

## Service and catalog

```text
GET /api/health
GET /api/session
GET /api/catalog/harnesses
```

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

## Runtime API

The in-sandbox Runtime API is private to `RuntimeClient`; it includes configure,
checkpoint restore/save, attachment/project materialization, Turn start/resume,
input, interaction, background job, cancel, diagnostics and artifact endpoints.
It is not a browser API and must not be exposed through nginx.
