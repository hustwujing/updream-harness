# Client API

The public API is Harness-neutral. Codex app-server, Claude and DeepSeek native protocols are translated by runtime adapters into these Thread, Turn and Item resources.

## Threads

```text
POST   /api/threads
GET    /api/threads?cursor=&state=active|archived
GET    /api/threads/{thread_id}
PATCH  /api/threads/{thread_id}
POST   /api/threads/{thread_id}/archive
POST   /api/threads/{thread_id}/resume
```

Creating a Thread freezes the default Harness, model and reasoning effort. A Turn may explicitly select another configured profile.

## Turns

```text
POST   /api/threads/{thread_id}/turns
GET    /api/threads/{thread_id}/turns
GET    /api/turns/{turn_id}
POST   /api/turns/{turn_id}/interrupt
POST   /api/turns/{turn_id}/steer
```

`POST /turns` accepts an idempotency key, message, attachments and frozen Harness configuration. If another Turn is active, `delivery_mode=queue` persists the Turn without starting it; `delivery_mode=steer` writes a steering message to the active Runtime.

## Items and Events

```text
GET /api/threads/{thread_id}/items?after={sequence}&limit=200
GET /api/turns/{turn_id}/events?after={sequence}       # SSE
```

SSE event names:

```text
item.created
item.updated
item.delta
turn.started
turn.waiting
turn.completed
turn.failed
turn.interrupted
approval.requested
artifact.created
sandbox.status
```

Events are replayable by sequence. The UI renders durable Items and applies deltas; reconnecting does not duplicate transcript rows.

## Approvals

```text
POST /api/approvals/{approval_id}/respond
```

The implemented endpoints are:

```text
GET  /api/threads/{thread_id}/interactions
POST /api/interactions/{interaction_id}/respond
```

The platform itself does not invent approvals. It transports native Codex requests or explicit `sandbox-interact` requests from any Harness and returns the user's response to that exact active Turn, Runtime execution and Sandbox. Responses use an idempotency key; foreign owners, old executions and repeated responses with a different key are rejected.

## Attachments and Artifacts

```text
POST /api/uploads
GET  /api/artifacts/{artifact_id}
GET  /api/threads/{thread_id}/project-files
GET  /api/project-files/{file_id}
POST /api/threads/{thread_id}/project-files
GET  /api/threads/{thread_id}/project-files/history?path=
POST /api/project-files/{file_id}/rename
DELETE /api/project-files/{file_id}
```

Uploads are content-addressed before a Turn references them. Artifact downloads validate thread ownership, object hash and byte limits.
Attachments referenced by a Turn become versioned project assets. Exported Artifacts become versioned project artifacts. Project-file list and download endpoints enforce the same browser owner boundary as Threads and Artifacts.

## Background Jobs

```text
GET  /api/turns/{turn_id}/background-jobs
POST /api/turns/{turn_id}/background-jobs/{job_id}/cancel
```

Harnesses start jobs inside the Sandbox with `sandbox-background start --name NAME -- argv...`. Runtime uses an explicit argv without a host shell, constrains cwd to the project, owns the process group, drains a bounded log and holds the Turn open until all jobs finish. A failed, interrupted or cancelled job prevents a successful Turn result.

## Runtime Metadata

```text
GET /api/catalog/harnesses
GET /api/catalog/models?harness=codex
GET /api/threads/{thread_id}/sandbox
```

The host discovers model IDs from each configured upstream at startup and every five minutes, then filters them by native Harness protocol family. Claude exposes only `claude-*`, DeepSeek only `deepseek-*`, and Codex compatible GPT text/reasoning models. The last successful catalog is persisted under the isolated data root and the built-in catalog remains the final fallback. Catalog data is configuration, not an entitlement claim. A successful Turn proves the selected route worked for that request.
