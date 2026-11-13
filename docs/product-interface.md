# Product Interface

The web client follows the interaction hierarchy of an agent work client rather than a workflow dashboard. It is informed by the official Codex thread/turn model, streamed item events, queued and steering prompts, attachments and on-demand review surfaces.

References:

- https://developers.openai.com/blog/codex-as-a-platform
- https://developers.openai.com/blog/mastering-codex-remote-for-engineering
- https://developers.openai.com/siwc/token-sharing-open-source/codex-app-server

## Desktop Layout

```text
┌──────────────────┬──────────────────────────────────────────────┐
│ New thread       │ Thread title       Harness · Model · Sandbox │
│ Search           ├──────────────────────────────────────────────┤
│                  │                                              │
│ Today            │ User prompt                                  │
│  Thread A        │                                              │
│  Thread B        │ Agent public response                        │
│                  │  ▸ Worked for 42s                            │
│ Previous         │    command / tool / progress items            │
│  Thread C        │                                              │
│                  │ Artifacts / changed files summary             │
│ Archived         │                                              │
│                  ├──────────────────────────────────────────────┤
│ Settings         │ Attach  Prompt...     Harness Model  Send     │
└──────────────────┴──────────────────────────────────────────────┘
```

The primary surface is the transcript. Workspace files, artifacts, logs and Sandbox details open in a side sheet when requested; they do not permanently reduce transcript width.

## Transcript Items

Items are durable, ordered records rather than reconstructed UI from raw logs:

- user message
- agent message
- reasoning summary
- plan update
- command execution
- tool call and result
- file change
- artifact
- approval request and response
- warning or error

Tool and command items are compact rows grouped under one collapsible activity section for a Turn. Only active items animate. Completed items never retain a cursor or pulse.

Native questions and approvals appear as dedicated interaction rows. Questions render their declared fields or choices; approvals expose explicit allow and reject commands. The response control disables while submitting and a resolved row no longer animates.

## Composer

The composer remains visible while a Turn runs. A new prompt can be sent as:

- `steer`: deliver to the active Turn at the next Harness message boundary.
- `queue`: create the next Turn after the active Turn completes.

The new-Thread composer includes Harness, model and reasoning controls. After a
Thread is created its Harness is fixed; the conversation composer exposes model
and reasoning controls for the next Turn but does not offer a Harness switch.
Every Turn persists its selected model and native reasoning effort, and an
active Turn is never mutated by later UI choices.

## Scrolling

The transcript follows streaming output only while the user is already near the bottom. Scrolling upward disables follow mode and exposes a return-to-latest control. A streaming cursor appears only while a public agent message is receiving deltas.

## File and Artifact Review

Images and videos are grouped into a responsive thumbnail gallery in the
transcript: four columns on desktop and two on narrow screens. Video thumbnails
show a non-playing early frame with a play marker. Selecting a tile opens the
full-screen ownership-checked image/player preview; each tile keeps a separate
download action. GLB artifacts use an interactive Three.js viewer with orbit,
zoom and pan controls. Non-previewable converted 3D formats retain a direct
download action. Non-media artifacts remain compact file rows. The Workspace
opens images, videos and text files in the same preview; unsupported types
download directly. No surface exposes object-store URLs, Runtime credentials or
private Harness state.

The Workspace side sheet lists current project assets and artifacts, version
history and active supervised background jobs. Managed input/output labels show
the authoritative source and version. Users can compare text versions and
restore an old immutable version as a new current version.

Static interactive output prepared by a Harness appears as a dedicated
confirmation card. After confirmation, a collapsible deployment panel shows
validation, upload and activation progress independently of the Agent Turn.
Successful Jobs provide the public link. Workspace lists deployed sites with
current status, immutable version history, rollback and downline actions.

The right inspector switches between Workspace and Sandbox. Sandbox provides
overview, live files, terminal, processes, private port previews and audit
history. Observation remains available while an Agent runs. A write terminal
requires explicit takeover; “stop and take over” first lets the active Turn
finish cancellation cleanup. Releasing saves a checkpoint before Agent input is
enabled again. A stopped Sandbox continues to show persistent ProjectFiles and
offers explicit restore.
The file view is a lazy directory tree rather than a flattened recursive list.
The terminal can expand into a viewport overlay without creating another PTY or
losing scrollback. Workspace disk usage reports the bytes actually present; if
OpenSandbox has no disk resource limit, the UI says `未设置容量上限` instead of
showing the gVisor filesystem sentinel capacity.
When terminal work changes a managed input or artifact, release shows the
affected paths and requires discard, preserve under `project/work`, or save as
new formal versions. It never silently accepts or overwrites those changes.

The current Thread ID is stored in the page query string. Refresh and browser
history restore only Threads owned by the signed browser identity. Terminal
Threads can be archived or deleted from the sidebar; active Threads must first
finish or be stopped. The archive view has its own searchable list. Archived
Threads are read-only until restored and retain their transcript, files and
history.

The sidebar search filters the already loaded owner-scoped Thread summaries by
title, Harness, model and current status. Matching is case-insensitive and uses
Unicode compatibility normalization; an empty query restores the Today/Previous
grouping and a query with no matches shows an explicit empty result.

FRPC ingress shows the account login surface before loading user data. Direct
ingress remains anonymous by default and exposes optional login from the top
bar. Authenticated users can change username/password or log out from the same
account dialog. Username changes preserve all account-owned conversations.

## Responsive Behavior

On narrow screens the thread list and artifact viewer are independent drawers. The composer and transcript remain the main surface. Tool details default to collapsed, long lines wrap, and controls retain stable dimensions.
