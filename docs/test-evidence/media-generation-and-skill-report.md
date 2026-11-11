# Runtime Skill and Media Generation Release Report

## Scope

This release bundles 25 pinned Skills into the Codex, Claude and DeepSeek
Runtime images, adds the media confirmation/Job/Artifact/Outbox flow, and runs
Provider execution in an independent `sandbox-harness-media-worker.service`.
No credential, signed Provider URL or raw Provider response is recorded here.

## Runtime images

The Runtime source revision is `c3e926a92b5a`. All images use immutable local
registry digests:

| Harness | Digest |
| --- | --- |
| Codex | `sha256:02c1dcb61abc1df8035275950c17c13eb2eb6a7bfe91d3af5dbde48adefba2df` |
| Claude | `sha256:ad7805e6b98dfeace301565bf3412d62a7423062d9d851b5d07153b809a3cafd` |
| DeepSeek | `sha256:ea26fc94666c99b80902decb4d6d5f0955862fc7acc3c1a9c121d81755884670` |

Each image passed the same UID 10001, network-disabled smoke. The smoke
materialized all 25 native Skill links for all three Harness layouts and
reported 25/25 passed. Executable evidence included draw.io validation/export,
Anatomy browser capture and Node test, diagram-design self-check/export, DOCX,
PPTX, XLSX formula recalc, PDF render, editable-PPT preparation, animated GIF,
54 fonts, MCP imports, Skill validation and web helper CLIs. Instruction-only
Skills were checked for their entry contract and required bundled resources.

## Real Provider results

Only the three previously probed catalog entries were enabled.

| Kind | Model | Result evidence |
| --- | --- | --- |
| Image | `seedream-5.0` | JPEG, 2048x2048, 98,849 bytes |
| Video | `sed2` | MP4 video stream, 5.04 seconds, 745,264 bytes |
| Audio | `speech-2.8-turbo` | MP3 audio stream, 2.55 seconds, 43,008 bytes |

All three reached `succeeded/committed/100%`; the stored byte count matched the
object, and terminal Outbox rows reached `delivered`. A bad upstream audio
`Content-Type: application/x-tar` exposed a bug: Worker trusted the header. The
fix validates signatures and media streams and now records `audio/mpeg`.

## Recovery evidence

| Fault | Evidence | Result |
| --- | --- | --- |
| API/Broker SIGKILL during a real video Provider Job | API PID changed; Media Worker PID did not; same external ID completed as MP4 | Passed |
| Media Worker SIGKILL after external ID persistence | API PID did not change; logs contain one `submitted`, one `resumed`, one `succeeded`; same external ID, 641,502-byte MP4 | Passed |
| OpenSandbox deletion during an audio Provider Job | Sandbox deletion converged; independent Worker completed the same external ID as a 41,984-byte MP3 | Passed |
| User disconnect/reconnect | Same owner Cookie recovered `200/succeeded`; a new identity received 404 | Passed |
| Turn recovery after API restart | New Broker acquired the lease and continued the same Runtime event sequence from 74 beyond 96 without replaying input | Passed |
| Warm Sandbox expires during a long Turn | Docker evidence showed the old Sandbox was killed exactly at its original one-hour `expiresAt`; bind renewal and active heartbeat renewal now extend it, including a real OpenSandbox renewal smoke | Passed |

Ambiguous submission is deliberately not replayed. A lease-expired
`requesting` Job without a persisted external ID becomes `unknown`; an async
Job with an external ID resumes polling. Worker lease owners are unique per
process. Artifact registration, terminal Job state and Outbox creation are one
PostgreSQL transaction, and media Job listing fetches Artifact IDs in one batch.

Media batches now seal the complete N-image/M-video/audio set before a single
continuation. The origin Turn must be terminal and every draft and confirmed
Job terminal. Cancelled and expired drafts are included, candidate outputs can
be inspected and explicitly promoted, and all-cancelled batches converge.

The Host release `c3e926a92b5a` also supports pasted attachments and lazy online
previews: PDF continuous pages, PPT/PPTX paging, DOCX scrolling, spreadsheet
sheet switching, matching document thumbnails and CSP-sandboxed SVG thumbnails.
The provided production PPT Artifact converted to a valid 207,429-byte PDF.

## Gates

- Python suite: `221 passed` after all service separation, recovery, MIME and
  ownership validation changes.
- Frontend production build: passed.
- Browser media Job Inspector: passed with no page errors.
- Browser media gallery: desktop 4 columns, mobile 2 columns, video thumbnail,
  SVG thumbnail, preview and download passed.
- Browser document suite: pasted file upload, four document thumbnails, PDF
  scrolling, PPT paging, DOCX scrolling and Excel scrolling/sheet switching
  passed with zero page errors.
- Browser media confirmation suite: desktop/mobile confirmation layout, long
  prompts, immediate post-confirm progress, Workspace details and automatic
  discovery of server-created continuation Turns passed with zero page errors.
- Media Worker concurrency continuously refills six slots; graceful restart
  releases leases. Runtime catalog responses include exact parameter schemas,
  and internal continuation payloads remain auditable without appearing as
  user-authored chat messages.
- Real OpenSandbox: final Codex digest started, exposed 25 Skills, executed
  `sandbox-media prepare`, then used a Turn Capability to execute
  `sandbox-media status` through Gateway against a same-Thread completed Job;
  the isolated Sandbox deleted cleanly.
