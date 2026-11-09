# Code Execution Capability Validation

Date: 2026-11-09

Scope: real deployed API, PostgreSQL, Redis, BOSS/S3 objects, OpenSandbox/gVisor sandboxes, capability Gateway, public HTTP proxy and native Codex, Claude and DeepSeek Harnesses. Local fake backends were not used for the real cases below.

## Result

The first product capability is accepted: each Harness can receive uploaded files, write and execute code in OpenSandbox, stream public output and tool steps, export verifiable artifacts, persist its project and native session state, and resume that state in a different sandbox. All real cases finished without an unresolved program error.

| Case | Harness / model | Result | Duration | Tool items | Verified outputs |
| --- | --- | --- | ---: | ---: | --- |
| Video processing | Codex / `gpt-5.6` | passed | 129.4 s | 4 | 256x144 H.264/yuv420p + AAC MP4, 1.8-2.2 s; ffprobe JSON |
| Batch image processing | Claude / `claude-4.6-sonnet` | passed | 90.2 s | 10 | three 160x90 PNGs, one 480x90 contact sheet, SHA-256 report |
| Error repair, audio and network | DeepSeek / `deepseek-v4.1-flash` | passed | 49.8 s | 9 | captured initial Python failure; repaired 48 kHz mono PCM WAV; fetched `example.com`; JSON report |

Every case used the upload API, a real immutable runtime image, streamed events, a committed checkpoint, artifact export to object storage, artifact download and independent host-side validation. The downloaded files and hashes are recorded in `.data/first-capability-report.json` on the deployed node.

## Checkpoint Recovery

The API was restarted after the initial cases so attached sandboxes were deleted. A second Turn then read the script created by the first Turn from `/workspace/project` and exported its size and SHA-256. All three resumed in a different `sandbox_id`:

| Harness | Restored file | Result |
| --- | --- | --- |
| Codex | `process_video.py` | passed after the resume-command fix |
| Claude | `batch_images.py` | passed |
| DeepSeek | `process_audio.py` | passed |

The first Codex resume exposed a program defect: its resume command omitted `--skip-git-repo-check`, so Codex exited before processing the user message. The command now preserves both `--skip-git-repo-check` and the frozen model. The focused regression passed and runtime image `sandbox-harness/codex@sha256:5f920576043c28f21d47415fd92772b2fba26732797eff4f7245616f559df443` passed the real retry.

## Discovered Models

Models discovered dynamically from upstream catalogs were tested through their actual Harness rather than accepted from `/models` alone:

| Harness | Discovered model | Result |
| --- | --- | --- |
| Codex | `gpt-6-sol` | passed; wrote and executed Python; exported exact JSON |
| Claude | `claude-sonnet-5-5` | passed; wrote and executed Python; exported exact JSON |
| DeepSeek | `deepseek-v4-pro` | passed; wrote and executed Python; exported exact JSON |

## Browser And Isolation

- Public HTTP was reproduced with `crypto.randomUUID` unavailable. Sending now uses `crypto.getRandomValues`; a real Turn creation returned HTTP 200 with no page exception.
- The redundant top-right overflow menu was removed. New conversation remains in the sidebar and Workspace remains in the top bar.
- Browser identities use a signed, HttpOnly, SameSite cookie. Thread listing and access, Turn APIs, event replay, upload references and artifact downloads are owner-scoped.
- Existing test conversations migrated to `legacy-unowned`; a new browser sees zero conversations.
- A live cross-identity test created a Turn as identity A and received HTTP 404 when identity B requested A's Thread.
- Desktop, 600-pixel short desktop and mobile browser checks passed, including the composer viewport constraint.

## Persistent Workspace Boundary

`/workspace/project` and `/workspace/state` are checkpointed to object storage and restore across nodes. Current-Turn attachments are durable in object storage and rematerialized into `/workspace/inbox`; outputs are exported individually as durable artifacts.

Previous-Turn inbox files and artifacts are not automatically rematerialized into the next sandbox. They are not lost, but the Harness can use them in a later Turn only if it copied them into `/workspace/project` or the user attaches them again. The planned project-level persistent-file phase should add a content-addressed asset/artifact manifest and restore files into stable project paths without repeatedly packing media into the checkpoint tar.

## Automated Checks

- Python suite: 26 passed, one upstream Starlette deprecation warning.
- Browser suite: desktop, short desktop and mobile passed.
- Services: API, Gateway, network rule and nginx active; OpenSandbox warm pool ready.
