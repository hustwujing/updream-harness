# Tripo 3D Integration Report

## Release

- Source revision: `0047a2c4658b9c7cef70c6798a7596f5b94b864a`
- Test date: 2026-11-12
- API/Gateway/Media Worker: active, API health `ok`
- Tripo catalog model: `v3.1-20260211`
- Tripo worker concurrency: 2

Runtime image digests:

| Harness | Digest |
| --- | --- |
| Codex | `sha256:e5d3911dbab183c5f8b239c4693873b14537cb158a76899cd08c4fd47eba81f8` |
| Claude | `sha256:718ca86874dce0a832612ddb484833cb3bc0a7f2fd14b6b6bdbc91a028c4c8c2` |
| DeepSeek | `sha256:272f51b0ca8ded88c6fd1a3348955be3059a196e38440da4a0fd30ba6795598c` |

## Delivered behavior

- `text_to_model`, `image_to_model`, `multiview_to_model`, `texture_model`,
  `decimate_model` and `convert_model` are exposed through the existing
  confirmation Draft and durable Media Job protocol.
- Image references are frozen ProjectFile versions, published to stable public
  BOSS HTTPS URLs, and never expose the internal BOSS endpoint or presigned PUT
  URL to Tripo.
- Post-processing accepts a same-owner, same-Thread successful platform Job ID;
  only the host resolves it to the Tripo task ID.
- Provider task ID is persisted before polling. Submission ambiguity becomes
  `UNKNOWN`; an existing task is polled after Worker restart without resubmit.
- Model and preview are committed together as output rows, Artifact chat items,
  ProjectFiles and terminal Outbox. GLB uses signature validation and Three.js
  online preview.

## Real end-to-end case

The production entry created a Codex Turn that prepared a user-confirmed
text-to-3D Draft. After confirmation, the test waited for the Tripo task ID to
be durable, sent `SIGKILL` to the Media Worker, and let systemd start a new
Worker.

| Evidence | Result |
| --- | --- |
| Thread | `e3bb4336716448758a7bb2704124785c` |
| Origin Turn | `061c0555a3494153a7d29d7e70cfbc47` completed |
| Media Job | `e28a603f77b74139b2c54ff4a93bd493` succeeded |
| Worker restart | PID changed after `SIGKILL` |
| Provider events | 1 submitted, 1 resumed, 1 succeeded |
| Outputs | 1 valid GLB primary + 1 PNG preview |
| Credits | 20 |
| Continuation | `91b3f06dc81f4c718bf1d0be1f0b4ecb` completed |
| Persistent files | model and preview under `artifacts/3d/<job>/` |
| Elapsed | 140.87 seconds |

No duplicate Provider submission occurred. Both output Artifact items appeared
in the transcript and both ProjectFiles were available to the continuation.

## Automated verification

- Full Python suite: 296 passed.
- Focused storage, catalog, API, Gateway, Worker and deletion recovery suite:
  98 passed.
- Additional Tripo adapter tests cover image/multiview BOSS URL translation,
  post-processing Job resolution, converted format signatures and public URL
  rejection.
- Frontend production build passed.
- Browser confirmation case passed on desktop, 390 px and 320 px layouts.
- Three.js GLB viewer passed desktop/mobile framing and Canvas pixel validation
  with 168 sampled colors and no page errors.

## Boundary coverage

The deterministic suite rejects unknown fields, mode-incompatible parameters,
invalid reference counts/roles, oversized image inputs, topology-specific face
limits, cross-Thread post-processing sources, internal/non-HTTPS result URLs,
invalid model signatures and deletion of a Thread with active Media Jobs.
Cancellation remains explicitly unsupported because the Provider does not
document a cancellation endpoint.

The billable real case exercised text-to-3D and restart recovery. Image,
multiview, texture, decimate and convert request shapes are covered by
deterministic adapter/contract tests; they were not each billed as separate
real Provider generations in this release.
