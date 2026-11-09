# Browser And Animation Rendering Validation

Date: 2026-11-09

Scope: real OpenSandbox/gVisor sandboxes using Chromium 156, Playwright 1.65, Three.js 0.187, esbuild 0.28, Mesa/ANGLE SwiftShader and FFmpeg. Results were exported through the deployed API and independently checked on the host.

## Runtime

- Node.js 22.14 and the JavaScript rendering packages are pinned in `runtime/toolchain.lock.json`.
- `sandbox-browser-render` loads local HTML, waits for explicit `window.__RENDER_READY__ === true`, captures console and page errors, calls deterministic `window.renderFrame(timeSeconds)`, records PNG frames and encodes H.264/yuv420p MP4.
- Multi-frame rendering fails when `renderFrame` is absent.
- Automatic frame directories live under `/tmp` and are removed after encoding; they cannot become exported Artifacts.
- Chromium uses a private writable HOME and software WebGL through ANGLE/SwiftShader. No host GPU is required.

## Real Cases

| Case | Harness / model | Result | Duration | Deterministic evidence |
| --- | --- | --- | ---: | --- |
| Canvas 2D and Chinese typography | Codex / `gpt-5.6` | passed | 135.3 s | 640x360, H.264/yuv420p, 60 frames, 2.0 s, 60 unique frame hashes |
| Three.js scene, lights, shadows and camera motion | Claude / `claude-sonnet-5-5` | passed | 86.0 s | WebGL true, ANGLE SwiftShader renderer, no page errors, 60 unique frames |
| Deliberate browser failure followed by repair | DeepSeek / `deepseek-v4-pro` | passed | 143.5 s | initial stderr retained, repaired diagnostics clean, 30 frames, 1.0 s, 30 unique hashes |

The downloaded artifact sizes and SHA-256 values are retained in `.data/browser-render-report-final.json` on the deployed node.

## Native Conversation Inputs

The same release was validated with a second user input submitted while each Harness was active:

| Harness | Delivery mode | Result |
| --- | --- | --- |
| Codex | `turn/steer` with `expectedTurnId` | passed |
| Claude | realtime stream-json input | passed |
| DeepSeek | same SDK Session at the next idle boundary | passed |

All cases used one public Turn and one sandbox ID. Inputs were persisted before delivery and reached `applied`. The second input created an independently downloaded text artifact.

## Capacity And Cleanup

- Warm targets are Codex 2, Claude 2 and DeepSeek 1.
- Product maximum is 12 sandboxes. Five are pristine warm capacity and at most seven are executing or attached; excess Turns wait for a slot.
- Claimed warm capacity is replenished with newly created pristine sandboxes.
- Used sandboxes are never returned to the public warm pool.
- Successful sessions may remain attached to the same Thread for 900 seconds. Expiry revokes access, terminates processes, deletes the exact sandbox and confirms absence.
- Cancellation, failure and service shutdown do not retain the sandbox.
