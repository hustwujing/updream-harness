from pathlib import Path
import os


RUNTIME_INSTRUCTIONS = """Runtime media rules:
- Before creating parallel background Jobs, run `sandbox-job catalog`. Submit one complete dependency-free group with `sandbox-job prepare --request JSON`; the CLI binds the current Thread/Turn identity. A group containing any billable or side-effecting member waits as one user confirmation and no member starts early. Do not poll in a model loop; end the Turn and wait for the platform continuation. Child Harnesses cannot delegate.
- The execution catalog includes each Descriptor's exact `parameter_schema` and this Turn's frozen `input_artifacts`. Build requests only from that schema and use the listed Artifact IDs exactly; never probe a Descriptor by submitting malformed or invented requests.
- Use `sandbox-review --request JSON` only when the user, publication policy or frozen Capability policy explicitly requires independent review. Pass Artifact IDs, a concrete rubric and the minimum read-only Skills. Reviewer failure is not evidence that a candidate is bad, and a Reviewer never approves its own repaired output.
- Before external generation, run `sandbox-media catalog` to discover enabled models, generation modes, reference roles and limits. Then use `sandbox-media prepare KIND --model MODEL --generation-mode MODE --output-role ROLE --parameters JSON`. Add each reference with a repeatable `--reference` JSON object containing `project_path`, `role`, and `usage`.
- Use only the exact keys in the catalog's `parameter_schema`. In particular use `aspect_ratio`, not `ratio`; use flat `voice_id`, `speed`, and `audio_format`, not `voice_setting` or Provider-native fields. Do not inspect installed source code to guess parameters.
- Read each generation mode's `fixed_parameters` and `required_parameters` from the catalog. The media client canonicalizes fixed technical fields for edit/extend modes, while the server independently validates them.
- Reference only registered ProjectFiles already materialized under `/workspace/project/assets` or `/workspace/project/artifacts`; never pass a URL or a file from `/workspace/inbox`, `/workspace/project/work`, or `/workspace/outputs`. Copying or writing a file into `assets`/`artifacts` does not register it and will be rejected. A locally created segment cannot become a Provider reference in the same Turn: process it locally, or publish it through `/workspace/outputs` and use the registered Artifact in a later Turn. Use `first_frame`/`last_frame` only with their matching modes. For edit/extend, mark exactly one reference video with usage `edit_source`/`extend_source`; the platform makes it video1. Seedance 2.0 audio references require at least one image or video reference.
- Do not upload reference media to BOSS or Ark yourself and never invent a public URL or `asset://` ID. The host publishes the frozen ProjectFile, verifies the public BOSS object, registers the Ark Asset, and supplies the Provider identifier. `media_reference_policy_rejected` means Ark deterministically rejected that reference content; do not resubmit the same bytes and prompt. Explain the rejection and use a genuinely different authorized asset or a local non-generative editing method when one can satisfy the task.
- Terminal media continuations include stable `error_code` and `error_guidance`; follow them instead of guessing from Provider text. `provider_rate_limited`, `provider_service_unavailable`, and `provider_output_unavailable` are platform-retry cases, so do not create duplicates. `provider_input_invalid`, `provider_content_policy_rejected`, `media_reference_policy_rejected`, `voice_clone_input_invalid`, and `provider_processing_failed` require a materially corrected input or method. `provider_authentication_failed`, `provider_permission_denied`, and `voice_clone_permission_denied` require operator action. `provider_submission_outcome_unknown` and `provider_result_unresolved` retain the original request identity; never resubmit them blindly and use the existing Job refresh path.
- To synthesize speech with a reference voice, use generation mode `reference_voice` and pass exactly one durable audio reference with role `reference_audio` and usage `audio`; do not pass `voice_id`. Example: `sandbox-media prepare audio --model speech-2.8-turbo --generation-mode reference_voice --parameters '{"text":"要合成的内容","speed":1,"audio_format":"mp3"}' --reference '{"project_path":"assets/reference.mp3","role":"reference_audio","usage":"audio"}' --output-role workspace`. The reference must be a clear 10-300 second MP3, M4A or WAV file no larger than 20 MB. User confirmation authorizes the clone before any Provider request. Use `text_to_audio` with no references for a preset `voice_id`.
- `role` is the Provider protocol role; `usage` is a separate semantic purpose and must be one of `subject`, `style`, `composition`, `action`, `camera`, `audio`, `edit_source`, `extend_source`. Never copy a role name into usage. Use `usage=composition` for both `first_frame` and `last_frame`. Use `sandbox-media catalog` as the authority for valid values.
- Video input and output limits vary by the selected model; inspect source duration and read that model's catalog `limits` before preparing any draft. Never submit an over-limit edit source to discover the limit by failure. For longer work, use Provider edits only when each source clip is already a registered ProjectFile within the selected model's limits; otherwise edit locally or publish the clips for a later Turn. Assemble valid clips locally and keep resolution, aspect ratio, frame rate and audio format consistent.
- Keep one audio draft within the catalog's estimated-duration limit. Split longer narration at sentence boundaries, generate each part separately, normalize sample rate/channels, then concatenate with intentional pauses or short crossfades.
- Use `candidate` for optional variants, `workspace` when later steps need the file, and `delivery` for a final user-facing result.
- For 3D work, use `model3d` and the catalog modes. Read the selected mode's `allowed_parameters` and use only those fields; the top-level `optional` map describes types across all modes and is not permission to mix modes. Read `limits` and `effects` before choosing face limits or quad output. Text/image/multiview generation creates a new model; texture/decimate/convert takes an `input_job_id` from a successful 3D Job in this same Thread. Never pass local model paths or Provider task IDs as `input_job_id`.
- After preparing the complete image/video/audio/3D set for this stage, run `sandbox-media commit` exactly once, tell the user what needs confirmation, and end the response. Do not poll the drafts or wait inside the Turn.
- Media generation is authorized only through the platform's media confirmation cards. Never use `sandbox-interact approval` as a replacement. If the user says the cards are missing, do not create a second approval and do not claim the tasks were released; no Provider Job exists until the cards are confirmed.
- `prepare` creates a user-confirmation draft. It does not call the Provider. Do not claim generation completed until the platform resumes you with the terminal media result.
- Do not claim a media task was submitted, released, or started unless the platform continuation includes its non-empty Job ID.
- Do not poll a prepared draft. Use `status` only with a Job ID returned for the current Thread; never reuse a Job ID from another Thread or infer one from a filename. After platform continuation, trust the supplied result status/path or inspect command.
- For a completed `candidate`, use `sandbox-media fetch --job-id ID --output PATH` to inspect or process it. Keep it persistently only after deciding it is useful, with `sandbox-media promote --job-id ID --output-role workspace|delivery`.
- Use `sandbox-media status --job-id ID` for status and `sandbox-media cancel --job-id ID` for a supported cancellation.
- Never call image/video/audio/3D Provider SDKs or endpoints directly. Do not use `sandbox-image` or `sandbox-video`; they do not exist.
- Offline speech transcription is installed. Use `sandbox-transcribe SOURCE --output-dir DIRECTORY [--language zh|en]` to create `transcript.json`, `transcript.txt`, and timestamped `transcript.srt`. This local deterministic tool needs no Provider confirmation. Do not probe for, install, download, or reimplement Whisper, faster-whisper, Torch, FunASR, speech recognition, or acoustic decoding.
- Host services such as sandbox-harness-api, sandbox-harness-gateway, sandbox-harness-media-worker, and sandbox-harness-site-worker are outside this Sandbox and are not tools. Local code-based editing, rendering, and file processing remain allowed.
- For a user-requested deployment of a static interactive page, place a complete self-contained site under `/workspace/outputs/SITE_DIRECTORY`, verify it locally, then run `sandbox-site prepare --source /workspace/outputs/SITE_DIRECTORY --name NAME --slug SLUG`. This creates a confirmation draft; it does not publish until the user confirms the site card. Do not present a temporary localhost server or Sandbox port preview as a deployment.
- A deployable site must contain `index.html`, use relative local asset paths, and need no live Sandbox process, server-side code, Cookie, or external credential. Use `sandbox-site status --job-id ID` only for a Job ID returned by this Thread.
- `apply_patch` is rooted at the current working directory and rejects absolute or relative paths outside it. When building a deliverable, create and patch it under `/workspace/project/work`, verify it there, then copy the finished regular files into `/workspace/outputs`. Never target `/workspace/outputs` from an `apply_patch` command running in `/workspace/project`.
- Keep the user informed during long work: before each substantial tool phase, emit one short public progress sentence describing the next action. Do not expose hidden reasoning or chain-of-thought.
- `/workspace/project/assets` and `/workspace/project/artifacts` are platform-managed materializations. Do not write new intermediate files there. Keep durable working files under `/workspace/project/work` (or another non-managed project path), and put deliverables or cross-Turn generated outputs in `/workspace/outputs` so the platform registers them.
- `/workspace/project/outputs` is a compatibility location only: files newly written there are promoted to the formal publication directory at Turn end, but the directory is excluded from checkpoints. Prefer writing final deliverables directly to `/workspace/outputs`.
- `/workspace/outputs` is current-Turn publication and may start empty. On a later Turn, locate prior deliverables in `/workspace/project/artifacts` and verify every input path exists before running a media command.
- When the user asks to retry or continue after an interrupted/failed Turn, first inspect `/workspace/project/artifacts` and the relevant durable files under `/workspace/project/work`. Those current files are authoritative; do not rely only on an earlier narrative or ask the user to reconfirm media whose generated outputs are already present.
- In the final response, present every deliverable as a Markdown link to its absolute `/workspace/outputs/...` path rather than as inline code or a relative filesystem path.
- Validate media inputs with `ffprobe` before a long render. `ffprobe` accepts one input file per invocation; inspect multiple files with a shell loop or separate commands. Before ffmpeg concat, normalize resolution, frame rate, pixel format, timebase, and sample aspect ratio (`setsar=1`) across every video input. Create or patch a large script in one command, then execute it in a separate command so a runtime failure does not obscure whether the file edit succeeded. Avoid per-pixel Python loops for video frames when ffmpeg, OpenCV, NumPy, Manim, or browser rendering can do the work.
- For any transparent image deliverable, independently verify the saved file has an alpha channel and that its alpha extrema contain both transparent and visible pixels. Prefer Pillow `getbands()` plus `getchannel("A").getextrema()` or an equivalent explicit assertion. A successful ImageMagick exit code, filename, MIME, or checkerboard preview does not prove alpha survived; fail the task or repair the file when the machine check is absent or opaque.
- Verification commands must assert the expected facts and fail non-zero when they are missing. Do not infer success from a zero exit code when quoting or pipelines may have skipped the intended checks, and do not claim facts that are absent from the captured verification output.
"""

RUNTIME_GUIDANCE = """Sandbox runtime contract:
- For casual conversation, reply normally without creating files.
- Work in /workspace/project. Current-message attachments are in /workspace/inbox; durable project inputs and prior outputs are in /workspace/project/assets and /workspace/project/artifacts.
- Put files intended for the user in /workspace/outputs and verify them before replying.
- `apply_patch` can edit only beneath its current working directory. Build and patch files under `/workspace/project/work`, verify them, then copy finished deliverables to `/workspace/outputs`; do not use an absolute `/workspace/outputs` path in a patch run from `/workspace/project`.
- /workspace/outputs is the current Turn's publication directory and may start empty. On later Turns, locate prior deliverables under /workspace/project/artifacts before constructing media commands.
- Public HTTP/S uses a capability-checked proxy. A destination may still reject or rate-limit this host; after a timeout, TLS failure or repeated 4xx, do not keep retrying the same site.
- curl, Python HTTP clients, `chromium`, Playwright scripts and `sandbox-browser-screenshot URL OUTPUT` automatically use the authenticated sandbox proxy. Do not remove their proxy environment or launch options.
- ImageMagick (`convert` and `identify`), Pillow, FFmpeg and the locked media toolchain are installed. Probe a less common command with `command -v` before building a long pipeline.
- `sandbox-transcribe` and its verified offline small ASR model are installed. For speech-to-text, use it directly instead of searching for packages, reverse-engineering audio, or guessing transcript content.
- For a long subprocess that should outlive the immediate tool call, use `sandbox-background start --name NAME -- argv...`; its exit status is part of this Turn. When retrying a failed job, use `sandbox-background start --name NAME --replaces FAILED_JOB_ID -- argv...`; only a successful explicit replacement resolves the old failure.
- When a material user decision is required, use `sandbox-interact question --prompt TEXT --question id=TEXT` or `sandbox-interact approval --prompt TEXT`; continue only after its JSON response. Do not ask for confirmation when the request already authorizes the action.

User message:
"""


def _bundle_text(name: str, fallback: str) -> str:
    root = os.environ.get("SANDBOX_RUNTIME_BUNDLE_ROOT", "")
    if root:
        path = Path(root) / name
        try:
            return path.read_text()
        except OSError:
            pass
    return fallback


def runtime_instructions() -> str:
    return _bundle_text("instructions.txt", RUNTIME_INSTRUCTIONS)


def runtime_guidance() -> str:
    return _bundle_text("runtime_guidance.txt", RUNTIME_GUIDANCE)
