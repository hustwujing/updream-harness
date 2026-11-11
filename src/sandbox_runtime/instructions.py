RUNTIME_INSTRUCTIONS = """Runtime media rules:
- Before external generation, run `sandbox-media catalog` to discover enabled models, then use `sandbox-media prepare KIND --model MODEL --output-role ROLE --parameters JSON`.
- Use only the exact keys in the catalog's `parameter_schema`. In particular use `aspect_ratio`, not `ratio`; use flat `voice_id`, `speed`, and `audio_format`, not `voice_setting` or Provider-native fields. Do not inspect installed source code to guess parameters.
- Use `candidate` for optional variants, `workspace` when later steps need the file, and `delivery` for a final user-facing result.
- After preparing the complete image/video/audio set for this stage, run `sandbox-media commit` exactly once, tell the user what needs confirmation, and end the response. Do not poll the drafts or wait inside the Turn.
- `prepare` creates a user-confirmation draft. It does not call the Provider. Do not claim generation completed until the platform resumes you with the terminal media result.
- For a completed `candidate`, use `sandbox-media fetch --job-id ID --output PATH` to inspect or process it. Keep it persistently only after deciding it is useful, with `sandbox-media promote --job-id ID --output-role workspace|delivery`.
- Use `sandbox-media status --job-id ID` for status and `sandbox-media cancel --job-id ID` for a supported cancellation.
- Never call image/video/audio Provider SDKs or endpoints directly. Do not use `sandbox-image` or `sandbox-video`; they do not exist.
- Host services such as sandbox-harness-api, sandbox-harness-gateway, and sandbox-harness-media-worker are outside this Sandbox and are not tools.
Local code-based editing, rendering, and file processing remain allowed.
- `/workspace/project/assets` and `/workspace/project/artifacts` are platform-managed materializations. Do not write new intermediate files there. Keep durable working files under `/workspace/project/work` (or another non-managed project path), and put deliverables or cross-Turn generated outputs in `/workspace/outputs` so the platform registers them.
- Validate media inputs with `ffprobe` before a long render. Create or patch a large script in one command, then execute it in a separate command so a runtime failure does not obscure whether the file edit succeeded. Avoid per-pixel Python loops for video frames when ffmpeg, OpenCV, NumPy, Manim, or browser rendering can do the work.
"""
