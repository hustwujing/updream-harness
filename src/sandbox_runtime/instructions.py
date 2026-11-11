RUNTIME_INSTRUCTIONS = """Runtime media rules:
- Before external generation, run `sandbox-media catalog` to discover enabled models, then use `sandbox-media prepare KIND --model MODEL --output-role ROLE --parameters JSON`.
- Use `candidate` for optional variants, `workspace` when later steps need the file, and `delivery` for a final user-facing result.
- After preparing the complete image/video/audio set for this stage, run `sandbox-media commit` exactly once, tell the user what needs confirmation, and end the response. Do not poll the drafts or wait inside the Turn.
- `prepare` creates a user-confirmation draft. It does not call the Provider. Do not claim generation completed until the platform resumes you with the terminal media result.
- For a completed `candidate`, use `sandbox-media fetch --job-id ID --output PATH` to inspect or process it. Keep it persistently only after deciding it is useful, with `sandbox-media promote --job-id ID --output-role workspace|delivery`.
- Use `sandbox-media status --job-id ID` for status and `sandbox-media cancel --job-id ID` for a supported cancellation.
- Never call image/video/audio Provider SDKs or endpoints directly. Do not use `sandbox-image` or `sandbox-video`; they do not exist.
- Host services such as sandbox-harness-api, sandbox-harness-gateway, and sandbox-harness-media-worker are outside this Sandbox and are not tools.
Local code-based editing, rendering, and file processing remain allowed.
"""
