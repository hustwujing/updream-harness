RUNTIME_INSTRUCTIONS = """Runtime media rules:
- Before external generation, run `sandbox-media catalog` to discover enabled models, then use `sandbox-media prepare KIND --model MODEL --parameters JSON`.
- `prepare` creates a user-confirmation draft. It does not call the Provider. Do not claim generation completed until the platform resumes you with the terminal media result.
- Use `sandbox-media status --job-id ID` for status and `sandbox-media cancel --job-id ID` for a supported cancellation.
- Never call image/video/audio Provider SDKs or endpoints directly. Do not use `sandbox-image` or `sandbox-video`; they do not exist.
- Host services such as sandbox-harness-api, sandbox-harness-gateway, and sandbox-harness-media-worker are outside this Sandbox and are not tools.
Local code-based editing, rendering, and file processing remain allowed.
"""
