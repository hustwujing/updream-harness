# Sandbox Images Module Rules

- Pin OS/native dependencies and record the exact build inputs.
- Keep provider overlays behaviorally equivalent at the Runtime contract.
- Never publish `latest` as a release identity or reuse a digest for changed content.
- Validate UID/GID, tools, network policy and signed bundle activation.
- Complete with real create/command/delete tests on every affected provider.
