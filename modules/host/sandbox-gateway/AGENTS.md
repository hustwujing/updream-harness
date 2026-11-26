# Sandbox Gateway Module Rules

- Keep all authorization Turn-scoped; never accept browser identity as capability proof.
- Never log prompts, bodies, credentials, arbitrary headers or raw upstream errors.
- Provider failure must not fall back to host execution.
- Use the repository allowlist; add cross-domain work through durable commands.
- Preserve bounded request, token, proxy and telemetry limits.
- Complete with gateway/security tests and both provider route checks.
