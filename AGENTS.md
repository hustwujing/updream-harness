# Development Rules

This repository is an independent product. Do not import source, database models, deployment scripts, runtime bundles or frontend assets from `video-use`, `distributed-agent-system` or `distributed-agent-sandbox-backend`.

## Product Boundary

- Every user message goes to the selected Codex, Claude or DeepSeek Harness inside OpenSandbox.
- The broker does not classify business intent, construct plans, review media or decide whether an artifact is correct.
- The host owns only identity, Session/Turn serialization, sandbox lifecycle, short-lived credentials, event transport, checkpoint transfer, artifact export and hard resource limits.
- OpenSandbox failure never falls back to a host model or local Agent.
- Sandboxes receive no long-lived model, Provider, database, Redis or object-storage credential.

## Engineering Rules

- Freeze harness, model, image digest, runtime protocol and capability scope on every Turn.
- Keep one active Turn per Session. Duplicate message keys return the existing Turn.
- Warm sandboxes are pristine and unassigned. A used sandbox may be retained only for the same Session TTL and is never returned to the public pool.
- Revoke the capability token and active Gateway connections before deleting a sandbox.
- Persist checkpoints before reporting a completed or waiting Turn. Failed checkpointing is a Turn failure.
- Stream events are ordered by `(turn_id, sequence)` and can be replayed after reconnect.
- Artifacts come only from `/workspace/outputs`; paths, links, special files and byte limits are validated without interpreting content quality.
- Tests use isolated data directories and fake lifecycle/upstream servers. Real OpenSandbox and Provider tests are separate and explicitly configured.

## Verification

Run focused tests first, then the full local suite:

```bash
.venv/bin/pytest
```

Document what was tested against fakes, a real sandbox, and real Providers separately.


## Logging and tracing

- Follow [the observability contract](docs/observability.md) when changing lifecycle, Gateway, persistence, cancellation or recovery code.
- Use the common structured logging helpers and standard OTel context; keep business IDs separate from Trace IDs and preserve recovery links.
- Never log prompts, generated content, file contents, credentials, arbitrary headers, raw exception messages or unfiltered URLs. New attributes require explicit allowlisting.
- Logging/export failures must not change business outcomes. Preserve durable state as the authority; test overload, abrupt interruption and missing receipts for affected flows.
- Include relevant targeted tests and identify which evidence comes from fakes, a real sandbox, and a real Provider.
