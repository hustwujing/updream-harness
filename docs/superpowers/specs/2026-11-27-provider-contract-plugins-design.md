# Provider Contract Plugins Design

## Intent

Executor-specific rules must be owned by the external Adapter that knows them.
The platform framework schedules durable work and projects stable errors; it
must not infer that Seedream, MiniMax, Kling, a Controlled Operation, Child
Harness, site publisher or a future executor share input and recovery rules.

## Goals

- Define one Provider contract protocol and registry.
- Move Ark image and MiniMax audio validation behind registered contracts.
- Keep stable error codes for orchestration while exposing one sanitized,
  bounded `error_reason` to the user, execution graph and model continuation.
- Reject an unregistered Provider adapter before confirmation or submission.
- Make a future Kling integration add its own contract and executor without
  changing the generic preflight or durable state machine.
- Apply the same public failure envelope to Provider, Operation, Child Harness,
  site publication and future asynchronous executors.

## Non-Goals

- Do not redesign Provider submission, polling, collection or billing in this
  change.
- Do not expose raw Provider bodies, URLs, request IDs, prompts or credentials.

## Architecture

`common/providers/contracts.py` defines the pure contract interface and
registry. Provider modules own public parameter metadata, local preflight and
request normalization. `MediaCatalog` resolves configured models and delegates
to the registered contract keyed by `adapter`; it contains no Provider-specific
validation branches. Every enabled Adapter must be registered; startup fails
when a contract is absent. There is no legacy fallback.

Workers retain the durable execution protocol. An executor uses its matching
contract to compile Provider parameters. Provider responses are converted to a
stable `error_code` plus a structured diagnostic envelope. Only an envelope
created by `external_error_detail()` can produce public `error_reason`.

Async Job outboxes, Media continuation, API views and execution graph all
project the same safe fields: `error_code`, `error_message`, `error_reason`,
`error_guidance`, phase, retry class and side-effect state.

The existing durable `JobAdapter` lifecycle remains the common asynchronous
execution interface for Operation, Provider and Child Harness work. Provider
contracts supply the Provider-specific pre-accept portion; site and other
workers use the same error envelope at their own Adapter boundary. A new
executor is incomplete unless its failure passes the cross-Adapter projection
contract.

## Current Provider Decisions

- Ark Seedream accepts explicit `WIDTHxHEIGHT` values. The deployed account was
  verified with `2560x1440`; `3K` is rejected and the current endpoint caps area
  at 4,624,220 pixels.
- MiniMax status `2039` means the deterministic voice ID already exists. If the
  ID is not visible to the current account, rotate the owner/reference revision
  once and retry with the same frozen audio. A second collision is terminal.

## Completion

Focused contract, worker, continuation, API projection, execution graph and
browser tests pass. Real Ark image and MiniMax reference-voice probes pass after
deployment. Existing unrelated real-case successes are retained.
