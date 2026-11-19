# Sandbox and Harness Development

## Applies to

Read this for Runtime APIs, native protocol adapters, images, workspace,
background jobs, interactions, attachments or outputs.

## Runtime contract

The host starts `sandbox-harness-runtime` in an immutable image digest. Runtime
owns the Harness subprocess and its stdout reader independently of the Broker's
stream connection. It journals normalized events, assigns monotonically
increasing sequence numbers and retains the terminal result for reattachment.

The Runtime HTTP API in `sandbox_runtime/app.py` is the host/data-plane
contract. Host calls go through `RuntimeClient`; do not make Broker code depend
on a native Harness protocol.

Each Turn freezes a content-addressed Runtime Code Bundle and Content Bundle.
The image starts only `sandbox_bootstrap` on port 8080. Before configuration,
the host injects the Turn's Code Bundle; Bootstrap verifies its Ed25519
signature, archive digest, Python ABI, dependency lock, allowed module paths and
every file hash, then starts the Runtime child on loopback port 18080 and streams
HTTP through to it. The Content Bundle is then injected through Runtime and
provides instructions and Skills. Both are excluded from checkpoints and
reapplied by digest after sandbox creation or restore.

Bootstrap refuses a Code Bundle switch while the child reports an active
execution. It supervises unexpected child exit and restarts the same digest.
Only signed `sandbox_runtime`, its required shared modules and declared CLI
packages are accepted; arbitrary package paths, links and special files are
rejected.

## Workspace ownership

```text
/workspace/project   checkpointed user work
/workspace/state     checkpointed native Harness state
/workspace/inbox     current input only
/workspace/outputs   current Turn publication only
/workspace/tmp       disposable
/run/harness         capability and Runtime configuration, never checkpointed
```

`PERSISTENT` in Runtime code is exactly `project` and `state`. Managed assets
and artifacts are restored from the host manifest. Outputs reset at a new
execution. Paths must pass `safe_relative`; symlinks, devices and unsafe archive
members must not become artifacts or checkpoint entries.

## Native Harness implementations

- Codex uses `codex_runner.py` and app-server control messages. It supports
  native steering and interactions.
- Claude uses CLI stream-json input/output and resumes with its native session
  identifier.
- DeepSeek uses `deepseek_runner.py` and a persistent SDK Session, accepting
  appended input at its controlled boundary. SDK `assistant/message` events are
  normalized as public text while the Run is active; the SDK
  `final_response` is a fallback snapshot for a Run that emitted no public
  assistant event.

Context history belongs to the selected native Harness and remains below
`/workspace/state` in the checkpoint. Codex app-server owns automatic and
standalone Responses compaction. Claude Code owns micro-compaction, automatic
compaction and transcript resume; its token-count request is a native auxiliary
call. DeepSeek Harness owns `compaction-basic`, image offload, tool-result
pruning and JSONL Session state. Platform developer instructions are added only
to the first prompt of a new DeepSeek Session; resumed Sessions and later user
inputs receive only their current message.

Runtime Skill directories and uploaded files are filesystem capabilities, not
text repeated by the host in every prompt. Each Harness decides when to read a
Skill or image. The Gateway supplies authenticated protocol access and durable
token accounting, but never summarizes or reconstructs native history.

`adapters.command` constructs commands and per-Harness environment. `normalize`
maps native output to the shared Runtime event contract. Unknown plain output is
public text/tool output; protocol changes must be tested with representative
native frames.

Text snapshot deduplication is per input rather than per Runtime process. This
matters for native appended input: text observed for one input must not suppress
the fallback response for a later input. Keep native message IDs in
`Execution.protocol_state` when a Harness can repeat or extend the same
snapshot.

## Process and input rules

- Execution creation and input submission are serialized.
- An input ID is bound to a body fingerprint. Exact replay is idempotent;
  conflicting reuse is rejected.
- EOF is not success for streaming Harnesses; pending inputs require explicit
  completion evidence.
- Cancellation stops the Harness process group and supervised background jobs.
- Long-lived work uses `sandbox-background`; user decisions use
  `sandbox-interact`. Both remain bound to the active execution.
- A completed or waiting outcome is not published until checkpoint succeeds.

## Image rules

OpenSandbox creation rejects mutable image references; deployment uses
`@sha256:` digests. `foundation.Dockerfile` owns OS/browser tools,
`base.Dockerfile` owns Runtime and common media dependencies, and Harness
Dockerfiles add native clients. `toolchain.lock.json` and probe scripts make the
foundation contract executable.

Python/media dependencies are installed from `runtime/requirements-runtime.txt`.
Runtime Python and CLI changes publish a signed Code Bundle; Skills and
instructions publish a signed Content Bundle. Neither requires an image build.
Native dependencies, helper executables, Harness clients, system packages,
Bootstrap or either Bundle verification protocol still require an image build.

Use the [Skill and Runtime Image Lifecycle](skill-and-image-lifecycle.md) when
a Skill crosses these boundaries. The Skill must be present in the Content
Bundle and its commands/imports must be proven in the selected final image;
there is no implicit dependency installation or silent image fallback.

Use [image dependency guidance](add-image-dependency.md) for package changes and
[Agent Skill guidance](add-agent-skill.md) for reusable Harness instructions or
assets. A dependency being present does not make a Skill discoverable, and a
Skill copied into one checkpoint is not an image-level capability.

## Required checks

- Adapter/native frames: `test_adapters.py`, `test_codex_runner.py`,
  `test_deepseek_runner.py`.
- Runtime concurrency and input boundaries: `test_runtime_boundaries.py`,
  `test_runtime_configuration.py`, `test_runtime_client.py`.
- Images and paths: `test_runtime_image_contract.py`, `test_runtime_paths.py`.
- Namespace/reconciliation: `test_sandbox_namespace.py`.
- A real OpenSandbox case for image, process, checkpoint, networking or native
  event-normalization changes. For assistant streaming, assert a durable
  `text_delta` in Turn history rather than relying only on the final UI text.
