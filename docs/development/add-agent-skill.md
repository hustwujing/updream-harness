# Adding an Agent Skill

## Applies to

Required when adding reusable instructions, scripts, references or assets that
a sandboxed Codex, Claude or DeepSeek Harness should discover and apply.

## Current state

This repository does not currently own a versioned Skill bundle or a common
Skill installer. Native Harness source images may contain built-in Skills, and
Harness state is checkpointed under `/workspace/state`, but that is not a
product-level distribution contract. Do not claim a Skill is available to all
Harnesses based on one source image or one retained sandbox.

Before the first repository-owned Skill, implement an explicit distribution
contract rather than copying files ad hoc during a test.

## Required Skill manifest

Each Skill needs a stable ID, version, supported Harnesses, entry instruction,
owned scripts/assets, required commands/libraries, network needs and test cases.
Skill text must describe task behavior; credentials and private service tokens
must remain Gateway capabilities.

The repository should keep one canonical Skill source plus a small
Harness-specific installer/adapter for each native discovery format. Verify the
actual discovery directory and precedence with the pinned native client version
before documenting it. Do not assume Codex, Claude and DeepSeek use identical
paths or metadata.

## Persistence and upgrades

Skill installation must define the interaction with checkpointed state:

- image-bundled version available in every fresh sandbox;
- whether a Thread checkpoint may override it;
- how a newer image upgrades an older retained/checkpointed copy;
- how removed Skills stop being discoverable;
- how version used by a Turn is observable without logging prompts/content.

Prefer immutable image-bundled sources and deterministic materialization at
Runtime configuration. Never mutate another Thread's checkpoint or a shared
warm sandbox.

## Safety

- Scripts execute inside the sandbox as the unprivileged Runtime user.
- Skill output follows normal workspace/artifact rules.
- Public network uses the capability proxy and allowlist.
- Provider actions require scoped Gateway routes and budgets.
- A Skill cannot widen approval, model, filesystem or credential authority.
- Instructions must not ask a Harness to reveal hidden state or bypass Runtime
  controls.

## Completion

Test manifest validation and installation for every declared Harness. In real
OpenSandbox cases, prove discovery in a fresh sandbox, execution, checkpoint,
resume in a different sandbox, version upgrade behavior and cleanup. Record the
Skill version and Runtime image digest. A response that happens to follow the
Skill is insufficient; verify native discovery evidence and expected artifact
or tool behavior.
