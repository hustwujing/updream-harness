# Adding an Agent Skill

## Applies to

Required when adding reusable instructions, scripts, references or assets that
a sandboxed Codex, Claude or DeepSeek Harness should discover and apply.

## Distribution contract

The repository-owned bundle lives in `runtime/skills/`. `sources.json` records
upstream commits, versions, licenses, dependencies, network needs and tests.
`runtime/skill_bundle.py freeze` computes content digests and writes
`manifest.json`; image builds run `verify` and fail on drift.

`sandbox_runtime.skills` materializes the same immutable bundle into each
native Harness root:

```text
Codex     /workspace/state/codex/skills
Claude    /workspace/state/claude/skills  (CLAUDE_CONFIG_DIR)
DeepSeek  /workspace/state/dsh/skills     (DSH_HOME)
```

Entries link to the read-only image bundle. Checkpoints exclude links. Restore
defers installation until extraction completes and then replaces prior managed
copies, so old state cannot shadow a new image or write through a bundle link.

## Required Skill manifest

Each Skill needs a stable ID, version, pinned source commit, supported Harnesses,
entry instruction, owned scripts/assets, required commands/libraries, network
needs, license provenance and test cases.
Skill text must describe task behavior; credentials and private service tokens
must remain Gateway capabilities.

Keep one canonical source in `runtime/skills`; do not fork instructions per
Harness. Verify native discovery with pinned client versions on Harness upgrade.

## Persistence and upgrades

Skill installation must define the interaction with checkpointed state:

- image-bundled version is available in every fresh sandbox;
- a Thread checkpoint cannot override a managed ID;
- the marker removes managed IDs deleted from a newer bundle;
- `runtime.skills.materialized` records bounded bundle version/count metadata.

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

Test manifest validation and installation for every declared Harness. Run
dependency imports/commands as UID 10001 in every final image. In real
OpenSandbox cases, prove native discovery, representative artifact execution,
checkpoint, resume in a different sandbox, version upgrade and cleanup. Record
the bundle version and image digest. A response that merely follows the Skill
is insufficient evidence.
