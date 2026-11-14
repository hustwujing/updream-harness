# Adding an Agent Skill

## Applies to

Required when adding reusable instructions, scripts, references or assets that
a sandboxed Codex, Claude or DeepSeek Harness should discover and apply.

For the complete change, build and release sequence, read [Skill and Runtime
Image Lifecycle](skill-and-image-lifecycle.md). This document defines the
Skill distribution contract and safety rules; the lifecycle document is the
cross-layer checklist when a Skill also needs native dependencies.

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

## Layering and heavy rendering dependencies

Separate a Skill's instruction bundle from the tools it requires to execute:

- `SKILL.md`, references, examples, schemas and small deterministic scripts
  belong in the signed Content Bundle. They are materialized per Harness and do
  not require an image rebuild.
- Python packages used by Runtime-owned commands belong in the Runtime base
  dependency profile and `requirements-runtime.txt`.
- OS packages, fonts, compilers, browsers, codecs and native renderers belong
  in `foundation.Dockerfile` or a deliberately scoped optional Runtime image.
- A large, infrequently used dependency must not be added to the common
  foundation only for convenience. Give it a named dependency profile,
  explicit capability detection and an image-size/cold-start review. If the
  current routing cannot select an optional image, keep the Skill unavailable
  until that image contract exists; do not ship instructions that advertise a
  command which is absent at runtime.

The Skill manifest records the split through `profile`, `dependencies` and
`network`. A Skill is executable only when its declared commands and imports
are present in the selected image. Content Bundle materialization alone is not
evidence that a native renderer is installed.

When a Skill needs an image dependency, update the dependency profile and the
image probes in the same change. Do not merge a content-only Bundle that
advertises a command which the deployed image cannot execute.

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

For a Skill with a native or heavy renderer, the completion evidence must also
include a low-quality deterministic render, output validation (codec, duration
or file structure as applicable), a bounded timeout, and a clear failure when
the selected image does not provide the capability.
