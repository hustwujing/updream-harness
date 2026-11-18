# Skill and Runtime Image Lifecycle

## Scope

Use this document when adding a Skill, changing Skill-owned scripts or
references, adding the dependencies a Skill needs, or changing the Runtime
image so a Harness can execute that Skill.

The repository has three separate delivery units:

| Unit | Owns | Requires an image build |
| --- | --- | --- |
| Content Bundle | `SKILL.md`, references, examples, schemas and small Skill scripts | No |
| Code Bundle | Runtime Python, Runtime CLIs and shared modules | No, unless Bootstrap or native dependencies change |
| Runtime image | OS packages, Python packages, Node packages, browsers, fonts, codecs, native renderers and Harness clients | Yes |

The authoritative source paths are `runtime/skills/`,
`runtime/skills/sources.json`, `runtime/skills/metadata.json`, `runtime/skill_bundle.py`,
`runtime/build_runtime_bundle.py`, the Runtime Dockerfiles and the probe
scripts. Do not install a Skill into a checkpoint or edit a Harness-specific
copy as a substitute for these sources.

## Change Classification

Classify the change before editing:

1. **Content-only:** instructions, references, examples, schemas or small
   deterministic scripts that use already available commands and imports.
2. **Runtime-code:** a Runtime CLI, parser, validator, event adapter or helper
   implemented in Python/Node and shipped through the signed Code Bundle.
3. **Image dependency:** an OS package, Python distribution, Node package,
   browser, font, codec, native library, renderer or Harness client.
4. **Cross-layer:** a Skill that needs both content and a new image/runtime
   capability. Manim is an example: its instructions are content, while Manim,
   LaTeX, dvisvgm, fonts and media libraries are image/runtime dependencies.

Never advertise a command in a Skill before the selected image exposes it.
Content Bundle materialization does not prove that a command or import exists.

## Adding A Skill

1. Copy or author one canonical directory under `runtime/skills/<skill-id>`.
2. Ensure `SKILL.md` has a stable `name`, a useful trigger description and
   instructions that do not grant authority or request credentials.
3. Record in `runtime/skills/sources.json`:
   - source repository and pinned commit;
   - version and license provenance;
   - supported Harnesses;
   - dependency profile and network policy;
   - representative test case.
4. Add the exact Skill ID to `runtime/skills/metadata.json` with complete
   summary, Harness, network, Capability and GPU metadata.
   Metadata describes requirements but cannot widen Profile/Gateway policy.
5. Add required files and representative artifact checks to
   `runtime/skill_smoke.py`.
6. Run `python3 runtime/skill_bundle.py freeze` and then `verify`. Review the
   resulting content digest and file count in `manifest.json`.
7. Verify materialization for Codex, Claude and DeepSeek, including checkpoint
   restore. Managed Skill links are read-only and must not be written through.

One Skill ID has one canonical source. Do not fork prompts or assets per
Harness. Harness-specific behavior belongs in the native adapter or the
capability contract.

## Dependency Layering

Use the narrowest layer that satisfies the runtime contract:

- **L1 Bootstrap:** minimal verifier/supervisor, non-root identity, health and
  process lifecycle. It contains no Skill, task code or credential.
- **L2 Common:** shared Runtime dependencies and generic CLI dispatch only.
- **L3 RuntimeDependencyProfile:** one registered immutable toolchain such as
  `general-cpu`, `media-cpu`, `office`, `web-browser`, `data-analysis` or
  `primary-full-cpu`. The Resolver selects the smallest authorized profile
  satisfying the selected Skills' declared dependencies.
- **L4 Harness:** exactly one native Codex, Claude or DeepSeek client. Final
  manifests form a digest-pinned `Profile x Harness` matrix while OCI reuses
  L1-L3 bytes.

Runtime Python/CLI changes use the signed Runtime Code Bundle. Skill content
uses the signed Skill Bundle. Neither is copied into an image layer.

For every image dependency:

- pin the distribution or OS package contract;
- preserve the configured domestic Debian, PyPI, npm and Playwright mirrors;
- add imports to `runtime/media_probe.py` when applicable;
- add commands/packages to `runtime/toolchain_probe.py`;
- regenerate `runtime/toolchain.lock.json` from the built image;
- add an image contract assertion and a UID 10001 command check.

The final image must retain runtime libraries, but it must not retain compiler
headers or temporary build packages that are only needed to compile a wheel.

## Verification Gates

Run checks in this order:

1. `python3 runtime/skill_bundle.py verify`.
2. Focused unit tests for the Skill, Runtime code and affected boundaries.
3. Image build from the changed Foundation/Base/Harness Dockerfiles.
4. `toolchain_probe.py --verify` and `media_probe.py` inside the image.
5. Native discovery and representative Skill artifact smoke as UID 10001.
6. For heavy/native renderers, a bounded low-quality render and output
   validation: codec, dimensions, duration or file structure as applicable.
7. A real OpenSandbox case covering execution, output registration, checkpoint,
   restore in another sandbox and cleanup when the change affects runtime
   state or artifacts.
8. The full Python and frontend suites required by the release checklist.

The smoke must fail clearly when a declared command/import is absent. A model
following the instructions, a `command -v` result alone, or a successful image
build without a real artifact is not sufficient evidence.

## Release And Rollback

Record the Content Bundle digest, Code Bundle digest, Runtime image digest and
Skill manifest version together. A Turn freezes these digests and does not
hot-update an active execution.

Content-only changes publish a new Content Bundle. Runtime-code changes publish
a new signed Code Bundle. Native dependency or Bootstrap changes require new
immutable image digests and the image contract checks. Never mutate an image
tag used by an active Turn.

If a probe or real case fails, do not publish a partial Skill. Keep the prior
Bundle/image available, record the failed evidence, and roll back by selecting
the previous immutable digest. Checkpoints must not be edited to hide a stale
Skill or dependency mismatch.

## Anti-Patterns

- Copying a Skill into `/workspace/state` or a checkpoint and calling it
  installed.
- Adding a native dependency only to the host API environment.
- Adding a shared dependency separately to each Harness image.
- Installing packages from a running sandbox.
- Hand-editing `manifest.json` or `toolchain.lock.json` to bypass verification.
- Omitting a Skill metadata row and relying on compiler defaults or Skill prose.
- Letting a Skill widen network, filesystem, model, approval or credential
  authority.
- Treating a successful `command -v` or JSON response as artifact validation.
