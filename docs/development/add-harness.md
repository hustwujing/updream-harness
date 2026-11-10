# Adding a Harness

## Applies to

Required when introducing a Harness beyond Codex, Claude and DeepSeek or
changing one Harness's native protocol, resume or input behavior.

## Current hard-coded surfaces

Adding a Harness is not a catalog-only change. The current implementation fixes
the set in multiple places:

- `domain.Harness` enum and Pydantic request patterns;
- `HarnessCatalog`, whose constructor requires exactly the enum set;
- Settings image and warm-pool fields plus `image_for`;
- Broker capability `ROUTES`;
- pool image/target maps built by `app.components`;
- OpenSandbox metadata parsing;
- `adapters.command` and `normalize`;
- model route defaults, discovery compatibility and probe protocol;
- Runtime/Harness Dockerfile and image build loop;
- frontend labels/icons/default selection;
- tests and deployment environment image digests.

## Implementation sequence

1. Define the native process protocol: start, resume, appended input,
   interaction, terminal result and native thread identifier.
2. Extend the enum/contracts and catalog with public reasoning levels mapped to
   native values.
3. Add an adapter command and normalization fixtures. Preserve the shared
   Runtime event/result contract.
4. Add the immutable image and toolchain probe. Do not install or execute the
   Harness on the host.
5. Add Gateway route scope, upstream/credential reference, discovery filter and
   a minimal health probe matching the native API.
6. Add pool/settings/deployment configuration and UI identity.
7. Exercise initial Turn, resume in a new sandbox, native appended input,
   interaction, cancellation and recovery.

## Protocol requirements

- Public output must stream as shared `text_delta` events.
- Tool/command IDs must remain stable across start/progress/finish frames.
- An appended input must produce explicit completion evidence.
- Resume must use checkpointed native state and the stored native identifier.
- Unknown termination cannot be reported as success.
- Native approval/questions must bind to execution-scoped interactions.

## Completion

Unit fixtures cover representative native frames and malformed frames; Runtime
boundary tests pass; the image is pinned by digest; and real OpenSandbox cases
prove initial execution, resume, steer/input, cancel, checkpoint and cleanup.
