# Capability Names And Coverage Design

## Intent

Make every Provider capability name describe the work it authorizes, so the
Primary Harness cannot infer video, audio or 3D behavior from the misleading
`image.generate` descriptor. Add a real-machine coverage suite for every signed
Skill, registered Operation and enabled Provider adapter, with an extended
campaign for every Provider generation mode.

## Capability Model

Capabilities describe stable business actions, not vendors or model names:

| Capability | Kind | Allowed modes |
|---|---|---|
| `image.generate` | image | `text_to_image`, `reference_image` |
| `video.generate` | video | `text_to_video`, `first_frame`, `first_last_frame`, `multimodal_reference` |
| `video.edit` | video | `edit_video`, `extend_video` |
| `audio.generate` | audio | `text_to_audio`, `reference_voice` |
| `model3d.generate` | model3d | `text_to_model`, `image_to_model`, `multiview_to_model` |
| `model3d.transform` | model3d | `texture_model`, `decimate_model`, `convert_model` |

The Media Catalog remains the authority for enabled providers, models, adapter
implementations and their exact parameter limits. A Capability Descriptor
freezes kind, allowed modes, input roles, output role, side effects, billing,
retry, cancellation and reconciliation. Provider/model selection cannot widen
that Descriptor.

`image.generate` remains valid but is restricted to images. No generic media
request, video, audio or 3D request may pass through it. Vendor-specific IDs are
not capabilities because changing a model route must not change Skill grants or
business plans.

## Profile And Skill Grants

- `child-general` may grant all six capabilities explicitly.
- The full CPU Runtime dependency profile declares all six internal Provider
  capabilities but does not grant them unless selected by a frozen Child Job.
- Skills declare only the capabilities they require. Existing presentation
  Skills continue to require `image.generate`.
- Gateway tokens contain exact capability IDs; `/v1/media` validates that the
  requested kind and generation mode belong to the granted capability.
- The execution catalog shows the six descriptors independently with compact
  guidance. The Harness never has to infer capability semantics from model IDs.

## Coverage Suite

The suite adds eleven complex parallel cases:

1. `S01` Web and visual: `algorithmic-art`, `anatomy`, `canvas-design`,
   `diagram-design`, `frontend-design`, `web-artifacts-builder`, `webapp-testing`.
2. `S02` Brand and presentation: `brand-guidelines`, `theme-factory`,
   `drawio-skill`, `image-to-editable-ppt`, `codex-ppt`.
3. `S03` Data and office: `data-quality-audit`, `programmatic-eda`,
   `query-validation`, `xlsx`, `docx`, `pdf`.
4. `S04` Media and animation: `media-processing`, `manim-video`,
   `slack-gif-creator`.
5. `S05` Documentation and communication: `academy-guide`,
   `doc-and-modernize`, `doc-coauthoring`, `internal-comms`, `discernment-nudge`.
6. `S06` Agent engineering: `brainstorming`, `writing-plans`,
   `systematic-debugging`, `verification-before-completion`, `skill-creator`,
   `mcp-builder`, `claude-api`.
7. `O01` Visual Operations: `artifact.inspect`, `image.segment`,
   `image.decompose`, `media.render`.
8. `O02` Audio/video Operations: `audio.transcribe`, `audio.process`,
   `media.concat_sequence`, `media.render`.
9. `V01` required Provider matrix: all six enabled Provider adapters, including
   image and audio reference modes.
10. `V02` extended video matrix: every remaining Seedance generation mode.
11. `V03` extended 3D matrix: image/multiview generation followed by
    texture/decimate/convert transformations.

Every Skill case uses isolated Child Harness jobs with frozen `skill_ids` and a
deterministic output contract. A successful case requires signed Skill injection,
actual job activity, valid output artifacts and overlapping independent attempts.
Instruction-only Skills produce a bounded evidence document whose content is
validated against that Skill's declared test case.

Every Operation case asserts successful Job rows for the exact Descriptor IDs,
valid Artifact collections and the intended stage dependency. Inline
`artifact.inspect` is exercised inside a frozen Child grant and audited through
Gateway activity rather than submitted as a Durable Job.

Every Provider case asserts the capability ID, model, adapter, generation mode,
one external submission identity, group confirmation, result validation and
immutable Artifact publication. Dependent 3D transformations run in a second
group after their source Job IDs are terminal.

## Campaign Policy

- `S01`-`S06`, `O01`-`O02` and `V01` are required release cases.
- `V02` and `V03` are tagged expensive and run in the complete real-machine
  campaign, not in ordinary unit/browser gates.
- A Provider outage, policy refusal or quota failure is a non-program failure;
  schema, grant, duplicate submission, state recovery or UI failures are program
  failures.
- Product code may not contain case IDs, prompts, filenames or special branches.

## Non-goals

- Do not create one capability per vendor or model.
- Do not collapse every media action into a generic `media.generate` capability.
- Do not claim Skill coverage from manifest presence alone.
- Do not require all expensive Provider modes in fast local unit gates.

## Verification

Catalog and grant tests prove semantic separation and reject wrong kind/mode
requests before Draft creation. Coverage tests prove the union of the eleven
case declarations equals the current signed Skill, Operation and enabled
Provider-mode catalogs. Real-machine reports retain Job, Attempt, external
execution and Artifact evidence for each declared member.
