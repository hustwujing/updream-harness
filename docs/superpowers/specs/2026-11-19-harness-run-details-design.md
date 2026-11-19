# Harness Run Details Design

## Intent

Clicking a Primary or Child Harness node must show a user-facing account of
what that Harness is doing. Raw transport events, protocol names and generic
node facts are not part of the product view.

## Chosen approach

Keep `job_activities` as the immutable diagnostic ledger and
`job_display_activities` as the stable semantic operation projection. Add an
explicit Harness detail contract to graph nodes and render a dedicated Harness
panel from projected activities. Do not expose a raw-event toggle in the
ordinary UI.

Alternatives rejected:

- Frontend command-text or regex interpretation is model-dependent and can lie.
- Model-generated progress narration cannot be the source of runtime truth.
- Rendering raw events with friendlier labels still produces an engineering
  log rather than a product timeline.

## Harness panel

The panel is one continuous view with no tabs or diagnostic switch:

1. Objective, current stage, state, execution location and live/final elapsed
   time.
2. Child-return progress when a wait barrier exists for the Harness Turn.
3. A semantic timeline built only from projected plan, tool, wait, artifact,
   warning and error records. Model streaming chunks never create rows.
4. Output Artifacts with preview/download links.

Projected tool calls are grouped by structured native tool type and
`commandActions[].type`. Groups default closed, running groups breathe, and
failures remain independent and visible. A running Harness with no active tool
shows one live `等待模型响应` stage rather than appearing idle.

## Graph contract

Primary and Child Harness graph nodes expose `harness_view`, `objective`,
`harness` and `model`. Child Harness identity is determined by frozen Job kind,
not by public role text. Other node types retain their existing generic detail
panel.

## Safety and recovery

The raw API remains owner-scoped for operators and recovery code, but the
browser does not request it. Timeline state is rebuildable from durable display
activities after refresh. No stream chunk, command output, internal path or raw
JSON is rendered in the Harness panel.

## Product identity and role visuals

The user-facing product name is **Updream Conductor**. The mark combines a
conductor baton with converging execution nodes, expressing one user-directed
system coordinating Harnesses, tools and external providers.

Every graph node exposes a stable `visual_role` enum. Fifteen public roles map
to distinct Lucide icons while sharing six restrained color families: user,
Harness, coordination, execution, external service and review. Status remains a
separate small badge; a role icon never doubles as execution status.
