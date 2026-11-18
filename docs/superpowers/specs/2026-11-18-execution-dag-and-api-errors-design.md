# Execution DAG and API Error Design

## Intent

Make the execution graph explain the real task topology at a glance, and prevent
proxy HTML error pages from appearing in product UI during transient API
unavailability.

## Approaches

- Keep the serpentine list and add connectors: rejected because branches and
  joins remain ambiguous.
- Render a real left-to-right DAG from the existing factual `nodes` and `edges`:
  selected because it exposes delegation, parallel work, review and decisions.
- Use a swimlane timeline: rejected for the primary view because it emphasizes
  duration and location rather than dependency.

## Execution Graph

The browser renders only factual edges returned by the execution-graph API. A
Dagre layout ranks nodes left to right and SVG paths connect them. The default
view shows the current Turn; users can switch Turns and expand history. Job
groups collapse their member Jobs by default when the graph would otherwise be
large, and expose a deterministic expand control. Node cards retain state,
location, duration and retry information; selecting one shows the existing
activity and artifact details.

Dragging blank canvas space pans the graph. Desktop users may also drag nodes to
adjust their local positions; manual positions persist only in browser session
storage and never modify server facts or edges. Mobile uses single-pointer canvas
panning, node taps for selection and a detail area below it. Every node type opens
the same non-navigation detail surface with role, title, state, location, timing,
retry count and type; Job nodes additionally show activities and artifacts. Node
selection never submits a form, opens a URL or replaces the current page. Active nodes and the top-bar
entry animate only while both the Turn and node are non-terminal. Empty,
truncated and refresh-error states remain explicit.

## API Errors

`api.js` remains the sole response-error mapping layer. JSON API errors keep
their stable code mapping. Non-JSON responses, HTML responses, network failures
and HTTP 502/503/504 become bounded public messages and never expose response
bodies. Background refresh retains last-known-good slices and automatically
retries; a transient service outage is described as recovery in progress.

## Verification

Unit tests cover response classification and deterministic DAG layout. Browser
tests cover real branches and joins, edge rendering, Turn filtering, group
collapse, canvas/node dragging, non-navigation details for every node type,
terminal animation closure, desktop/mobile overflow and proxy HTML
suppression. Build and deployed-ingress browser checks must pass without page
errors.

## Approved Follow-up: Child Capability and Wrapped Graph Corrections

- Compact Child delegation remains least-privilege. It must validate frozen
  Skill dependency, capability and network requirements against the parent
  authorization scope before submission; unsupported requirements fail before
  a Job or Sandbox is created.
- A deliverable that requires a registered Operation such as `image.segment`
  remains an Operation Job owned by the parent JobGroup. It is not converted to
  a Child Harness objective and Child Harnesses remain unable to recursively
  create Jobs.
- Skills such as `codex-ppt` that require Provider access and nested page work
  cannot silently run under an incompatible ordinary Child profile. The
  Primary retains that work unless a compatible bounded profile exists.
- `result_mode=artifacts` compiles to a non-empty output contract. A Child that
  reports a blocker or produces no required output cannot become `SUCCEEDED`.
- The orphan reaper treats non-terminal Job identities as live throughout the
  Sandbox creation-to-acceptance window.
- Job-scoped model activity carries `job_id`; terminalizing the origin Turn
  does not interrupt model requests still owned by a non-terminal Job.
- Execution-node detail values are rendered structurally and protected by a
  local error boundary, so selecting a node cannot blank the page.
- DAG ranks wrap to viewport-width serpentine bands. Odd bands reverse visual
  direction, edges retain factual direction, and the detail pane never covers
  the last visible rank.
