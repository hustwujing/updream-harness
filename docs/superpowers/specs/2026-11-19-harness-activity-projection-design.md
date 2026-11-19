# Harness Activity Projection Design

## Intent

Child Harness streams are transport-level evidence. They must remain durable for
recovery and debugging, but users must see stable semantic operations rather
than one execution step per text or tool-output chunk.

## Chosen approach

Keep `job_activities` as the immutable raw event ledger and add a durable
`job_display_activities` projection. Projection happens in the same database
transaction as raw append. Display records have a stable semantic key and are
updated in place as matching events arrive.

Alternatives rejected:

- Frontend adjacent-row deduplication leaves API counts, recovery and other
  clients incorrect.
- Dropping or batching raw events loses audit and reconnect evidence.
- Parsing natural-language output to infer steps makes behavior model- and
  language-dependent.

## Projection rules

- `text_delta` remains raw and never creates a display step. Child summaries
  continue to read the raw ledger.
- `tool_started`, `tool_output` and `tool_finished` with one `tool_use_id`
  update one display record. Output is bounded and stored in its detail payload.
- Plan progress and structured step lifecycle events update a stable activity
  key when the native event supplies an ID. Events without identity remain
  separate rather than being guessed from natural language.
- Protocol bookkeeping such as session initialization, input completion and
  final response snapshots is hidden from display activities.
- Warnings and errors always remain separate visible records.
- Raw and display rows preserve first/last sequence, timestamps and raw event
  count. Status and elapsed time use the semantic operation lifecycle.

## API and UI

`GET /api/jobs/{job_id}/activities` returns projected display activities by
default. `view=raw` returns the immutable ledger for diagnostics. Display rows
carry stable `id`, first `sequence` and latest `cursor_sequence`; clients merge
by ID and advance by the maximum cursor sequence.

The execution graph activity count uses display rows. The details panel applies
one more presentation-only grouping layer over projected tool activities. It
groups tools by structured native metadata into user-facing phases such as
reading material, changing files, running processing commands, inspecting
images and calling external tools. One phase shows an operation count and can
be expanded to the individual projected tools. Failed tools, warnings, errors
and user waits never enter a group and remain independently visible.

The technical-details toggle may show raw event order, but its first level must
still use localized titles and bounded summaries. It must not dump native event
JSON, internal absolute paths or transport field names into the timeline. The
immutable raw API remains available for diagnostics. No model text, command
text or regular expression is used to infer user intent; grouping uses native
tool type and structured `commandActions[].type` only.

## Controlled segmentation failures

`no_target_found` is a semantic terminal result for one frozen selector, not a
transient infrastructure failure. The same Job must not retry it. A replacement
Job may use a broader category or inspected box/points. Compound marks, text
wordmarks and uniform-background crop/matte requests must not be sent as
prompt-only semantic segmentation without inspection: use explicit geometry
when semantic matting is required, or a scoped `media-processing` Child Harness
for deterministic crop/background removal and validate alpha, bounds and
excluded content.

## Recovery and bounds

Projection is idempotent under raw event replay and is rebuilt per appended raw
event. Payloads and tool-output tails are bounded. Existing raw rows can be
projected lazily when a display view is first requested, so deployment does not
require replaying every historical Job synchronously.
