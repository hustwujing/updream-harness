# Cross-Harness Context And Budget Design

## Intent

Codex, Claude and DeepSeek must keep their native session and compaction
semantics while the host Gateway applies one truthful, cache-aware Turn budget.
A local budget failure must never be presented as upstream rate limiting.

## Evidence

- Codex carried 113k-138k logical input tokens for fourteen successful model
  calls in one Turn. Its native auto-compaction threshold was unset and the
  custom provider did not declare remote compaction.
- Claude usage samples reported `input_tokens=2` while returning substantial
  output, because cache-read and cache-creation fields were ignored.
- DeepSeek exposes cache hit/miss fields and manages caching automatically; its
  Responses implementation does not support `context_management`.
- The shared Gateway parser currently retains only aggregate input/output.

## Approaches

Raising the 2M budget is rejected because it only delays the same failure.
Replacing all native histories with a host summary is rejected because it
breaks native session state and preserved reasoning. The selected approach
normalizes usage at the Gateway and leaves compaction with each Harness.

## Usage Contract

Every completed model request produces four non-overlapping dimensions:

- ordinary input;
- cache-read input;
- cache-write input;
- output.

Logical input is the sum of the three input dimensions. Budget charge is
ordinary input plus output plus cache-read and cache-write input multiplied by
configurable integer percentages. Defaults are 10% and 125%. These are policy
units, not a pricing promise. Raw logical input remains telemetry for context
and throughput analysis.

Before a response, only requested output is hard-reserved against the remaining
weighted budget because cache outcome is unknowable. The existing full-remaining
reservation still serializes concurrent calls. Settlement uses provider usage;
missing or malformed usage conservatively consumes the reservation.

## Harness Policy

- Codex config freezes an explicit auto-compaction limit and declares remote
  compaction unsupported, which selects Codex local compaction. A real probe
  showed the configured upstream returns 502 for `/responses/compact`.
- Claude Code keeps its native auto-compaction. Gateway parses
  `cache_read_input_tokens` and `cache_creation_input_tokens`.
- DeepSeek Harness keeps its native Session behavior. Gateway parses
  `cached_tokens` or prompt cache hit/miss fields and never sends unsupported
  `context_management`.

## Failure Identity

Gateway starts a durable model activity before budget reservation. A local
exhaustion terminalizes that activity as `model_token_budget_exhausted`.
When a native Harness reduces the same failure to generic HTTP 429, Broker uses
the durable model activity to restore the stable local code. Genuine upstream
429 remains `model_rate_limited`.

## User Failure Tip

The latest failed Turn exposes one dismissible tip below the top bar. Its text
comes only from the sanitized public Error Item, never `turn.error_detail`.
Dismissal is stored per Turn in browser local storage, so refresh does not
reopen an acknowledged failure; a later failed Turn remains visible. A
recovered, successful or active Turn removes the tip automatically.

## Non-goals

- Replacing Provider account TPM limits.
- Claiming exact monetary cost from policy weights.
- Implementing an independent host summarizer.
