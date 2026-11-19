# Model Budget Estimation And Interaction Lock Design

## Intent

Large visual context must not be misreported as Provider rate limiting, native
Harness context management must work through the Capability Gateway, and a user
decision must become non-editable immediately after submission.

## Goals

- Estimate model input tokens from protocol structure instead of treating each
  JSON transport byte as one token.
- Estimate inline image cost from decoded image dimensions; never count Base64
  transport expansion as text tokens.
- Preserve the hard per-Capability token budget and durable reservation ledger.
- Expose local `model_token_budget_exhausted` separately from Provider HTTP 429.
- Lock or replace interaction controls immediately after the user submits.
- Restore controls only after a definite submission failure.
- Let Codex, Claude Code and DeepSeek Harness own compaction, image offload,
  tool-result pruning and durable conversation state.
- Proxy each Harness's bounded native auxiliary endpoints without introducing a
  platform summary format.
- Inject the 23 KB platform developer instructions once per DeepSeek Session,
  rather than repeating them in every user message.

## Non-goals

- This does not increase model budgets or bypass Capability authorization.
- It does not attempt exact Provider billing tokenization.
- It does not change interaction delivery idempotency or Runtime receipts.
- It does not add a shared platform summarizer or rewrite native Harness history.

## Native Harness Context Ownership

The Runtime persists each native state directory in the existing workspace
checkpoint. The Gateway treats native compaction as model protocol traffic and
must preserve unknown request fields, response items and headers:

- Codex app-server owns automatic compaction. The Gateway supports `/responses`
  and `/responses/compact`; `context_management`, compaction triggers and
  compaction output items pass through unchanged.
- Claude Code owns micro-compaction and automatic compaction. The Gateway
  supports `/messages` plus the read-only `/messages/count_tokens` auxiliary
  endpoint. Claude's full response content remains native session state.
- DeepSeek Harness `sdk` owns `compaction-basic`, image offload, tool-result
  pruning and JSONL session checkpointing. Its compaction calls continue through
  `/chat/completions`; DeepSeek's stateless upstream cache is not treated as a
  substitute for Harness compaction.

Generation endpoints use the durable reservation ledger. Token-counting is an
authenticated, request-counted auxiliary call but does not reserve generation
tokens or create a visible model activity. Standalone Codex compaction reserves
the remaining grant like a generation request but does not inject an unsupported
`max_output_tokens` field into its body.

Skills remain filesystem capabilities. Materializing a selected Skill does not
paste its body into every user message; each native Harness decides when to read
it. Uploaded images likewise remain files until a native image tool reads them.

## Model Input Estimate

The Gateway parses the already bounded JSON body and computes a conservative
input estimate:

- ordinary UTF-8 text and JSON structure: `ceil(bytes / 3)`;
- inline PNG/JPEG/GIF/WebP images: decoded dimensions converted to conservative
  512px tiles, with a bounded per-image floor and ceiling;
- remote/file image references: a conservative fixed visual allowance;
- unknown binary data: decoded byte length divided by three;
- malformed or unrecognized structures: fall back to the current raw byte
  ceiling, never a lower unverified estimate.

The estimate affects only the preflight output allowance. Durable finalization
continues charging Provider-reported input plus output usage.

Local budget rejection uses stable code `model_token_budget_exhausted`; native
Runtime adapters must preserve it before generic HTTP 429 mapping. Provider 429
continues to use `model_rate_limited`.

## Interaction UI

Clicking approve, reject or submit creates an optimistic local receipt before
the network call starts. The editable fields and action buttons are replaced by
a read-only `正在确认送达` summary. On success, the durable `responding` or
`responded` state replaces the optimistic receipt. On definite request failure,
the optimistic receipt is cleared and the original controls return with an
error. Repeated clicks cannot create another idempotency key while locked.

## Verification

- Gateway tests cover large inline images, text-only payloads, malformed data
  fallback and hard budget rejection.
- Adapter tests distinguish local budget exhaustion from Provider rate limit.
- Browser tests assert immediate removal of editable fields, duplicate-click
  suppression, failure restoration and durable responding state after refresh.
- A deployed Gateway probe confirms a large contact sheet no longer receives a
  local false 429 while genuine budget exhaustion remains blocked.
- Protocol tests cover Codex standalone compaction, Claude token counting and
  rejection of every other model-route suffix.
- Runtime tests prove DeepSeek sends the full platform instructions only on the
  first input and resumes the same native Session for later inputs.
- Checkpoint coverage proves all three native state roots, including compacted
  history and image-offload records, remain inside the persisted workspace.
