# General Delegation, Input State, And Skill Metadata Design

**Status:** Approved for implementation on 2026-11-18.

## Intent

Improve the platform generically rather than special-casing one Thread or media type. Primary Harnesses
remain responsible for business decomposition. The host supplies a smaller typed delegation interface,
validates and persists any delegation that occurs, and projects only persisted execution facts.

## Goals

- Make processed appended input monotonic and independent from a later Turn failure.
- Normalize upstream model errors before they enter typed Runtime results.
- Add a metadata-independent high-level delegation compiler over the existing durable Job protocol.
- Preserve `JobGroupCreate`, JobCoordinator, authorization snapshots, Worker recovery and continuation as
  the only authoritative delegated execution path.
- Complete machine-readable metadata for every installed Skill and make metadata validation part of bundle
  freeze/verify.
- Remove the overlapping generic `pptx` Skill and retain `codex-ppt` as the PPT creation workflow.
- Distinguish tool concurrency, Child Harness Jobs and Job Groups in public activity and UI wording.

## Non-Goals

- The host does not classify user intent or automatically construct a business DAG.
- Delegation is not mandatory for every Turn and no preflight decision table is added.
- Skill metadata is not required to run a generic Child Harness Job.
- Child Harnesses do not recursively delegate.
- This change does not silently preserve the removed `pptx` Skill contract.

## Follow-up Decisions: Parallelism, Retry, Authorization And GPU

The post-deployment review of Thread `bb283e45c33a49ccbdefb761f390028e`
confirmed the following direction for the next implementation phase:

1. **Use prompt guidance for semantic parallel decomposition.** Primary remains
   responsible for recognizing independent user goals and deciding when to call
   the existing high-level delegation and JobGroup tools. Do not add an
   `ExecutionPlan` protocol, a host-generated business DAG, or another required
   planning document before execution. Keep `JobGroup`, `Job` and `JobAttempt`
   as the durable execution model. Prompt changes must be generic and backed by
   decision-layer real cases rather than keywords for PPT, PDF, images or one
   motivating Thread.
2. **Remove infrastructure retry decisions from the LLM.** Stable error
   taxonomy and descriptor policy determine whether the same immutable Job gets
   another `JobAttempt`, waits for a node-health generation change, or becomes
   terminal. Infrastructure failures must not cause the Primary to alter task
   semantics or create speculative replacement Jobs.
3. **Inherit authorization by frozen scope.** A retry or replacement may reuse
   the prior authorization only when operation/capability identity, input
   Artifacts, output roles/dispositions, network/capability boundary and budget
   remain within the confirmed scope. Scope expansion requires a new
   confirmation; an unchanged infrastructure retry does not.
4. **Make controlled GPU health match the real isolation boundary.** A GPU node
   advertises capacity only after a probe succeeds with the same systemd unit,
   bubblewrap namespace, UID/groups, device mounts, libraries and minimal CUDA
   allocation used by a real controlled Operation. Failures use a stable GPU
   runtime code and phase instead of the misleading `operation_start_failed`.
5. **Add decision-layer real cases.** Keep compiler/protocol cases such as P22,
   and add user-message-driven cases that verify the Primary actually chooses
   parallel execution, selects the intended Skill, survives a Primary 429 after
   durable Job submission, avoids LLM-created infrastructure retries, reuses
   authorization only within scope, and produces a real GPU-backed transparent
   image with complete cleanup evidence.

These decisions deliberately accept that prompt-guided semantic decomposition
is best effort. Reliability for retry, authorization, resource health and
lifecycle closure must not depend on that best-effort model behavior.

### Skill Metadata Scope Decision

The Skill metadata model will not become a programmatic semantic router. Keep
metadata that already has a concrete bundle, runtime or security consumer:

- source/version/commit/license, content digest and retirement state;
- dependency profile, required commands and Python/Node modules;
- supported Harnesses, required Capabilities, allowed network origins and GPU
  requirement.

Keep `summary` as concise Catalog and LLM-facing discovery text. Remove semantic
`routing` fields that have no execution consumer: `intents`,
`input_mime_patterns`, `output_kinds`, `priority` and `supersedes`. Skill
selection remains a Primary Harness decision guided by non-overlapping Skill
descriptions; no host-side Skill router is introduced.

Do not connect the Skill-level `delegation` metadata block to
`sandbox-job delegate`, `JobCoordinator` normalization or low-level
`JobGroupCreate` validation. `JobRequest.expected_outputs` remains the sole
authoritative result contract for each task. Multiple Skills contribute only
their frozen dependencies and permission requirements; differences between
Skill-level result hints must never make an otherwise valid Job unexecutable.
Both the compact delegate path and the exact low-level Job contract therefore
remain usable without merging Skill output policies. The unused Skill-level
`delegation` block is removed from the metadata schema; it is not execution
authority.

## Input State

The successful appended-input path is monotonic: `pending -> delivered -> applied`. An explicit Runtime
input-completion receipt means that input was consumed even if later work in the same Turn fails. A Turn,
model, checkpoint or cleanup failure never changes `applied` back to `failed`.

Delivery rejection and delivery uncertainty remain separate facts. On terminalization, an input that never
left `pending` may become failed; a delivered input without an authoritative completion becomes unknown.
The browser presents input processing separately from Turn outcome, for example: `消息已处理，但本轮后续执行因限流失败`.

## Generic Delegation Compiler

Add a high-level Runtime CLI command over `sandbox-job prepare`. Its input is a compact group of generic
Child Harness tasks:

```json
{
  "tasks": [{
    "id": "task-a",
    "objective": "Complete one independently reviewable result",
    "inputs": ["assets/source.bin"],
    "skill_ids": []
  }]
}
```

The compiler obtains Thread, Turn, Harness, model, Gateway and capability identity from Runtime environment;
frozen Child Harness descriptor fields and available Skills from `/v1/execution/catalog`; and Artifact IDs,
hashes and MIME from the current Turn's `input_artifacts`. It generates deterministic group/job idempotency
keys and compiles each task into the existing `JobRequest`/`JobGroupCreate`.

The metadata-independent baseline uses `child-general`, the frozen Harness delegation descriptor, inherited
Harness/model, explicit frozen input Artifacts and one generic candidate output contract named
`child_deliverables` with bounded item/byte limits and unrestricted MIME patterns. Text results remain in the
Job result. Primary decides which candidates become workspace or delivery outputs after continuation.

Skill metadata may narrow Runtime selection, dependencies, capabilities, routing and default result shape,
but absence of those optimizations never authorizes guessing. Network, Provider, GPU and billable access are
default-deny and cannot be inferred from Skill prose. The resulting low-level request still passes all current
JobCoordinator, ProfileResolver, Artifact, authorization, budget and confirmation checks.

## Skill Metadata Schema

Every source Skill entry explicitly declares:

- stable `id`, `version`, source, commit and license;
- supported Harnesses and dependency profile;
- short Catalog/LLM-facing summary;
- machine-readable network mode plus allowed network origins;
- required capability IDs and GPU requirement;
- representative deterministic test case.

`runtime/skill_bundle.py` validates this schema, rejects unknown/missing fields, resolves dependency profiles,
and writes the complete frozen metadata to `manifest.json`. ProfileResolver
uses the frozen fields instead of hard-coded all-Harness/default-empty values.

The metadata schema is authorization-descriptive, never authority-granting. Catalog/Profile policy remains the
upper bound. `SKILL.md` natural language is never parsed into capabilities, network permission or budget.

## PPT Skill Decision

Remove `runtime/skills/pptx`, its source entry and every Profile `allowed_skill_ids` reference. Record it as
retired in the bundle. Keep `codex-ppt` as the PPT creation Skill and rewrite its generic subagent language to
the platform's exact `sandbox-job catalog/prepare/group/job` contract. The Primary Harness remains the only
delegator; slide workers are leaf Child Harness Jobs.

Underlying libraries such as PptxGenJS, python-pptx, LibreOffice and validation scripts may remain as frozen
Runtime dependencies. Existing-PPT editing and editable-slide workflows require an explicitly selected supported
Skill/capability; they do not fall back to the removed Skill invisibly.

## Error Normalization

Gateway/Runtime adapters normalize arbitrary upstream error objects to stable string codes before constructing
`RuntimeResult`. HTTP 429 maps to the rate-limit category. Bounded structural diagnostics are stored separately;
raw objects and exception bodies do not enter `error_code` or public detail.

## Public Truth

Tool activity is projected from Runtime tool Items. Child Harness activity requires persisted Job/Attempt rows.
Job Group activity requires a persisted group. Model prose never creates those labels. The frontend displays
input processing state independently from Turn outcome and uses persisted execution graph/activity data for
parallelism wording.

## Validation

- Unit and boundary tests for input receipts, terminal races and model error normalization.
- CLI compiler tests for environment binding, catalog templates, Artifact resolution, deterministic idempotency,
  malformed/foreign input rejection and metadata-free delegation.
- Bundle tests proving all 32 retained Skills have complete metadata and `pptx` is retired and absent.
- Profile/catalog tests proving metadata is frozen and policy cannot be widened.
- Frontend tests for input/Turn status separation and tool/Child/Group wording.
- Real OpenSandbox cases for one direct task, independent delegated tasks, tool-only concurrency, unavailable
  delegation fallback, Child output continuation and the motivating multi-deliverable scenario.
