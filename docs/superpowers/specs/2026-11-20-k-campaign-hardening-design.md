# K Campaign Hardening Design

## Intent

Turn the K campaign from a broad execution-plane smoke matrix into reliable
evidence for production behavior, while removing the recurring runtime waste
that the campaign exposed. This work covers every accepted review item except
adding a Claude/DeepSeek execution matrix.

## Approaches

Three approaches were considered:

1. Add prompt rules for every observed failure. This is cheap but leaves
   collection recovery, unavailable capabilities and invalid Provider requests
   dependent on model behavior, so it is rejected.
2. Add case-specific fixtures and retry branches. This makes K green but does
   not improve user workloads, so it is rejected.
3. Strengthen the existing durable Job, Catalog/Profile and campaign boundaries.
   This is selected because the same controls apply to every Skill, Operation
   and Provider.

## Durable Collection Recovery

An accepted external execution that has completed must never be resubmitted
because artifact collection failed. A `collect_same_execution` failure moves
the same Job to `WAITING_EXTERNAL`, closes the current Attempt as
`COLLECTION_RETRY`, releases its lease and preserves the accepted external ID.
The next worker generation queries the same execution and retries collection.
The Descriptor's `max_collection_attempts` bounds this behavior; exhaustion
produces the existing structured terminal error. Worker restart between
attempts is supported by persisted Job/Attempt state.

## Availability And Preflight

Catalog entries may declare a host readiness probe. The execution catalog
publishes `available`, a stable reason, checked time and generation. Preparing,
editing or confirming a group refreshes readiness and rejects an unavailable
Descriptor before a confirmation card or Job exists. A transition back to
healthy automatically increments the generation and re-enables selection.

Provider Job inputs receive the same side-effect-free validation used by the
Provider adapter before confirmation: Capability scope, model/mode parameters,
reference bindings, artifact bytes and media metadata. No MediaJob or Provider
request is created by preflight.

## Offline Skill Closure

`web-artifacts-builder` must not download packages at execution time. Its Web
Runtime Profile owns pinned Node dependencies; its Skill bundle owns the
project scaffold and scripts. Initialization links the immutable global
packages into the generated project, and bundling uses only those packages.
The Skill manifest declares the full dependency closure, and static/image
probes fail when the Profile does not provide it.

## Truthful K Evidence

K01-K06 publish two artifacts per Child Harness: structured evidence and a
bounded product archive. The host independently validates archive safety,
non-emptiness and the presence of a non-evidence product. Self-reported command
lists are not sufficient evidence.

K09-K11 reports distinguish a clean full-matrix pass from an affected-member
rerun. A final report may say `composite_verified` but not `success` unless one
complete current-revision run passed.

Three new real cases cover the user path:

- Skill fanout followed by one Primary continuation and a final delivery.
- A mixed Operation/Child group with partial progress, continuation and final
  delivery.
- A two-stage Provider workflow with confirmation per dependency-free group,
  immutable handoff and final delivery.

## Progress And Observability

Wait barriers expose remaining Job names, states and elapsed time. Harness
details continue to collapse raw events into semantic categories; a complex
browser fixture proves that thousands of raw activities do not become thousands
of visible rows. K reports record queue, execution, long-tail and continuation
latency separately.

## Non-goals

- No Claude/DeepSeek K matrix.
- No case-specific production behavior.
- No Provider retry after an unknown submission without a trustworthy handle.
- No weakening of confirmation, budget, Artifact or authorization boundaries.
