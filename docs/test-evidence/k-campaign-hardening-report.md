# K Campaign Hardening Report

Date: 2026-11-20

## Implemented

- Accepted external executions retry artifact collection as persisted
  `COLLECTION_RETRY` Attempts. Worker takeover reuses the original external ID;
  Descriptor `max_collection_attempts` bounds the retry and Provider submission
  is never repeated.
- Execution Catalog metadata includes dynamic readiness. The unavailable local
  `image.decompose` service is advertised as `available=false` with
  `connection_unavailable`, and prepare/edit/confirm reject it before a card or
  Job is created. Provider references and mode parameters receive the same
  side-effect-free validation before confirmation.
- `web-artifacts-builder` is network-free at execution time. Its scaffold and
  bundler are in the Skill Bundle; pinned React/Tailwind/shadcn dependencies are
  in the Web Profile. A real `--network none` container initialized, bundled and
  rendered the artifact successfully. Localhost browser validation bypasses the
  public proxy and explicitly cleans up its server.
- K01-K06 require a structured evidence Artifact plus a host-validated,
  path-safe product archive. Campaign reports distinguish `clean_run` from
  `composite_verified` evidence.
- Wait barriers expose remaining Job names, states and live elapsed time without
  changing the graph cursor on every tick.

## Real Cases

- K12: two parallel Skill Harnesses, one Continuation, final
  `k12-summary.md`; passed in 346.580s, including about 147s waiting behind the
  former global protocol lock.
- K13: one Operation plus one Child Harness, one Continuation, `2/2` barrier and
  final JSON delivery; passed in 146.955s.
- K14: two confirmed Provider stages, two Continuations, immutable Artifact
  handoff and final delivery; passed in 57.348s.
- K01 reruns independently verified real product archives for multiple Web and
  visual Skills and exposed generic defects subsequently fixed: Profile closure,
  gzip MIME detection, read-only scaffold permissions, localhost proxy routing,
  abandoned background cleanup and empty background error details. The evidence
  remains composite rather than a claimed clean K01 pass; one long Anatomy
  attempt encountered model rate limiting.

The protocol lock is now limited to P13/P19/P21/P27, the cases that actually
restart shared services. Ordinary K cases can run concurrently.
