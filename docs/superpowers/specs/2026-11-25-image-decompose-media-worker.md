# Image decomposition through MediaWorker

## Confirmed direction

The user selected MediaWorker ownership for the existing external
`image.decompose` HTTP service and explicitly requested regression cases, real
machine verification, removal of obsolete code and GitHub publication. Retain
the independent inference service; no move into ControlledWorker or change to
its GPU scheduling is part of this work.

## Current boundary and change

Today `ImageDecomposeAdapter` performs submit, poll, cancel and downloads inside
JobWorker, and keeps a local receipt/cache. Other external media executions use
MediaWorker with database leases, persisted provider IDs, staged outputs and
atomic terminal publication. Move decomposition into that existing lifecycle.

The public Operation descriptor, input Artifact binding, JobGroup authorization,
one `layer_package` collection, optional MIME selection and continuation remain
unchanged. JobWorker keeps a database-only bridge for the Operation. MediaWorker
alone talks to the decomposition service. This is a functional ownership
migration, not merely running another JobWorker loop in the media process.

## Durable ownership

- Add an optional unique source Job link to MediaJob. Creating a delegated
  media execution validates the current source Job/lease generation and creates
  its internal confirmed draft and MediaJob atomically. Retry/recovery finds
  the same row; cancellation cannot leave an unowned queued media submission.
- Preserve owner/thread scope and frozen parameter/input digests. Direct
  source-Job continuation owns publication; the internal media draft has no
  origin Turn batch and cannot create a second Primary continuation.
- Use the existing MediaWorker lease, provider-result receipt and staged-output
  transactions. Persist the provider ID before another poll. Losing a submit
  response becomes UNKNOWN; do not adopt an unrelated sole active service job
  or treat an empty service listing as permission to resubmit.
- Definite busy/rate-limit rejection waits; transport/poll/download failure
  retains the original ID. Cancellation before submit performs no POST;
  cancellation after acceptance must observe the same external job reaching a
  terminal state. Unknown cancellation retains uncertainty.
- Import a known legacy accepted provider handle when resuming an older Job;
  this migration path queues only observation and never creates external work.
  Preserve old completed evidence. Remove the old direct HTTP adapter lifecycle
  and obsolete recovery heuristics once its required behavior has moved.

## Provider transport and outputs

Keep service-specific HTTP in a focused media provider module. Use the existing
loopback route and protected session-cookie file, with host-only credentials.
MediaWorker uses a small registered provider interface for submit, poll, cancel
and authenticated artifact retrieval; shared state transitions stay generic.

Reuse MIME detection and immutable object storage. Freeze the returned output
manifest; validate IDs, filenames, actual MIME and byte limits. Support multiple
layers and PNG/PSD/ORA/report outputs with stable unique output identities.
Apply the source Job's frozen MIME selection before writing excluded formats
to object storage. Resume partial downloads without repeating provider work.
Collect selected outputs into the existing lease-fenced ArtifactGateway with
dense positions and unchanged min/max cardinality validation.

## Verification and release

Use failing tests for database-only submission, concurrent enqueue/recovery,
stale generation/owner rejection, cancellation races, unknown submit, repeated
poll/download after Worker replacement, and exact multi-format output selection.
Run isolated PostgreSQL concurrency/migration checks and broader Python tests
for shared media/Store changes. Real verification includes C09/K07 plus focused
new cases for media-worker restart, cancellation and ambiguous submission.
Run representative affected existing MediaWorker provider/continuation cases;
preserve unrelated previous successes with their revisions.

All code/state/log/cache work stays in the Ali checkout under `/data2/wujing03`.
Update README/development guidance, deploy only affected Ali services, verify
health and actual output bytes, clean owned test resources, scan and push small
commits. The original project and the seven historical unknown sandbox
allocations remain outside this migration's mutation scope.
