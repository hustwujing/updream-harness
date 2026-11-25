# Bounded managed-file upload

C29 produced valid artifacts, but a 64,782,809-byte Matroska file failed during
materialization into a new Runtime. An independent real sandbox accepted
31 MiB, rejected 33 MiB with ReadError, then accepted a small file; see
`.data/evidence/allocation-recovery/upload-size-probe.json`. Retrying an entire
body cannot cross a provider's per-request ingress limit.

The shared Runtime protocol will upload managed project files larger than
4 MiB as sequential chunks no larger than 4 MiB, with a request UUID, declared
file size and a final SHA-256. Both byte and streaming client APIs use this
protocol. Smaller files retain the existing wire protocol. No provider,
operation or case selection appears in production behavior.

Each upload owns a private staging file outside checkpoints. Only a complete,
size- and digest-verified file is atomically moved to its managed target.
Repeated chunks must match their original offset and bytes; a lost commit reply
is answered from its receipt without publishing again. Starting another write
to the same path or resetting managed files fences previous unfinished uploads.
Cancellation requests abort; expired/abandoned uploads are reclaimed on later
upload activity and Runtime startup. Staging is bounded to 4 active uploads,
2 GiB total declared bytes, and 128 short-lived receipts; file limit remains
2 GiB. Unexpired receipts are never evicted to admit a new upload. Initialization
freezes the Runtime epoch and issuance timestamp, with a 60-second client
deadline and a 10-minute server validity window (60 seconds of future clock
skew allowed). Reset and Runtime replacement rotate the epoch; stale begin
requests cannot create a fresh writer after either boundary or receipt expiry.
Uploads expire after 10 minutes without activity. Runtime restart may
require restarting an unfinished upload; no business execution is replayed.

The client buffers at most one 4 MiB chunk from streaming input, retries only
replayable chunks and commit requests, and checks returned bytes and digest.
An older Runtime without the new endpoint fails explicitly for large files;
it must not fall back to an oversized request or publish a partial file.
Checkpoint, attachment and signed-bundle transport are not changed by this fix.
Their existing individual limits and transfer contracts remain separately tested.

Validation covers the ingress limit, lost chunk/commit replies, uneven streams,
short/long/corrupt bodies, duplicate/conflicting offsets, concurrent writes,
reset/cancellation, expiry, staging budget and path/symlink boundaries. Real
validation repeats a >64 MiB upload and C29; completed small-file cases retain
their evidence. Inspector streaming restore receives a focused real check.
