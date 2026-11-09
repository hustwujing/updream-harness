# Data Model

## Thread

```text
id, owner_id, title, state
default_harness, default_model, default_reasoning_effort
native_thread_id, checkpoint_ref
active_turn_id, queued_turn_count
created_at, updated_at, archived_at
```

The native thread identifier belongs to the selected Harness and is opaque to the broker.

## Turn

```text
id, thread_id, idempotency_key, status, delivery_mode
harness, model, reasoning_effort
lease_owner, lease_expires_at, recovery_generation
sandbox_id, capability_nonce
runtime_execution_id, runtime_sequence
pending_final_state
error_code, error_detail
created_at, updated_at
```

Statuses are `QUEUED`, `RESTORING`, `STARTING`, `RUNNING`, `DRAINING_INPUTS`, `CHECKPOINTING`, `CLEANING`, `WAITING_USER`, `COMPLETED`, `FAILED` and `CANCELLED`.

`recovery_generation` is the fencing token. `runtime_sequence` advances only after the corresponding transcript event is durable. `pending_final_state` is populated before cleanup and cleared when the terminal state is committed.

## Item

```text
id, thread_id, turn_id, sequence
type, status, parent_item_id
summary, payload_json
created_at, updated_at
```

Item types are `user_message`, `agent_message`, `reasoning`, `plan`, `command`, `tool`, `file_change`, `artifact`, `approval`, `warning` and `error`. Public text deltas update one agent-message Item; they do not create one database row per token.

## Event

```text
turn_id, sequence, event_type, item_id, payload_json
source_execution_id, source_sequence, created_at
```

Events are the ordered replay and SSE transport log. Runtime source coordinates make Broker reattachment idempotent. Items are the durable product transcript.

## Sandbox Attempt

```text
id, turn_id, generation
sandbox_id, backend, runtime_profile, image_digest, bundle_digest
state, create_request_id
capability_nonce, capability_expires_at
created_at, attached_at, cleanup_started_at, deleted_at
```

## Checkpoint

```text
id, thread_id, turn_id, object_ref, sha256, bytes
native_thread_id, workspace_manifest_json
created_at
```

## Attachment and Artifact

Attachments are immutable Turn inputs. Artifacts are immutable files exported from `/workspace/outputs`. Both store object reference, hash, bytes, MIME type and original display name.

## Project File

```text
id, thread_id, path, version
source_type, source_turn_id
object_ref, sha256, bytes, mime
created_at
```

Current logical paths are `assets/<upload-name>` and `artifacts/<output-path>`. The database retains older versions for audit; the Workspace API and Sandbox materialization return only the highest version of each path.

## Approval

```text
id, thread_id, turn_id, item_id
request_json, status, response_json
created_at, responded_at
```

Only the active Sandbox Attempt may resolve its approval. Replayed or expired responses are rejected.

## Capability Grant

```text
nonce, sandbox_attempt_id
route_prefixes, model, request_limit, token_budget
issued_at, expires_at, revoked_at
request_count, input_tokens, output_tokens
```

This table supports revocation, usage and audit. It is infrastructure state, not a workflow plan.
