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
input_message_item_id
sandbox_attempt_id
error_code, error_detail
created_at, started_at, finished_at
```

Statuses are `queued`, `restoring`, `starting`, `running`, `waiting_user`, `checkpointing`, `completed`, `failed`, `interrupted` and `cancelled`.

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
turn_id, sequence, event_type, item_id, payload_json, created_at
```

Events are the ordered replay and SSE transport log. Items are the durable product transcript.

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

