# Runtime Recovery Real-Case Design

## Intent

Add deployed, real-sandbox evidence for the recovery defects found while auditing a
failed media continuation. The gate must prove that transient Runtime transport
failure does not lose checkpoint bytes, that a failed internal completion notice is
made available to the next user Turn without replaying completed work, and that a
successful workspace media output is promoted before its continuation is dispatched.

## Approach

Extend the existing production-evolution catalog because these cases combine deployed
services, direct authoritative-state assertions, fault injection and real Ali
sandboxes. A standalone supplemental runner would leave them outside the release gate;
reusing P18 alone would not inject the transport failure or validate continuation
delivery. The catalog therefore grows from 190 to 193 cases.

## Cases

- `EVO16` creates an isolated real Ali sandbox, writes and downloads a checkpoint,
  then restores it through a transport that lets the first idempotent PUT complete
  before raising `ReadError`. It asserts one retry, full request-body replay and the
  original workspace bytes after restore.
- `EVO17` creates an isolated Thread with a failed internal completion Turn whose
  Primary Runtime never started, submits an unrelated user message through the
  deployed API and asserts that the real Harness sees both the factual completion
  marker and current request. It submits one further user Turn and asserts the marker
  is absent. No Media Job or external execution may be created.
- `EVO18` submits one real `speech-2.8-turbo` workspace draft, waits for Provider
  success and PlatformWorker continuation, then asserts the output has one Project
  File path, the continuation contains that path, and only one Provider execution ID
  exists.

## Constraints

- Use unique Thread, allocation and idempotency identities and clean only resources
  created by each case.
- Do not touch or recover the historical failing Thread.
- Fault injection applies only to the case-owned Runtime client; deployed global
  networking and unrelated work remain untouched.
- A completion result is factual context, not a command. The host never resubmits the
  completed media task.
- Evidence records sandbox, Turn, Media Job, Project File and external execution
  identities needed to audit duplicate work.

