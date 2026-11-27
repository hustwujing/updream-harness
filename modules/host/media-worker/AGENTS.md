# Media Worker Module Rules

- Consume a durable permit before provider submission.
- Persist provider IDs and staged outputs before later polling/collection steps.
- Unknown create outcomes are observed or cancelled, never replayed blindly.
- Keep provider transport generic; no case- or provider-specific state-machine branches.
- Keep each Provider's model schema, reference rules and request normalization
  in its registered `common/providers/` contract. Missing contracts fail startup;
  there is no legacy or kind-based fallback.
- Persist stable error codes for orchestration. Provider text reaches users and
  models only through the sanitized external error envelope.
- Publish only validated output contracts and acknowledge Core promotion identities.
- Complete with crash/restart, cancellation, multi-output and real-provider tests.
