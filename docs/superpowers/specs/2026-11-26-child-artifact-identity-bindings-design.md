# Child Artifact Identity Bindings

## Intent

Child Harnesses and Reviewers must be able to associate every frozen input
Artifact with the filename recorded by the platform. The Artifact ID remains
the authoritative identity. The filename is read from Core after the Job has
been accepted and is never supplied or overridden by the caller.

## Current failure

The child runtime currently materializes an input as
`assets/inputs/<role>-<artifact_id>`. When multiple inputs use generic roles,
the child can infer the wrong semantic name from objective order. The R03 real
case demonstrated this: the two immutable Artifact IDs were valid, but the
child named their contents in reverse and the Reviewer validated that same
incorrect mapping.

## Design

After resolving each `JobArtifactInput` through Core, JobWorker derives a safe
basename from the stored Artifact path. It materializes the object at
`assets/inputs/<role>-<artifact_id>/<filename>` and includes that exact path,
Artifact ID, role, filename, digest, MIME type and size in the frozen execution
prompt. The same binding is shown to normal Child Harness and read-only
Reviewer executions.

The public `JobRequest` schema does not change. Callers cannot assert a
filename, and no provider-specific behavior is introduced. Stored paths are
reduced to their basename before use so a historical or malformed path cannot
escape the input directory.

## Verification

- Prompt tests prove two generic input roles retain distinct authoritative
  filenames and digests.
- Runtime preparation tests prove the uploaded path equals the prompt path.
- Existing Child Harness, Reviewer, JobWorker and contract tests continue to
  pass.
- The affected R03 real case is rerun after publishing the JobWorker release.

