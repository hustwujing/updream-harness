# Image Inference Module Rules

- Keep HTTP transport and model execution independent from platform databases.
- Bind only to loopback; credentials and runtime state stay under the Ali `.data` tree.
- Model/code/environment identities must be recorded before release selection.
- A create response loss is not permission for MediaWorker to resubmit.
- Complete with real inference, cancellation, restart and multi-file validation.
