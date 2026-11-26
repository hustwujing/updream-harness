# Controlled Runtime Module Rules

- Keep the snapshot read-only and content-addressed.
- Record every model, interpreter, native library and program in the manifest.
- Programs receive bounded files/JSON and may not access platform credentials.
- Changing any input creates a new digest; never mutate an active snapshot.
- Complete with clean imports and actual segmentation/transcription outputs.
