# Sandbox Bootstrap Module Rules

- Keep dependencies minimal and compatible with the provider foundation image.
- Verify signatures/digests before activation; never execute partial bundles.
- Do not embed durable host credentials or mutable checkout paths.
- Failure must remain an infrastructure failure with no host fallback.
- Complete with tamper, relocation and real provider startup tests.
