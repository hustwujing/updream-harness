# Controlled Runtime

Manifest-addressed programs, interpreters, native libraries and model inputs used
by ControlledWorker. It is mounted read-only and is not a long-running service.

- Programs: `sam3_job.py`, `transcribe_job.py`
- External identity: controlled runtime manifest SHA-256
- Verification: manifest builder, import closure, model/binary identity and real Operations
