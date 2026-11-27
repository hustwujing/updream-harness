# Ali Asynchronous Execution 196-Case Report

The impact-assessed composite contains 196 successful latest results, zero
program failures and zero non-program failures. Unaffected evidence retains its
actual revision; the report is `composite_verified`.

Evidence: `.data/evidence/real-campaign/final-196-composite-293cff7.json`.

This increment adds K17, a real structured `media.concat_sequence` run using
two frozen video Artifacts, a soundtrack, SRT subtitles and watermark text. It
produced both `rendered_media` and `compiler_report`.

The release also separates asynchronous counters by meaning:

- model request rounds;
- Turn recovery count;
- Job attempt, policy retry and accepted external-execution counts;
- Provider submission and status-query counts;
- continuation delivery attempts.

Media generation no longer presents normal Provider polling generations as
business retries. Pause-completed Media batches persist as
`waiting_thread_resume` and are woken by a durable Core-to-Media resume command.

Validation:

- Python: `1620 passed, 23 skipped`;
- frontend: `37/37` and production build passed;
- focused execution graph, fault matrix, catalog and media-render gates:
  `78 passed`;
- K17 real case: success.
