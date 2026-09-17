# Bronze No-Data Runbook

1. Confirm the workflow task is still running and the Structured Streaming query is active.
2. Check Kafka hostname resolution and TCP connectivity from the AIDP cluster.
3. Confirm the configured topics exist and the Credential Store entry returns nonblank `username` and `password` without printing them.
4. Compare Kafka end offsets with `MAX(kafka_offset)` by topic/partition in Bronze.
5. Inspect `lastProgress` for `numInputRows`, source offsets, and sink commit progress.
6. Validate that the checkpoint path exists under the configured managed volume and was not unintentionally changed.
7. If Kafka advances but Bronze does not, preserve the checkpoint and investigate authentication, schema projection, sink errors, and query exceptions before any reset.

Never delete or change a production checkpoint as a first response; doing so can replay or skip data depending on `startingOffsets` and retained Kafka history.
