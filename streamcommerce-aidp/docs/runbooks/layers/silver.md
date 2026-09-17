# Silver No-Data Runbook

1. Compare Bronze `MAX(kafka_offset)` with pipeline-control `last_kafka_offset` for every topic/partition.
2. If all values are equal, `NO_NEW_DATA` is healthy and no repair is required.
3. If Bronze is ahead, capture the fixed `(last_offset, upper_offset]` bounds printed for the cycle.
4. Count bounded Bronze rows, valid Silver rows for the run, and quarantine rows for the run.
5. Require `valid + quarantine = bounded Bronze` and `count_gap=0` before allowing control offsets to advance.
6. Inspect DQ rule, scorecard, run-summary, and alert-state writes if classification completed but the cycle did not commit.
7. Check for a failed cycle after data writes but before offset advancement; retrying should be idempotent, but verify run identifiers and duplicates.
