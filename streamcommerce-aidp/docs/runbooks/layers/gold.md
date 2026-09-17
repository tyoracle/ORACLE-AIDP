# Gold No-Data Runbook

1. Confirm the selected Silver Delta version exists and has rows.
2. Find the successful DQ run tied to that exact Silver version; do not substitute only the newest DQ row.
3. Read the Gold snapshot-control row for `gold_run_id`, source versions, DQ gate, run status, and activation flags.
4. Interpret `ALREADY_ACTIVE` as healthy when the source versions and transformation version are unchanged.
5. If a snapshot is empty, inspect the individual domain filter/join and compare source event types and keys.
6. If snapshots have rows but serving tables do not, inspect `READY_TO_ACTIVATE`, activation MERGEs, uniqueness checks, and active-run selection.
7. If AIDP serving is correct but ADW is empty, inspect publication status separately, then compare per-table AIDP and ADW counts.

Expected last-known AIDP counts are recorded in the dated operational snapshot, not hard-coded as permanent thresholds.
