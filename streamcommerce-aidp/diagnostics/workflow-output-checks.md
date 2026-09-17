# Workflow Output Checks

Search task output for these markers before reading the full log.

| Layer | Healthy markers | Investigate immediately |
|---|---|---|
| Bronze | credential resolved, TCP succeeded, query started | DNS, TCP, SASL, checkpoint, or sink exception |
| Silver | fixed bounds, reconciliation counts, `SUCCESS` or `NO_NEW_DATA` | `count_gap != 0`, failed governed write, offset update without summary |
| Gold | pinned versions, snapshot counts, activation, ADW count match, `SUCCESS`/`ALREADY_ACTIVE` | DQ timeout/block, empty domain snapshot, activation failure, ADW mismatch |
| Alert | pinned Silver/DQ, bootstrap/control state, transition totals, dispatch totals | DQ not ready, repeated prepared events, `OPENED` without dispatch audit |

For every incident, capture the Job Run ID, Task Run ID, notebook path, source Delta versions, deterministic run ID, and the last successful stage. Do not copy resolved secrets or endpoint values from logs.
