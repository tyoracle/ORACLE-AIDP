# No-Data Troubleshooting

Use this order to find the first broken boundary. Do not restart downstream jobs until the upstream data and control state are understood.

## Five-minute decision tree

| Check | Healthy signal | If unhealthy |
|---|---|---|
| Workflow task | `RUNNING` for streaming/timer jobs, or recent `SUCCESS` | Inspect task output and cluster/session state |
| Bronze freshness | Recent `ingestion_timestamp`; Kafka max offsets increase | Check Kafka DNS/TCP/auth/topic subscription and checkpoint |
| Silver bounds | Bronze max offset is greater than control `last_kafka_offset` | If equal, `NO_NEW_DATA` is expected; if greater, inspect bounded batch failure |
| Silver reconciliation | `valid + quarantine = bounded Bronze`, `count_gap=0` | Stop offset advancement; inspect parser/DQ write/count failure |
| DQ readiness | Successful DQ run for the Silver version | Gold/Alert may wait or block; inspect DQ summary and rules |
| Gold activation | Snapshot control is `SUCCESS` or `ALREADY_ACTIVE` | Distinguish build, DQ gate, activation, and ADW publication failures |
| Alert transition | New `OPENED` event exists | Zero dispatch is expected without a new stockout transition |

## Layer isolation

1. Run `diagnostics/layer-health.sql` and record table versions, counts, and timestamps.
2. Compare the producer's latest committed version with the consumer's pinned or control version.
3. Inspect the notebook output markers listed in `diagnostics/workflow-output-checks.md`.
4. Use the matching layer runbook in `docs/runbooks/layers/`.
5. Record evidence in a new dated file under `docs/operations/`; include run IDs, versions, counts, status, error classification, and remediation.

## Common false alarms

- Silver `NO_NEW_DATA` while Bronze offsets equal the control offsets.
- Gold `ALREADY_ACTIVE` when `SKIP_IF_UNCHANGED=true`.
- Alert zero transitions/dispatches when no product changed into stockout.
- DQ severity `CRITICAL` when the configured Gold DQ gate is `WARN`; this should be documented but does not necessarily block business Gold.
