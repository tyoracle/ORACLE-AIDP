# Data Contracts

## Bronze

- Every row retains `source_topic`, `kafka_partition`, `kafka_offset`, Kafka timestamp, raw JSON, and ingestion timestamps.
- A topic/partition/offset tuple is the source replay coordinate.
- The checkpoint must be a child of the configured managed volume.

## Silver and quarantine

- Each cycle reads only rows inside a fixed topic/partition offset interval.
- Valid rows are written to Silver; invalid rows are written to quarantine with rule results.
- Reconciliation invariant: `bounded_bronze_count = silver_count + quarantine_count`.
- Control offsets advance only after writes, DQ scorecard, run summary, and reconciliation complete.
- `NO_NEW_DATA` is a successful idle cycle, not a failure.

## Gold

- Each Gold run pins one Silver Delta version and one DQ scorecard Delta version.
- The same source versions produce the same deterministic Gold run identifier.
- Snapshot tables are immutable by run; serving tables represent the active run.
- AIDP and ADW target counts must match their corresponding snapshots.
- `ALREADY_ACTIVE` means the current deterministic snapshot is already serving.

## Alert

- Alert processing requires a successful DQ run for the pinned Silver version.
- Only an `OPENED` stockout transition requires a notification.
- Bootstrap suppresses historical notification replay.
- Event, state, and dispatch identifiers are deterministic to make retries safe.
- A zero-transition cycle is normal when inventory state has not changed.
