# StreamCommerce AIDP Medallion Workflows

Source-controlled AIDP notebooks, workflow inventory, data contracts, and operational runbooks for the StreamCommerce Bronze → Silver → Gold → Alert pipeline.

## Repository layout

- `notebooks/`: output-free AIDP notebooks plus review-friendly Python exports.
- `config/workflows.example.yaml`: non-secret workflow parameter examples.
- `diagnostics/`: Spark SQL and output checks for rapid layer triage.
- `docs/runbooks/`: cross-layer and layer-specific troubleshooting procedures.
- `docs/operations/`: dated operational snapshots and investigation records.
- `tools/build_sanitized_notebooks.py`: repeatable sanitizer for exported executed notebooks.

## Active data flow

1. Bronze continuously consumes Kafka and persists raw events with Kafka coordinates.
2. Silver processes fixed `(last_offset, upper_offset]` ranges, applies data-quality rules, and writes valid and quarantined rows atomically from an operational perspective.
3. Gold pins successful Silver and DQ Delta versions, builds deterministic snapshots, activates serving tables, and validates the ADW publication.
4. Alert pins a DQ-approved Silver version, detects inventory state transitions, and records dispatch attempts.

## Safety

- Git artifacts contain no notebook execution outputs.
- Kafka credentials remain in AIDP Credential Store.
- Kafka hosts, OCI OCIDs, Agent/Function endpoints, email addresses, private keys, and local OCI configuration are not committed.
- Runtime-specific values must be supplied as AIDP workflow parameters.

Start troubleshooting with `docs/runbooks/no-data-troubleshooting.md` and run `diagnostics/layer-health.sql` in an AIDP SQL notebook.
