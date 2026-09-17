# Security and Publication Rules

- Store Kafka, ADW, and notification credentials only in AIDP Credential Store or an approved secret manager.
- Supply environment hosts, endpoints, OCIDs, regions, and protected config paths as workflow parameters.
- Commit source notebooks only; never commit executed outputs or downloaded task logs.
- Redact endpoint values and identities from incident records. Prefer deterministic run IDs, table versions, counts, status codes, and endpoint fingerprints.
- Review `notebooks/manifest.json` and run a secret scan before every Git push.
