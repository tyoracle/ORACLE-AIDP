# Alert No-Data Runbook

1. Confirm the latest Silver version has a successful matching DQ run.
2. Verify the detector control row's `last_source_silver_version` is not ahead of the current Silver version.
3. Count normalized inventory events and inspect product/warehouse/on-hand fields.
4. Check whether the cycle produced `OPENED`, `REOBSERVED`, `RESOLVED`, stale, or invalid transitions.
5. Zero transitions is healthy when inventory state did not change; zero dispatches is healthy without `OPENED` events.
6. If an `OPENED` event exists, inspect its dispatch row and classify Agent failure separately from Function delivery failure.
7. Keep endpoints and OCIDs out of evidence; record fingerprints, HTTP status, result code, retryability, and request IDs only when policy permits.
