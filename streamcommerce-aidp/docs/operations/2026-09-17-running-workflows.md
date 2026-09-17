# Running Workflow Snapshot — 2026-09-17

Snapshot evidence was refreshed at **2026-09-17 06:25 UTC**. All four Job Runs and Task Runs were `RUNNING` on `AICLUSTER`.

| Layer | Job Run ID | Task Run ID | Notebook |
|---|---|---|---|
| Bronze | `c75e6228-5e99-4784-aaae-f21ef4c53de6` | `e7876c38-e9b5-4583-bcb6-7c7f9b681671` | `/Workspace/Bronze/01_kafka_to_bronze_workflow_v2.ipynb` |
| Silver | `dc632a0b-0c5b-4961-b325-183af60f1c5d` | `ac502d94-9b42-402a-a24c-b91ccc7bd4b7` | `/Workspace/silver/02_bronze_to_silver_timed_streaming_workflow_dq_v3_fixed_bounds.ipynb` |
| Gold | `f5215af9-19b7-4d15-8f3c-2e616a9f82e2` | `2ab48648-67ce-4900-92f1-12f3de47a29a` | `/Workspace/Gold/04d_silver_to_dual_gold_workflow_v3_3_timer_retry_safe.ipynb` |
| Alert | `5d04a2c5-7644-4502-94d0-7639525af5c3` | `761aaf89-f173-439b-b8bb-d9227f44f1a8` | `/Workspace/EMAIL_ALERT/05_inventory_stockout_alert_workflow_v3_0_agent_function.ipynb` |

## Last-known processing evidence

- Bronze Kafka connectivity, Credential Store lookup, managed volume, Bronze schema, and streaming query startup succeeded.
- Silver successful DQ run `37f43d341d90cc9e30cc9ca25d062b6a51ca21b03c2555417169249c70ab572e`: bounded Bronze `3,924`, Silver `3,646`, quarantine `278`, `count_gap=0`; later cycles reported `NO_NEW_DATA`.
- Gold run `d0ce66bce002d1cd2a880546a6d55f0e15b9529e0f0da321d5f94b6d44781954` pinned Silver version `42` (`446,180` rows) and DQ version `39` (`143` rows).
- Gold AIDP/ADW counts matched: daily revenue `17`, product performance `120`, customer 360 `500`, inventory health `120`, payment reliability `17`, DQ scorecard `143`; publication status was `SUCCESS`.
- Alert used Silver version `42` and the matching successful DQ run. Current cycles produced zero new transitions and zero dispatches, which is expected without a new stockout transition.

## Interpretation

- Silver `NO_NEW_DATA` indicates Bronze offsets did not exceed committed pipeline offsets.
- Gold `ALREADY_ACTIVE` is expected with unchanged source versions and `SKIP_IF_UNCHANGED=true`.
- Gold DQ gate was `WARN`, so a `CRITICAL` DQ severity was visible but did not block business Gold publication.
