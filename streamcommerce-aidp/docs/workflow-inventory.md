# Workflow Inventory

| Layer | Workflow job | Notebook | Primary output/control |
|---|---|---|---|
| Bronze | `stream_bronze.job` | `notebooks/bronze/01_kafka_to_bronze_workflow_v2.ipynb` | `streamcommerce_bronze_events`, streaming checkpoint |
| Silver | `silver_timed_streaming_workflow_V3.job` | `notebooks/silver/02_bronze_to_silver_timed_streaming_workflow_dq_v3_fixed_bounds.ipynb` | Silver, quarantine, pipeline offsets, DQ tables |
| Gold | `streamcommerce_silver_to_gold_dual_V3.job` | `notebooks/gold/04d_silver_to_dual_gold_workflow_v3_3_timer_retry_safe.ipynb` | Gold snapshots/control, AIDP serving, ADW serving |
| Alert | `streamcommerce_inventory_stockout_alert_v3.job` | `notebooks/alert/05_inventory_stockout_alert_workflow_v3_0_agent_function.ipynb` | Alert state, events, dispatch audit |

All workflow names and paths are documentation values. Environment bindings belong in AIDP workflow parameters and must not be copied into Git.
