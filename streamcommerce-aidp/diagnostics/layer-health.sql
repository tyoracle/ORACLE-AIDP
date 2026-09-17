-- StreamCommerce layer health checks for an AIDP Spark SQL notebook.
-- Run sections independently if one downstream table has not been created yet.

-- COMMAND ----------
-- Bronze freshness and Kafka coordinates.
SELECT
  source_topic,
  kafka_partition,
  COUNT(*) AS row_count,
  MAX(kafka_offset) AS max_kafka_offset,
  MAX(kafka_timestamp) AS max_kafka_timestamp,
  MAX(ingestion_timestamp) AS max_ingestion_timestamp
FROM default.default.streamcommerce_bronze_events
GROUP BY source_topic, kafka_partition
ORDER BY source_topic, kafka_partition;

-- COMMAND ----------
-- Silver consumption boundary. Bronze max should equal last_kafka_offset when idle.
WITH bronze AS (
  SELECT source_topic, kafka_partition, MAX(kafka_offset) AS bronze_max_offset
  FROM default.default.streamcommerce_bronze_events
  GROUP BY source_topic, kafka_partition
), control AS (
  SELECT source_topic, kafka_partition, MAX(last_kafka_offset) AS last_kafka_offset
  FROM default.default.streamcommerce_pipeline_offsets
  WHERE pipeline_name = 'streamcommerce_bronze_to_silver'
  GROUP BY source_topic, kafka_partition
)
SELECT
  COALESCE(bronze.source_topic, control.source_topic) AS source_topic,
  COALESCE(bronze.kafka_partition, control.kafka_partition) AS kafka_partition,
  bronze.bronze_max_offset,
  control.last_kafka_offset,
  bronze.bronze_max_offset - COALESCE(control.last_kafka_offset, -1) AS unconsumed_offset_span
FROM bronze
FULL OUTER JOIN control
  ON bronze.source_topic = control.source_topic
 AND bronze.kafka_partition = control.kafka_partition
ORDER BY source_topic, kafka_partition;

-- COMMAND ----------
-- Recent Silver/DQ outcomes and reconciliation evidence.
SELECT *
FROM default.default.streamcommerce_dq_run_summary
WHERE pipeline_name = 'streamcommerce_bronze_to_silver'
ORDER BY run_completed_at DESC
LIMIT 20;

-- COMMAND ----------
-- Current layer counts. Use timestamps/run IDs for incident-specific comparisons.
SELECT 'bronze' AS layer, COUNT(*) AS row_count FROM default.default.streamcommerce_bronze_events
UNION ALL
SELECT 'silver', COUNT(*) FROM default.default.streamcommerce_silver_events
UNION ALL
SELECT 'quarantine', COUNT(*) FROM default.default.streamcommerce_quarantine_events
UNION ALL
SELECT 'gold_daily_revenue', COUNT(*) FROM default.default.streamcommerce_gold_daily_revenue
UNION ALL
SELECT 'gold_product_performance', COUNT(*) FROM default.default.streamcommerce_gold_product_performance
UNION ALL
SELECT 'gold_customer_360', COUNT(*) FROM default.default.streamcommerce_gold_customer_360
UNION ALL
SELECT 'gold_inventory_health', COUNT(*) FROM default.default.streamcommerce_gold_inventory_health
UNION ALL
SELECT 'gold_payment_reliability', COUNT(*) FROM default.default.streamcommerce_gold_payment_reliability
UNION ALL
SELECT 'gold_dq_scorecard', COUNT(*) FROM default.default.streamcommerce_gold_data_quality_scorecard;

-- COMMAND ----------
-- Gold control state. Inspect source versions and activation flags together.
SELECT *
FROM default.default.streamcommerce_gold_snapshot_control
ORDER BY updated_at DESC
LIMIT 20;

-- COMMAND ----------
-- Alert transition and dispatch state.
SELECT transition_type, COUNT(*) AS event_count
FROM default.default.streamcommerce_inventory_alert_events
GROUP BY transition_type
ORDER BY transition_type;

SELECT dispatch_status, COUNT(*) AS dispatch_count
FROM default.default.streamcommerce_inventory_alert_dispatch
GROUP BY dispatch_status
ORDER BY dispatch_status;
