"""Source export generated from the sanitized AIDP notebook."""

# %% [code cell 1]
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true",
)

spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true",
)

# 当前批次只有数百到数千行，可先从 16 或 32 测试
spark.conf.set(
    "spark.sql.shuffle.partitions",
    "16",
)

# %% [code cell 2]
import re
from typing import Any


def _workflow_parameter(name: str, default: str) -> str:
    utility = globals().get("oidlUtils")

    if utility is None:
        return default

    try:
        value = utility.parameters.getParameter(name, default)
    except Exception:
        return default

    if value is None:
        return default

    value_text = str(value).strip()
    return value_text if value_text else default


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _validate_three_part_name(value: str, parameter_name: str) -> str:
    pattern = (
        r"^[A-Za-z_][A-Za-z0-9_]*\."
        r"[A-Za-z_][A-Za-z0-9_]*\."
        r"[A-Za-z_][A-Za-z0-9_]*$"
    )

    if not re.fullmatch(pattern, value):
        raise ValueError(
            f"{parameter_name} must be a three-part identifier "
            f"catalog.schema.object; received {value!r}."
        )

    return value


BRONZE_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "BRONZE_TABLE",
        "default.default.streamcommerce_bronze_events",
    ),
    "BRONZE_TABLE",
)

SILVER_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "SILVER_TABLE",
        "default.default.streamcommerce_silver_events",
    ),
    "SILVER_TABLE",
)

QUARANTINE_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "QUARANTINE_TABLE",
        "default.default.streamcommerce_quarantine_events",
    ),
    "QUARANTINE_TABLE",
)

CONTROL_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "CONTROL_TABLE",
        "default.default.streamcommerce_pipeline_offsets",
    ),
    "CONTROL_TABLE",
)

PIPELINE_NAME = _workflow_parameter(
    "PIPELINE_NAME",
    "streamcommerce_bronze_to_silver",
)

SILVER_RULE_VERSION = _workflow_parameter(
    "SILVER_RULE_VERSION",
    "streamcommerce-silver-v3.0-fixed-bounds",
)

AUTO_CREATE_TABLES = _as_bool(
    _workflow_parameter(
        "AUTO_CREATE_TABLES",
        "true",
    )
)


POLL_INTERVAL = _workflow_parameter(
    "POLL_INTERVAL",
    "5 minutes",
)

TIMER_CHECKPOINT_LOCATION = _workflow_parameter(
    "TIMER_CHECKPOINT_LOCATION",
    (
        "/Volumes/default/default/streamcommerce_vol/"
        "checkpoints/streamcommerce_bronze_to_silver_timer_v3"
    ),
)

QUERY_NAME = _workflow_parameter(
    "QUERY_NAME",
    "streamcommerce_bronze_to_silver_timer_v3",
)



DQ_RULES_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_RULES_TABLE",
        "default.default.streamcommerce_dq_rules",
    ),
    "DQ_RULES_TABLE",
)

DQ_SCORECARD_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_SCORECARD_TABLE",
        "default.default.streamcommerce_dq_scorecard",
    ),
    "DQ_SCORECARD_TABLE",
)

DQ_RUN_SUMMARY_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_RUN_SUMMARY_TABLE",
        "default.default.streamcommerce_dq_run_summary",
    ),
    "DQ_RUN_SUMMARY_TABLE",
)

DQ_ALERT_STATE_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_ALERT_STATE_TABLE",
        "default.default.streamcommerce_dq_alert_state",
    ),
    "DQ_ALERT_STATE_TABLE",
)

DQ_ALERT_EVENTS_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_ALERT_EVENTS_TABLE",
        "default.default.streamcommerce_dq_alert_events",
    ),
    "DQ_ALERT_EVENTS_TABLE",
)

MAX_EVENT_LATENESS_SECONDS = int(
    _workflow_parameter(
        "MAX_EVENT_LATENESS_SECONDS",
        "345600",
    )
)

NOTIFICATION_ENABLED = _as_bool(
    _workflow_parameter(
        "NOTIFICATION_ENABLED",
        "false",
    )
)

OCI_NOTIFICATION_TOPIC_OCID = _workflow_parameter(
    "OCI_NOTIFICATION_TOPIC_OCID",
    "",
)

OCI_CONFIG_FILE = _workflow_parameter(
    "OCI_CONFIG_FILE",
    "~/.oci/config",
)

OCI_CONFIG_PROFILE = _workflow_parameter(
    "OCI_CONFIG_PROFILE",
    "DEFAULT",
)


spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Bronze table        :", BRONZE_TABLE)
print("Silver table        :", SILVER_TABLE)
print("Quarantine table    :", QUARANTINE_TABLE)
print("Control table       :", CONTROL_TABLE)
print("Pipeline name       :", PIPELINE_NAME)
print("Silver rule version :", SILVER_RULE_VERSION)
print("Auto-create tables  :", AUTO_CREATE_TABLES)
print("Poll interval       :", POLL_INTERVAL)
print("Timer checkpoint    :", TIMER_CHECKPOINT_LOCATION)
print("Query name          :", QUERY_NAME)
print("DQ rules table      :", DQ_RULES_TABLE)
print("DQ scorecard table  :", DQ_SCORECARD_TABLE)
print("DQ run summary      :", DQ_RUN_SUMMARY_TABLE)
print("DQ alert state      :", DQ_ALERT_STATE_TABLE)
print("DQ alert events     :", DQ_ALERT_EVENTS_TABLE)
print("Max lateness sec    :", MAX_EVENT_LATENESS_SECONDS)
print("Notifications       :", NOTIFICATION_ENABLED)

# %% [code cell 3]
SILVER_COLUMNS = [
    "event_id",
    "correlation_id",
    "event_timestamp",
    "event_date",
    "schema_version",
    "sequence_number",
    "source_system",
    "source_topic",
    "event_type",
    "customer_id",
    "session_id",
    "device_type",
    "location",
    "campaign_id",
    "page",
    "product_id",
    "product_name",
    "category",
    "seller_id",
    "is_active",
    "order_id",
    "order_status",
    "payment_id",
    "payment_status",
    "payment_method",
    "provider_reference",
    "failure_reason",
    "shipment_id",
    "shipment_status",
    "carrier",
    "tracking_reference",
    "delay_reason",
    "warehouse_id",
    "inventory_status",
    "quantity",
    "price",
    "amount",
    "currency",
    "simulation_is_late",
    "simulation_late_by_seconds",
    "simulation_transport_delay_seconds",
    "message_key",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "bronze_ingestion_timestamp",
    "silver_rule_version",
    "silver_processed_timestamp",
]

QUARANTINE_COLUMNS = [
    "source_topic",
    "kafka_partition",
    "kafka_offset",
    "message_key",
    "raw_json",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "bronze_ingestion_timestamp",
    "event_id",
    "correlation_id",
    "event_timestamp_raw",
    "event_timestamp",
    "source_system",
    "event_type",
    "defect_type",
    "dq_reason",
    "dq_reason_count",
    "quarantined_timestamp",
    "quarantine_date",
]

CONTROL_COLUMNS = [
    "pipeline_name",
    "source_topic",
    "kafka_partition",
    "last_kafka_offset",
    "updated_at",
]


if AUTO_CREATE_TABLES:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        (
            event_id                              STRING,
            correlation_id                        STRING,
            event_timestamp                       TIMESTAMP,
            event_date                            DATE,
            schema_version                        INT,
            sequence_number                       INT,
            source_system                         STRING,
            source_topic                          STRING,
            event_type                            STRING,
            customer_id                           STRING,
            session_id                            STRING,
            device_type                           STRING,
            location                              STRING,
            campaign_id                           STRING,
            page                                  STRING,
            product_id                            STRING,
            product_name                          STRING,
            category                              STRING,
            seller_id                             STRING,
            is_active                             BOOLEAN,
            order_id                              STRING,
            order_status                          STRING,
            payment_id                            STRING,
            payment_status                        STRING,
            payment_method                        STRING,
            provider_reference                    STRING,
            failure_reason                        STRING,
            shipment_id                           STRING,
            shipment_status                       STRING,
            carrier                               STRING,
            tracking_reference                    STRING,
            delay_reason                          STRING,
            warehouse_id                          STRING,
            inventory_status                      STRING,
            quantity                              INT,
            price                                 DOUBLE,
            amount                                DOUBLE,
            currency                              STRING,
            simulation_is_late                    BOOLEAN,
            simulation_late_by_seconds            BIGINT,
            simulation_transport_delay_seconds    DOUBLE,
            message_key                           STRING,
            kafka_partition                       INT,
            kafka_offset                          BIGINT,
            kafka_timestamp                       TIMESTAMP,
            kafka_timestamp_type                  INT,
            bronze_ingestion_timestamp            TIMESTAMP,
            silver_rule_version                   STRING,
            silver_processed_timestamp            TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (event_date)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE}
        (
            source_topic                  STRING,
            kafka_partition               INT,
            kafka_offset                  BIGINT,
            message_key                   STRING,
            raw_json                      STRING,
            kafka_timestamp               TIMESTAMP,
            kafka_timestamp_type          INT,
            bronze_ingestion_timestamp    TIMESTAMP,
            event_id                      STRING,
            correlation_id                STRING,
            event_timestamp_raw           STRING,
            event_timestamp               TIMESTAMP,
            source_system                 STRING,
            event_type                    STRING,
            defect_type                   STRING,
            dq_reason                     STRING,
            dq_reason_count               INT,
            quarantined_timestamp         TIMESTAMP,
            quarantine_date               DATE
        )
        USING DELTA
        PARTITIONED BY (quarantine_date)
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE}
        (
            pipeline_name         STRING,
            source_topic          STRING,
            kafka_partition       INT,
            last_kafka_offset     BIGINT,
            updated_at            TIMESTAMP
        )
        USING DELTA
    """)


def _verify_table_columns(
    table_name: str,
    expected_columns: list[str],
) -> None:
    try:
        actual_columns = spark.table(table_name).columns
    except Exception as exc:
        raise RuntimeError(
            f"Table {table_name} does not exist or is inaccessible."
        ) from exc

    if actual_columns != expected_columns:
        raise RuntimeError(
            f"Schema mismatch for {table_name}.\n"
            f"Expected: {expected_columns}\n"
            f"Actual  : {actual_columns}"
        )


required_bronze_columns = {
    "source_topic",
    "kafka_partition",
    "kafka_offset",
    "message_key",
    "raw_json",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "ingestion_timestamp",
}

try:
    bronze_columns = set(spark.table(BRONZE_TABLE).columns)
except Exception as exc:
    raise RuntimeError(
        f"Bronze table {BRONZE_TABLE} does not exist or is inaccessible."
    ) from exc

missing_bronze_columns = sorted(
    required_bronze_columns.difference(bronze_columns)
)

if missing_bronze_columns:
    raise RuntimeError(
        "Bronze table is missing required columns: "
        + ", ".join(missing_bronze_columns)
    )

_verify_table_columns(SILVER_TABLE, SILVER_COLUMNS)
_verify_table_columns(QUARANTINE_TABLE, QUARANTINE_COLUMNS)
_verify_table_columns(CONTROL_TABLE, CONTROL_COLUMNS)

control_duplicates = (
    spark.table(CONTROL_TABLE)
    .filter(f"pipeline_name = '{PIPELINE_NAME}'")
    .groupBy(
        "pipeline_name",
        "source_topic",
        "kafka_partition",
    )
    .count()
    .filter("count > 1")
)

if control_duplicates.limit(1).count() > 0:
    print("Duplicate control rows:")
    control_duplicates.show(truncate=False)

    raise RuntimeError(
        "The pipeline control table contains duplicate rows for the "
        "same pipeline/topic/partition. Repair the control table before "
        "continuing."
    )



# Verify only the managed Volume root. Spark creates the timer checkpoint
# child directory automatically when the streaming query starts.
_timer_checkpoint_parts = TIMER_CHECKPOINT_LOCATION.split("/")

if (
    len(_timer_checkpoint_parts) < 6
    or _timer_checkpoint_parts[1] != "Volumes"
):
    raise RuntimeError(
        "TIMER_CHECKPOINT_LOCATION must use an AIDP managed Volume path; "
        f"received {TIMER_CHECKPOINT_LOCATION!r}."
    )

_TIMER_VOLUME_ROOT = "/" + "/".join(
    _timer_checkpoint_parts[1:5]
)

import os as _os

if not _os.path.isdir(_TIMER_VOLUME_ROOT):
    raise RuntimeError(
        f"Managed Volume path is unavailable: {_TIMER_VOLUME_ROOT}. "
        "Create the managed Volume first in Master Catalog."
    )




GOVERNANCE_TABLE_COLUMNS = {
    DQ_RULES_TABLE: [
        "rule_id",
        "reason_code",
        "rule_name",
        "quality_dimension",
        "source_topic",
        "severity",
        "min_pass_percentage",
        "max_failed_records",
        "alert_enabled",
        "owner",
        "description",
        "remediation_action",
        "active",
        "updated_at",
    ],
    DQ_SCORECARD_TABLE: [
        "run_id",
        "pipeline_name",
        "timer_batch_id",
        "source_topic",
        "rule_id",
        "reason_code",
        "rule_name",
        "quality_dimension",
        "severity",
        "total_record_count",
        "failed_record_count",
        "passed_record_count",
        "pass_percentage",
        "failure_percentage",
        "min_pass_percentage",
        "max_failed_records",
        "alert_enabled",
        "threshold_breached",
        "check_status",
        "silver_rule_version",
        "checked_at",
        "check_date",
    ],
    DQ_RUN_SUMMARY_TABLE: [
        "run_id",
        "pipeline_name",
        "timer_batch_id",
        "cycle_started_at",
        "cycle_completed_at",
        "duration_seconds",
        "run_status",
        "overall_quality_status",
        "bronze_record_count",
        "silver_candidate_count",
        "quarantine_candidate_count",
        "quarantine_rate_percentage",
        "source_topic_count",
        "evaluated_rule_count",
        "warning_breach_count",
        "critical_breach_count",
        "max_event_lag_seconds",
        "max_ingestion_lag_seconds",
        "error_message",
        "run_date",
    ],
    DQ_ALERT_STATE_TABLE: [
        "alert_key",
        "pipeline_name",
        "source_topic",
        "rule_id",
        "severity",
        "current_status",
        "first_detected_at",
        "last_detected_at",
        "resolved_at",
        "consecutive_breaches",
        "consecutive_passes",
        "last_run_id",
        "observed_value",
        "threshold_value",
        "message",
        "updated_at",
    ],
    DQ_ALERT_EVENTS_TABLE: [
        "alert_event_id",
        "alert_key",
        "run_id",
        "event_type",
        "pipeline_name",
        "source_topic",
        "rule_id",
        "severity",
        "observed_value",
        "threshold_value",
        "message",
        "created_at",
        "notification_status",
        "notification_message_id",
        "notification_error",
        "notified_at",
        "event_date",
    ],
}

for governance_table, expected_columns in (
    GOVERNANCE_TABLE_COLUMNS.items()
):
    _verify_table_columns(
        governance_table,
        expected_columns,
    )

active_rule_count = (
    spark.table(DQ_RULES_TABLE)
    .filter("active = true")
    .count()
)

if active_rule_count == 0:
    raise RuntimeError(
        "No active data-quality rules were found. Run the DQ "
        "governance setup notebook before this workflow."
    )


print("Bronze source verified     :", BRONZE_TABLE)
print("Silver target verified     :", SILVER_TABLE)
print("Quarantine target verified :", QUARANTINE_TABLE)
print("Control table verified     :", CONTROL_TABLE)
print("Timer Volume verified      :", _TIMER_VOLUME_ROOT)
print("Active DQ rules verified   :", active_rule_count)

# %% [code cell 4]
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("correlation_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("sequence_number", IntegerType(), True),

    StructField("source_system", StringType(), True),
    StructField("event_type", StringType(), True),

    StructField("customer_id", StringType(), True),
    StructField("session_id", StringType(), True),

    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("page", StringType(), True),

    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("is_active", BooleanType(), True),

    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),

    StructField("payment_id", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("provider_reference", StringType(), True),
    StructField("failure_reason", StringType(), True),

    StructField("shipment_id", StringType(), True),
    StructField("shipment_status", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("tracking_reference", StringType(), True),
    StructField("delay_reason", StringType(), True),

    StructField("warehouse_id", StringType(), True),
    StructField("inventory_status", StringType(), True),

    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),

    StructField("_defect_type", StringType(), True),
    StructField("_simulation_is_late", BooleanType(), True),
    StructField(
        "_simulation_late_by_seconds",
        LongType(),
        True,
    ),
    StructField(
        "_simulation_transport_delay_seconds",
        DoubleType(),
        True,
    ),

    StructField("_corrupt_record", StringType(), True),
])


KNOWN_TOPICS = [
    "customer_events",
    "orders",
    "payments",
    "inventory_updates",
    "product_catalog_updates",
    "shipment_events",
]

VALID_PAYMENT_STATUSES = [
    "PENDING",
    "SUCCESS",
    "FAILED",
    "REFUNDED",
]

VALID_ORDER_STATUSES = [
    "CREATED",
    "CONFIRMED",
    "CANCELLED",
    "RETURNED",
]

VALID_SHIPMENT_STATUSES = [
    "CREATED",
    "DELAYED",
    "SHIPPED",
    "DELIVERED",
    "RETURNED",
]


def _any_null(*column_names: str):
    result = F.lit(False)

    for column_name in column_names:
        result = result | F.col(column_name).isNull()

    return result


def parse_and_validate(bronze_df):
    parsed_df = (
        bronze_df

        .withColumn(
            "event",
            F.from_json(
                F.col("raw_json"),
                EVENT_SCHEMA,
                {
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                },
            ),
        )

        .select(
            "source_topic",
            "kafka_partition",
            "kafka_offset",
            "message_key",
            "raw_json",
            "kafka_timestamp",
            "kafka_timestamp_type",

            F.col("ingestion_timestamp").alias(
                "bronze_ingestion_timestamp"
            ),

            F.col("event.event_id").alias("event_id"),
            F.col("event.correlation_id").alias("correlation_id"),
            F.col("event.event_timestamp").alias(
                "event_timestamp_raw"
            ),
            F.col("event.schema_version").alias("schema_version"),
            F.col("event.sequence_number").alias("sequence_number"),

            F.col("event.source_system").alias("source_system"),
            F.col("event.event_type").alias("event_type"),

            F.col("event.customer_id").alias("customer_id"),
            F.col("event.session_id").alias("session_id"),

            F.col("event.device_type").alias("device_type"),
            F.col("event.location").alias("location"),
            F.col("event.campaign_id").alias("campaign_id"),
            F.col("event.page").alias("page"),

            F.col("event.product_id").alias("product_id"),
            F.col("event.product_name").alias("product_name"),
            F.col("event.category").alias("category"),
            F.col("event.seller_id").alias("seller_id"),
            F.col("event.is_active").alias("is_active"),

            F.col("event.order_id").alias("order_id"),
            F.col("event.order_status").alias("order_status"),

            F.col("event.payment_id").alias("payment_id"),
            F.col("event.payment_status").alias("payment_status"),
            F.col("event.payment_method").alias("payment_method"),
            F.col("event.provider_reference").alias(
                "provider_reference"
            ),
            F.col("event.failure_reason").alias("failure_reason"),

            F.col("event.shipment_id").alias("shipment_id"),
            F.col("event.shipment_status").alias("shipment_status"),
            F.col("event.carrier").alias("carrier"),
            F.col("event.tracking_reference").alias(
                "tracking_reference"
            ),
            F.col("event.delay_reason").alias("delay_reason"),

            F.col("event.warehouse_id").alias("warehouse_id"),
            F.col("event.inventory_status").alias(
                "inventory_status"
            ),

            F.col("event.quantity").alias("quantity"),
            F.col("event.price").alias("price"),
            F.col("event.amount").alias("amount"),
            F.col("event.currency").alias("currency"),

            F.col("event._defect_type").alias("defect_type"),
            F.col("event._simulation_is_late").alias(
                "simulation_is_late"
            ),
            F.col("event._simulation_late_by_seconds").alias(
                "simulation_late_by_seconds"
            ),
            F.col(
                "event._simulation_transport_delay_seconds"
            ).alias(
                "simulation_transport_delay_seconds"
            ),
            F.col("event._corrupt_record").alias("corrupt_record"),
        )

        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp_raw"),
        )

        .withColumn(
            "event_lag_seconds",
            F.when(
                F.col("event_timestamp").isNotNull()
                & F.col("kafka_timestamp").isNotNull(),
                F.greatest(
                    F.lit(0),
                    F.unix_timestamp("kafka_timestamp")
                    - F.unix_timestamp("event_timestamp"),
                ),
            ),
        )
    )

    duplicate_rank_window = (
        Window.partitionBy("event_id")
        .orderBy(
            F.col("bronze_ingestion_timestamp").asc(),
            F.col("source_topic").asc(),
            F.col("kafka_partition").asc(),
            F.col("kafka_offset").asc(),
        )
    )

    parsed_df = parsed_df.withColumn(
        "_event_id_rank_in_batch",
        F.when(
            F.col("event_id").isNotNull(),
            F.row_number().over(duplicate_rank_window),
        ),
    )

    dq_candidates = F.array(
        F.when(
            F.col("raw_json").isNull()
            | (F.length(F.trim(F.col("raw_json"))) == 0),
            F.lit("empty_raw_json"),
        ),

        F.when(
            F.col("corrupt_record").isNotNull()
            | (
                F.col("event_id").isNull()
                & F.col("source_system").isNull()
                & F.col("event_type").isNull()
            ),
            F.lit("malformed_json"),
        ),

        F.when(
            F.col("defect_type").isNotNull(),
            F.concat(
                F.lit("simulated_defect:"),
                F.col("defect_type"),
            ),
        ),

        F.when(
            F.col("event_id").isNull(),
            F.lit("missing_event_id"),
        ),

        F.when(
            F.col("correlation_id").isNull(),
            F.lit("missing_correlation_id"),
        ),

        F.when(
            F.col("event_timestamp").isNull(),
            F.lit("missing_or_invalid_event_timestamp"),
        ),

        F.when(
            F.col("schema_version").isNull(),
            F.lit("missing_schema_version"),
        ),

        F.when(
            F.col("sequence_number").isNull(),
            F.lit("missing_sequence_number"),
        ),

        F.when(
            F.col("source_system").isNull(),
            F.lit("missing_source_system"),
        ),

        F.when(
            F.col("event_type").isNull(),
            F.lit("missing_event_type"),
        ),

        F.when(
            ~F.col("source_topic").isin(KNOWN_TOPICS),
            F.lit("unknown_source_topic"),
        ),

        F.when(
            F.col("source_system").isNotNull()
            & (F.col("source_topic") != F.col("source_system")),
            F.lit("topic_source_mismatch"),
        ),

        F.when(
            (F.col("source_topic") == "customer_events")
            & _any_null(
                "customer_id",
                "session_id",
                "product_id",
            ),
            F.lit("customer_event_required_fields_missing"),
        ),

        F.when(
            (F.col("source_topic") == "orders")
            & _any_null(
                "order_id",
                "customer_id",
                "product_id",
                "amount",
                "order_status",
            ),
            F.lit("order_required_fields_missing"),
        ),

        F.when(
            (F.col("source_topic") == "payments")
            & _any_null(
                "order_id",
                "payment_id",
                "payment_status",
                "amount",
            ),
            F.lit("payment_required_fields_missing"),
        ),

        F.when(
            (F.col("source_topic") == "inventory_updates")
            & _any_null(
                "product_id",
                "warehouse_id",
                "quantity",
                "inventory_status",
            ),
            F.lit("inventory_required_fields_missing"),
        ),

        F.when(
            (F.col("source_topic") == "product_catalog_updates")
            & _any_null(
                "product_id",
                "product_name",
                "category",
                "price",
            ),
            F.lit("product_required_fields_missing"),
        ),

        F.when(
            (F.col("source_topic") == "shipment_events")
            & _any_null(
                "order_id",
                "shipment_id",
                "shipment_status",
            ),
            F.lit("shipment_required_fields_missing"),
        ),

        F.when(
            F.col("quantity").isNotNull()
            & (F.col("quantity") < 0),
            F.lit("negative_quantity"),
        ),

        F.when(
            F.col("price").isNotNull()
            & (F.col("price") < 0),
            F.lit("negative_price"),
        ),

        F.when(
            F.col("amount").isNotNull()
            & (F.col("amount") < 0),
            F.lit("negative_amount"),
        ),

        F.when(
            F.col("product_id") == "SKU-DOES-NOT-EXIST",
            F.lit("invalid_product_id"),
        ),

        F.when(
            (F.col("source_topic") == "payments")
            & F.col("payment_status").isNotNull()
            & ~F.col("payment_status").isin(
                VALID_PAYMENT_STATUSES
            ),
            F.lit("invalid_payment_status"),
        ),

        F.when(
            (F.col("source_topic") == "orders")
            & F.col("order_status").isNotNull()
            & ~F.col("order_status").isin(
                VALID_ORDER_STATUSES
            ),
            F.lit("invalid_order_status"),
        ),

        F.when(
            (F.col("source_topic") == "shipment_events")
            & F.col("shipment_status").isNotNull()
            & ~F.col("shipment_status").isin(
                VALID_SHIPMENT_STATUSES
            ),
            F.lit("invalid_shipment_status"),
        ),

        F.when(
            F.col("_event_id_rank_in_batch") > 1,
            F.lit("duplicate_event_id_in_batch"),
        ),

        F.when(
            F.col("event_lag_seconds").isNotNull()
            & (
                F.col("event_lag_seconds")
                > F.lit(MAX_EVENT_LATENESS_SECONDS)
            ),
            F.lit("event_too_late"),
        ),
    )

    return (
        parsed_df

        .withColumn(
            "dq_reasons",
            F.filter(
                dq_candidates,
                lambda reason: reason.isNotNull(),
            ),
        )

        .withColumn(
            "dq_reason_count",
            F.size("dq_reasons"),
        )

        .withColumn(
            "dq_reason",
            F.concat_ws(
                "|",
                F.col("dq_reasons"),
            ),
        )
    )


print("Commerce event parser and DQ rules initialized.")

# %% [code cell 5]

import hashlib
import json
import os
from datetime import datetime, timezone


def _merge_dataframe(
    dataframe,
    target_table: str,
    source_view: str,
    merge_sql: str,
) -> None:
    dataframe.createOrReplaceTempView(source_view)

    try:
        spark.sql(merge_sql)
    finally:
        try:
            spark.catalog.dropTempView(source_view)
        except Exception:
            pass


def _build_run_id(
    timer_batch_id: int,
    offset_upper_bounds,
) -> str:
    offset_parts = [
        (
            f"{row['source_topic']}:"
            f"{row['kafka_partition']}:"
            f"{row['last_kafka_offset']}"
        )
        for row in offset_upper_bounds
    ]

    signature = "|".join(
        sorted(offset_parts)
    )

    raw_value = (
        f"{PIPELINE_NAME}|{timer_batch_id}|{signature}"
    )

    return hashlib.sha256(
        raw_value.encode("utf-8")
    ).hexdigest()


def write_dq_scorecard(
    validated_df,
    run_id: str,
    timer_batch_id: int,
):
    total_by_topic = (
        validated_df

        .groupBy("source_topic")

        .agg(
            F.count("*")
             .cast("long")
             .alias("total_record_count")
        )
    )

    reason_failures = (
        validated_df

        .select(
            "source_topic",
            F.explode_outer(
                "dq_reasons"
            ).alias("reason_code"),
        )

        .filter(
            F.col("reason_code").isNotNull()
        )

        .groupBy(
            "source_topic",
            "reason_code",
        )

        .agg(
            F.count("*")
             .cast("long")
             .alias("failed_record_count")
        )
    )

    invalid_failures = (
        validated_df

        .filter(
            F.col("dq_reason_count") > 0
        )

        .groupBy("source_topic")

        .agg(
            F.count("*")
             .cast("long")
             .alias("failed_record_count")
        )

        .withColumn(
            "reason_code",
            F.lit("__record_invalid__"),
        )

        .select(
            "source_topic",
            "reason_code",
            "failed_record_count",
        )
    )

    failure_counts = (
        reason_failures
        .unionByName(invalid_failures)
    )

    active_rules = (
        spark.table(DQ_RULES_TABLE)

        .filter(
            F.col("active") == F.lit(True)
        )

        .select(
            "rule_id",
            "reason_code",
            "rule_name",
            "quality_dimension",
            F.col("source_topic").alias(
                "rule_scope_topic"
            ),
            "severity",
            "min_pass_percentage",
            "max_failed_records",
            "alert_enabled",
        )
    )

    topics = total_by_topic.select(
        "source_topic"
    )

    applicable_rules = (
        topics.crossJoin(active_rules)

        .filter(
            (F.col("rule_scope_topic") == "*")
            | (
                F.col("rule_scope_topic")
                == F.col("source_topic")
            )
        )
    )

    scorecard = (
        applicable_rules

        .join(
            total_by_topic,
            on="source_topic",
            how="inner",
        )

        .join(
            failure_counts,
            on=[
                "source_topic",
                "reason_code",
            ],
            how="left",
        )

        .withColumn(
            "failed_record_count",
            F.coalesce(
                F.col("failed_record_count"),
                F.lit(0),
            ).cast("long"),
        )

        .withColumn(
            "passed_record_count",
            (
                F.col("total_record_count")
                - F.col("failed_record_count")
            ).cast("long"),
        )

        .withColumn(
            "pass_percentage",
            F.when(
                F.col("total_record_count") > 0,
                F.round(
                    (
                        F.col("passed_record_count")
                        / F.col("total_record_count")
                    ) * 100.0,
                    4,
                ),
            ).otherwise(F.lit(100.0)),
        )

        .withColumn(
            "failure_percentage",
            F.round(
                100.0 - F.col("pass_percentage"),
                4,
            ),
        )

        .withColumn(
            "_pass_threshold_breached",
            F.col("min_pass_percentage").isNotNull()
            & (
                F.col("pass_percentage")
                < F.col("min_pass_percentage")
            ),
        )

        .withColumn(
            "_count_threshold_breached",
            F.col("max_failed_records").isNotNull()
            & (
                F.col("failed_record_count")
                > F.col("max_failed_records")
            ),
        )

        .withColumn(
            "threshold_breached",
            (
                F.col("_pass_threshold_breached")
                | F.col("_count_threshold_breached")
            ),
        )

        .withColumn(
            "check_status",
            F.when(
                ~F.col("alert_enabled"),
                F.lit("OBSERVE"),
            )
            .when(
                F.col("threshold_breached")
                & (F.upper("severity") == "CRITICAL"),
                F.lit("FAIL"),
            )
            .when(
                F.col("threshold_breached"),
                F.lit("WARN"),
            )
            .otherwise(
                F.lit("PASS")
            ),
        )

        .withColumn(
            "run_id",
            F.lit(run_id),
        )

        .withColumn(
            "pipeline_name",
            F.lit(PIPELINE_NAME),
        )

        .withColumn(
            "timer_batch_id",
            F.lit(timer_batch_id).cast("long"),
        )

        .withColumn(
            "silver_rule_version",
            F.lit(SILVER_RULE_VERSION),
        )

        .withColumn(
            "checked_at",
            F.current_timestamp(),
        )

        .withColumn(
            "check_date",
            F.current_date(),
        )

        .drop(
            "rule_scope_topic",
            "_pass_threshold_breached",
            "_count_threshold_breached",
        )

        .select(
            "run_id",
            "pipeline_name",
            "timer_batch_id",
            "source_topic",
            "rule_id",
            "reason_code",
            "rule_name",
            "quality_dimension",
            "severity",
            "total_record_count",
            "failed_record_count",
            "passed_record_count",
            "pass_percentage",
            "failure_percentage",
            "min_pass_percentage",
            "max_failed_records",
            "alert_enabled",
            "threshold_breached",
            "check_status",
            "silver_rule_version",
            "checked_at",
            "check_date",
        )

        .persist()
    )

    _merge_dataframe(
        scorecard,
        DQ_SCORECARD_TABLE,
        "_streamcommerce_dq_scorecard_batch",
        f"""
            MERGE INTO {DQ_SCORECARD_TABLE} AS target

            USING _streamcommerce_dq_scorecard_batch AS source

            ON  target.run_id = source.run_id
            AND target.source_topic = source.source_topic
            AND target.rule_id = source.rule_id

            WHEN NOT MATCHED THEN INSERT *
        """,
    )

    return scorecard


def update_alert_state_and_events(
    scorecard_df,
    run_id: str,
) -> int:
    candidates = (
        scorecard_df

        .filter(
            F.col("alert_enabled")
        )

        .withColumn(
            "alert_key",
            F.concat_ws(
                "|",
                F.lit(PIPELINE_NAME),
                F.col("source_topic"),
                F.col("rule_id"),
            ),
        )

        .withColumn(
            "observed_value",
            F.when(
                F.col("min_pass_percentage").isNotNull(),
                F.col("pass_percentage"),
            ).otherwise(
                F.col("failed_record_count").cast("double")
            ),
        )

        .withColumn(
            "threshold_value",
            F.when(
                F.col("min_pass_percentage").isNotNull(),
                F.col("min_pass_percentage"),
            ).otherwise(
                F.col("max_failed_records").cast("double")
            ),
        )

        .withColumn(
            "message",
            F.concat(
                F.lit("DQ rule "),
                F.col("rule_id"),
                F.lit(" on topic "),
                F.col("source_topic"),
                F.lit(": pass="),
                F.format_number(
                    F.col("pass_percentage"),
                    4,
                ),
                F.lit("%, failed="),
                F.col("failed_record_count").cast("string"),
                F.lit(", threshold pass>="),
                F.coalesce(
                    F.col("min_pass_percentage").cast("string"),
                    F.lit("n/a"),
                ),
                F.lit("%, max_failed<="),
                F.coalesce(
                    F.col("max_failed_records").cast("string"),
                    F.lit("n/a"),
                ),
            ),
        )
    )

    state = (
        spark.table(DQ_ALERT_STATE_TABLE)

        .select(
            F.col("alert_key").alias(
                "state_alert_key"
            ),
            F.col("current_status").alias(
                "state_current_status"
            ),
            F.col("first_detected_at").alias(
                "state_first_detected_at"
            ),
            F.col("last_detected_at").alias(
                "state_last_detected_at"
            ),
            F.col("resolved_at").alias(
                "state_resolved_at"
            ),
            F.col("consecutive_breaches").alias(
                "state_consecutive_breaches"
            ),
            F.col("consecutive_passes").alias(
                "state_consecutive_passes"
            ),
        )
    )

    joined = (
        candidates.alias("candidate")

        .join(
            state.alias("state"),
            F.col("candidate.alert_key")
            == F.col("state.state_alert_key"),
            how="left",
        )
    )

    transitions = (
        joined

        .withColumn(
            "event_type",
            F.when(
                F.col("candidate.threshold_breached")
                & (
                    F.col("state.state_current_status").isNull()
                    | (
                        F.col("state.state_current_status")
                        != "OPEN"
                    )
                ),
                F.lit("OPENED"),
            )
            .when(
                ~F.col("candidate.threshold_breached")
                & (
                    F.col("state.state_current_status")
                    == "OPEN"
                ),
                F.lit("RESOLVED"),
            ),
        )

        .filter(
            F.col("event_type").isNotNull()
        )

        .select(
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(run_id),
                    F.col("candidate.alert_key"),
                    F.col("event_type"),
                ),
                256,
            ).alias("alert_event_id"),

            F.col("candidate.alert_key").alias(
                "alert_key"
            ),

            F.lit(run_id).alias("run_id"),

            "event_type",

            F.lit(PIPELINE_NAME).alias(
                "pipeline_name"
            ),

            F.col("candidate.source_topic").alias(
                "source_topic"
            ),

            F.col("candidate.rule_id").alias(
                "rule_id"
            ),

            F.col("candidate.severity").alias(
                "severity"
            ),

            F.col("candidate.observed_value").alias(
                "observed_value"
            ),

            F.col("candidate.threshold_value").alias(
                "threshold_value"
            ),

            F.col("candidate.message").alias(
                "message"
            ),

            F.current_timestamp().alias(
                "created_at"
            ),

            F.lit("PENDING").alias(
                "notification_status"
            ),

            F.lit(None).cast("string").alias(
                "notification_message_id"
            ),

            F.lit(None).cast("string").alias(
                "notification_error"
            ),

            F.lit(None).cast("timestamp").alias(
                "notified_at"
            ),

            F.current_date().alias(
                "event_date"
            ),
        )

        .persist()
    )

    transition_count = transitions.count()

    if transition_count > 0:
        _merge_dataframe(
            transitions,
            DQ_ALERT_EVENTS_TABLE,
            "_streamcommerce_dq_alert_event_batch",
            f"""
                MERGE INTO {DQ_ALERT_EVENTS_TABLE} AS target

                USING _streamcommerce_dq_alert_event_batch AS source

                ON target.alert_event_id =
                   source.alert_event_id

                WHEN NOT MATCHED THEN INSERT *
            """,
        )

    state_updates = (
        joined

        .select(
            F.col("candidate.alert_key").alias(
                "alert_key"
            ),

            F.lit(PIPELINE_NAME).alias(
                "pipeline_name"
            ),

            F.col("candidate.source_topic").alias(
                "source_topic"
            ),

            F.col("candidate.rule_id").alias(
                "rule_id"
            ),

            F.col("candidate.severity").alias(
                "severity"
            ),

            F.when(
                F.col("candidate.threshold_breached"),
                F.lit("OPEN"),
            ).otherwise(
                F.lit("RESOLVED")
            ).alias("current_status"),

            F.when(
                F.col("candidate.threshold_breached")
                & F.col(
                    "state.state_first_detected_at"
                ).isNull(),
                F.current_timestamp(),
            ).otherwise(
                F.col(
                    "state.state_first_detected_at"
                )
            ).alias("first_detected_at"),

            F.when(
                F.col("candidate.threshold_breached"),
                F.current_timestamp(),
            ).otherwise(
                F.col(
                    "state.state_last_detected_at"
                )
            ).alias("last_detected_at"),

            F.when(
                ~F.col("candidate.threshold_breached")
                & (
                    F.col("state.state_current_status")
                    == "OPEN"
                ),
                F.current_timestamp(),
            ).otherwise(
                F.col("state.state_resolved_at")
            ).alias("resolved_at"),

            F.when(
                F.col("candidate.threshold_breached"),
                F.coalesce(
                    F.col(
                        "state.state_consecutive_breaches"
                    ),
                    F.lit(0),
                ) + F.lit(1),
            ).otherwise(
                F.lit(0)
            ).cast("int").alias(
                "consecutive_breaches"
            ),

            F.when(
                ~F.col("candidate.threshold_breached"),
                F.coalesce(
                    F.col(
                        "state.state_consecutive_passes"
                    ),
                    F.lit(0),
                ) + F.lit(1),
            ).otherwise(
                F.lit(0)
            ).cast("int").alias(
                "consecutive_passes"
            ),

            F.lit(run_id).alias(
                "last_run_id"
            ),

            F.col("candidate.observed_value").alias(
                "observed_value"
            ),

            F.col("candidate.threshold_value").alias(
                "threshold_value"
            ),

            F.col("candidate.message").alias(
                "message"
            ),

            F.current_timestamp().alias(
                "updated_at"
            ),
        )
    )

    _merge_dataframe(
        state_updates,
        DQ_ALERT_STATE_TABLE,
        "_streamcommerce_dq_alert_state_batch",
        f"""
            MERGE INTO {DQ_ALERT_STATE_TABLE} AS target

            USING _streamcommerce_dq_alert_state_batch AS source

            ON target.alert_key = source.alert_key

            WHEN MATCHED THEN UPDATE SET
                target.pipeline_name =
                    source.pipeline_name,
                target.source_topic =
                    source.source_topic,
                target.rule_id =
                    source.rule_id,
                target.severity =
                    source.severity,
                target.current_status =
                    source.current_status,
                target.first_detected_at =
                    source.first_detected_at,
                target.last_detected_at =
                    source.last_detected_at,
                target.resolved_at =
                    source.resolved_at,
                target.consecutive_breaches =
                    source.consecutive_breaches,
                target.consecutive_passes =
                    source.consecutive_passes,
                target.last_run_id =
                    source.last_run_id,
                target.observed_value =
                    source.observed_value,
                target.threshold_value =
                    source.threshold_value,
                target.message =
                    source.message,
                target.updated_at =
                    source.updated_at

            WHEN NOT MATCHED THEN INSERT *
        """,
    )

    transitions.unpersist()
    return transition_count


def write_run_summary(
    *,
    run_id: str,
    timer_batch_id: int,
    cycle_started_at,
    run_status: str,
    overall_quality_status: str,
    bronze_record_count: int,
    silver_candidate_count: int,
    quarantine_candidate_count: int,
    source_topic_count: int,
    evaluated_rule_count: int,
    warning_breach_count: int,
    critical_breach_count: int,
    max_event_lag_seconds,
    max_ingestion_lag_seconds,
    error_message,
) -> None:
    completed_at = datetime.now(timezone.utc)

    duration_seconds = (
        completed_at - cycle_started_at
    ).total_seconds()

    quarantine_rate = (
        0.0
        if bronze_record_count == 0
        else round(
            (
                quarantine_candidate_count
                / bronze_record_count
            ) * 100.0,
            4,
        )
    )

    summary_rows = [(
        run_id,
        PIPELINE_NAME,
        int(timer_batch_id),
        cycle_started_at,
        completed_at,
        float(duration_seconds),
        run_status,
        overall_quality_status,
        int(bronze_record_count),
        int(silver_candidate_count),
        int(quarantine_candidate_count),
        float(quarantine_rate),
        int(source_topic_count),
        int(evaluated_rule_count),
        int(warning_breach_count),
        int(critical_breach_count),
        (
            None
            if max_event_lag_seconds is None
            else int(max_event_lag_seconds)
        ),
        (
            None
            if max_ingestion_lag_seconds is None
            else int(max_ingestion_lag_seconds)
        ),
        (
            None
            if error_message is None
            else str(error_message)[:4000]
        ),
        completed_at.date(),
    )]

    summary_schema = (
        spark.table(DQ_RUN_SUMMARY_TABLE)
        .schema
    )

    summary_df = spark.createDataFrame(
        summary_rows,
        schema=summary_schema,
    )

    _merge_dataframe(
        summary_df,
        DQ_RUN_SUMMARY_TABLE,
        "_streamcommerce_dq_run_summary_batch",
        f"""
            MERGE INTO {DQ_RUN_SUMMARY_TABLE} AS target

            USING _streamcommerce_dq_run_summary_batch AS source

            ON target.run_id = source.run_id

            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """,
    )


def publish_pending_alerts(run_id: str) -> None:
    if not NOTIFICATION_ENABLED:
        return

    if not OCI_NOTIFICATION_TOPIC_OCID:
        print(
            "NOTIFICATION_ENABLED=true but "
            "OCI_NOTIFICATION_TOPIC_OCID is empty."
        )
        return

    pending_rows = (
        spark.table(DQ_ALERT_EVENTS_TABLE)

        .filter(
            (F.col("run_id") == run_id)
            & (
                F.col("notification_status")
                == "PENDING"
            )
        )

        .collect()
    )

    if not pending_rows:
        return

    try:
        import oci
        from oci.ons.models import MessageDetails

        config = oci.config.from_file(
            os.path.expanduser(OCI_CONFIG_FILE),
            OCI_CONFIG_PROFILE,
        )

        client = oci.ons.NotificationDataPlaneClient(
            config
        )

    except Exception as exc:
        print(
            "OCI Notifications client could not be "
            f"initialized: {exc}"
        )
        return

    update_rows = []

    for row in pending_rows:
        title = (
            f"[{row['severity']}] StreamCommerce DQ "
            f"{row['event_type']}"
        )

        body = json.dumps(
            {
                "event_type": row["event_type"],
                "pipeline_name": row["pipeline_name"],
                "source_topic": row["source_topic"],
                "rule_id": row["rule_id"],
                "severity": row["severity"],
                "observed_value": row["observed_value"],
                "threshold_value": row["threshold_value"],
                "message": row["message"],
                "run_id": row["run_id"],
                "created_at": str(row["created_at"]),
            },
            indent=2,
            default=str,
        )

        try:
            response = client.publish_message(
                topic_id=OCI_NOTIFICATION_TOPIC_OCID,
                message_details=MessageDetails(
                    title=title,
                    body=body,
                ),
            )

            update_rows.append((
                row["alert_event_id"],
                "SENT",
                response.data.message_id,
                None,
                datetime.now(timezone.utc),
            ))

        except Exception as exc:
            update_rows.append((
                row["alert_event_id"],
                "FAILED",
                None,
                str(exc)[:2000],
                datetime.now(timezone.utc),
            ))

    update_schema = """
        alert_event_id STRING,
        notification_status STRING,
        notification_message_id STRING,
        notification_error STRING,
        notified_at TIMESTAMP
    """

    updates_df = spark.createDataFrame(
        update_rows,
        schema=update_schema,
    )

    _merge_dataframe(
        updates_df,
        DQ_ALERT_EVENTS_TABLE,
        "_streamcommerce_notification_updates",
        f"""
            MERGE INTO {DQ_ALERT_EVENTS_TABLE} AS target

            USING _streamcommerce_notification_updates AS source

            ON target.alert_event_id =
               source.alert_event_id

            WHEN MATCHED THEN UPDATE SET
                target.notification_status =
                    source.notification_status,
                target.notification_message_id =
                    source.notification_message_id,
                target.notification_error =
                    source.notification_error,
                target.notified_at =
                    source.notified_at
        """,
    )


print("DQ governance helpers initialized.")

# %% [code cell 6]

from pyspark import StorageLevel
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


OFFSET_RANGE_SCHEMA = StructType([
    StructField("source_topic", StringType(), False),
    StructField("kafka_partition", IntegerType(), False),
    StructField("last_kafka_offset", LongType(), False),
    StructField("upper_kafka_offset", LongType(), False),
])


def _capture_fixed_offset_ranges():
    """
    Capture immutable lower/upper Kafka offset bounds on the driver.

    The returned DataFrame contains only partitions with at least one
    unprocessed Bronze record.
    """
    control_rows = (
        spark.table(CONTROL_TABLE)

        .filter(
            F.col("pipeline_name")
            == F.lit(PIPELINE_NAME)
        )

        .select(
            "source_topic",
            "kafka_partition",
            "last_kafka_offset",
        )

        .collect()
    )

    control_map = {
        (
            row["source_topic"],
            int(row["kafka_partition"]),
        ): int(row["last_kafka_offset"])
        for row in control_rows
    }

    upper_rows = (
        spark.table(BRONZE_TABLE)

        .groupBy(
            "source_topic",
            "kafka_partition",
        )

        .agg(
            F.max("kafka_offset")
             .cast("long")
             .alias("upper_kafka_offset")
        )

        .collect()
    )

    range_rows = []

    for row in upper_rows:
        source_topic = row["source_topic"]
        kafka_partition = int(row["kafka_partition"])
        upper_offset = int(row["upper_kafka_offset"])

        last_offset = int(
            control_map.get(
                (source_topic, kafka_partition),
                -1,
            )
        )

        if upper_offset > last_offset:
            range_rows.append((
                source_topic,
                kafka_partition,
                last_offset,
                upper_offset,
            ))

    return spark.createDataFrame(
        range_rows,
        schema=OFFSET_RANGE_SCHEMA,
    )


def _build_bounded_bronze_snapshot(
    offset_ranges_df,
):
    """
    Build a stable source dataset bounded by the offsets captured at the
    start of this cycle. Bronze may continue receiving new rows, but rows
    above upper_kafka_offset cannot enter the current cycle.
    """
    return (
        spark.table(BRONZE_TABLE)
        .alias("bronze")

        .join(
            F.broadcast(
                offset_ranges_df.alias("range")
            ),
            on=[
                F.col("bronze.source_topic")
                == F.col("range.source_topic"),

                F.col("bronze.kafka_partition")
                == F.col("range.kafka_partition"),
            ],
            how="inner",
        )

        .filter(
            (
                F.col("bronze.kafka_offset")
                > F.col("range.last_kafka_offset")
            )
            & (
                F.col("bronze.kafka_offset")
                <= F.col("range.upper_kafka_offset")
            )
        )

        .select(
            F.col("bronze.*")
        )

        # Defensive protection if Bronze was accidentally written more
        # than once for the same Kafka source position.
        .dropDuplicates([
            "source_topic",
            "kafka_partition",
            "kafka_offset",
        ])

        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )


def _assert_classification_counts(
    *,
    bronze_count: int,
    valid_count: int,
    invalid_count: int,
) -> None:
    classified_count = valid_count + invalid_count
    count_gap = classified_count - bronze_count

    print("Count reconciliation:")
    print("  bounded Bronze rows :", bronze_count)
    print("  Silver candidates   :", valid_count)
    print("  Quarantine candidates:", invalid_count)
    print("  classified rows     :", classified_count)
    print("  count_gap           :", count_gap)

    if count_gap != 0:
        raise RuntimeError(
            "DQ classification count invariant failed: "
            f"bronze={bronze_count}, valid={valid_count}, "
            f"invalid={invalid_count}, count_gap={count_gap}. "
            "Offsets will not be advanced."
        )


def _assert_scorecard_counts(
    *,
    scorecard_df,
    bronze_count: int,
    valid_count: int,
    invalid_count: int,
) -> None:
    record_validity = (
        scorecard_df

        .filter(
            F.col("rule_id")
            == F.lit("record_validity")
        )

        .agg(
            F.sum("total_record_count")
             .cast("long")
             .alias("scorecard_total"),

            F.sum("passed_record_count")
             .cast("long")
             .alias("scorecard_passed"),

            F.sum("failed_record_count")
             .cast("long")
             .alias("scorecard_failed"),
        )

        .first()
    )

    scorecard_total = int(
        record_validity["scorecard_total"] or 0
    )
    scorecard_passed = int(
        record_validity["scorecard_passed"] or 0
    )
    scorecard_failed = int(
        record_validity["scorecard_failed"] or 0
    )

    print("Scorecard reconciliation:")
    print("  scorecard total  :", scorecard_total)
    print("  scorecard passed :", scorecard_passed)
    print("  scorecard failed :", scorecard_failed)

    if (
        scorecard_total != bronze_count
        or scorecard_passed != valid_count
        or scorecard_failed != invalid_count
    ):
        raise RuntimeError(
            "DQ Scorecard count invariant failed: "
            f"expected total/passed/failed="
            f"{bronze_count}/{valid_count}/{invalid_count}, "
            f"actual="
            f"{scorecard_total}/{scorecard_passed}/"
            f"{scorecard_failed}. "
            "Offsets will not be advanced."
        )


def run_incremental_batch(
    timer_batch_id: int,
) -> dict[str, Any]:
    cycle_started_at = datetime.now(timezone.utc)

    run_id = hashlib.sha256(
        (
            f"{PIPELINE_NAME}|NO_DATA|"
            f"{timer_batch_id}|"
            f"{cycle_started_at.isoformat()}"
        ).encode("utf-8")
    ).hexdigest()

    summary_written = False
    bounded_bronze_df = None
    validated_df = None
    offset_upper_bounds = None
    scorecard_df = None

    new_row_count = 0
    valid_count = 0
    invalid_count = 0

    try:
        # ----------------------------------------------------
        # 1. Capture immutable topic/partition offset ranges
        # ----------------------------------------------------
        offset_ranges_df = (
            _capture_fixed_offset_ranges()
        )

        offset_range_count = (
            offset_ranges_df.count()
        )

        if offset_range_count == 0:
            print(
                "No new Bronze offsets were found. "
                "The timer cycle will finish successfully."
            )

            write_run_summary(
                run_id=run_id,
                timer_batch_id=timer_batch_id,
                cycle_started_at=cycle_started_at,
                run_status="NO_NEW_DATA",
                overall_quality_status="NO_DATA",
                bronze_record_count=0,
                silver_candidate_count=0,
                quarantine_candidate_count=0,
                source_topic_count=0,
                evaluated_rule_count=0,
                warning_breach_count=0,
                critical_breach_count=0,
                max_event_lag_seconds=None,
                max_ingestion_lag_seconds=None,
                error_message=None,
            )

            summary_written = True

            return {
                "run_id": run_id,
                "status": "NO_NEW_DATA",
                "bronze_rows": 0,
                "silver_candidates": 0,
                "quarantine_candidates": 0,
                "count_gap": 0,
                "alert_transitions": 0,
            }

        print("Captured fixed offset ranges:")

        offset_ranges_df.orderBy(
            "source_topic",
            "kafka_partition",
        ).show(
            500,
            truncate=False,
        )

        # ----------------------------------------------------
        # 2. Materialize one stable bounded Bronze snapshot
        # ----------------------------------------------------
        bounded_bronze_df = (
            _build_bounded_bronze_snapshot(
                offset_ranges_df
            )
        )

        new_row_count = int(
            bounded_bronze_df.count()
        )

        if new_row_count == 0:
            raise RuntimeError(
                "Offset ranges indicated new Bronze data, but the "
                "bounded source snapshot contained zero rows. "
                "Offsets will not be advanced."
            )

        print(
            "Bounded Bronze records:",
            new_row_count,
        )

        # Use the actual offsets present in the materialized bounded
        # dataset for the final control-table update.
        offset_upper_bounds = (
            bounded_bronze_df

            .groupBy(
                "source_topic",
                "kafka_partition",
            )

            .agg(
                F.max("kafka_offset")
                 .cast("long")
                 .alias("last_kafka_offset")
            )

            .withColumn(
                "pipeline_name",
                F.lit(PIPELINE_NAME),
            )

            .withColumn(
                "updated_at",
                F.current_timestamp(),
            )

            .select(
                "pipeline_name",
                "source_topic",
                "kafka_partition",
                "last_kafka_offset",
                "updated_at",
            )

            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        offset_upper_bound_rows = (
            offset_upper_bounds

            .select(
                "source_topic",
                "kafka_partition",
                "last_kafka_offset",
            )

            .collect()
        )

        run_id = _build_run_id(
            timer_batch_id,
            offset_upper_bound_rows,
        )

        # ----------------------------------------------------
        # 3. Parse and validate only the bounded source rows
        # ----------------------------------------------------
        validated_df = (
            parse_and_validate(
                bounded_bronze_df
            )

            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        classification = (
            validated_df

            .agg(
                F.count("*")
                 .cast("long")
                 .alias("total_count"),

                F.sum(
                    F.when(
                        F.col("dq_reason_count") == 0,
                        1,
                    ).otherwise(0)
                ).cast("long").alias(
                    "valid_count"
                ),

                F.sum(
                    F.when(
                        F.col("dq_reason_count") > 0,
                        1,
                    ).otherwise(0)
                ).cast("long").alias(
                    "invalid_count"
                ),
            )

            .first()
        )

        classified_total = int(
            classification["total_count"] or 0
        )
        valid_count = int(
            classification["valid_count"] or 0
        )
        invalid_count = int(
            classification["invalid_count"] or 0
        )

        if classified_total != new_row_count:
            raise RuntimeError(
                "Validated DataFrame row count changed after the "
                "bounded snapshot was materialized: "
                f"bounded={new_row_count}, "
                f"validated={classified_total}. "
                "Offsets will not be advanced."
            )

        _assert_classification_counts(
            bronze_count=new_row_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
        )

        valid_df = validated_df.filter(
            F.col("dq_reason_count") == 0
        )

        invalid_df = validated_df.filter(
            F.col("dq_reason_count") > 0
        )

        # ----------------------------------------------------
        # 4. Valid events -> Silver
        # ----------------------------------------------------
        if valid_count > 0:
            event_rank = Window.partitionBy(
                "event_id"
            ).orderBy(
                F.col(
                    "bronze_ingestion_timestamp"
                ).asc(),
                F.col("source_topic").asc(),
                F.col("kafka_partition").asc(),
                F.col("kafka_offset").asc(),
            )

            silver_batch = (
                valid_df

                .withColumn(
                    "_event_rank",
                    F.row_number().over(
                        event_rank
                    ),
                )

                .filter(
                    F.col("_event_rank") == 1
                )

                .drop("_event_rank")

                .select(
                    "event_id",
                    "correlation_id",
                    "event_timestamp",
                    F.to_date(
                        "event_timestamp"
                    ).alias("event_date"),
                    "schema_version",
                    "sequence_number",
                    "source_system",
                    "source_topic",
                    "event_type",
                    "customer_id",
                    "session_id",
                    "device_type",
                    "location",
                    "campaign_id",
                    "page",
                    "product_id",
                    "product_name",
                    "category",
                    "seller_id",
                    "is_active",
                    "order_id",
                    "order_status",
                    "payment_id",
                    "payment_status",
                    "payment_method",
                    "provider_reference",
                    "failure_reason",
                    "shipment_id",
                    "shipment_status",
                    "carrier",
                    "tracking_reference",
                    "delay_reason",
                    "warehouse_id",
                    "inventory_status",
                    "quantity",
                    "price",
                    "amount",
                    "currency",
                    "simulation_is_late",
                    "simulation_late_by_seconds",
                    "simulation_transport_delay_seconds",
                    "message_key",
                    "kafka_partition",
                    "kafka_offset",
                    "kafka_timestamp",
                    "kafka_timestamp_type",
                    "bronze_ingestion_timestamp",
                    F.lit(
                        SILVER_RULE_VERSION
                    ).alias(
                        "silver_rule_version"
                    ),
                    F.current_timestamp().alias(
                        "silver_processed_timestamp"
                    ),
                )
            )

            silver_view = (
                "_streamcommerce_silver_"
                "fixed_bound_batch"
            )

            silver_batch.createOrReplaceTempView(
                silver_view
            )

            try:
                spark.sql(f"""
                    MERGE INTO {SILVER_TABLE}
                    AS target

                    USING {silver_view}
                    AS source

                    ON target.event_id =
                       source.event_id

                    WHEN NOT MATCHED
                    THEN INSERT *
                """)

            finally:
                try:
                    spark.catalog.dropTempView(
                        silver_view
                    )
                except Exception:
                    pass

        # ----------------------------------------------------
        # 5. Invalid events -> Quarantine
        # ----------------------------------------------------
        if invalid_count > 0:
            quarantine_batch = (
                invalid_df

                .dropDuplicates([
                    "source_topic",
                    "kafka_partition",
                    "kafka_offset",
                ])

                .select(
                    "source_topic",
                    "kafka_partition",
                    "kafka_offset",
                    "message_key",
                    "raw_json",
                    "kafka_timestamp",
                    "kafka_timestamp_type",
                    "bronze_ingestion_timestamp",
                    "event_id",
                    "correlation_id",
                    "event_timestamp_raw",
                    "event_timestamp",
                    "source_system",
                    "event_type",
                    "defect_type",
                    "dq_reason",
                    "dq_reason_count",
                    F.current_timestamp().alias(
                        "quarantined_timestamp"
                    ),
                    F.current_date().alias(
                        "quarantine_date"
                    ),
                )
            )

            quarantine_view = (
                "_streamcommerce_quarantine_"
                "fixed_bound_batch"
            )

            quarantine_batch.createOrReplaceTempView(
                quarantine_view
            )

            try:
                spark.sql(f"""
                    MERGE INTO {QUARANTINE_TABLE}
                    AS target

                    USING {quarantine_view}
                    AS source

                    ON  target.source_topic =
                            source.source_topic
                    AND target.kafka_partition =
                            source.kafka_partition
                    AND target.kafka_offset =
                            source.kafka_offset

                    WHEN NOT MATCHED
                    THEN INSERT *
                """)

            finally:
                try:
                    spark.catalog.dropTempView(
                        quarantine_view
                    )
                except Exception:
                    pass

        # ----------------------------------------------------
        # 6. DQ Scorecard and count reconciliation
        # ----------------------------------------------------
        scorecard_df = write_dq_scorecard(
            validated_df,
            run_id,
            timer_batch_id,
        )

        _assert_scorecard_counts(
            scorecard_df=scorecard_df,
            bronze_count=new_row_count,
            valid_count=valid_count,
            invalid_count=invalid_count,
        )

        breach_metrics = (
            scorecard_df

            .agg(
                F.count("*")
                 .cast("int")
                 .alias(
                     "evaluated_rule_count"
                 ),

                F.sum(
                    F.when(
                        F.col(
                            "threshold_breached"
                        )
                        & (
                            F.upper("severity")
                            == "WARNING"
                        ),
                        1,
                    ).otherwise(0)
                ).cast("int").alias(
                    "warning_breach_count"
                ),

                F.sum(
                    F.when(
                        F.col(
                            "threshold_breached"
                        )
                        & (
                            F.upper("severity")
                            == "CRITICAL"
                        ),
                        1,
                    ).otherwise(0)
                ).cast("int").alias(
                    "critical_breach_count"
                ),
            )

            .first()
        )

        evaluated_rule_count = int(
            breach_metrics[
                "evaluated_rule_count"
            ] or 0
        )

        warning_breach_count = int(
            breach_metrics[
                "warning_breach_count"
            ] or 0
        )

        critical_breach_count = int(
            breach_metrics[
                "critical_breach_count"
            ] or 0
        )

        alert_transition_count = (
            update_alert_state_and_events(
                scorecard_df,
                run_id,
            )
        )

        lag_metrics = (
            validated_df

            .agg(
                F.max(
                    F.col("event_lag_seconds")
                ).cast("long").alias(
                    "max_event_lag_seconds"
                ),

                F.max(
                    F.greatest(
                        F.lit(0),
                        F.unix_timestamp(
                            F.current_timestamp()
                        )
                        - F.unix_timestamp(
                            "bronze_ingestion_timestamp"
                        ),
                    )
                ).cast("long").alias(
                    "max_ingestion_lag_seconds"
                ),

                F.countDistinct(
                    "source_topic"
                ).cast("int").alias(
                    "source_topic_count"
                ),
            )

            .first()
        )

        max_event_lag_seconds = (
            lag_metrics[
                "max_event_lag_seconds"
            ]
        )

        max_ingestion_lag_seconds = (
            lag_metrics[
                "max_ingestion_lag_seconds"
            ]
        )

        source_topic_count = int(
            lag_metrics[
                "source_topic_count"
            ] or 0
        )

        # ----------------------------------------------------
        # 7. Advance offsets only after all governed writes and
        #    both count invariants succeed
        # ----------------------------------------------------
        control_view = (
            "_streamcommerce_fixed_bound_"
            "offset_upper_bounds"
        )

        offset_upper_bounds.createOrReplaceTempView(
            control_view
        )

        try:
            spark.sql(f"""
                MERGE INTO {CONTROL_TABLE}
                AS target

                USING {control_view}
                AS source

                ON  target.pipeline_name =
                        source.pipeline_name
                AND target.source_topic =
                        source.source_topic
                AND target.kafka_partition =
                        source.kafka_partition

                WHEN MATCHED THEN UPDATE SET
                    target.last_kafka_offset =
                        greatest(
                            target.last_kafka_offset,
                            source.last_kafka_offset
                        ),
                    target.updated_at =
                        source.updated_at

                WHEN NOT MATCHED
                THEN INSERT *
            """)

        finally:
            try:
                spark.catalog.dropTempView(
                    control_view
                )
            except Exception:
                pass

        overall_quality_status = (
            "CRITICAL"
            if critical_breach_count > 0
            else (
                "WARNING"
                if warning_breach_count > 0
                else "HEALTHY"
            )
        )

        write_run_summary(
            run_id=run_id,
            timer_batch_id=timer_batch_id,
            cycle_started_at=cycle_started_at,
            run_status="SUCCESS",
            overall_quality_status=(
                overall_quality_status
            ),
            bronze_record_count=new_row_count,
            silver_candidate_count=valid_count,
            quarantine_candidate_count=(
                invalid_count
            ),
            source_topic_count=(
                source_topic_count
            ),
            evaluated_rule_count=(
                evaluated_rule_count
            ),
            warning_breach_count=(
                warning_breach_count
            ),
            critical_breach_count=(
                critical_breach_count
            ),
            max_event_lag_seconds=(
                max_event_lag_seconds
            ),
            max_ingestion_lag_seconds=(
                max_ingestion_lag_seconds
            ),
            error_message=None,
        )

        summary_written = True

        publish_pending_alerts(run_id)

        result = {
            "run_id": run_id,
            "status": "SUCCESS",
            "quality_status": (
                overall_quality_status
            ),
            "bronze_rows": new_row_count,
            "silver_candidates": valid_count,
            "quarantine_candidates": (
                invalid_count
            ),
            "count_gap": (
                valid_count
                + invalid_count
                - new_row_count
            ),
            "evaluated_rules": (
                evaluated_rule_count
            ),
            "warning_breaches": (
                warning_breach_count
            ),
            "critical_breaches": (
                critical_breach_count
            ),
            "alert_transitions": (
                alert_transition_count
            ),
        }

        print(
            "Fixed-bound incremental "
            "batch result:",
            result,
        )

        return result

    except Exception as exc:
        if not summary_written:
            try:
                write_run_summary(
                    run_id=run_id,
                    timer_batch_id=(
                        timer_batch_id
                    ),
                    cycle_started_at=(
                        cycle_started_at
                    ),
                    run_status="FAILED",
                    overall_quality_status=(
                        "UNKNOWN"
                    ),
                    bronze_record_count=(
                        new_row_count
                    ),
                    silver_candidate_count=(
                        valid_count
                    ),
                    quarantine_candidate_count=(
                        invalid_count
                    ),
                    source_topic_count=0,
                    evaluated_rule_count=0,
                    warning_breach_count=0,
                    critical_breach_count=0,
                    max_event_lag_seconds=None,
                    max_ingestion_lag_seconds=None,
                    error_message=str(exc),
                )
            except Exception as summary_exc:
                print(
                    "Failed to write DQ "
                    "failure summary:",
                    summary_exc,
                )

        raise

    finally:
        if scorecard_df is not None:
            try:
                scorecard_df.unpersist()
            except Exception:
                pass

        if offset_upper_bounds is not None:
            try:
                offset_upper_bounds.unpersist()
            except Exception:
                pass

        if validated_df is not None:
            try:
                validated_df.unpersist()
            except Exception:
                pass

        if bounded_bronze_df is not None:
            try:
                bounded_bronze_df.unpersist()
            except Exception:
                pass


print(
    "Fixed-bound incremental "
    "batch function initialized."
)

# %% [code cell 7]

import json
from datetime import datetime, timezone


existing_queries = [
    query
    for query in spark.streams.active
    if query.name == QUERY_NAME
]

if existing_queries:
    raise RuntimeError(
        f"An active streaming query named {QUERY_NAME!r} already exists "
        "in this Spark session."
    )


def process_timer_microbatch(_timer_batch_df, batch_id: int) -> None:
    started_at = datetime.now(timezone.utc)

    print(
        "\n============================================================"
    )
    print("Timer micro-batch ID :", batch_id)
    print("Cycle started UTC    :", started_at.isoformat())

    result = run_incremental_batch(batch_id)

    completed_at = datetime.now(timezone.utc)

    print(
        "Cycle completed UTC  :",
        completed_at.isoformat(),
    )
    print(
        "Cycle duration sec   :",
        round(
            (completed_at - started_at).total_seconds(),
            3,
        ),
    )
    print(
        "Cycle result         :",
        json.dumps(
            result,
            sort_keys=True,
            default=str,
        ),
    )


timer_stream = (
    spark.readStream
         .format("rate")
         .option("rowsPerSecond", 1)
         .option("numPartitions", 1)
         .load()
)


timed_pipeline_query = (
    timer_stream.writeStream

    .queryName(
        QUERY_NAME
    )

    .outputMode(
        "append"
    )

    .foreachBatch(
        process_timer_microbatch
    )

    .option(
        "checkpointLocation",
        TIMER_CHECKPOINT_LOCATION,
    )

    .trigger(
        processingTime=POLL_INTERVAL,
    )

    .start()
)


print("Timed Bronze -> Silver Workflow started.")
print("Query ID          :", timed_pipeline_query.id)
print("Run ID            :", timed_pipeline_query.runId)
print("Query name        :", timed_pipeline_query.name)
print("Poll interval     :", POLL_INTERVAL)
print("Timer checkpoint  :", TIMER_CHECKPOINT_LOCATION)
print("Waiting until the Workflow task is stopped or fails...")


timed_pipeline_query.awaitTermination()
