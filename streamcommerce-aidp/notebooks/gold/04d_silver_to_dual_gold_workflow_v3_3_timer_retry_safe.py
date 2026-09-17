"""Source export generated from the sanitized AIDP notebook."""

# %% [code cell 1]
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

    text = str(value).strip()
    return text if text else default


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {
        "1", "true", "yes", "y", "on"
    }


def _as_int(value: Any, name: str, minimum: int = 0) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception as exc:
        raise ValueError(f"{name} must be an integer; received {value!r}.") from exc

    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}; received {parsed}.")

    return parsed


def _validate_three_part_name(value: str, name: str) -> str:
    pattern = (
        r"^[A-Za-z_][A-Za-z0-9_]*\."
        r"[A-Za-z_][A-Za-z0-9_]*\."
        r"[A-Za-z_][A-Za-z0-9_]*$"
    )

    if not re.fullmatch(pattern, value):
        raise ValueError(
            f"{name} must be catalog.schema.object; "
            f"received {value!r}."
        )

    return value


GOLD_PIPELINE_NAME = (
    "streamcommerce_silver_to_gold_dual"
)

# Version the deterministic Gold Run identity separately from physical schemas.
GOLD_RUN_KEY_VERSION = "v3.3-timer-stream-stable-business-key"

TRANSFORMATION_VERSION = _workflow_parameter(
    "TRANSFORMATION_VERSION",
    "streamcommerce-gold-v3.3-aidp-adw-timer-retry-safe",
)

LEGACY_NON_IDEMPOTENT_TRANSFORMATION_VERSIONS = {
    "streamcommerce-gold-v3.0-aidp-adw",
    "streamcommerce-gold-v3.1-aidp-adw",
    "streamcommerce-gold-v3.1-aidp-adw-retry-safe",
    "streamcommerce-gold-v3.2-aidp-adw-silver-stream-retry-safe",
}

if TRANSFORMATION_VERSION in LEGACY_NON_IDEMPOTENT_TRANSFORMATION_VERSIONS:
    raise RuntimeError(
        "This runtime-compatible retry-safe notebook must use a new "
        "TRANSFORMATION_VERSION. Remove the legacy Workflow override or set "
        "TRANSFORMATION_VERSION to "
        "streamcommerce-gold-v3.3-aidp-adw-timer-retry-safe."
    )

WORKFLOW_JOB_NAME = _workflow_parameter(
    "WORKFLOW_JOB_NAME",
    "streamcommerce_silver_to_gold_dual",
)

WORKFLOW_RUN_ID = _workflow_parameter(
    "WORKFLOW_RUN_ID",
    "",
)

NOTEBOOK_PATH = _workflow_parameter(
    "NOTEBOOK_PATH",
    (
        "/Workspace/Gold/"
        "04d_silver_to_dual_gold_workflow_v3_3_timer_retry_safe.ipynb"
    ),
)

SILVER_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "SILVER_TABLE",
        "default.default.streamcommerce_silver_events",
    ),
    "SILVER_TABLE",
)

DQ_SCORECARD_SOURCE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_SCORECARD_SOURCE",
        "default.default.streamcommerce_dq_scorecard",
    ),
    "DQ_SCORECARD_SOURCE",
)

DQ_RUN_SUMMARY_SOURCE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_RUN_SUMMARY_SOURCE",
        "default.default.streamcommerce_dq_run_summary",
    ),
    "DQ_RUN_SUMMARY_SOURCE",
)

SILVER_PIPELINE_NAME = _workflow_parameter(
    "SILVER_PIPELINE_NAME",
    "streamcommerce_bronze_to_silver",
)

GOLD_DAILY_REVENUE_SNAPSHOT = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_DAILY_REVENUE_SNAPSHOT",
        "default.default.streamcommerce_gold_daily_revenue_snapshot",
    ),
    "GOLD_DAILY_REVENUE_SNAPSHOT",
)

GOLD_PRODUCT_PERFORMANCE_SNAPSHOT = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_PRODUCT_PERFORMANCE_SNAPSHOT",
        "default.default.streamcommerce_gold_product_performance_snapshot",
    ),
    "GOLD_PRODUCT_PERFORMANCE_SNAPSHOT",
)

GOLD_CUSTOMER_360_SNAPSHOT = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_CUSTOMER_360_SNAPSHOT",
        "default.default.streamcommerce_gold_customer_360_snapshot",
    ),
    "GOLD_CUSTOMER_360_SNAPSHOT",
)

GOLD_INVENTORY_HEALTH_SNAPSHOT = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_INVENTORY_HEALTH_SNAPSHOT",
        "default.default.streamcommerce_gold_inventory_health_snapshot",
    ),
    "GOLD_INVENTORY_HEALTH_SNAPSHOT",
)

GOLD_PAYMENT_RELIABILITY_SNAPSHOT = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_PAYMENT_RELIABILITY_SNAPSHOT",
        "default.default.streamcommerce_gold_payment_reliability_snapshot",
    ),
    "GOLD_PAYMENT_RELIABILITY_SNAPSHOT",
)

GOLD_DQ_SCORECARD_SNAPSHOT = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_DQ_SCORECARD_SNAPSHOT",
        "default.default.streamcommerce_gold_data_quality_scorecard_snapshot",
    ),
    "GOLD_DQ_SCORECARD_SNAPSHOT",
)

GOLD_DAILY_REVENUE = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_DAILY_REVENUE",
        "default.default.streamcommerce_gold_daily_revenue",
    ),
    "GOLD_DAILY_REVENUE",
)

GOLD_PRODUCT_PERFORMANCE = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_PRODUCT_PERFORMANCE",
        "default.default.streamcommerce_gold_product_performance",
    ),
    "GOLD_PRODUCT_PERFORMANCE",
)

GOLD_CUSTOMER_360 = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_CUSTOMER_360",
        "default.default.streamcommerce_gold_customer_360",
    ),
    "GOLD_CUSTOMER_360",
)

GOLD_INVENTORY_HEALTH = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_INVENTORY_HEALTH",
        "default.default.streamcommerce_gold_inventory_health",
    ),
    "GOLD_INVENTORY_HEALTH",
)

GOLD_PAYMENT_RELIABILITY = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_PAYMENT_RELIABILITY",
        "default.default.streamcommerce_gold_payment_reliability",
    ),
    "GOLD_PAYMENT_RELIABILITY",
)

GOLD_DQ_SCORECARD = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_DQ_SCORECARD",
        "default.default.streamcommerce_gold_data_quality_scorecard",
    ),
    "GOLD_DQ_SCORECARD",
)

GOLD_SNAPSHOT_CONTROL = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_SNAPSHOT_CONTROL",
        "default.default.streamcommerce_gold_snapshot_control",
    ),
    "GOLD_SNAPSHOT_CONTROL",
)

LINEAGE_PIPELINE_RUNS = _validate_three_part_name(
    _workflow_parameter(
        "LINEAGE_PIPELINE_RUNS",
        "default.default.streamcommerce_lineage_pipeline_runs",
    ),
    "LINEAGE_PIPELINE_RUNS",
)

LINEAGE_ASSET_RUNS = _validate_three_part_name(
    _workflow_parameter(
        "LINEAGE_ASSET_RUNS",
        "default.default.streamcommerce_lineage_asset_runs",
    ),
    "LINEAGE_ASSET_RUNS",
)

GOLD_ROW_LINEAGE_BRIDGE = _validate_three_part_name(
    _workflow_parameter(
        "GOLD_ROW_LINEAGE_BRIDGE",
        "default.default.streamcommerce_gold_row_lineage_bridge",
    ),
    "GOLD_ROW_LINEAGE_BRIDGE",
)

POLL_INTERVAL = _workflow_parameter(
    "POLL_INTERVAL",
    "1 minute",
)

QUERY_NAME = _workflow_parameter(
    "QUERY_NAME",
    "streamcommerce_silver_to_gold_dual_timer_retry_safe_v3",
)

TIMER_CHECKPOINT_LOCATION = _workflow_parameter(
    "TIMER_CHECKPOINT_LOCATION",
    (
        "/Volumes/default/default/streamcommerce_vol/"
        "checkpoints/streamcommerce_silver_to_gold_dual_timer_retry_safe_v3"
    ),
)

RUN_INITIAL_REFRESH = _as_bool(
    _workflow_parameter(
        "RUN_INITIAL_REFRESH",
        "true",
    )
)

WAIT_FOR_DQ_READY = _as_bool(
    _workflow_parameter(
        "WAIT_FOR_DQ_READY",
        "true",
    )
)

DQ_READY_TIMEOUT_SECONDS = _as_int(
    _workflow_parameter(
        "DQ_READY_TIMEOUT_SECONDS",
        "900",
    ),
    "DQ_READY_TIMEOUT_SECONDS",
    minimum=1,
)

DQ_READY_POLL_SECONDS = _as_int(
    _workflow_parameter(
        "DQ_READY_POLL_SECONDS",
        "10",
    ),
    "DQ_READY_POLL_SECONDS",
    minimum=1,
)

SKIP_ALREADY_PUBLISHED_ASSET_ON_RETRY = _as_bool(
    _workflow_parameter(
        "SKIP_ALREADY_PUBLISHED_ASSET_ON_RETRY",
        "true",
    )
)

DQ_GATE_MODE = _workflow_parameter(
    "DQ_GATE_MODE",
    "WARN",
).upper()

SKIP_IF_UNCHANGED = _as_bool(
    _workflow_parameter(
        "SKIP_IF_UNCHANGED",
        "true",
    )
)

ENABLE_ROW_LINEAGE_BRIDGE = _as_bool(
    _workflow_parameter(
        "ENABLE_ROW_LINEAGE_BRIDGE",
        "true",
    )
)

if DQ_GATE_MODE not in {"OFF", "WARN", "BLOCK"}:
    raise ValueError(
        "DQ_GATE_MODE must be OFF, WARN, or BLOCK."
    )

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true",
)

print("Pipeline name          :", GOLD_PIPELINE_NAME)
print("Transformation version :", TRANSFORMATION_VERSION)
print("Gold run key version   :", GOLD_RUN_KEY_VERSION)
print("Silver source          :", SILVER_TABLE)
print("Silver pipeline        :", SILVER_PIPELINE_NAME)
print("DQ source              :", DQ_SCORECARD_SOURCE)
print("DQ run summary         :", DQ_RUN_SUMMARY_SOURCE)
print("Trigger mode           :", "RATE_TIMER_STREAM")
print("Poll interval          :", POLL_INTERVAL)
print("Run initial refresh    :", RUN_INITIAL_REFRESH)
print("Wait for DQ ready      :", WAIT_FOR_DQ_READY)
print("DQ ready timeout sec   :", DQ_READY_TIMEOUT_SECONDS)
print("DQ gate mode           :", DQ_GATE_MODE)
print("Skip if unchanged      :", SKIP_IF_UNCHANGED)
print("Reuse published asset  :", SKIP_ALREADY_PUBLISHED_ASSET_ON_RETRY)
print("Row lineage bridge     :", ENABLE_ROW_LINEAGE_BRIDGE)
print("Timer checkpoint       :", TIMER_CHECKPOINT_LOCATION)

# ------------------------------------------------------------------
# ADW external catalog and serving schema.
# Change ADW_SCHEMA through Workflow parameters when the external
# catalog uses a schema other than ADMIN.
# ------------------------------------------------------------------
ADW_CATALOG = _workflow_parameter(
    "ADW_CATALOG",
    "vector_db_aidpdb",
)

ADW_SCHEMA = _workflow_parameter(
    "ADW_SCHEMA",
    "ADMIN",
)

PUBLISH_ROW_LINEAGE_TO_ADW = _as_bool(
    _workflow_parameter(
        "PUBLISH_ROW_LINEAGE_TO_ADW",
        "false",
    )
)

ADW_SKIP_OOS_STAGING = _as_bool(
    _workflow_parameter(
        "ADW_SKIP_OOS_STAGING",
        "true",
    )
)

ADW_MERGE_STAGING_USING_USERNAME = _as_bool(
    _workflow_parameter(
        "ADW_MERGE_STAGING_USING_USERNAME",
        "true",
    )
)

AIDP_ADW_PUBLISH_RUNS = _validate_three_part_name(
    _workflow_parameter(
        "AIDP_ADW_PUBLISH_RUNS",
        "default.default.streamcommerce_adw_publish_runs",
    ),
    "AIDP_ADW_PUBLISH_RUNS",
)


def adw_object(name: str) -> str:
    return f"{ADW_CATALOG}.{ADW_SCHEMA}.{name}"


ADW_GOLD_DAILY_REVENUE_SNAPSHOT = adw_object(
    "SC_GOLD_DAILY_REVENUE_S"
)
ADW_GOLD_PRODUCT_PERFORMANCE_SNAPSHOT = adw_object(
    "SC_GOLD_PRODUCT_PERFORMANCE_S"
)
ADW_GOLD_CUSTOMER_360_SNAPSHOT = adw_object(
    "SC_GOLD_CUSTOMER_360_S"
)
ADW_GOLD_INVENTORY_HEALTH_SNAPSHOT = adw_object(
    "SC_GOLD_INVENTORY_HEALTH_S"
)
ADW_GOLD_PAYMENT_RELIABILITY_SNAPSHOT = adw_object(
    "SC_GOLD_PAYMENT_RELIABILITY_S"
)
ADW_GOLD_DQ_SCORECARD_SNAPSHOT = adw_object(
    "SC_GOLD_DQ_SCORECARD_S"
)
ADW_GOLD_SNAPSHOT_CONTROL = adw_object(
    "SC_GOLD_SNAPSHOT_CONTROL"
)
ADW_LINEAGE_PIPELINE_RUNS = adw_object(
    "SC_LINEAGE_PIPELINE_RUNS"
)
ADW_LINEAGE_ASSET_RUNS = adw_object(
    "SC_LINEAGE_ASSET_RUNS"
)
ADW_LINEAGE_EDGES = adw_object(
    "SC_LINEAGE_EDGES"
)
ADW_COLUMN_LINEAGE = adw_object(
    "SC_COLUMN_LINEAGE"
)
ADW_GOLD_ROW_LINEAGE = adw_object(
    "SC_GOLD_ROW_LINEAGE"
)
ADW_PUBLISH_RUNS = adw_object(
    "SC_ADW_PUBLISH_RUNS"
)

ADW_GOLD_DAILY_REVENUE = adw_object(
    "SC_GOLD_DAILY_REVENUE"
)
ADW_GOLD_PRODUCT_PERFORMANCE = adw_object(
    "SC_GOLD_PRODUCT_PERFORMANCE"
)
ADW_GOLD_CUSTOMER_360 = adw_object(
    "SC_GOLD_CUSTOMER_360"
)
ADW_GOLD_INVENTORY_HEALTH = adw_object(
    "SC_GOLD_INVENTORY_HEALTH"
)
ADW_GOLD_PAYMENT_RELIABILITY = adw_object(
    "SC_GOLD_PAYMENT_RELIABILITY"
)
ADW_GOLD_DQ_SCORECARD = adw_object(
    "SC_GOLD_DQ_SCORECARD"
)

print("ADW catalog             :", ADW_CATALOG)
print("ADW schema              :", ADW_SCHEMA)
print("Publish row lineage ADW :", PUBLISH_ROW_LINEAGE_TO_ADW)

# %% [code cell 2]

import os

AIDP_REQUIRED_OBJECTS = [
    SILVER_TABLE,
    DQ_SCORECARD_SOURCE,
    DQ_RUN_SUMMARY_SOURCE,
    GOLD_DAILY_REVENUE_SNAPSHOT,
    GOLD_PRODUCT_PERFORMANCE_SNAPSHOT,
    GOLD_CUSTOMER_360_SNAPSHOT,
    GOLD_INVENTORY_HEALTH_SNAPSHOT,
    GOLD_PAYMENT_RELIABILITY_SNAPSHOT,
    GOLD_DQ_SCORECARD_SNAPSHOT,
    GOLD_DAILY_REVENUE,
    GOLD_PRODUCT_PERFORMANCE,
    GOLD_CUSTOMER_360,
    GOLD_INVENTORY_HEALTH,
    GOLD_PAYMENT_RELIABILITY,
    GOLD_DQ_SCORECARD,
    GOLD_SNAPSHOT_CONTROL,
    LINEAGE_PIPELINE_RUNS,
    LINEAGE_ASSET_RUNS,
    GOLD_ROW_LINEAGE_BRIDGE,
    AIDP_ADW_PUBLISH_RUNS,
]

for object_name in AIDP_REQUIRED_OBJECTS:
    try:
        spark.table(object_name).limit(0).collect()
    except Exception as exc:
        raise RuntimeError(
            f"Required AIDP object is missing or inaccessible: {object_name}"
        ) from exc

try:
    spark.sql(
        f"REFRESH SCHEMA IN EXTERNAL CATALOG "
        f"{ADW_CATALOG}.{ADW_SCHEMA}"
    )
except Exception as exc:
    print(
        "External schema refresh returned an exception. Continuing with "
        "the currently harvested metadata:",
        exc,
    )

ADW_REQUIRED_OBJECTS = [
    ADW_GOLD_DAILY_REVENUE_SNAPSHOT,
    ADW_GOLD_PRODUCT_PERFORMANCE_SNAPSHOT,
    ADW_GOLD_CUSTOMER_360_SNAPSHOT,
    ADW_GOLD_INVENTORY_HEALTH_SNAPSHOT,
    ADW_GOLD_PAYMENT_RELIABILITY_SNAPSHOT,
    ADW_GOLD_DQ_SCORECARD_SNAPSHOT,
    ADW_GOLD_SNAPSHOT_CONTROL,
    ADW_LINEAGE_PIPELINE_RUNS,
    ADW_LINEAGE_ASSET_RUNS,
    ADW_LINEAGE_EDGES,
    ADW_COLUMN_LINEAGE,
    ADW_GOLD_ROW_LINEAGE,
    ADW_PUBLISH_RUNS,
    ADW_GOLD_DAILY_REVENUE,
    ADW_GOLD_PRODUCT_PERFORMANCE,
    ADW_GOLD_CUSTOMER_360,
    ADW_GOLD_INVENTORY_HEALTH,
    ADW_GOLD_PAYMENT_RELIABILITY,
    ADW_GOLD_DQ_SCORECARD,
]

for object_name in ADW_REQUIRED_OBJECTS:
    try:
        spark.table(object_name).limit(0).collect()
    except Exception as exc:
        raise RuntimeError(
            f"Required ADW object is missing or inaccessible: {object_name}. "
            "Run 04b_adw_streamcommerce_serving_schema.sql in ADW and "
            "refresh vector_db_aidpdb."
        ) from exc


legacy_checkpoints = {
    (
        "/Volumes/default/default/streamcommerce_vol/"
        "checkpoints/streamcommerce_silver_to_gold_dual_v1"
    ),
    (
        "/Volumes/default/default/streamcommerce_vol/"
        "checkpoints/streamcommerce_silver_to_gold_dual_silver_stream_v1"
    ),
}

if TIMER_CHECKPOINT_LOCATION.rstrip("/") in {
    path.rstrip("/") for path in legacy_checkpoints
}:
    raise RuntimeError(
        "Use the new v3.3 TIMER_CHECKPOINT_LOCATION. Do not reuse a checkpoint "
        "created by the legacy timer workflow or the failed Silver Delta "
        "stream attempt."
    )

checkpoint_parts = TIMER_CHECKPOINT_LOCATION.split("/")
if len(checkpoint_parts) < 6 or checkpoint_parts[1] != "Volumes":
    raise RuntimeError(
        "TIMER_CHECKPOINT_LOCATION must use an AIDP managed Volume path."
    )

volume_root = "/" + "/".join(checkpoint_parts[1:5])
if not os.path.isdir(volume_root):
    raise RuntimeError(f"Managed Volume is unavailable: {volume_root}")

print("AIDP and ADW objects verified.")
print("Managed Volume verified:", volume_root)

# %% [code cell 3]
import hashlib
import json
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def latest_delta_version(table_name: str) -> int:
    row = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .agg(F.max("version").alias("version"))
        .first()
    )

    if row is None or row["version"] is None:
        raise RuntimeError(
            f"No Delta history is available for {table_name}."
        )

    return int(row["version"])


def read_delta_version(
    table_name: str,
    version: int,
):
    dataframe = (
        spark.read
        .format("delta")
        .option("versionAsOf", str(version))
        .table(table_name)
    )

    return dataframe


def delta_version_timestamp(table_name: str, version: int):
    rows = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .filter(F.col("version") == F.lit(int(version)))
        .select("timestamp")
        .limit(1)
        .collect()
    )

    if not rows:
        raise RuntimeError(
            f"Delta version {version} is unavailable for {table_name}."
        )

    return rows[0]["timestamp"]


def wait_for_dq_run_for_silver_version(silver_version: int):
    """Resolve the successful DQ run whose cycle contains the Silver commit."""
    if not WAIT_FOR_DQ_READY:
        return None

    silver_commit_timestamp = delta_version_timestamp(
        SILVER_TABLE,
        silver_version,
    )
    deadline = time.monotonic() + DQ_READY_TIMEOUT_SECONDS

    print(
        "Waiting for DQ run corresponding to Silver version",
        silver_version,
        "committed at",
        silver_commit_timestamp,
    )

    while time.monotonic() < deadline:
        summaries = (
            spark.table(DQ_RUN_SUMMARY_SOURCE)
            .filter(
                (F.col("pipeline_name") == F.lit(SILVER_PIPELINE_NAME))
                & (F.col("run_status") == F.lit("SUCCESS"))
                & (F.coalesce(F.col("bronze_record_count"), F.lit(0)) > 0)
                & (F.col("cycle_completed_at") >= F.lit(silver_commit_timestamp))
            )
            .withColumn(
                "_contains_silver_commit",
                F.when(
                    (F.col("cycle_started_at") <= F.lit(silver_commit_timestamp))
                    & (F.col("cycle_completed_at") >= F.lit(silver_commit_timestamp)),
                    F.lit(0),
                ).otherwise(F.lit(1)),
            )
            .orderBy(
                F.col("_contains_silver_commit").asc(),
                F.col("cycle_completed_at").asc(),
                F.col("run_id").asc(),
            )
            .limit(1)
            .collect()
        )

        if summaries:
            summary = summaries[0]
            run_id = summary["run_id"]
            scorecard_exists = bool(
                spark.table(DQ_SCORECARD_SOURCE)
                .filter(F.col("run_id") == F.lit(run_id))
                .limit(1)
                .collect()
            )

            if scorecard_exists:
                result = {
                    "run_id": run_id,
                    "cycle_started_at": summary["cycle_started_at"],
                    "cycle_completed_at": summary["cycle_completed_at"],
                    "silver_commit_timestamp": silver_commit_timestamp,
                }
                print("Matched DQ run:", result)
                return result

        print(
            "DQ run is not ready yet; sleeping",
            DQ_READY_POLL_SECONDS,
            "seconds.",
        )
        time.sleep(DQ_READY_POLL_SECONDS)

    raise RuntimeError(
        "Timed out waiting for a successful DQ run associated with Silver "
        f"version {silver_version}. Timeout={DQ_READY_TIMEOUT_SECONDS}s."
    )


def schema_sha256(dataframe) -> str:
    return hashlib.sha256(
        dataframe.schema.json().encode("utf-8")
    ).hexdigest()


def deterministic_gold_run_id(
    silver_version: int,
    dq_version: int,
) -> str:
    raw = "|".join([
        GOLD_PIPELINE_NAME,
        GOLD_RUN_KEY_VERSION,
        str(silver_version),
        str(dq_version),
        TRANSFORMATION_VERSION,
        DQ_GATE_MODE,
        str(bool(ENABLE_ROW_LINEAGE_BRIDGE)),
        ADW_CATALOG,
        ADW_SCHEMA,
        str(bool(PUBLISH_ROW_LINEAGE_TO_ADW)),
    ])

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()


def merge_dataframe(
    dataframe,
    target_table: str,
    source_view: str,
    merge_condition: str,
) -> int:
    target_columns = spark.table(target_table).columns
    ordered = dataframe.select(*target_columns)
    row_count = int(ordered.count())

    ordered.createOrReplaceTempView(source_view)

    try:
        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING {source_view} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        try:
            spark.catalog.dropTempView(source_view)
        except Exception:
            pass

    return row_count


def upsert_pipeline_run(payload: dict) -> None:
    schema = spark.table(LINEAGE_PIPELINE_RUNS).schema
    dataframe = spark.createDataFrame(
        [payload],
        schema=schema,
    )

    dataframe.createOrReplaceTempView(
        "_streamcommerce_lineage_pipeline_run"
    )

    try:
        spark.sql(f"""
            MERGE INTO {LINEAGE_PIPELINE_RUNS} AS target
            USING _streamcommerce_lineage_pipeline_run AS source
            ON target.gold_run_id = source.gold_run_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(
            "_streamcommerce_lineage_pipeline_run"
        )


def upsert_asset_run(payload: dict) -> None:
    schema = spark.table(LINEAGE_ASSET_RUNS).schema
    dataframe = spark.createDataFrame(
        [payload],
        schema=schema,
    )

    dataframe.createOrReplaceTempView(
        "_streamcommerce_lineage_asset_run"
    )

    try:
        spark.sql(f"""
            MERGE INTO {LINEAGE_ASSET_RUNS} AS target
            USING _streamcommerce_lineage_asset_run AS source
            ON target.asset_run_id = source.asset_run_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(
            "_streamcommerce_lineage_asset_run"
        )


def activate_snapshot(
    *,
    asset_group: str,
    gold_run_id: str,
    silver_version: int,
    dq_version: int,
    dq_run_id: str,
) -> None:
    now = datetime.now(timezone.utc)

    payload = [(
        GOLD_PIPELINE_NAME,
        asset_group,
        gold_run_id,
        int(silver_version),
        int(dq_version),
        dq_run_id,
        TRANSFORMATION_VERSION,
        now,
        now,
    )]

    schema = spark.table(
        GOLD_SNAPSHOT_CONTROL
    ).schema

    dataframe = spark.createDataFrame(
        payload,
        schema=schema,
    )

    dataframe.createOrReplaceTempView(
        "_streamcommerce_gold_snapshot_activation"
    )

    try:
        spark.sql(f"""
            MERGE INTO {GOLD_SNAPSHOT_CONTROL} AS target
            USING _streamcommerce_gold_snapshot_activation
                  AS source
            ON  target.pipeline_name = source.pipeline_name
            AND target.asset_group = source.asset_group

            WHEN MATCHED THEN UPDATE SET
                target.active_gold_run_id =
                    source.active_gold_run_id,
                target.source_silver_version =
                    source.source_silver_version,
                target.source_dq_scorecard_version =
                    source.source_dq_scorecard_version,
                target.source_dq_run_id =
                    source.source_dq_run_id,
                target.transformation_version =
                    source.transformation_version,
                target.activated_at =
                    source.activated_at,
                target.updated_at =
                    source.updated_at

            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(
            "_streamcommerce_gold_snapshot_activation"
        )


def source_signature_already_evaluated(
    *,
    silver_version: int,
    dq_version: int,
) -> bool:
    if not SKIP_IF_UNCHANGED:
        return False

    rows = (
        spark.table(LINEAGE_PIPELINE_RUNS)

        .filter(
            (F.col("pipeline_name") == GOLD_PIPELINE_NAME)
            & (F.col("source_silver_version") == silver_version)
            & (
                F.col("source_dq_scorecard_version")
                == dq_version
            )
            & (
                F.col("transformation_version")
                == TRANSFORMATION_VERSION
            )
            & (F.col("dq_gate_mode") == DQ_GATE_MODE)
            & (
                F.col("row_lineage_enabled")
                == F.lit(bool(ENABLE_ROW_LINEAGE_BRIDGE))
            )
            & F.col("run_status").isin(
                "SUCCESS",
                "BLOCKED_BY_DQ",
            )
        )

        .limit(1)

        .collect()
    )

    return bool(rows)


def latest_dq_snapshot(dq_df, preferred_run_id: str | None = None):
    if preferred_run_id:
        selected_df = dq_df.filter(
            F.col("run_id") == F.lit(preferred_run_id)
        )
        if not selected_df.limit(1).collect():
            raise RuntimeError(
                f"DQ run {preferred_run_id} is not present in the pinned "
                "DQ Scorecard Delta version."
            )
        run_id = preferred_run_id
    else:
        latest_row = (
            dq_df
            .select("run_id", "checked_at")
            .orderBy(
                F.col("checked_at").desc(),
                F.col("run_id").desc(),
            )
            .limit(1)
            .collect()
        )

        if not latest_row:
            empty = dq_df.limit(0)
            return {
                "run_id": "",
                "status": "NO_DQ_RESULT",
                "critical_breaches": 0,
                "dataframe": empty,
            }

        run_id = latest_row[0]["run_id"]
        selected_df = dq_df.filter(F.col("run_id") == F.lit(run_id))

    critical_breaches = int(
        selected_df
        .filter(
            F.col("threshold_breached")
            & (F.upper(F.col("severity")) == F.lit("CRITICAL"))
        )
        .count()
    )

    status = "CRITICAL" if critical_breaches > 0 else "PASS"

    return {
        "run_id": run_id,
        "status": status,
        "critical_breaches": critical_breaches,
        "dataframe": selected_df,
    }


print("Reproducible snapshot, stream, and DQ synchronization helpers initialized.")

# %% [code cell 4]
def add_snapshot_metadata(
    dataframe,
    *,
    gold_run_id: str,
    silver_version: int,
    dq_version: int,
    refreshed_at,
):
    return (
        dataframe
        .withColumn(
            "gold_run_id",
            F.lit(gold_run_id),
        )
        .withColumn(
            "transformation_version",
            F.lit(TRANSFORMATION_VERSION),
        )
        .withColumn(
            "source_silver_version",
            F.lit(silver_version).cast("long"),
        )
        .withColumn(
            "source_dq_scorecard_version",
            F.lit(dq_version).cast("long"),
        )
        .withColumn(
            "snapshot_date",
            F.lit(refreshed_at.date()).cast("date"),
        )
        .withColumn(
            "refreshed_at",
            F.lit(refreshed_at).cast("timestamp"),
        )
    )


def build_gold_bundle(
    *,
    silver_df,
    latest_dq_df,
    source_dq_run_id: str,
    gold_run_id: str,
    silver_version: int,
    dq_version: int,
    refreshed_at,
):
    # --------------------------------------------------------
    # Canonical orders
    # --------------------------------------------------------
    order_events = (
        silver_df
        .filter(
            (F.col("source_topic") == "orders")
            & F.col("order_id").isNotNull()
        )
    )

    order_base_window = (
        Window.partitionBy("order_id")
        .orderBy(
            F.when(
                F.col("event_type") == "order_created",
                F.lit(0),
            ).otherwise(F.lit(1)),
            F.col("event_timestamp").asc_nulls_last(),
            F.col("sequence_number").asc_nulls_last(),
            F.col("kafka_offset").asc_nulls_last(),
        )
    )

    order_final_window = (
        Window.partitionBy("order_id")
        .orderBy(
            F.col("sequence_number").desc_nulls_last(),
            F.col("event_timestamp").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last(),
        )
    )

    order_base = (
        order_events
        .withColumn(
            "_order_base_rank",
            F.row_number().over(order_base_window),
        )
        .filter(F.col("_order_base_rank") == 1)
        .select(
            "order_id",
            "customer_id",
            "session_id",
            "product_id",
            "product_name",
            "category",
            "seller_id",
            "quantity",
            "price",
            "amount",
            "currency",
            F.col("event_id").alias("order_event_id"),
            F.col("source_topic").alias("order_source_topic"),
            F.col("kafka_partition").alias(
                "order_kafka_partition"
            ),
            F.col("kafka_offset").alias(
                "order_kafka_offset"
            ),
            F.col("event_timestamp").alias(
                "order_timestamp"
            ),
            F.to_date("event_timestamp").alias(
                "event_date"
            ),
        )
    )

    order_final = (
        order_events
        .withColumn(
            "_order_final_rank",
            F.row_number().over(order_final_window),
        )
        .filter(F.col("_order_final_rank") == 1)
        .select(
            "order_id",
            F.col("order_status").alias(
                "final_order_status"
            ),
        )
    )

    orders_fact = order_base.join(
        order_final,
        on="order_id",
        how="left",
    )

    # --------------------------------------------------------
    # Payment lifecycle
    # --------------------------------------------------------
    payment_events = (
        silver_df
        .filter(
            (F.col("source_topic") == "payments")
            & F.col("payment_id").isNotNull()
        )
    )

    payment_final_window = (
        Window.partitionBy("payment_id")
        .orderBy(
            F.col("sequence_number").desc_nulls_last(),
            F.col("event_timestamp").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last(),
        )
    )

    final_payments = (
        payment_events
        .withColumn(
            "_payment_final_rank",
            F.row_number().over(payment_final_window),
        )
        .filter(F.col("_payment_final_rank") == 1)
        .select(
            "event_id",
            "source_topic",
            "kafka_partition",
            "kafka_offset",
            "payment_id",
            "order_id",
            "customer_id",
            "product_id",
            "payment_status",
            "amount",
            "currency",
            F.col("event_timestamp").alias(
                "payment_timestamp"
            ),
            F.to_date("event_timestamp").alias(
                "event_date"
            ),
        )
    )

    payment_by_order = (
        payment_events
        .groupBy("order_id")
        .agg(
            F.max(
                F.when(
                    F.col("payment_status") == "SUCCESS",
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("ever_paid"),
            F.sum(
                F.when(
                    F.col("payment_status") == "REFUNDED",
                    F.coalesce(F.col("amount"), F.lit(0.0)),
                ).otherwise(F.lit(0.0))
            ).alias("refunded_amount"),
        )
    )

    paid_orders = (
        orders_fact
        .join(
            payment_by_order,
            on="order_id",
            how="inner",
        )
        .filter(F.col("ever_paid") == 1)
        .withColumn(
            "refunded_amount",
            F.coalesce(
                F.col("refunded_amount"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "net_order_revenue",
            F.coalesce(F.col("amount"), F.lit(0.0))
            - F.col("refunded_amount"),
        )
    )

    # --------------------------------------------------------
    # Daily revenue
    # --------------------------------------------------------
    daily_revenue = (
        paid_orders
        .groupBy("event_date", "currency")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.countDistinct("order_id").cast("long").alias(
                "total_orders"
            ),
            F.sum("refunded_amount").alias(
                "refunded_amount"
            ),
            F.sum("net_order_revenue").alias(
                "net_revenue"
            ),
            F.countDistinct("customer_id").cast("long").alias(
                "paid_customers"
            ),
            F.countDistinct(
                F.when(
                    F.col("final_order_status") == "CANCELLED",
                    F.col("order_id"),
                )
            ).cast("long").alias("cancelled_orders"),
            F.countDistinct(
                F.when(
                    F.col("final_order_status") == "RETURNED",
                    F.col("order_id"),
                )
            ).cast("long").alias("returned_orders"),
        )
        .withColumn(
            "average_order_value",
            F.when(
                F.col("total_orders") > 0,
                F.col("total_revenue")
                / F.col("total_orders"),
            ).otherwise(F.lit(0.0)),
        )
    )

    daily_revenue = add_snapshot_metadata(
        daily_revenue,
        gold_run_id=gold_run_id,
        silver_version=silver_version,
        dq_version=dq_version,
        refreshed_at=refreshed_at,
    ).select(
        "gold_run_id",
        "transformation_version",
        "source_silver_version",
        "source_dq_scorecard_version",
        "snapshot_date",
        "event_date",
        "currency",
        "total_revenue",
        "total_orders",
        "average_order_value",
        "refunded_amount",
        "net_revenue",
        "paid_customers",
        "cancelled_orders",
        "returned_orders",
        "refreshed_at",
    )

    # --------------------------------------------------------
    # Product performance
    # --------------------------------------------------------
    product_performance = (
        paid_orders
        .groupBy(
            "product_id",
            "product_name",
            "category",
            "seller_id",
            "currency",
        )
        .agg(
            F.sum(
                F.coalesce(F.col("quantity"), F.lit(0))
            ).cast("long").alias("units_sold"),
            F.sum("amount").alias("revenue"),
            F.countDistinct("order_id").cast("long").alias(
                "orders"
            ),
            F.countDistinct("customer_id").cast("long").alias(
                "customers"
            ),
            F.sum("refunded_amount").alias(
                "refunded_amount"
            ),
            F.sum("net_order_revenue").alias(
                "net_revenue"
            ),
            F.max("order_timestamp").alias(
                "last_order_timestamp"
            ),
        )
        .withColumn(
            "average_unit_price",
            F.when(
                F.col("units_sold") > 0,
                F.col("revenue") / F.col("units_sold"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "average_order_value",
            F.when(
                F.col("orders") > 0,
                F.col("revenue") / F.col("orders"),
            ).otherwise(F.lit(0.0)),
        )
    )

    product_performance = add_snapshot_metadata(
        product_performance,
        gold_run_id=gold_run_id,
        silver_version=silver_version,
        dq_version=dq_version,
        refreshed_at=refreshed_at,
    ).select(
        "gold_run_id",
        "transformation_version",
        "source_silver_version",
        "source_dq_scorecard_version",
        "snapshot_date",
        "product_id",
        "product_name",
        "category",
        "seller_id",
        "currency",
        "units_sold",
        "revenue",
        "orders",
        "customers",
        "average_unit_price",
        "average_order_value",
        "refunded_amount",
        "net_revenue",
        "last_order_timestamp",
        "refreshed_at",
    )

    # --------------------------------------------------------
    # Customer 360
    # --------------------------------------------------------
    customer_events = (
        silver_df
        .filter(
            (F.col("source_topic") == "customer_events")
            & F.col("customer_id").isNotNull()
        )
    )

    customer_engagement = (
        customer_events
        .groupBy("customer_id")
        .agg(
            F.countDistinct("session_id").cast("long").alias(
                "sessions"
            ),
            F.sum(
                F.when(
                    F.col("event_type") == "product_view",
                    1,
                ).otherwise(0)
            ).cast("long").alias("product_views"),
            F.sum(
                F.when(
                    F.col("event_type") == "add_to_cart",
                    1,
                ).otherwise(0)
            ).cast("long").alias("add_to_cart_events"),
            F.sum(
                F.when(
                    F.col("event_type")
                    == "checkout_started",
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "checkout_started_events"
            ),
            F.min("event_timestamp").alias(
                "first_seen_timestamp"
            ),
            F.max("event_timestamp").alias(
                "last_seen_timestamp"
            ),
        )
    )

    customer_orders = (
        paid_orders
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.sum("amount").alias("lifetime_value"),
            F.sum("net_order_revenue").alias(
                "net_lifetime_value"
            ),
            F.countDistinct("order_id").cast("long").alias(
                "orders"
            ),
            F.min("event_date").alias("first_order_date"),
            F.max("event_date").alias("last_order_date"),
        )
        .withColumn(
            "average_order_value",
            F.when(
                F.col("orders") > 0,
                F.col("lifetime_value")
                / F.col("orders"),
            ).otherwise(F.lit(0.0)),
        )
    )

    customer_360 = (
        customer_orders
        .join(
            customer_engagement,
            on="customer_id",
            how="full",
        )
        .fillna({
            "lifetime_value": 0.0,
            "net_lifetime_value": 0.0,
            "orders": 0,
            "average_order_value": 0.0,
            "sessions": 0,
            "product_views": 0,
            "add_to_cart_events": 0,
            "checkout_started_events": 0,
        })
        .withColumn(
            "repeat_purchase_flag",
            F.col("orders") > 1,
        )
        .withColumn(
            "conversion_rate",
            F.when(
                F.col("sessions") > 0,
                F.col("orders") / F.col("sessions"),
            ).otherwise(F.lit(0.0)),
        )
    )

    customer_360 = add_snapshot_metadata(
        customer_360,
        gold_run_id=gold_run_id,
        silver_version=silver_version,
        dq_version=dq_version,
        refreshed_at=refreshed_at,
    ).select(
        "gold_run_id",
        "transformation_version",
        "source_silver_version",
        "source_dq_scorecard_version",
        "snapshot_date",
        "customer_id",
        "lifetime_value",
        "net_lifetime_value",
        "orders",
        "sessions",
        "repeat_purchase_flag",
        "average_order_value",
        "product_views",
        "add_to_cart_events",
        "checkout_started_events",
        "conversion_rate",
        "first_seen_timestamp",
        "last_seen_timestamp",
        "first_order_date",
        "last_order_date",
        "refreshed_at",
    )

    # --------------------------------------------------------
    # Inventory health
    # --------------------------------------------------------
    inventory_events = (
        silver_df
        .filter(
            (F.col("source_topic") == "inventory_updates")
            & F.col("product_id").isNotNull()
        )
    )

    inventory_latest_window = (
        Window.partitionBy("product_id")
        .orderBy(
            F.col("event_timestamp").desc_nulls_last(),
            F.col("sequence_number").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last(),
        )
    )

    inventory_latest = (
        inventory_events
        .withColumn(
            "_inventory_rank",
            F.row_number().over(inventory_latest_window),
        )
        .filter(F.col("_inventory_rank") == 1)
        .select(
            "product_id",
            F.col("inventory_status").alias(
                "latest_inventory_status"
            ),
            F.col("warehouse_id").alias(
                "latest_warehouse_id"
            ),
            F.col("event_timestamp").alias(
                "last_inventory_event_timestamp"
            ),
        )
    )

    product_latest_window = (
        Window.partitionBy("product_id")
        .orderBy(
            F.col("event_timestamp").desc_nulls_last(),
            F.col("sequence_number").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last(),
        )
    )

    product_latest = (
        silver_df
        .filter(
            (F.col("source_topic")
             == "product_catalog_updates")
            & F.col("product_id").isNotNull()
        )
        .withColumn(
            "_product_rank",
            F.row_number().over(product_latest_window),
        )
        .filter(F.col("_product_rank") == 1)
        .select(
            "product_id",
            F.col("product_name").alias(
                "catalog_product_name"
            ),
            F.col("category").alias(
                "catalog_category"
            ),
        )
    )

    inventory_health = (
        inventory_events
        .groupBy("product_id")
        .agg(
            F.count("*").cast("long").alias(
                "stock_events"
            ),
            F.sum(
                F.when(
                    F.col("event_type") == "stock_added",
                    1,
                ).otherwise(0)
            ).cast("long").alias("stock_added_events"),
            F.sum(
                F.when(
                    F.col("event_type") == "stock_reserved",
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "stock_reserved_events"
            ),
            F.sum(
                F.when(
                    F.col("event_type") == "stock_released",
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "stock_released_events"
            ),
            F.sum(
                F.when(
                    F.col("event_type") == "stockout_warning",
                    1,
                ).otherwise(0)
            ).cast("long").alias("stockout_warnings"),
            F.sum(
                F.when(
                    F.col("event_type").isin(
                        "stock_added",
                        "stock_released",
                    ),
                    F.coalesce(
                        F.col("quantity"),
                        F.lit(0),
                    ),
                )
                .when(
                    F.col("event_type") == "stock_reserved",
                    -F.coalesce(
                        F.col("quantity"),
                        F.lit(0),
                    ),
                )
                .otherwise(F.lit(0))
            ).cast("long").alias(
                "estimated_net_stock_change"
            ),
            F.max("product_name").alias(
                "event_product_name"
            ),
            F.max("category").alias(
                "event_category"
            ),
        )
        .withColumn(
            "stockout_risk",
            F.when(
                F.col("stock_events") > 0,
                F.col("stockout_warnings")
                / F.col("stock_events"),
            ).otherwise(F.lit(0.0)),
        )
        .join(
            inventory_latest,
            on="product_id",
            how="left",
        )
        .join(
            product_latest,
            on="product_id",
            how="left",
        )
        .withColumn(
            "product_name",
            F.coalesce(
                F.col("catalog_product_name"),
                F.col("event_product_name"),
            ),
        )
        .withColumn(
            "category",
            F.coalesce(
                F.col("catalog_category"),
                F.col("event_category"),
            ),
        )
    )

    inventory_health = add_snapshot_metadata(
        inventory_health,
        gold_run_id=gold_run_id,
        silver_version=silver_version,
        dq_version=dq_version,
        refreshed_at=refreshed_at,
    ).select(
        "gold_run_id",
        "transformation_version",
        "source_silver_version",
        "source_dq_scorecard_version",
        "snapshot_date",
        "product_id",
        "product_name",
        "category",
        "stock_events",
        "stock_added_events",
        "stock_reserved_events",
        "stock_released_events",
        "stockout_warnings",
        "stockout_risk",
        "estimated_net_stock_change",
        "latest_inventory_status",
        "latest_warehouse_id",
        "last_inventory_event_timestamp",
        "refreshed_at",
    )

    # --------------------------------------------------------
    # Payment reliability
    # --------------------------------------------------------
    payment_reliability = (
        final_payments
        .groupBy("event_date", "currency")
        .agg(
            F.countDistinct("payment_id").cast("long").alias(
                "payments"
            ),
            F.sum(
                F.when(
                    F.col("payment_status") == "SUCCESS",
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "successful_payments"
            ),
            F.sum(
                F.when(
                    F.col("payment_status") == "FAILED",
                    1,
                ).otherwise(0)
            ).cast("long").alias("failed_payments"),
            F.sum(
                F.when(
                    F.col("payment_status") == "PENDING",
                    1,
                ).otherwise(0)
            ).cast("long").alias("pending_payments"),
            F.sum(
                F.when(
                    F.col("payment_status") == "REFUNDED",
                    1,
                ).otherwise(0)
            ).cast("long").alias(
                "refunded_payments"
            ),
            F.sum(
                F.when(
                    F.col("payment_status") == "SUCCESS",
                    F.coalesce(
                        F.col("amount"),
                        F.lit(0.0),
                    ),
                ).otherwise(F.lit(0.0))
            ).alias("successful_amount"),
            F.sum(
                F.when(
                    F.col("payment_status") == "FAILED",
                    F.coalesce(
                        F.col("amount"),
                        F.lit(0.0),
                    ),
                ).otherwise(F.lit(0.0))
            ).alias("failed_amount"),
            F.sum(
                F.when(
                    F.col("payment_status") == "REFUNDED",
                    F.coalesce(
                        F.col("amount"),
                        F.lit(0.0),
                    ),
                ).otherwise(F.lit(0.0))
            ).alias("refunded_amount"),
        )
        .withColumn(
            "failed_payment_rate",
            F.when(
                F.col("payments") > 0,
                F.col("failed_payments")
                / F.col("payments"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "success_rate",
            F.when(
                F.col("payments") > 0,
                F.col("successful_payments")
                / F.col("payments"),
            ).otherwise(F.lit(0.0)),
        )
    )

    payment_reliability = add_snapshot_metadata(
        payment_reliability,
        gold_run_id=gold_run_id,
        silver_version=silver_version,
        dq_version=dq_version,
        refreshed_at=refreshed_at,
    ).select(
        "gold_run_id",
        "transformation_version",
        "source_silver_version",
        "source_dq_scorecard_version",
        "snapshot_date",
        "event_date",
        "currency",
        "payments",
        "successful_payments",
        "failed_payments",
        "pending_payments",
        "refunded_payments",
        "successful_amount",
        "failed_amount",
        "refunded_amount",
        "failed_payment_rate",
        "success_rate",
        "refreshed_at",
    )

    # --------------------------------------------------------
    # Latest DQ scorecard snapshot
    # --------------------------------------------------------
    gold_dq = (
        latest_dq_df
        .withColumn(
            "table_name",
            F.col("source_topic"),
        )
        .withColumn(
            "check_name",
            F.col("rule_id"),
        )
        .withColumn(
            "passed",
            F.col("failed_record_count") == 0,
        )
        .withColumn(
            "pass_percentage_pct",
            F.col("pass_percentage").cast("double"),
        )
        .withColumn(
            "pass_percentage",
            F.col("pass_percentage").cast("double")
            / 100.0,
        )
        .withColumn(
            "severity",
            F.lower(F.col("severity")),
        )
        .withColumn(
            "source_dq_run_id",
            F.lit(source_dq_run_id),
        )
    )

    gold_dq = add_snapshot_metadata(
        gold_dq,
        gold_run_id=gold_run_id,
        silver_version=silver_version,
        dq_version=dq_version,
        refreshed_at=refreshed_at,
    ).select(
        "gold_run_id",
        "transformation_version",
        "source_silver_version",
        "source_dq_scorecard_version",
        "source_dq_run_id",
        "snapshot_date",
        "run_id",
        "table_name",
        "check_name",
        "passed",
        F.col("failed_record_count").cast("long"),
        F.col("total_record_count").cast("long"),
        "pass_percentage",
        "pass_percentage_pct",
        "severity",
        "reason_code",
        "quality_dimension",
        "check_status",
        "threshold_breached",
        "checked_at",
        "check_date",
        "refreshed_at",
    )

    return {
        "gold_frames": {
            "daily_revenue": daily_revenue,
            "product_performance": product_performance,
            "customer_360": customer_360,
            "inventory_health": inventory_health,
            "payment_reliability": payment_reliability,
            "data_quality_scorecard": gold_dq,
        },
        "contexts": {
            "order_events": order_events,
            "paid_orders": paid_orders,
            "payment_events": payment_events,
            "final_payments": final_payments,
            "customer_events": customer_events,
            "inventory_events": inventory_events,
        },
    }


print("Gold transformations initialized.")

# %% [code cell 5]

def build_row_lineage_bridge(
    *,
    contexts: dict,
    gold_run_id: str,
    refreshed_at,
):
    order_events = contexts["order_events"]
    paid_orders = contexts["paid_orders"]
    payment_events = contexts["payment_events"]
    final_payments = contexts["final_payments"]
    customer_events = contexts["customer_events"]
    inventory_events = contexts["inventory_events"]

    paid_order_keys = (
        paid_orders
        .select(
            "order_id",
            "event_date",
            "currency",
            "product_id",
            "customer_id",
        )
        .dropDuplicates(["order_id"])
    )

    paid_order_events = (
        order_events.alias("event")
        .join(
            paid_order_keys.alias("paid"),
            on="order_id",
            how="inner",
        )
    )

    paid_payment_events = (
        payment_events.alias("event")
        .join(
            paid_order_keys.alias("paid"),
            on="order_id",
            how="inner",
        )
    )

    # --------------------------------------------------------
    # Daily revenue lineage: all paid order-state events
    # and all payment lifecycle events for those orders.
    # --------------------------------------------------------
    daily_order = (
        paid_order_events
        .select(
            F.lit(GOLD_DAILY_REVENUE).alias(
                "target_asset"
            ),
            F.concat_ws(
                "|",
                F.date_format(
                    F.col("paid.event_date"),
                    "yyyy-MM-dd",
                ),
                F.coalesce(
                    F.col("paid.currency"),
                    F.lit(""),
                ),
            ).alias("target_business_key"),
            F.col("event.event_id").alias(
                "source_event_id"
            ),
            F.col("event.source_topic").alias(
                "source_topic"
            ),
            F.col("event.kafka_partition").cast("int").alias(
                "source_kafka_partition"
            ),
            F.col("event.kafka_offset").cast("long").alias(
                "source_kafka_offset"
            ),
            F.col("event.event_timestamp").alias(
                "source_event_timestamp"
            ),
            F.col("event.order_id").alias(
                "source_order_id"
            ),
            F.col("event.payment_id").alias(
                "source_payment_id"
            ),
            F.col("paid.product_id").alias(
                "source_product_id"
            ),
            F.col("paid.customer_id").alias(
                "source_customer_id"
            ),
            F.lit("ORDER_LIFECYCLE").alias(
                "lineage_role"
            ),
        )
    )

    daily_payment = (
        paid_payment_events
        .select(
            F.lit(GOLD_DAILY_REVENUE).alias(
                "target_asset"
            ),
            F.concat_ws(
                "|",
                F.date_format(
                    F.col("paid.event_date"),
                    "yyyy-MM-dd",
                ),
                F.coalesce(
                    F.col("paid.currency"),
                    F.lit(""),
                ),
            ).alias("target_business_key"),
            F.col("event.event_id").alias(
                "source_event_id"
            ),
            F.col("event.source_topic").alias(
                "source_topic"
            ),
            F.col("event.kafka_partition").cast("int").alias(
                "source_kafka_partition"
            ),
            F.col("event.kafka_offset").cast("long").alias(
                "source_kafka_offset"
            ),
            F.col("event.event_timestamp").alias(
                "source_event_timestamp"
            ),
            F.col("event.order_id").alias(
                "source_order_id"
            ),
            F.col("event.payment_id").alias(
                "source_payment_id"
            ),
            F.col("paid.product_id").alias(
                "source_product_id"
            ),
            F.col("paid.customer_id").alias(
                "source_customer_id"
            ),
            F.lit("PAYMENT_LIFECYCLE").alias(
                "lineage_role"
            ),
        )
    )

    # --------------------------------------------------------
    # Product-performance lineage
    # --------------------------------------------------------
    product_order = (
        daily_order
        .withColumn(
            "target_asset",
            F.lit(GOLD_PRODUCT_PERFORMANCE),
        )
        .drop("target_business_key")
        .join(
            paid_order_keys.select(
                "order_id",
                "product_id",
                "currency",
            ).alias("paid"),
            F.col("source_order_id")
            == F.col("paid.order_id"),
            how="inner",
        )
        .withColumn(
            "target_business_key",
            F.concat_ws(
                "|",
                F.col("paid.product_id"),
                F.coalesce(
                    F.col("paid.currency"),
                    F.lit(""),
                ),
            ),
        )
        .drop("order_id", "product_id", "currency")
    )

    product_payment = (
        daily_payment
        .withColumn(
            "target_asset",
            F.lit(GOLD_PRODUCT_PERFORMANCE),
        )
        .drop("target_business_key")
        .join(
            paid_order_keys.select(
                "order_id",
                "product_id",
                "currency",
            ).alias("paid"),
            F.col("source_order_id")
            == F.col("paid.order_id"),
            how="inner",
        )
        .withColumn(
            "target_business_key",
            F.concat_ws(
                "|",
                F.col("paid.product_id"),
                F.coalesce(
                    F.col("paid.currency"),
                    F.lit(""),
                ),
            ),
        )
        .drop("order_id", "product_id", "currency")
    )

    # --------------------------------------------------------
    # Customer-360 lineage: behavior + order + payment events.
    # --------------------------------------------------------
    customer_behavior = (
        customer_events
        .select(
            F.lit(GOLD_CUSTOMER_360).alias(
                "target_asset"
            ),
            F.col("customer_id").alias(
                "target_business_key"
            ),
            F.col("event_id").alias(
                "source_event_id"
            ),
            "source_topic",
            F.col("kafka_partition").cast("int").alias(
                "source_kafka_partition"
            ),
            F.col("kafka_offset").cast("long").alias(
                "source_kafka_offset"
            ),
            F.col("event_timestamp").alias(
                "source_event_timestamp"
            ),
            F.col("order_id").alias(
                "source_order_id"
            ),
            F.col("payment_id").alias(
                "source_payment_id"
            ),
            F.col("product_id").alias(
                "source_product_id"
            ),
            F.col("customer_id").alias(
                "source_customer_id"
            ),
            F.lit("CUSTOMER_BEHAVIOR").alias(
                "lineage_role"
            ),
        )
    )

    customer_order = (
        daily_order
        .withColumn(
            "target_asset",
            F.lit(GOLD_CUSTOMER_360),
        )
        .withColumn(
            "target_business_key",
            F.col("source_customer_id"),
        )
        .withColumn(
            "lineage_role",
            F.lit("PAID_ORDER_LIFECYCLE"),
        )
    )

    customer_payment = (
        daily_payment
        .withColumn(
            "target_asset",
            F.lit(GOLD_CUSTOMER_360),
        )
        .withColumn(
            "target_business_key",
            F.col("source_customer_id"),
        )
        .withColumn(
            "lineage_role",
            F.lit("CUSTOMER_PAYMENT_LIFECYCLE"),
        )
    )

    # --------------------------------------------------------
    # Inventory-health lineage
    # --------------------------------------------------------
    inventory_bridge = (
        inventory_events
        .select(
            F.lit(GOLD_INVENTORY_HEALTH).alias(
                "target_asset"
            ),
            F.col("product_id").alias(
                "target_business_key"
            ),
            F.col("event_id").alias(
                "source_event_id"
            ),
            "source_topic",
            F.col("kafka_partition").cast("int").alias(
                "source_kafka_partition"
            ),
            F.col("kafka_offset").cast("long").alias(
                "source_kafka_offset"
            ),
            F.col("event_timestamp").alias(
                "source_event_timestamp"
            ),
            F.col("order_id").alias(
                "source_order_id"
            ),
            F.col("payment_id").alias(
                "source_payment_id"
            ),
            F.col("product_id").alias(
                "source_product_id"
            ),
            F.col("customer_id").alias(
                "source_customer_id"
            ),
            F.lit("INVENTORY_EVENT").alias(
                "lineage_role"
            ),
        )
    )

    # --------------------------------------------------------
    # Payment-reliability lineage uses the final payment event.
    # --------------------------------------------------------
    payment_bridge = (
        final_payments
        .select(
            F.lit(GOLD_PAYMENT_RELIABILITY).alias(
                "target_asset"
            ),
            F.concat_ws(
                "|",
                F.date_format("event_date", "yyyy-MM-dd"),
                F.coalesce(F.col("currency"), F.lit("")),
            ).alias("target_business_key"),
            F.col("event_id").alias(
                "source_event_id"
            ),
            "source_topic",
            F.col("kafka_partition").cast("int").alias(
                "source_kafka_partition"
            ),
            F.col("kafka_offset").cast("long").alias(
                "source_kafka_offset"
            ),
            F.col("payment_timestamp").alias(
                "source_event_timestamp"
            ),
            F.col("order_id").alias(
                "source_order_id"
            ),
            F.col("payment_id").alias(
                "source_payment_id"
            ),
            F.col("product_id").alias(
                "source_product_id"
            ),
            F.col("customer_id").alias(
                "source_customer_id"
            ),
            F.lit("FINAL_PAYMENT").alias(
                "lineage_role"
            ),
        )
    )

    bridge = (
        daily_order
        .unionByName(daily_payment)
        .unionByName(product_order)
        .unionByName(product_payment)
        .unionByName(customer_behavior)
        .unionByName(customer_order)
        .unionByName(customer_payment)
        .unionByName(inventory_bridge)
        .unionByName(payment_bridge)
        .filter(F.col("source_event_id").isNotNull())
        .filter(F.col("target_business_key").isNotNull())
        .dropDuplicates([
            "target_asset",
            "target_business_key",
            "source_event_id",
            "lineage_role",
        ])
        .withColumn(
            "gold_run_id",
            F.lit(gold_run_id),
        )
        .withColumn(
            "transformation_version",
            F.lit(TRANSFORMATION_VERSION),
        )
        .withColumn(
            "recorded_at",
            F.lit(refreshed_at).cast("timestamp"),
        )
        .withColumn(
            "snapshot_date",
            F.lit(refreshed_at.date()).cast("date"),
        )
        .select(
            "gold_run_id",
            "target_asset",
            "target_business_key",
            "source_event_id",
            "source_topic",
            "source_kafka_partition",
            "source_kafka_offset",
            "source_event_timestamp",
            "source_order_id",
            "source_payment_id",
            "source_product_id",
            "source_customer_id",
            "lineage_role",
            "transformation_version",
            "recorded_at",
            "snapshot_date",
        )
    )

    return bridge


print("Optional row-level lineage bridge initialized.")

# %% [code cell 6]
# Stable AIDP snapshot identity: gold_run_id + asset business key.
# snapshot_date/refreshed_at are execution metadata and are not identity keys.

ASSET_SPECS = {
    "daily_revenue": {
        "target_asset": GOLD_DAILY_REVENUE,
        "snapshot_table": GOLD_DAILY_REVENUE_SNAPSHOT,
        "current_view": GOLD_DAILY_REVENUE,
        "source_asset": SILVER_TABLE,
        "transformation_name": "Daily revenue aggregation",
        "merge_condition": (
            "target.gold_run_id = source.gold_run_id "
            "AND target.event_date <=> source.event_date "
            "AND target.currency <=> source.currency"
        ),
    },
    "product_performance": {
        "target_asset": GOLD_PRODUCT_PERFORMANCE,
        "snapshot_table": GOLD_PRODUCT_PERFORMANCE_SNAPSHOT,
        "current_view": GOLD_PRODUCT_PERFORMANCE,
        "source_asset": SILVER_TABLE,
        "transformation_name": "Product performance aggregation",
        "merge_condition": (
            "target.gold_run_id = source.gold_run_id "
            "AND target.product_id <=> source.product_id "
            "AND target.currency <=> source.currency"
        ),
    },
    "customer_360": {
        "target_asset": GOLD_CUSTOMER_360,
        "snapshot_table": GOLD_CUSTOMER_360_SNAPSHOT,
        "current_view": GOLD_CUSTOMER_360,
        "source_asset": SILVER_TABLE,
        "transformation_name": "Customer 360 aggregation",
        "merge_condition": (
            "target.gold_run_id = source.gold_run_id "
            "AND target.customer_id <=> source.customer_id"
        ),
    },
    "inventory_health": {
        "target_asset": GOLD_INVENTORY_HEALTH,
        "snapshot_table": GOLD_INVENTORY_HEALTH_SNAPSHOT,
        "current_view": GOLD_INVENTORY_HEALTH,
        "source_asset": SILVER_TABLE,
        "transformation_name": "Inventory health aggregation",
        "merge_condition": (
            "target.gold_run_id = source.gold_run_id "
            "AND target.product_id <=> source.product_id"
        ),
    },
    "payment_reliability": {
        "target_asset": GOLD_PAYMENT_RELIABILITY,
        "snapshot_table": GOLD_PAYMENT_RELIABILITY_SNAPSHOT,
        "current_view": GOLD_PAYMENT_RELIABILITY,
        "source_asset": SILVER_TABLE,
        "transformation_name": "Payment reliability aggregation",
        "merge_condition": (
            "target.gold_run_id = source.gold_run_id "
            "AND target.event_date <=> source.event_date "
            "AND target.currency <=> source.currency"
        ),
    },
    "data_quality_scorecard": {
        "target_asset": GOLD_DQ_SCORECARD,
        "snapshot_table": GOLD_DQ_SCORECARD_SNAPSHOT,
        "current_view": GOLD_DQ_SCORECARD,
        "source_asset": DQ_SCORECARD_SOURCE,
        "transformation_name": "Publish latest DQ scorecard",
        "merge_condition": (
            "target.gold_run_id = source.gold_run_id "
            "AND target.source_dq_run_id <=> source.source_dq_run_id "
            "AND target.run_id <=> source.run_id "
            "AND target.table_name <=> source.table_name "
            "AND target.check_name <=> source.check_name"
        ),
    },
}


def write_snapshot_asset(
    *,
    asset_name: str,
    dataframe,
    gold_run_id: str,
    source_version: int,
    source_row_count: int,
    source_schema_hash: str,
    source_min_timestamp,
    source_max_timestamp,
) -> dict:
    spec = ASSET_SPECS[asset_name]
    started_at = datetime.now(timezone.utc)

    asset_run_id = hashlib.sha256(
        f"{gold_run_id}|{spec['target_asset']}".encode("utf-8")
    ).hexdigest()

    try:
        output_count = merge_dataframe(
            dataframe,
            spec["snapshot_table"],
            f"_streamcommerce_gold_{asset_name}_snapshot",
            spec["merge_condition"],
        )

        target_version = latest_delta_version(
            spec["snapshot_table"]
        )

        completed_at = datetime.now(timezone.utc)

        payload = {
            "asset_run_id": asset_run_id,
            "gold_run_id": gold_run_id,
            "pipeline_name": GOLD_PIPELINE_NAME,
            "source_asset": spec["source_asset"],
            "source_version": int(source_version),
            "target_asset": spec["target_asset"],
            "target_snapshot_table": spec["snapshot_table"],
            "target_current_view": spec["current_view"],
            "target_delta_version": int(target_version),
            "transformation_name": spec[
                "transformation_name"
            ],
            "transformation_version": TRANSFORMATION_VERSION,
            "input_row_count": int(source_row_count),
            "output_row_count": int(output_count),
            "source_schema_sha256": source_schema_hash,
            "target_schema_sha256": schema_sha256(
                spark.table(spec["snapshot_table"])
            ),
            "min_event_timestamp": source_min_timestamp,
            "max_event_timestamp": source_max_timestamp,
            "run_status": "SUCCESS",
            "started_at": started_at,
            "completed_at": completed_at,
            "error_message": None,
            "run_date": completed_at.date(),
        }

        upsert_asset_run(payload)

        return {
            "asset_name": asset_name,
            "output_count": output_count,
            "target_version": target_version,
        }

    except Exception as exc:
        completed_at = datetime.now(timezone.utc)

        failure_payload = {
            "asset_run_id": asset_run_id,
            "gold_run_id": gold_run_id,
            "pipeline_name": GOLD_PIPELINE_NAME,
            "source_asset": spec["source_asset"],
            "source_version": int(source_version),
            "target_asset": spec["target_asset"],
            "target_snapshot_table": spec["snapshot_table"],
            "target_current_view": spec["current_view"],
            "target_delta_version": None,
            "transformation_name": spec[
                "transformation_name"
            ],
            "transformation_version": TRANSFORMATION_VERSION,
            "input_row_count": int(source_row_count),
            "output_row_count": 0,
            "source_schema_sha256": source_schema_hash,
            "target_schema_sha256": None,
            "min_event_timestamp": source_min_timestamp,
            "max_event_timestamp": source_max_timestamp,
            "run_status": "FAILED",
            "started_at": started_at,
            "completed_at": completed_at,
            "error_message": str(exc)[:4000],
            "run_date": completed_at.date(),
        }

        upsert_asset_run(failure_payload)
        raise


def write_row_lineage_bridge(
    bridge_df,
) -> int:
    return merge_dataframe(
        bridge_df,
        GOLD_ROW_LINEAGE_BRIDGE,
        "_streamcommerce_gold_row_lineage_batch",
        (
            "target.gold_run_id = source.gold_run_id "
            "AND target.target_asset = source.target_asset "
            "AND target.target_business_key "
            "<=> source.target_business_key "
            "AND target.source_event_id "
            "<=> source.source_event_id "
            "AND target.lineage_role "
            "<=> source.lineage_role"
        ),
    )


print("Snapshot, asset-lineage, and activation helpers initialized.")

# %% [code cell 7]

from pyspark.sql.types import BooleanType

ADW_ASSET_SPECS = {
    "daily_revenue": {
        "source_aidp_asset": GOLD_DAILY_REVENUE_SNAPSHOT,
        "target_table": ADW_GOLD_DAILY_REVENUE_SNAPSHOT,
        "target_view": ADW_GOLD_DAILY_REVENUE,
        "merge_keys": [
            "GOLD_RUN_ID", "EVENT_DATE", "CURRENCY"
        ],
    },
    "product_performance": {
        "source_aidp_asset": GOLD_PRODUCT_PERFORMANCE_SNAPSHOT,
        "target_table": ADW_GOLD_PRODUCT_PERFORMANCE_SNAPSHOT,
        "target_view": ADW_GOLD_PRODUCT_PERFORMANCE,
        "merge_keys": [
            "GOLD_RUN_ID", "PRODUCT_ID", "CURRENCY"
        ],
    },
    "customer_360": {
        "source_aidp_asset": GOLD_CUSTOMER_360_SNAPSHOT,
        "target_table": ADW_GOLD_CUSTOMER_360_SNAPSHOT,
        "target_view": ADW_GOLD_CUSTOMER_360,
        "merge_keys": [
            "GOLD_RUN_ID", "CUSTOMER_ID"
        ],
    },
    "inventory_health": {
        "source_aidp_asset": GOLD_INVENTORY_HEALTH_SNAPSHOT,
        "target_table": ADW_GOLD_INVENTORY_HEALTH_SNAPSHOT,
        "target_view": ADW_GOLD_INVENTORY_HEALTH,
        "merge_keys": [
            "GOLD_RUN_ID", "PRODUCT_ID"
        ],
    },
    "payment_reliability": {
        "source_aidp_asset": GOLD_PAYMENT_RELIABILITY_SNAPSHOT,
        "target_table": ADW_GOLD_PAYMENT_RELIABILITY_SNAPSHOT,
        "target_view": ADW_GOLD_PAYMENT_RELIABILITY,
        "merge_keys": [
            "GOLD_RUN_ID", "EVENT_DATE", "CURRENCY"
        ],
    },
    "data_quality_scorecard": {
        "source_aidp_asset": GOLD_DQ_SCORECARD_SNAPSHOT,
        "target_table": ADW_GOLD_DQ_SCORECARD_SNAPSHOT,
        "target_view": ADW_GOLD_DQ_SCORECARD,
        "merge_keys": [
            "GOLD_RUN_ID", "SOURCE_DQ_RUN_ID",
            "RUN_ID", "TABLE_NAME", "CHECK_NAME"
        ],
    },
}


def _target_column_name(target_table: str, logical_name: str) -> str:
    for column_name in spark.table(target_table).columns:
        if column_name.upper() == logical_name.upper():
            return column_name
    raise RuntimeError(
        f"Column {logical_name} is absent from external target {target_table}."
    )


def prepare_external_dataframe(dataframe, target_table: str):
    target_schema = spark.table(target_table).schema
    source_lookup = {name.upper(): name for name in dataframe.columns}
    expressions = []

    for field in target_schema.fields:
        source_name = source_lookup.get(field.name.upper())
        if source_name is None:
            raise RuntimeError(
                f"Source DataFrame lacks target column {field.name} "
                f"required by {target_table}."
            )

        source_field = dataframe.schema[source_name]
        column = F.col(source_name)

        if isinstance(source_field.dataType, BooleanType):
            column = F.when(column, F.lit(1)).otherwise(F.lit(0))

        expressions.append(
            column.cast(field.dataType).alias(field.name)
        )

    return dataframe.select(*expressions)


def _actual_source_key_columns(dataframe, merge_keys: list[str]) -> list[str]:
    source_lookup = {name.upper(): name for name in dataframe.columns}
    missing_keys = [
        key for key in merge_keys if key.upper() not in source_lookup
    ]

    if missing_keys:
        raise RuntimeError(
            f"Merge source is missing keys {missing_keys}. "
            f"Available columns: {dataframe.columns}"
        )

    return [source_lookup[key.upper()] for key in merge_keys]


def assert_unique_merge_source(
    dataframe,
    target_table: str,
    merge_keys: list[str],
) -> None:
    actual_keys = _actual_source_key_columns(dataframe, merge_keys)
    duplicate_rows = (
        dataframe
        .groupBy(*[F.col(key) for key in actual_keys])
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )

    if duplicate_rows:
        raise RuntimeError(
            f"Source rows are not unique for ADW merge keys {merge_keys} "
            f"targeting {target_table}. Sample duplicate keys: "
            f"{[row.asDict(recursive=True) for row in duplicate_rows]}"
        )


def assert_non_null_merge_source(
    dataframe,
    target_table: str,
    merge_keys: list[str],
) -> None:
    actual_keys = _actual_source_key_columns(dataframe, merge_keys)
    null_condition = None

    for key in actual_keys:
        condition = F.col(key).isNull()
        null_condition = condition if null_condition is None else (null_condition | condition)

    if null_condition is None:
        return

    null_rows = dataframe.filter(null_condition).select(*actual_keys).limit(5).collect()
    if null_rows:
        raise RuntimeError(
            f"ADW MERGE keys must be non-null for {target_table}. "
            f"Keys={merge_keys}; sample null-key rows="
            f"{[row.asDict(recursive=True) for row in null_rows]}"
        )


def assert_unique_target_slice(
    target_table: str,
    gold_run_id: str,
    merge_keys: list[str],
) -> None:
    target_df = spark.table(target_table)
    run_column = _target_column_name(target_table, "GOLD_RUN_ID")
    target_slice = target_df.filter(F.col(run_column) == F.lit(gold_run_id))
    actual_keys = _actual_source_key_columns(target_slice, merge_keys)

    duplicate_rows = (
        target_slice
        .groupBy(*[F.col(key) for key in actual_keys])
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )

    if duplicate_rows:
        raise RuntimeError(
            f"The ADW target already contains duplicate merge keys for "
            f"gold_run_id={gold_run_id} in {target_table}. One-time cleanup "
            f"of this inactive run is required. Sample duplicates: "
            f"{[row.asDict(recursive=True) for row in duplicate_rows]}"
        )


def external_merge(dataframe, target_table: str, merge_keys: list[str]) -> int:
    prepared = prepare_external_dataframe(dataframe, target_table)
    assert_unique_merge_source(prepared, target_table, merge_keys)
    assert_non_null_merge_source(prepared, target_table, merge_keys)
    row_count = int(prepared.count())

    if row_count == 0:
        return 0

    writer = (
        prepared.write
        .option("write.mode", "MERGE")
        .option("write.merge.keys", ",".join(merge_keys))
        .option(
            "skip.oos.staging",
            str(bool(ADW_SKIP_OOS_STAGING)).lower(),
        )
        .option(
            "merge.staging.using.username",
            str(bool(ADW_MERGE_STAGING_USING_USERNAME)).lower(),
        )
    )

    writer.insertInto(target_table)
    return row_count


def external_count_for_gold_run(target_table: str, gold_run_id: str) -> int:
    run_column = _target_column_name(target_table, "GOLD_RUN_ID")
    return int(
        spark.table(target_table)
        .filter(F.col(run_column) == F.lit(gold_run_id))
        .count()
    )


def _publish_audit_payload(
    *,
    publish_run_id: str,
    gold_run_id: str,
    asset_name: str,
    source_aidp_asset: str,
    target_adw_table: str,
    target_adw_view: str,
    source_row_count: int,
    target_row_count: int,
    publish_status: str,
    started_at,
    completed_at,
    error_message,
):
    return {
        "publish_run_id": publish_run_id,
        "gold_run_id": gold_run_id,
        "pipeline_name": GOLD_PIPELINE_NAME,
        "asset_name": asset_name,
        "source_aidp_asset": source_aidp_asset,
        "target_adw_table": target_adw_table,
        "target_adw_view": target_adw_view,
        "write_mode": "MERGE",
        "source_row_count": int(source_row_count),
        "target_row_count": int(target_row_count),
        "publish_status": publish_status,
        "started_at": started_at,
        "completed_at": completed_at,
        "duration_seconds": float(
            (completed_at - started_at).total_seconds()
        ),
        "error_message": (
            None if error_message is None else str(error_message)[:4000]
        ),
        "run_date": completed_at.date(),
    }


def record_publish_audit(payload: dict) -> None:
    aidp_schema = spark.table(AIDP_ADW_PUBLISH_RUNS).schema
    aidp_df = spark.createDataFrame([payload], schema=aidp_schema)

    aidp_df.createOrReplaceTempView("_streamcommerce_adw_publish_audit")
    try:
        spark.sql(f"""
            MERGE INTO {AIDP_ADW_PUBLISH_RUNS} AS target
            USING _streamcommerce_adw_publish_audit AS source
            ON target.publish_run_id = source.publish_run_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView("_streamcommerce_adw_publish_audit")

    external_merge(aidp_df, ADW_PUBLISH_RUNS, ["PUBLISH_RUN_ID"])


def publish_gold_asset_to_adw(
    *,
    asset_name: str,
    dataframe,
    gold_run_id: str,
) -> dict:
    spec = ADW_ASSET_SPECS[asset_name]
    started_at = datetime.now(timezone.utc)
    publish_run_id = hashlib.sha256(
        f"{gold_run_id}|ADW|{asset_name}".encode("utf-8")
    ).hexdigest()
    source_count = int(dataframe.count())

    source_run_column = next(
        (
            column_name
            for column_name in dataframe.columns
            if column_name.upper() == "GOLD_RUN_ID"
        ),
        None,
    )
    if source_run_column is None:
        raise RuntimeError(
            f"Source DataFrame for {asset_name} does not contain GOLD_RUN_ID."
        )

    source_run_ids = [
        row[0]
        for row in (
            dataframe
            .select(F.col(source_run_column).cast("string"))
            .distinct()
            .collect()
        )
    ]
    if source_count > 0 and source_run_ids != [gold_run_id]:
        raise RuntimeError(
            f"Source DataFrame for {asset_name} must contain exactly the "
            f"expected gold_run_id={gold_run_id}; found {source_run_ids}."
        )

    target_count_before = external_count_for_gold_run(
        spec["target_table"],
        gold_run_id,
    )

    if target_count_before > 0:
        assert_unique_target_slice(
            spec["target_table"],
            gold_run_id,
            spec["merge_keys"],
        )

    if target_count_before not in {0, source_count}:
        raise RuntimeError(
            f"Existing ADW slice is incomplete or duplicated for {asset_name}: "
            f"gold_run_id={gold_run_id}, source={source_count}, "
            f"target_before={target_count_before}. Clean only this inactive "
            "Gold Run from the corresponding _S table before retrying."
        )

    if (
        SKIP_ALREADY_PUBLISHED_ASSET_ON_RETRY
        and source_count > 0
        and target_count_before == source_count
    ):
        completed_at = datetime.now(timezone.utc)
        payload = _publish_audit_payload(
            publish_run_id=publish_run_id,
            gold_run_id=gold_run_id,
            asset_name=asset_name,
            source_aidp_asset=spec["source_aidp_asset"],
            target_adw_table=spec["target_table"],
            target_adw_view=spec["target_view"],
            source_row_count=source_count,
            target_row_count=target_count_before,
            publish_status="SUCCESS",
            started_at=started_at,
            completed_at=completed_at,
            error_message=None,
        )
        print(
            f"ADW retry reuse asset={asset_name} "
            f"gold_run_id={gold_run_id} rows={source_count}"
        )
        record_publish_audit(payload)
        return payload

    try:
        external_merge(
            dataframe,
            spec["target_table"],
            spec["merge_keys"],
        )

        target_count = external_count_for_gold_run(
            spec["target_table"],
            gold_run_id,
        )

        print(
            f"ADW publish validation asset={asset_name} "
            f"gold_run_id={gold_run_id} "
            f"source_rows={source_count} "
            f"target_rows_before={target_count_before} "
            f"target_rows_after={target_count}"
        )

        if target_count != source_count:
            raise RuntimeError(
                f"ADW count validation failed for {asset_name}: "
                f"gold_run_id={gold_run_id}, source={source_count}, "
                f"target_before={target_count_before}, "
                f"target_after={target_count}."
            )

        completed_at = datetime.now(timezone.utc)
        payload = _publish_audit_payload(
            publish_run_id=publish_run_id,
            gold_run_id=gold_run_id,
            asset_name=asset_name,
            source_aidp_asset=spec["source_aidp_asset"],
            target_adw_table=spec["target_table"],
            target_adw_view=spec["target_view"],
            source_row_count=source_count,
            target_row_count=target_count,
            publish_status="SUCCESS",
            started_at=started_at,
            completed_at=completed_at,
            error_message=None,
        )
        record_publish_audit(payload)
        return payload

    except Exception as exc:
        completed_at = datetime.now(timezone.utc)
        payload = _publish_audit_payload(
            publish_run_id=publish_run_id,
            gold_run_id=gold_run_id,
            asset_name=asset_name,
            source_aidp_asset=spec["source_aidp_asset"],
            target_adw_table=spec["target_table"],
            target_adw_view=spec["target_view"],
            source_row_count=source_count,
            target_row_count=(
                external_count_for_gold_run(
                    spec["target_table"],
                    gold_run_id,
                )
                if gold_run_id
                else 0
            ),
            publish_status="FAILED",
            started_at=started_at,
            completed_at=completed_at,
            error_message=exc,
        )
        try:
            record_publish_audit(payload)
        except Exception as audit_exc:
            print(
                "Failed to persist ADW publish failure audit:",
                audit_exc,
            )
        raise


def activate_adw_snapshot(
    *,
    asset_group: str,
    gold_run_id: str,
    silver_version: int,
    dq_version: int,
    dq_run_id: str,
) -> None:
    now = datetime.now(timezone.utc)
    source_schema = spark.table(GOLD_SNAPSHOT_CONTROL).schema
    dataframe = spark.createDataFrame(
        [(
            GOLD_PIPELINE_NAME,
            asset_group,
            gold_run_id,
            int(silver_version),
            int(dq_version),
            dq_run_id,
            TRANSFORMATION_VERSION,
            now,
            now,
        )],
        schema=source_schema,
    )

    external_merge(
        dataframe,
        ADW_GOLD_SNAPSHOT_CONTROL,
        ["PIPELINE_NAME", "ASSET_GROUP"],
    )


def publish_runtime_lineage_to_adw(gold_run_id: str) -> None:
    pipeline_df = (
        spark.table(LINEAGE_PIPELINE_RUNS)
        .filter(F.col("gold_run_id") == F.lit(gold_run_id))
    )
    asset_df = (
        spark.table(LINEAGE_ASSET_RUNS)
        .filter(F.col("gold_run_id") == F.lit(gold_run_id))
    )

    external_merge(
        pipeline_df,
        ADW_LINEAGE_PIPELINE_RUNS,
        ["GOLD_RUN_ID"],
    )

    if asset_df.limit(1).count() > 0:
        external_merge(
            asset_df,
            ADW_LINEAGE_ASSET_RUNS,
            ["ASSET_RUN_ID"],
        )


def publish_row_lineage_to_adw(bridge_df, gold_run_id: str) -> int:
    if not PUBLISH_ROW_LINEAGE_TO_ADW:
        return 0

    asset_map = {
        GOLD_DAILY_REVENUE: ADW_GOLD_DAILY_REVENUE,
        GOLD_PRODUCT_PERFORMANCE: ADW_GOLD_PRODUCT_PERFORMANCE,
        GOLD_CUSTOMER_360: ADW_GOLD_CUSTOMER_360,
        GOLD_INVENTORY_HEALTH: ADW_GOLD_INVENTORY_HEALTH,
        GOLD_PAYMENT_RELIABILITY: ADW_GOLD_PAYMENT_RELIABILITY,
        GOLD_DQ_SCORECARD: ADW_GOLD_DQ_SCORECARD,
    }

    target_asset = F.col("target_asset")
    for aidp_asset, adw_asset in asset_map.items():
        target_asset = F.when(
            F.col("target_asset") == F.lit(aidp_asset),
            F.lit(adw_asset),
        ).otherwise(target_asset)

    prepared = (
        bridge_df
        .withColumn("target_asset", target_asset)
        .withColumn(
            "row_lineage_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("gold_run_id"),
                    F.col("target_asset"),
                    F.coalesce(F.col("target_business_key"), F.lit("")),
                    F.coalesce(F.col("source_event_id"), F.lit("")),
                    F.coalesce(F.col("lineage_role"), F.lit("")),
                ),
                256,
            ),
        )
        .select(
            "row_lineage_id",
            "gold_run_id",
            "target_asset",
            "target_business_key",
            "source_event_id",
            "source_topic",
            "source_kafka_partition",
            "source_kafka_offset",
            "source_event_timestamp",
            "source_order_id",
            "source_payment_id",
            "source_product_id",
            "source_customer_id",
            "lineage_role",
            "transformation_version",
            "recorded_at",
            "snapshot_date",
        )
    )

    return external_merge(
        prepared,
        ADW_GOLD_ROW_LINEAGE,
        ["ROW_LINEAGE_ID"],
    )


def adw_signature_is_active(gold_run_id: str, expected_group: str) -> bool:
    pipeline_col = _target_column_name(
        ADW_GOLD_SNAPSHOT_CONTROL,
        "PIPELINE_NAME",
    )
    group_col = _target_column_name(
        ADW_GOLD_SNAPSHOT_CONTROL,
        "ASSET_GROUP",
    )
    run_col = _target_column_name(
        ADW_GOLD_SNAPSHOT_CONTROL,
        "ACTIVE_GOLD_RUN_ID",
    )

    return bool(
        spark.table(ADW_GOLD_SNAPSHOT_CONTROL)
        .filter(
            (F.col(pipeline_col) == F.lit(GOLD_PIPELINE_NAME))
            & (F.col(group_col) == F.lit(expected_group))
            & (F.col(run_col) == F.lit(gold_run_id))
        )
        .limit(1)
        .collect()
    )


print("ADW publication helpers initialized.")

# %% [code cell 8]

def refresh_dual_gold(timer_batch_id: int) -> dict:
    started_at = datetime.now(timezone.utc)
    silver_df = None
    dq_df = None
    latest_dq_df = None
    gold_run_id = None
    dq_run_id = ""
    dq_gate_status = "UNKNOWN"
    dq_snapshot_activated = False
    business_snapshot_activated = False
    row_lineage_row_count = 0
    adw_row_lineage_row_count = 0
    source_silver_version = None
    source_dq_version = None
    silver_schema_hash = None
    dq_schema_hash = None
    silver_row_count = 0
    dq_row_count = 0
    source_min_event_timestamp = None
    source_max_event_timestamp = None

    try:
        source_silver_version = latest_delta_version(SILVER_TABLE)

        dq_sync = wait_for_dq_run_for_silver_version(
            source_silver_version
        )
        preferred_dq_run_id = (
            dq_sync["run_id"] if dq_sync is not None else None
        )

        # Pin the DQ Delta version only after the associated DQ run exists.
        source_dq_version = latest_delta_version(DQ_SCORECARD_SOURCE)
        gold_run_id = deterministic_gold_run_id(
            source_silver_version,
            source_dq_version,
        )

        adw_business_active = adw_signature_is_active(
            gold_run_id,
            "BUSINESS",
        )
        adw_dq_active = adw_signature_is_active(
            gold_run_id,
            "DQ",
        )

        if adw_business_active and adw_dq_active:
            result = {
                "status": "ALREADY_ACTIVE",
                "gold_run_id": gold_run_id,
                "source_silver_version": source_silver_version,
                "source_dq_scorecard_version": source_dq_version,
            }
            print("Dual-Gold source signature is already active:", result)
            return result

        if (
            source_signature_already_evaluated(
                silver_version=source_silver_version,
                dq_version=source_dq_version,
            )
            and (adw_business_active or adw_dq_active)
        ):
            result = {
                "status": "NO_CHANGE",
                "gold_run_id": gold_run_id,
                "source_silver_version": source_silver_version,
                "source_dq_scorecard_version": source_dq_version,
            }
            print("Dual-Gold source signature unchanged:", result)
            return result

        silver_df = (
            read_delta_version(SILVER_TABLE, source_silver_version)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        dq_df = (
            read_delta_version(DQ_SCORECARD_SOURCE, source_dq_version)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        silver_row_count = int(silver_df.count())
        silver_schema_hash = schema_sha256(silver_df)
        dq_schema_hash = schema_sha256(dq_df)

        silver_metrics = (
            silver_df.agg(
                F.min("event_timestamp").alias("min_event_timestamp"),
                F.max("event_timestamp").alias("max_event_timestamp"),
            ).first()
        )
        source_min_event_timestamp = silver_metrics["min_event_timestamp"]
        source_max_event_timestamp = silver_metrics["max_event_timestamp"]

        dq_snapshot = latest_dq_snapshot(
            dq_df,
            preferred_run_id=preferred_dq_run_id,
        )
        dq_run_id = dq_snapshot["run_id"]
        dq_gate_status = dq_snapshot["status"]
        latest_dq_df = (
            dq_snapshot["dataframe"]
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        dq_row_count = int(latest_dq_df.count())

        running_payload = {
            "gold_run_id": gold_run_id,
            "pipeline_name": GOLD_PIPELINE_NAME,
            "timer_batch_id": int(timer_batch_id),
            "workflow_job_name": WORKFLOW_JOB_NAME,
            "workflow_run_id": WORKFLOW_RUN_ID,
            "notebook_path": NOTEBOOK_PATH,
            "transformation_version": TRANSFORMATION_VERSION,
            "source_silver_table": SILVER_TABLE,
            "source_silver_version": int(source_silver_version),
            "source_dq_scorecard_table": DQ_SCORECARD_SOURCE,
            "source_dq_scorecard_version": int(source_dq_version),
            "source_dq_run_id": dq_run_id,
            "source_silver_schema_sha256": silver_schema_hash,
            "source_dq_schema_sha256": dq_schema_hash,
            "source_silver_row_count": int(silver_row_count),
            "source_dq_row_count": int(dq_row_count),
            "source_min_event_timestamp": source_min_event_timestamp,
            "source_max_event_timestamp": source_max_event_timestamp,
            "dq_gate_mode": DQ_GATE_MODE,
            "dq_gate_status": dq_gate_status,
            "run_status": "RUNNING",
            "business_snapshot_activated": False,
            "dq_snapshot_activated": False,
            "row_lineage_enabled": bool(ENABLE_ROW_LINEAGE_BRIDGE),
            "row_lineage_row_count": 0,
            "started_at": started_at,
            "completed_at": None,
            "duration_seconds": None,
            "error_message": None,
            "run_date": started_at.date(),
        }
        upsert_pipeline_run(running_payload)

        refreshed_at = datetime.now(timezone.utc)
        bundle = build_gold_bundle(
            silver_df=silver_df,
            latest_dq_df=latest_dq_df,
            source_dq_run_id=dq_run_id,
            gold_run_id=gold_run_id,
            silver_version=source_silver_version,
            dq_version=source_dq_version,
            refreshed_at=refreshed_at,
        )
        gold_frames = bundle["gold_frames"]

        # ----------------------------------------------------
        # DQ snapshot: AIDP -> ADW -> ADW control -> AIDP control
        # ----------------------------------------------------
        dq_min_max = latest_dq_df.agg(
            F.min("checked_at").alias("min_ts"),
            F.max("checked_at").alias("max_ts"),
        ).first()

        dq_asset_result = write_snapshot_asset(
            asset_name="data_quality_scorecard",
            dataframe=gold_frames["data_quality_scorecard"],
            gold_run_id=gold_run_id,
            source_version=source_dq_version,
            source_row_count=dq_row_count,
            source_schema_hash=dq_schema_hash,
            source_min_timestamp=dq_min_max["min_ts"],
            source_max_timestamp=dq_min_max["max_ts"],
        )

        dq_publish_result = publish_gold_asset_to_adw(
            asset_name="data_quality_scorecard",
            dataframe=gold_frames["data_quality_scorecard"],
            gold_run_id=gold_run_id,
        )

        activate_adw_snapshot(
            asset_group="DQ",
            gold_run_id=gold_run_id,
            silver_version=source_silver_version,
            dq_version=source_dq_version,
            dq_run_id=dq_run_id,
        )
        activate_snapshot(
            asset_group="DQ",
            gold_run_id=gold_run_id,
            silver_version=source_silver_version,
            dq_version=source_dq_version,
            dq_run_id=dq_run_id,
        )
        dq_snapshot_activated = True

        block_business = (
            DQ_GATE_MODE == "BLOCK"
            and dq_gate_status == "CRITICAL"
        )

        if block_business:
            completed_at = datetime.now(timezone.utc)
            blocked_payload = dict(running_payload)
            blocked_payload.update({
                "run_status": "BLOCKED_BY_DQ",
                "dq_gate_status": dq_gate_status,
                "business_snapshot_activated": False,
                "dq_snapshot_activated": True,
                "completed_at": completed_at,
                "duration_seconds": float(
                    (completed_at - started_at).total_seconds()
                ),
                "error_message": (
                    "Business publication blocked by a CRITICAL DQ result."
                ),
                "run_date": completed_at.date(),
            })
            upsert_pipeline_run(blocked_payload)
            publish_runtime_lineage_to_adw(gold_run_id)
            return {
                "gold_run_id": gold_run_id,
                "status": "BLOCKED_BY_DQ",
                "dq_gate_status": dq_gate_status,
                "dq_publish": dq_publish_result,
            }

        # ----------------------------------------------------
        # Business AIDP snapshots
        # ----------------------------------------------------
        business_results = {}
        for asset_name in [
            "daily_revenue",
            "product_performance",
            "customer_360",
            "inventory_health",
            "payment_reliability",
        ]:
            business_results[asset_name] = write_snapshot_asset(
                asset_name=asset_name,
                dataframe=gold_frames[asset_name],
                gold_run_id=gold_run_id,
                source_version=source_silver_version,
                source_row_count=silver_row_count,
                source_schema_hash=silver_schema_hash,
                source_min_timestamp=source_min_event_timestamp,
                source_max_timestamp=source_max_event_timestamp,
            )

        bridge_df = None
        if ENABLE_ROW_LINEAGE_BRIDGE:
            bridge_df = build_row_lineage_bridge(
                contexts=bundle["contexts"],
                gold_run_id=gold_run_id,
                refreshed_at=refreshed_at,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            row_lineage_row_count = write_row_lineage_bridge(bridge_df)

        # ----------------------------------------------------
        # Publish all business snapshots to ADW and validate counts
        # ----------------------------------------------------
        adw_business_results = {}
        for asset_name in [
            "daily_revenue",
            "product_performance",
            "customer_360",
            "inventory_health",
            "payment_reliability",
        ]:
            adw_business_results[asset_name] = publish_gold_asset_to_adw(
                asset_name=asset_name,
                dataframe=gold_frames[asset_name],
                gold_run_id=gold_run_id,
            )

        if bridge_df is not None:
            adw_row_lineage_row_count = publish_row_lineage_to_adw(
                bridge_df,
                gold_run_id,
            )
            bridge_df.unpersist()
            bridge_df = None

        # Mark runtime lineage ready before serving activation.
        ready_at = datetime.now(timezone.utc)
        ready_payload = dict(running_payload)
        ready_payload.update({
            "run_status": "READY_TO_ACTIVATE",
            "dq_gate_status": dq_gate_status,
            "row_lineage_row_count": int(row_lineage_row_count),
            "completed_at": ready_at,
            "duration_seconds": float(
                (ready_at - started_at).total_seconds()
            ),
            "run_date": ready_at.date(),
        })
        upsert_pipeline_run(ready_payload)
        publish_runtime_lineage_to_adw(gold_run_id)

        # ADW is the OAC serving layer: activate it first, then mirror AIDP.
        activate_adw_snapshot(
            asset_group="BUSINESS",
            gold_run_id=gold_run_id,
            silver_version=source_silver_version,
            dq_version=source_dq_version,
            dq_run_id=dq_run_id,
        )
        activate_snapshot(
            asset_group="BUSINESS",
            gold_run_id=gold_run_id,
            silver_version=source_silver_version,
            dq_version=source_dq_version,
            dq_run_id=dq_run_id,
        )
        business_snapshot_activated = True

        completed_at = datetime.now(timezone.utc)
        success_payload = dict(running_payload)
        success_payload.update({
            "run_status": "SUCCESS",
            "dq_gate_status": dq_gate_status,
            "business_snapshot_activated": True,
            "dq_snapshot_activated": True,
            "row_lineage_row_count": int(row_lineage_row_count),
            "completed_at": completed_at,
            "duration_seconds": float(
                (completed_at - started_at).total_seconds()
            ),
            "run_date": completed_at.date(),
        })
        upsert_pipeline_run(success_payload)
        publish_runtime_lineage_to_adw(gold_run_id)

        result = {
            "gold_run_id": gold_run_id,
            "status": "SUCCESS",
            "dq_gate_status": dq_gate_status,
            "source_silver_version": source_silver_version,
            "source_dq_scorecard_version": source_dq_version,
            "silver_rows": silver_row_count,
            "dq_rows": dq_row_count,
            "aidp_row_lineage_rows": row_lineage_row_count,
            "adw_row_lineage_rows": adw_row_lineage_row_count,
            "aidp_business_assets": business_results,
            "adw_business_assets": adw_business_results,
            "dq_asset": dq_asset_result,
            "dq_publish": dq_publish_result,
        }
        print("Dual-Gold build completed:")
        print(json.dumps(result, indent=2, default=str))
        return result

    except Exception as exc:
        completed_at = datetime.now(timezone.utc)
        if gold_run_id is None:
            gold_run_id = hashlib.sha256(
                (
                    f"{GOLD_PIPELINE_NAME}|FAILED|{timer_batch_id}|"
                    f"{started_at.isoformat()}"
                ).encode("utf-8")
            ).hexdigest()

        failure_payload = {
            "gold_run_id": gold_run_id,
            "pipeline_name": GOLD_PIPELINE_NAME,
            "timer_batch_id": int(timer_batch_id),
            "workflow_job_name": WORKFLOW_JOB_NAME,
            "workflow_run_id": WORKFLOW_RUN_ID,
            "notebook_path": NOTEBOOK_PATH,
            "transformation_version": TRANSFORMATION_VERSION,
            "source_silver_table": SILVER_TABLE,
            "source_silver_version": source_silver_version,
            "source_dq_scorecard_table": DQ_SCORECARD_SOURCE,
            "source_dq_scorecard_version": source_dq_version,
            "source_dq_run_id": dq_run_id,
            "source_silver_schema_sha256": silver_schema_hash,
            "source_dq_schema_sha256": dq_schema_hash,
            "source_silver_row_count": int(silver_row_count),
            "source_dq_row_count": int(dq_row_count),
            "source_min_event_timestamp": source_min_event_timestamp,
            "source_max_event_timestamp": source_max_event_timestamp,
            "dq_gate_mode": DQ_GATE_MODE,
            "dq_gate_status": dq_gate_status,
            "run_status": "FAILED",
            "business_snapshot_activated": bool(business_snapshot_activated),
            "dq_snapshot_activated": bool(dq_snapshot_activated),
            "row_lineage_enabled": bool(ENABLE_ROW_LINEAGE_BRIDGE),
            "row_lineage_row_count": int(row_lineage_row_count),
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": float(
                (completed_at - started_at).total_seconds()
            ),
            "error_message": str(exc)[:4000],
            "run_date": completed_at.date(),
        }
        try:
            upsert_pipeline_run(failure_payload)
            publish_runtime_lineage_to_adw(gold_run_id)
        except Exception as lineage_exc:
            print("Failed to publish failure lineage:", lineage_exc)
        raise

    finally:
        if latest_dq_df is not None:
            latest_dq_df.unpersist()
        if dq_df is not None:
            dq_df.unpersist()
        if silver_df is not None:
            silver_df.unpersist()


print("Dual-Gold refresh function initialized.")

# %% [code cell 9]
existing_queries = [
    query
    for query in spark.streams.active
    if query.name == QUERY_NAME
]

if existing_queries:
    raise RuntimeError(
        f"An active streaming query named {QUERY_NAME!r} "
        "already exists in this Spark session."
    )

# Build the currently available pinned snapshot once before starting the
# continuous timer. A failure here is visible immediately and no streaming
# checkpoint progress is committed.
if RUN_INITIAL_REFRESH:
    print("Running initial governed Gold refresh before timer stream start...")
    refresh_dual_gold(-1)


def process_timer_microbatch(
    timer_batch_df,
    batch_id: int,
) -> None:
    trigger_rows = int(timer_batch_df.count())

    print("\n============================================================")
    print("Gold timer micro-batch:", batch_id)
    print("Timer trigger rows     :", trigger_rows)
    print("Poll interval          :", POLL_INTERVAL)

    if trigger_rows == 0:
        print("Empty timer batch; skipping.")
        return

    # refresh_dual_gold pins exact Silver and DQ Delta versions. With
    # SKIP_IF_UNCHANGED=true, unchanged successful source signatures return
    # without rebuilding or republishing the six Gold assets.
    refresh_dual_gold(batch_id)


timer_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
)

gold_lineage_query = (
    timer_stream.writeStream
    .queryName(QUERY_NAME)
    .outputMode("append")
    .foreachBatch(process_timer_microbatch)
    .option(
        "checkpointLocation",
        TIMER_CHECKPOINT_LOCATION,
    )
    .trigger(
        processingTime=POLL_INTERVAL,
    )
    .start()
)

print("Runtime-compatible AIDP + ADW dual-Gold Workflow started.")
print("Query ID       :", gold_lineage_query.id)
print("Run ID         :", gold_lineage_query.runId)
print("Trigger source : Spark rate timer")
print("Poll interval  :", POLL_INTERVAL)
print("Checkpoint     :", TIMER_CHECKPOINT_LOCATION)
print("Waiting until the Workflow task is stopped or fails...")

gold_lineage_query.awaitTermination()
