"""Source export generated from the sanitized AIDP notebook."""

# %% [code cell 1]
# Configuration and fail-closed validation
import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def _workflow_parameter(name: str, default: str) -> str:
    utility = globals().get("oidlUtils")
    if utility is None:
        return str(default).strip()
    try:
        value = utility.parameters.getParameter(name, default)
    except Exception as exc:
        raise RuntimeError(
            f"AIDP parameter API failed while reading {name}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if value is None or not str(value).strip():
        return str(default).strip()
    return str(value).strip()


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_int(value: Any, name: str, minimum: int = 0, maximum: int | None = None) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception as exc:
        raise ValueError(f"{name} must be an integer; received {value!r}.") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}; received {parsed}.")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be <= {maximum}; received {parsed}.")
    return parsed


def _as_enum(value: Any, name: str, allowed: set[str]) -> str:
    parsed = str(value).strip().upper()
    if parsed not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}; received {value!r}.")
    return parsed


def _as_int_list(value: Any, name: str) -> list[int]:
    parts = [part.strip() for part in str(value).split(",") if part.strip()]
    if not parts:
        raise ValueError(f"{name} must contain at least one integer.")
    return [_as_int(part, name, minimum=1) for part in parts]


def _validate_three_part_name(value: str, name: str) -> str:
    pattern = (
        r"^[A-Za-z_][A-Za-z0-9_]*\."
        r"[A-Za-z_][A-Za-z0-9_]*\."
        r"[A-Za-z_][A-Za-z0-9_]*$"
    )
    if not re.fullmatch(pattern, value):
        raise ValueError(f"{name} must be catalog.schema.object; received {value!r}.")
    return value


def _validate_https_url(value: str, name: str, required: bool) -> str:
    text = str(value).strip()
    if not text and not required:
        return ""
    if not text:
        raise ValueError(f"{name} is required for the selected mode.")
    if not re.fullmatch(r"https://[^\s]+", text):
        raise ValueError(f"{name} must be a complete https:// URL.")
    return text.rstrip("/")


def _validate_protected_path(value: str, name: str, required: bool) -> str:
    text = str(value).strip()
    if not text and not required:
        return ""
    if not text:
        raise ValueError(f"{name} is required for the selected mode.")
    if "\n" in text or "\r" in text:
        raise ValueError(f"{name} must be one filesystem path.")
    return text


def _parse_utc_timestamp(value: str, name: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError(f"{name} is required.")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception as exc:
        raise ValueError(
            f"{name} must be ISO-8601, for example 2026-08-24T00:00:00Z."
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


PIPELINE_NAME = "streamcommerce_inventory_stockout_alert"
TRANSFORMATION_VERSION = _workflow_parameter(
    "TRANSFORMATION_VERSION",
    "streamcommerce-inventory-stockout-alert-v3.0-silver-agent-function",
)
WORKFLOW_JOB_NAME = _workflow_parameter(
    "WORKFLOW_JOB_NAME",
    "streamcommerce_inventory_stockout_alert",
)
WORKFLOW_RUN_ID = _workflow_parameter("WORKFLOW_RUN_ID", "")
NOTEBOOK_PATH = _workflow_parameter(
    "NOTEBOOK_PATH",
    "/Workspace/Alerts/05_inventory_stockout_alert_workflow_v3_0_agent_function.ipynb",
)

EXECUTION_MODE = _as_enum(
    _workflow_parameter("EXECUTION_MODE", "RUN_ONCE"),
    "EXECUTION_MODE",
    {"RUN_ONCE", "STREAMING"},
)

SILVER_TABLE = _validate_three_part_name(
    _workflow_parameter("SILVER_TABLE", "default.default.streamcommerce_silver_events"),
    "SILVER_TABLE",
)
DQ_RUN_SUMMARY_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "DQ_RUN_SUMMARY_TABLE",
        "default.default.streamcommerce_dq_run_summary",
    ),
    "DQ_RUN_SUMMARY_TABLE",
)
SILVER_PIPELINE_NAME = _workflow_parameter(
    "SILVER_PIPELINE_NAME",
    "streamcommerce_bronze_to_silver",
)

ALERT_STATE_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "ALERT_STATE_TABLE",
        "default.default.streamcommerce_inventory_alert_state",
    ),
    "ALERT_STATE_TABLE",
)
ALERT_EVENTS_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "ALERT_EVENTS_TABLE",
        "default.default.streamcommerce_inventory_alert_events",
    ),
    "ALERT_EVENTS_TABLE",
)
ALERT_DISPATCH_TABLE = _validate_three_part_name(
    _workflow_parameter(
        "ALERT_DISPATCH_TABLE",
        "default.default.streamcommerce_inventory_alert_dispatch",
    ),
    "ALERT_DISPATCH_TABLE",
)

POLL_INTERVAL = _workflow_parameter("POLL_INTERVAL", "1 minute")
QUERY_NAME = _workflow_parameter(
    "QUERY_NAME",
    "streamcommerce_inventory_stockout_alert_timer_v3",
)
TIMER_CHECKPOINT_LOCATION = _workflow_parameter(
    "TIMER_CHECKPOINT_LOCATION",
    "/Volumes/default/default/streamcommerce_vol/checkpoints/streamcommerce_inventory_stockout_alert_timer_v3",
)
RUN_INITIAL_CYCLE = _as_bool(_workflow_parameter("RUN_INITIAL_CYCLE", "true"))

DISPATCH_MODE = _as_enum(
    _workflow_parameter("DISPATCH_MODE", "OFF"),
    "DISPATCH_MODE",
    {"OFF", "DRAFT_ONLY", "LIVE"},
)
OFF_OPEN_POLICY = _as_enum(
    _workflow_parameter("OFF_OPEN_POLICY", "QUEUE"),
    "OFF_OPEN_POLICY",
    {"QUEUE", "SUPPRESS"},
)

AGENT_CHAT_URL = _validate_https_url(
    _workflow_parameter("AGENT_CHAT_URL", ""),
    "AGENT_CHAT_URL",
    required=DISPATCH_MODE != "OFF",
)
NOTIFICATION_FUNCTION_OCID = _workflow_parameter(
    "NOTIFICATION_FUNCTION_OCID",
    "",
).strip()
NOTIFICATION_FUNCTION_INVOKE_ENDPOINT = _validate_https_url(
    _workflow_parameter("NOTIFICATION_FUNCTION_INVOKE_ENDPOINT", ""),
    "NOTIFICATION_FUNCTION_INVOKE_ENDPOINT",
    required=DISPATCH_MODE == "LIVE",
)
if DISPATCH_MODE == "LIVE" and not NOTIFICATION_FUNCTION_OCID.startswith("ocid1.fnfunc."):
    raise ValueError(
        "NOTIFICATION_FUNCTION_OCID must be the deployed dispatcher Function OCID in LIVE mode."
    )

OCI_CONFIG_FILE = _validate_protected_path(
    _workflow_parameter("OCI_CONFIG_FILE", ""),
    "OCI_CONFIG_FILE",
    required=DISPATCH_MODE != "OFF",
)
OCI_PROFILE = _workflow_parameter("OCI_PROFILE", "DEFAULT")
REGION = _workflow_parameter("REGION", "us-chicago-1")

AGENT_TIMEOUT_SECONDS = _as_int(
    _workflow_parameter("AGENT_TIMEOUT_SECONDS", "120"),
    "AGENT_TIMEOUT_SECONDS",
    minimum=10,
    maximum=300,
)
FUNCTION_TIMEOUT_SECONDS = _as_int(
    _workflow_parameter("FUNCTION_TIMEOUT_SECONDS", "90"),
    "FUNCTION_TIMEOUT_SECONDS",
    minimum=10,
    maximum=300,
)
AGENT_TRACE = _as_bool(_workflow_parameter("AGENT_TRACE", "false"))
STORE_ENDPOINTS_IN_AUDIT = _as_bool(
    _workflow_parameter("STORE_ENDPOINTS_IN_AUDIT", "false")
)
MAX_AGENT_RESPONSE_CHARS = _as_int(
    _workflow_parameter("MAX_AGENT_RESPONSE_CHARS", "8000"),
    "MAX_AGENT_RESPONSE_CHARS",
    minimum=500,
    maximum=50000,
)
MAX_FUNCTION_RESPONSE_CHARS = _as_int(
    _workflow_parameter("MAX_FUNCTION_RESPONSE_CHARS", "8000"),
    "MAX_FUNCTION_RESPONSE_CHARS",
    minimum=500,
    maximum=50000,
)
MAX_DRAFT_TITLE_CHARS = _as_int(
    _workflow_parameter("MAX_DRAFT_TITLE_CHARS", "256"),
    "MAX_DRAFT_TITLE_CHARS",
    minimum=32,
    maximum=1024,
)
MAX_DRAFT_BODY_CHARS = _as_int(
    _workflow_parameter("MAX_DRAFT_BODY_CHARS", "4000"),
    "MAX_DRAFT_BODY_CHARS",
    minimum=100,
    maximum=60000,
)
REQUIRED_TITLE_PREFIX = _workflow_parameter(
    "REQUIRED_TITLE_PREFIX",
    "[StreamCommerce",
)

BOOTSTRAP_MODE = _as_enum(
    _workflow_parameter("BOOTSTRAP_MODE", "LATEST_NO_NOTIFY"),
    "BOOTSTRAP_MODE",
    {"LATEST_NO_NOTIFY", "FROM_TIMESTAMP", "ALL_WITH_NOTIFICATIONS"},
)
START_FROM_UTC_TEXT = _workflow_parameter("START_FROM_UTC", "")
ALLOW_HISTORICAL_NOTIFICATIONS = _as_bool(
    _workflow_parameter("ALLOW_HISTORICAL_NOTIFICATIONS", "false")
)
if BOOTSTRAP_MODE == "FROM_TIMESTAMP":
    START_FROM_UTC = _parse_utc_timestamp(START_FROM_UTC_TEXT, "START_FROM_UTC")
else:
    START_FROM_UTC = None
if BOOTSTRAP_MODE == "ALL_WITH_NOTIFICATIONS" and not ALLOW_HISTORICAL_NOTIFICATIONS:
    raise ValueError(
        "BOOTSTRAP_MODE=ALL_WITH_NOTIFICATIONS requires "
        "ALLOW_HISTORICAL_NOTIFICATIONS=true. This prevents an accidental historical email storm."
    )

REQUIRE_SUCCESSFUL_DQ_CYCLE = _as_bool(
    _workflow_parameter("REQUIRE_SUCCESSFUL_DQ_CYCLE", "true")
)
EVENT_TIME_SETTLE_SECONDS = _as_int(
    _workflow_parameter("EVENT_TIME_SETTLE_SECONDS", "30"),
    "EVENT_TIME_SETTLE_SECONDS",
    minimum=0,
    maximum=3600,
)
MAX_CANDIDATE_EVENTS_PER_CYCLE = _as_int(
    _workflow_parameter("MAX_CANDIDATE_EVENTS_PER_CYCLE", "500"),
    "MAX_CANDIDATE_EVENTS_PER_CYCLE",
    minimum=1,
    maximum=5000,
)
MAX_PREPARED_RECOVERY_PER_CYCLE = _as_int(
    _workflow_parameter("MAX_PREPARED_RECOVERY_PER_CYCLE", "1000"),
    "MAX_PREPARED_RECOVERY_PER_CYCLE",
    minimum=1,
    maximum=10000,
)
MAX_DISPATCHES_PER_CYCLE = _as_int(
    _workflow_parameter("MAX_DISPATCHES_PER_CYCLE", "5"),
    "MAX_DISPATCHES_PER_CYCLE",
    minimum=1,
    maximum=10,
)
MAX_NOTIFICATION_ATTEMPTS = _as_int(
    _workflow_parameter("MAX_NOTIFICATION_ATTEMPTS", "3"),
    "MAX_NOTIFICATION_ATTEMPTS",
    minimum=1,
    maximum=10,
)
RETRY_DELAYS_MINUTES = _as_int_list(
    _workflow_parameter("RETRY_DELAYS_MINUTES", "5,15,30"),
    "RETRY_DELAYS_MINUTES",
)
PROCESSING_LEASE_MINUTES = _as_int(
    _workflow_parameter("PROCESSING_LEASE_MINUTES", "10"),
    "PROCESSING_LEASE_MINUTES",
    minimum=1,
    maximum=1440,
)
UNKNOWN_RETRY_ENABLED = _as_bool(
    _workflow_parameter("UNKNOWN_RETRY_ENABLED", "false")
)
ALLOW_ALERT_TABLE_SCHEMA_EVOLUTION = _as_bool(
    _workflow_parameter("ALLOW_ALERT_TABLE_SCHEMA_EVOLUTION", "true")
)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

agent_endpoint_fingerprint = (
    hashlib.sha256(AGENT_CHAT_URL.encode("utf-8")).hexdigest()[:16]
    if AGENT_CHAT_URL else "NOT_CONFIGURED"
)
function_endpoint_fingerprint = (
    hashlib.sha256(
        NOTIFICATION_FUNCTION_INVOKE_ENDPOINT.encode("utf-8")
    ).hexdigest()[:16]
    if NOTIFICATION_FUNCTION_INVOKE_ENDPOINT else "NOT_CONFIGURED"
)

print("Pipeline name                :", PIPELINE_NAME)
print("Transformation version       :", TRANSFORMATION_VERSION)
print("Execution mode               :", EXECUTION_MODE)
print("Silver source                :", SILVER_TABLE)
print("DQ cycle gate                :", REQUIRE_SUCCESSFUL_DQ_CYCLE)
print("Alert state table            :", ALERT_STATE_TABLE)
print("Alert events table           :", ALERT_EVENTS_TABLE)
print("Alert dispatch table         :", ALERT_DISPATCH_TABLE)
print("Dispatch mode                :", DISPATCH_MODE)
print("OFF open policy              :", OFF_OPEN_POLICY)
print("Agent endpoint fingerprint   :", agent_endpoint_fingerprint)
print("Function endpoint fingerprint:", function_endpoint_fingerprint)
print("Bootstrap mode               :", BOOTSTRAP_MODE)
print("Poll interval                :", POLL_INTERVAL)
print("Max dispatches per cycle     :", MAX_DISPATCHES_PER_CYCLE)
print("Unknown auto-retry           :", UNKNOWN_RETRY_ENABLED)

# %% [code cell 2]
# Alert table schemas, non-destructive creation/evolution, and merge helpers
STATE_SPEC = [
    ("record_type", "STRING", T.StringType()),
    ("alert_key", "STRING", T.StringType()),
    ("control_name", "STRING", T.StringType()),
    ("control_value", "STRING", T.StringType()),
    ("product_id", "STRING", T.StringType()),
    ("product_name", "STRING", T.StringType()),
    ("warehouse_id", "STRING", T.StringType()),
    ("alert_type", "STRING", T.StringType()),
    ("alert_status", "STRING", T.StringType()),
    ("inventory_status", "STRING", T.StringType()),
    ("current_alert_event_id", "STRING", T.StringType()),
    ("opened_alert_event_id", "STRING", T.StringType()),
    ("opened_source_event_id", "STRING", T.StringType()),
    ("source_event_id", "STRING", T.StringType()),
    ("source_event_timestamp", "TIMESTAMP", T.TimestampType()),
    ("source_silver_version", "BIGINT", T.LongType()),
    ("source_dq_run_id", "STRING", T.StringType()),
    ("source_sequence_number", "BIGINT", T.LongType()),
    ("source_kafka_partition", "INT", T.IntegerType()),
    ("source_kafka_offset", "BIGINT", T.LongType()),
    ("source_silver_processed_timestamp", "TIMESTAMP", T.TimestampType()),
    ("opened_at", "TIMESTAMP", T.TimestampType()),
    ("last_seen_at", "TIMESTAMP", T.TimestampType()),
    ("last_notified_at", "TIMESTAMP", T.TimestampType()),
    ("resolved_at", "TIMESTAMP", T.TimestampType()),
    ("notification_status", "STRING", T.StringType()),
    ("notification_message_id", "STRING", T.StringType()),
    ("notification_attempts", "BIGINT", T.LongType()),
    ("next_retry_at", "TIMESTAMP", T.TimestampType()),
    ("last_error", "STRING", T.StringType()),
    ("last_transition_type", "STRING", T.StringType()),
    ("updated_at", "TIMESTAMP", T.TimestampType()),
]

EVENT_SPEC = [
    ("alert_event_id", "STRING", T.StringType()),
    ("source_event_id", "STRING", T.StringType()),
    ("alert_key", "STRING", T.StringType()),
    ("processing_status", "STRING", T.StringType()),
    ("state_applied_at", "TIMESTAMP", T.TimestampType()),
    ("transition_type", "STRING", T.StringType()),
    ("alert_type", "STRING", T.StringType()),
    ("product_id", "STRING", T.StringType()),
    ("product_name", "STRING", T.StringType()),
    ("warehouse_id", "STRING", T.StringType()),
    ("inventory_status", "STRING", T.StringType()),
    ("quantity", "BIGINT", T.LongType()),
    ("source_event_timestamp", "TIMESTAMP", T.TimestampType()),
    ("source_sequence_number", "BIGINT", T.LongType()),
    ("source_topic", "STRING", T.StringType()),
    ("kafka_partition", "INT", T.IntegerType()),
    ("kafka_offset", "BIGINT", T.LongType()),
    ("source_silver_processed_timestamp", "TIMESTAMP", T.TimestampType()),
    ("source_silver_version", "BIGINT", T.LongType()),
    ("source_dq_run_id", "STRING", T.StringType()),
    ("detected_at", "TIMESTAMP", T.TimestampType()),
    ("notification_required", "BOOLEAN", T.BooleanType()),
    ("notification_status", "STRING", T.StringType()),
    ("notification_attempts", "BIGINT", T.LongType()),
    ("retryable", "BOOLEAN", T.BooleanType()),
    ("next_retry_at", "TIMESTAMP", T.TimestampType()),
    ("last_notification_at", "TIMESTAMP", T.TimestampType()),
    ("notification_message_id", "STRING", T.StringType()),
    ("dispatch_owner", "STRING", T.StringType()),
    ("dispatch_stage", "STRING", T.StringType()),
    ("dispatch_started_at", "TIMESTAMP", T.TimestampType()),
    ("dispatch_lease_expires_at", "TIMESTAMP", T.TimestampType()),
    ("draft_status", "STRING", T.StringType()),
    ("draft_title", "STRING", T.StringType()),
    ("draft_body", "STRING", T.StringType()),
    ("draft_sha256", "STRING", T.StringType()),
    ("draft_created_at", "TIMESTAMP", T.TimestampType()),
    ("agent_session_key", "STRING", T.StringType()),
    ("agent_http_status", "INT", T.IntegerType()),
    ("agent_opc_request_id", "STRING", T.StringType()),
    ("function_status", "STRING", T.StringType()),
    ("function_http_status", "INT", T.IntegerType()),
    ("function_opc_request_id", "STRING", T.StringType()),
    ("last_error", "STRING", T.StringType()),
    ("event_payload_json", "STRING", T.StringType()),
    ("updated_at", "TIMESTAMP", T.TimestampType()),
]

DISPATCH_SPEC = [
    ("dispatch_id", "STRING", T.StringType()),
    ("alert_event_id", "STRING", T.StringType()),
    ("alert_key", "STRING", T.StringType()),
    ("attempt_number", "BIGINT", T.LongType()),
    ("dispatch_stage", "STRING", T.StringType()),
    ("agent_session_key", "STRING", T.StringType()),
    ("agent_endpoint", "STRING", T.StringType()),
    ("function_endpoint", "STRING", T.StringType()),
    ("dispatch_status", "STRING", T.StringType()),
    ("delivery_certainty", "STRING", T.StringType()),
    ("request_payload_json", "STRING", T.StringType()),
    ("attempted_at", "TIMESTAMP", T.TimestampType()),
    ("completed_at", "TIMESTAMP", T.TimestampType()),
    ("http_status", "INT", T.IntegerType()),
    ("agent_http_status", "INT", T.IntegerType()),
    ("function_http_status", "INT", T.IntegerType()),
    ("notification_message_id", "STRING", T.StringType()),
    ("agent_result_code", "STRING", T.StringType()),
    ("function_result_code", "STRING", T.StringType()),
    ("draft_title", "STRING", T.StringType()),
    ("draft_body_sha256", "STRING", T.StringType()),
    ("agent_response", "STRING", T.StringType()),
    ("function_response", "STRING", T.StringType()),
    ("error_message", "STRING", T.StringType()),
    ("opc_request_id", "STRING", T.StringType()),
    ("agent_opc_request_id", "STRING", T.StringType()),
    ("function_opc_request_id", "STRING", T.StringType()),
    ("updated_at", "TIMESTAMP", T.TimestampType()),
]


def _struct_type(spec: list[tuple[str, str, T.DataType]]) -> T.StructType:
    return T.StructType([T.StructField(name, dtype, True) for name, _, dtype in spec])


STATE_SCHEMA = _struct_type(STATE_SPEC)
EVENT_SCHEMA = _struct_type(EVENT_SPEC)
DISPATCH_SCHEMA = _struct_type(DISPATCH_SPEC)


def _quote_column(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe column name: {name!r}")
    return f"`{name}`"


def _canonical_type_name(data_type: T.DataType) -> str:
    return data_type.simpleString().lower()


def ensure_delta_table(
    table_name: str,
    spec: list[tuple[str, str, T.DataType]],
) -> None:
    columns_sql = ",\n".join(
        f"    {_quote_column(name)} {sql_type}"
        for name, sql_type, _ in spec
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
        )
        USING DELTA
    """)

    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").select("format").first()
    if detail is None or str(detail["format"]).strip().lower() != "delta":
        raise RuntimeError(f"{table_name} must be a Delta table.")

    existing = {
        field.name: _canonical_type_name(field.dataType)
        for field in spark.table(table_name).schema.fields
    }
    expected = {
        name: _canonical_type_name(dtype)
        for name, _, dtype in spec
    }

    for name, sql_type, _ in spec:
        if name in existing:
            if existing[name] != expected[name]:
                raise RuntimeError(
                    f"Schema mismatch for {table_name}.{name}: "
                    f"expected {expected[name]}, found {existing[name]}."
                )
            continue
        if not ALLOW_ALERT_TABLE_SCHEMA_EVOLUTION:
            raise RuntimeError(
                f"Missing required column {table_name}.{name}; "
                "set ALLOW_ALERT_TABLE_SCHEMA_EVOLUTION=true or add it manually."
            )
        spark.sql(
            f"ALTER TABLE {table_name} ADD COLUMNS "
            f"({_quote_column(name)} {sql_type})"
        )
        print("Added alert-table column:", f"{table_name}.{name}")


def _rows_dataframe(
    rows: list[dict[str, Any]],
    spec: list[tuple[str, str, T.DataType]],
    schema: T.StructType,
) -> DataFrame:
    names = [name for name, _, _ in spec]
    tuples = [tuple(row.get(name) for name in names) for row in rows]
    return spark.createDataFrame(tuples, schema=schema)


def merge_dataframe(
    dataframe: DataFrame,
    target_table: str,
    key_columns: list[str],
    spec: list[tuple[str, str, T.DataType]],
    *,
    update_existing: bool,
) -> None:
    names = [name for name, _, _ in spec]
    source = dataframe
    # DataFrame-based bootstrap/state builders may predate newly added
    # audit columns. Add any missing target columns as typed NULLs so
    # schema evolution remains non-destructive and backward compatible.
    for name, _, dtype in spec:
        if name not in source.columns:
            source = source.withColumn(name, F.lit(None).cast(dtype))
    source = source.select(*names)
    temp_view = f"_sc_alert_merge_{uuid.uuid4().hex}"
    source.createOrReplaceTempView(temp_view)

    on_clause = " AND ".join(
        f"target.{_quote_column(name)} = source.{_quote_column(name)}"
        for name in key_columns
    )
    insert_columns = ", ".join(_quote_column(name) for name in names)
    insert_values = ", ".join(f"source.{_quote_column(name)}" for name in names)
    update_clause = ""
    if update_existing:
        assignments = ",\n".join(
            f"target.{_quote_column(name)} = source.{_quote_column(name)}"
            for name in names
            if name not in key_columns
        )
        update_clause = f"WHEN MATCHED THEN UPDATE SET\n{assignments}"

    try:
        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING {temp_view} AS source
            ON {on_clause}
            {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """)
    finally:
        try:
            spark.catalog.dropTempView(temp_view)
        except Exception:
            pass


def merge_rows(
    rows: list[dict[str, Any]],
    target_table: str,
    key_columns: list[str],
    spec: list[tuple[str, str, T.DataType]],
    schema: T.StructType,
    *,
    update_existing: bool,
) -> None:
    if not rows:
        return
    merge_dataframe(
        _rows_dataframe(rows, spec, schema),
        target_table,
        key_columns,
        spec,
        update_existing=update_existing,
    )


def row_dict(row: Row | None) -> dict[str, Any] | None:
    return None if row is None else row.asDict(recursive=True)


ensure_delta_table(ALERT_STATE_TABLE, STATE_SPEC)
ensure_delta_table(ALERT_EVENTS_TABLE, EVENT_SPEC)
ensure_delta_table(ALERT_DISPATCH_TABLE, DISPATCH_SPEC)
print("Dedicated alert Delta tables are ready.")

# %% [code cell 3]
# Source pinning, schema discovery, DQ-cycle gate, and integrity checks
REQUIRED_SILVER_COLUMNS = {
    "event_id",
    "event_timestamp",
    "source_topic",
    "event_type",
    "product_id",
    "warehouse_id",
    "inventory_status",
    "kafka_partition",
    "kafka_offset",
    "silver_processed_timestamp",
}
OPTIONAL_SILVER_COLUMNS = {
    "product_name",
    "quantity",
    "sequence_number",
}
REQUIRED_DQ_RUN_SUMMARY_COLUMNS = {
    "run_id",
    "pipeline_name",
    "run_status",
    "cycle_started_at",
    "cycle_completed_at",
}


def latest_delta_version(table_name: str) -> int:
    row = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .agg(F.max("version").alias("version"))
        .first()
    )
    if row is None or row["version"] is None:
        raise RuntimeError(f"No Delta history is available for {table_name}.")
    return int(row["version"])


def delta_version_timestamp(table_name: str, version: int) -> datetime:
    rows = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .filter(F.col("version") == F.lit(int(version)))
        .select("timestamp")
        .limit(1)
        .collect()
    )
    if not rows:
        raise RuntimeError(f"Delta version {version} is unavailable for {table_name}.")
    value = rows[0]["timestamp"]
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def read_delta_version(table_name: str, version: int) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .option("versionAsOf", str(int(version)))
        .table(table_name)
    )


def require_columns(table_name: str, required: set[str]) -> None:
    actual = set(spark.table(table_name).columns)
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(f"{table_name} is missing required columns: {missing}")


def assert_unique_non_null(table_name: str, column_name: str, where_sql: str = "") -> None:
    condition = f"WHERE {where_sql}" if where_sql else ""
    rows = spark.sql(f"""
        SELECT {_quote_column(column_name)} AS key_value, COUNT(*) AS row_count
        FROM {table_name}
        {condition}
        GROUP BY {_quote_column(column_name)}
        HAVING {_quote_column(column_name)} IS NULL OR COUNT(*) > 1
        LIMIT 10
    """).collect()
    if rows:
        raise RuntimeError(
            f"{table_name}.{column_name} must be non-null and logically unique; "
            f"violations={[(row['key_value'], row['row_count']) for row in rows]}"
        )


def find_successful_dq_run_for_silver_version(silver_version: int) -> str | None:
    if not REQUIRE_SUCCESSFUL_DQ_CYCLE:
        return ""

    commit_timestamp = delta_version_timestamp(SILVER_TABLE, silver_version)
    summaries = spark.table(DQ_RUN_SUMMARY_TABLE).filter(
        (F.col("pipeline_name") == F.lit(SILVER_PIPELINE_NAME))
        & (F.col("run_status") == F.lit("SUCCESS"))
        & (F.col("cycle_completed_at") >= F.lit(commit_timestamp))
    )
    if "bronze_record_count" in summaries.columns:
        summaries = summaries.filter(
            F.coalesce(F.col("bronze_record_count"), F.lit(0)) > F.lit(0)
        )

    rows = (
        summaries
        .withColumn(
            "_contains_silver_commit",
            F.when(
                (F.col("cycle_started_at") <= F.lit(commit_timestamp))
                & (F.col("cycle_completed_at") >= F.lit(commit_timestamp)),
                F.lit(0),
            ).otherwise(F.lit(1)),
        )
        .orderBy(
            F.col("_contains_silver_commit").asc(),
            F.col("cycle_completed_at").asc(),
            F.col("run_id").asc(),
        )
        .select("run_id")
        .limit(1)
        .collect()
    )
    return str(rows[0]["run_id"]) if rows else None


require_columns(SILVER_TABLE, REQUIRED_SILVER_COLUMNS)
if REQUIRE_SUCCESSFUL_DQ_CYCLE:
    require_columns(DQ_RUN_SUMMARY_TABLE, REQUIRED_DQ_RUN_SUMMARY_COLUMNS)

assert_unique_non_null(
    ALERT_STATE_TABLE,
    "alert_key",
    "COALESCE(record_type, 'ALERT') IN ('ALERT', 'CONTROL')",
)
assert_unique_non_null(ALERT_EVENTS_TABLE, "alert_event_id")
assert_unique_non_null(ALERT_EVENTS_TABLE, "source_event_id")
assert_unique_non_null(ALERT_DISPATCH_TABLE, "dispatch_id")

print("Source schema and dedicated alert-table integrity checks passed.")

# %% [code cell 4]
# Inventory-event normalization and deterministic identifiers
ALERT_TYPE = "PRODUCT_OUT_OF_STOCK"
EVENT_ID_VERSION = "streamcommerce-inventory-alert-event-v2"
ALERT_KEY_VERSION = "streamcommerce-inventory-alert-key-v2"
DISPATCH_ID_VERSION = "streamcommerce-inventory-alert-dispatch-v2"
CONTROL_ALERT_KEY = "__CONTROL__|streamcommerce_inventory_stockout_alert_v2"
CONTROL_NAME = "DETECTOR_CONTROL_V2"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def stable_sha256(*parts: Any) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def make_alert_key(product_id: str, warehouse_id: str) -> str:
    return stable_sha256(ALERT_KEY_VERSION, ALERT_TYPE, product_id, warehouse_id)


def make_alert_event_id(source_event_id: str) -> str:
    return stable_sha256(EVENT_ID_VERSION, source_event_id)


def make_dispatch_id(alert_event_id: str, attempt_number: int) -> str:
    return stable_sha256(DISPATCH_ID_VERSION, alert_event_id, int(attempt_number))


def _column_or_null(dataframe: DataFrame, name: str, data_type: str):
    if name in dataframe.columns:
        return F.col(name).cast(data_type)
    return F.lit(None).cast(data_type)


def normalize_inventory_events(silver_df: DataFrame) -> DataFrame:
    projected = silver_df.select(
        F.trim(F.col("event_id").cast("string")).alias("event_id"),
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.trim(F.col("source_topic").cast("string")).alias("source_topic"),
        F.trim(F.col("event_type").cast("string")).alias("event_type"),
        F.trim(F.col("product_id").cast("string")).alias("product_id"),
        _column_or_null(silver_df, "product_name", "string").alias("product_name"),
        F.trim(F.col("warehouse_id").cast("string")).alias("warehouse_id"),
        F.trim(F.col("inventory_status").cast("string")).alias("inventory_status"),
        _column_or_null(silver_df, "quantity", "bigint").alias("quantity"),
        _column_or_null(silver_df, "sequence_number", "bigint").alias("sequence_number"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset").cast("bigint").alias("kafka_offset"),
        F.col("silver_processed_timestamp").cast("timestamp").alias(
            "silver_processed_timestamp"
        ),
    )

    open_condition = (
        (F.col("source_topic") == F.lit("inventory_updates"))
        & (F.col("event_type") == F.lit("stockout_warning"))
        & (F.col("inventory_status") == F.lit("OUT_OF_STOCK"))
    )
    resolve_condition = (
        (F.col("source_topic") == F.lit("inventory_updates"))
        & F.col("event_type").isin("stock_added", "stock_released")
        & F.col("inventory_status").isin("IN_STOCK", "LOW_STOCK")
    )

    return (
        projected
        .withColumn(
            "event_kind",
            F.when(open_condition, F.lit("OPEN"))
            .when(resolve_condition, F.lit("RESOLVE")),
        )
        .filter(F.col("event_kind").isNotNull())
    )


def sanitized_event_payload(event: dict[str, Any], transition_type: str) -> str:
    payload = {
        "transition_type": transition_type,
        "alert_type": ALERT_TYPE,
        "product_id": event.get("product_id"),
        "product_name": event.get("product_name"),
        "warehouse_id": event.get("warehouse_id"),
        "inventory_status": event.get("inventory_status"),
        "quantity": event.get("quantity"),
        "source_event_id": event.get("event_id"),
        "source_event_timestamp": (
            ensure_utc(event.get("event_timestamp")).isoformat()
            if event.get("event_timestamp") else None
        ),
        "source_topic": event.get("source_topic"),
        "kafka_partition": event.get("kafka_partition"),
        "kafka_offset": event.get("kafka_offset"),
        "source_silver_processed_timestamp": (
            ensure_utc(event.get("silver_processed_timestamp")).isoformat()
            if event.get("silver_processed_timestamp") else None
        ),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


print("Inventory event normalization and deterministic identifiers initialized.")

# %% [code cell 5]
# Bootstrap control: initialize current state without replaying historical emails

def load_control() -> dict[str, Any] | None:
    rows = (
        spark.table(ALERT_STATE_TABLE)
        .filter(F.col("alert_key") == F.lit(CONTROL_ALERT_KEY))
        .select("control_value")
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError("Multiple detector control rows exist; repair before continuing.")
    if not rows:
        return None
    text = rows[0]["control_value"]
    if not text:
        return {}
    try:
        value = json.loads(text)
    except Exception as exc:
        raise RuntimeError("Detector control_value is not valid JSON.") from exc
    if not isinstance(value, dict):
        raise RuntimeError("Detector control_value must contain a JSON object.")
    return value


def write_control(payload: dict[str, Any]) -> None:
    now = utc_now()
    row = {
        "record_type": "CONTROL",
        "alert_key": CONTROL_ALERT_KEY,
        "control_name": CONTROL_NAME,
        "control_value": json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ),
        "updated_at": now,
    }
    merge_rows(
        [row],
        ALERT_STATE_TABLE,
        ["alert_key"],
        STATE_SPEC,
        STATE_SCHEMA,
        update_existing=True,
    )


def _bootstrap_event_dataframe(
    historical_df: DataFrame,
    silver_version: int,
    dq_run_id: str,
    now: datetime,
) -> DataFrame:
    alert_key_expr = F.sha2(
        F.concat_ws(
            "|",
            F.lit(ALERT_KEY_VERSION),
            F.lit(ALERT_TYPE),
            F.coalesce(F.col("product_id"), F.lit("")),
            F.coalesce(F.col("warehouse_id"), F.lit("")),
        ),
        256,
    )
    event_id_expr = F.sha2(
        F.concat_ws("|", F.lit(EVENT_ID_VERSION), F.col("event_id")),
        256,
    )
    payload_expr = F.to_json(
        F.struct(
            F.lit("BOOTSTRAP_SUPPRESSED").alias("transition_type"),
            F.lit(ALERT_TYPE).alias("alert_type"),
            "product_id",
            "product_name",
            "warehouse_id",
            "inventory_status",
            "quantity",
            F.col("event_id").alias("source_event_id"),
            F.col("event_timestamp").alias("source_event_timestamp"),
            "source_topic",
            "kafka_partition",
            "kafka_offset",
            F.col("silver_processed_timestamp").alias(
                "source_silver_processed_timestamp"
            ),
        )
    )

    return historical_df.select(
        event_id_expr.alias("alert_event_id"),
        F.col("event_id").alias("source_event_id"),
        alert_key_expr.alias("alert_key"),
        F.lit("APPLIED").alias("processing_status"),
        F.lit(now).cast("timestamp").alias("state_applied_at"),
        F.lit("BOOTSTRAP_SUPPRESSED").alias("transition_type"),
        F.lit(ALERT_TYPE).alias("alert_type"),
        "product_id",
        "product_name",
        "warehouse_id",
        "inventory_status",
        "quantity",
        F.col("event_timestamp").alias("source_event_timestamp"),
        F.col("sequence_number").alias("source_sequence_number"),
        "source_topic",
        "kafka_partition",
        "kafka_offset",
        F.col("silver_processed_timestamp").alias(
            "source_silver_processed_timestamp"
        ),
        F.lit(int(silver_version)).cast("bigint").alias("source_silver_version"),
        F.lit(dq_run_id).cast("string").alias("source_dq_run_id"),
        F.lit(now).cast("timestamp").alias("detected_at"),
        F.lit(False).cast("boolean").alias("notification_required"),
        F.lit("SUPPRESSED").alias("notification_status"),
        F.lit(0).cast("bigint").alias("notification_attempts"),
        F.lit(False).cast("boolean").alias("retryable"),
        F.lit(None).cast("timestamp").alias("next_retry_at"),
        F.lit(None).cast("timestamp").alias("last_notification_at"),
        F.lit(None).cast("string").alias("notification_message_id"),
        F.lit(None).cast("string").alias("dispatch_owner"),
        F.lit(None).cast("timestamp").alias("dispatch_started_at"),
        F.lit(None).cast("timestamp").alias("dispatch_lease_expires_at"),
        F.lit("Historical event suppressed during safe bootstrap.").alias("last_error"),
        payload_expr.alias("event_payload_json"),
        F.lit(now).cast("timestamp").alias("updated_at"),
    )


def bootstrap_if_needed(
    normalized_df: DataFrame,
    silver_version: int,
    dq_run_id: str,
) -> dict[str, Any]:
    existing = load_control()
    if existing is not None:
        return existing

    now = utc_now()
    if BOOTSTRAP_MODE == "LATEST_NO_NOTIFY":
        historical_df = normalized_df
        bootstrap_boundary = now
    elif BOOTSTRAP_MODE == "FROM_TIMESTAMP":
        assert START_FROM_UTC is not None
        historical_df = normalized_df.filter(
            F.col("silver_processed_timestamp") < F.lit(START_FROM_UTC)
        )
        bootstrap_boundary = START_FROM_UTC
    else:
        historical_df = normalized_df.limit(0)
        bootstrap_boundary = datetime(1970, 1, 1, tzinfo=timezone.utc)

    null_event_ids = historical_df.filter(F.col("event_id").isNull()).limit(1).collect()
    if null_event_ids:
        raise RuntimeError("Relevant historical Silver inventory event has null event_id.")

    historical_count = int(historical_df.count())
    if historical_count > 0:
        bootstrap_events = _bootstrap_event_dataframe(
            historical_df,
            silver_version,
            dq_run_id,
            now,
        )
        merge_dataframe(
            bootstrap_events,
            ALERT_EVENTS_TABLE,
            ["source_event_id"],
            EVENT_SPEC,
            update_existing=False,
        )

        valid_history = historical_df.filter(
            F.col("product_id").isNotNull()
            & (F.length(F.trim(F.col("product_id"))) > 0)
            & F.col("warehouse_id").isNotNull()
            & (F.length(F.trim(F.col("warehouse_id"))) > 0)
        ).withColumn(
            "alert_key",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.lit(ALERT_KEY_VERSION),
                    F.lit(ALERT_TYPE),
                    F.col("product_id"),
                    F.col("warehouse_id"),
                ),
                256,
            ),
        ).withColumn(
            "alert_event_id",
            F.sha2(
                F.concat_ws("|", F.lit(EVENT_ID_VERSION), F.col("event_id")),
                256,
            ),
        )

        latest_window = Window.partitionBy("alert_key").orderBy(
            F.col("event_timestamp").desc_nulls_last(),
            F.col("silver_processed_timestamp").desc_nulls_last(),
            F.col("kafka_partition").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last(),
            F.col("event_id").desc_nulls_last(),
        )
        latest = valid_history.withColumn(
            "_rank", F.row_number().over(latest_window)
        ).filter(F.col("_rank") == 1)

        bootstrap_state = latest.select(
            F.lit("ALERT").alias("record_type"),
            "alert_key",
            F.lit(None).cast("string").alias("control_name"),
            F.lit(None).cast("string").alias("control_value"),
            "product_id",
            "product_name",
            "warehouse_id",
            F.lit(ALERT_TYPE).alias("alert_type"),
            F.when(F.col("event_kind") == "OPEN", F.lit("OPEN"))
            .otherwise(F.lit("RESOLVED")).alias("alert_status"),
            "inventory_status",
            F.col("alert_event_id").alias("current_alert_event_id"),
            F.when(F.col("event_kind") == "OPEN", F.col("alert_event_id"))
            .otherwise(F.lit(None).cast("string")).alias("opened_alert_event_id"),
            F.when(F.col("event_kind") == "OPEN", F.col("event_id"))
            .otherwise(F.lit(None).cast("string")).alias("opened_source_event_id"),
            F.col("event_id").alias("source_event_id"),
            F.col("event_timestamp").alias("source_event_timestamp"),
            F.lit(int(silver_version)).cast("bigint").alias("source_silver_version"),
            F.lit(dq_run_id).cast("string").alias("source_dq_run_id"),
            F.col("sequence_number").cast("bigint").alias("source_sequence_number"),
            F.col("kafka_partition").cast("int").alias("source_kafka_partition"),
            F.col("kafka_offset").cast("bigint").alias("source_kafka_offset"),
            F.col("silver_processed_timestamp").alias(
                "source_silver_processed_timestamp"
            ),
            F.when(F.col("event_kind") == "OPEN", F.col("event_timestamp"))
            .otherwise(F.lit(None).cast("timestamp")).alias("opened_at"),
            F.col("event_timestamp").alias("last_seen_at"),
            F.lit(None).cast("timestamp").alias("last_notified_at"),
            F.when(F.col("event_kind") == "RESOLVE", F.col("event_timestamp"))
            .otherwise(F.lit(None).cast("timestamp")).alias("resolved_at"),
            F.when(F.col("event_kind") == "OPEN", F.lit("SUPPRESSED"))
            .otherwise(F.lit("NOT_REQUIRED")).alias("notification_status"),
            F.lit(None).cast("string").alias("notification_message_id"),
            F.lit(0).cast("bigint").alias("notification_attempts"),
            F.lit(None).cast("timestamp").alias("next_retry_at"),
            F.lit("Initialized from historical Silver without notification.").alias(
                "last_error"
            ),
            F.when(F.col("event_kind") == "OPEN", F.lit("BOOTSTRAP_OPEN"))
            .otherwise(F.lit("BOOTSTRAP_RESOLVED")).alias("last_transition_type"),
            F.lit(now).cast("timestamp").alias("updated_at"),
        )
        merge_dataframe(
            bootstrap_state,
            ALERT_STATE_TABLE,
            ["alert_key"],
            STATE_SPEC,
            update_existing=True,
        )

    control = {
        "pipeline_name": PIPELINE_NAME,
        "transformation_version": TRANSFORMATION_VERSION,
        "bootstrap_mode": BOOTSTRAP_MODE,
        "bootstrap_boundary_utc": bootstrap_boundary.isoformat(),
        "bootstrap_silver_version": int(silver_version),
        "bootstrap_dq_run_id": dq_run_id,
        "historical_relevant_events_suppressed": historical_count,
        "created_at_utc": now.isoformat(),
    }
    write_control(control)
    print("Bootstrap completed:", control)
    return control

# %% [code cell 6]
# Deterministic transition planning and recoverable two-table application

def state_rows_for_keys(alert_keys: list[str]) -> dict[str, dict[str, Any]]:
    if not alert_keys:
        return {}
    rows = (
        spark.table(ALERT_STATE_TABLE)
        .filter(F.coalesce(F.col("record_type"), F.lit("ALERT")) == F.lit("ALERT"))
        .filter(F.col("alert_key").isin(alert_keys))
        .collect()
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = row.asDict(recursive=True)
        key = item["alert_key"]
        if key in result:
            raise RuntimeError(f"Duplicate alert state rows for alert_key={key}")
        result[key] = item
    return result


def _int_or_default(value: Any, default: int = -1) -> int:
    return default if value is None else int(value)


def event_order_key(event: dict[str, Any]) -> tuple[Any, ...]:
    minimum = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        ensure_utc(event.get("event_timestamp")) or minimum,
        ensure_utc(event.get("silver_processed_timestamp")) or minimum,
        _int_or_default(event.get("kafka_partition")),
        _int_or_default(event.get("kafka_offset")),
        str(event.get("event_id") or ""),
    )


def state_order_key(state: dict[str, Any]) -> tuple[Any, ...]:
    minimum = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        ensure_utc(state.get("source_event_timestamp")) or minimum,
        ensure_utc(state.get("source_silver_processed_timestamp")) or minimum,
        _int_or_default(state.get("source_kafka_partition")),
        _int_or_default(state.get("source_kafka_offset")),
        str(state.get("source_event_id") or ""),
    )


def event_is_stale(event: dict[str, Any], state: dict[str, Any] | None) -> bool:
    if not state or not state.get("source_event_id"):
        return False
    if str(event.get("event_id")) == str(state.get("source_event_id")):
        return True
    return event_order_key(event) <= state_order_key(state)


def fresh_state_template(existing: dict[str, Any] | None) -> dict[str, Any]:
    return dict(existing or {})


def build_transition(
    event: dict[str, Any],
    state: dict[str, Any] | None,
    silver_version: int,
    dq_run_id: str,
    detected_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    source_event_id = str(event.get("event_id") or "").strip()
    if not source_event_id:
        raise RuntimeError("Relevant Silver inventory event has no event_id.")

    product_id = str(event.get("product_id") or "").strip()
    warehouse_id = str(event.get("warehouse_id") or "").strip()
    valid_key = bool(product_id and warehouse_id)
    alert_key = make_alert_key(product_id, warehouse_id)
    alert_event_id = make_alert_event_id(source_event_id)

    if not valid_key:
        transition_type = "INVALID_SUPPRESSED"
    elif event_is_stale(event, state):
        transition_type = "STALE_SUPPRESSED"
    elif event.get("event_kind") == "OPEN":
        transition_type = "REOBSERVED" if state and state.get("alert_status") == "OPEN" else "OPENED"
    elif event.get("event_kind") == "RESOLVE":
        transition_type = "RESOLVED" if state and state.get("alert_status") == "OPEN" else "RESOLVE_NOOP"
    else:
        transition_type = "INVALID_SUPPRESSED"

    notification_required = transition_type == "OPENED"
    if not notification_required:
        notification_status = "NOT_REQUIRED"
        retryable = False
    elif DISPATCH_MODE == "OFF" and OFF_OPEN_POLICY == "SUPPRESS":
        notification_status = "SUPPRESSED"
        retryable = False
    else:
        # OFF+QUEUE keeps the event pending without external calls.
        # Switching the Workflow to LIVE later can dispatch it if the
        # corresponding alert is still the current OPEN condition.
        notification_status = "PENDING"
        retryable = True

    event_row = {
        "alert_event_id": alert_event_id,
        "source_event_id": source_event_id,
        "alert_key": alert_key,
        "processing_status": "PREPARED",
        "state_applied_at": None,
        "transition_type": transition_type,
        "alert_type": ALERT_TYPE,
        "product_id": product_id or None,
        "product_name": event.get("product_name"),
        "warehouse_id": warehouse_id or None,
        "inventory_status": event.get("inventory_status"),
        "quantity": event.get("quantity"),
        "source_event_timestamp": ensure_utc(event.get("event_timestamp")),
        "source_sequence_number": event.get("sequence_number"),
        "source_topic": event.get("source_topic"),
        "kafka_partition": event.get("kafka_partition"),
        "kafka_offset": event.get("kafka_offset"),
        "source_silver_processed_timestamp": ensure_utc(
            event.get("silver_processed_timestamp")
        ),
        "source_silver_version": int(silver_version),
        "source_dq_run_id": dq_run_id,
        "detected_at": detected_at,
        "notification_required": notification_required,
        "notification_status": notification_status,
        "notification_attempts": 0,
        "retryable": retryable,
        "next_retry_at": None,
        "last_notification_at": None,
        "notification_message_id": None,
        "dispatch_owner": None,
        "dispatch_stage": None,
        "dispatch_started_at": None,
        "dispatch_lease_expires_at": None,
        "draft_status": None,
        "draft_title": None,
        "draft_body": None,
        "draft_sha256": None,
        "draft_created_at": None,
        "agent_session_key": None,
        "agent_http_status": None,
        "agent_opc_request_id": None,
        "function_status": None,
        "function_http_status": None,
        "function_opc_request_id": None,
        "last_error": (
            "Missing product_id or warehouse_id."
            if transition_type == "INVALID_SUPPRESSED"
            else "Older than the current state event; suppressed."
            if transition_type == "STALE_SUPPRESSED"
            else None
        ),
        "event_payload_json": sanitized_event_payload(event, transition_type),
        "updated_at": detected_at,
    }

    if transition_type in {"INVALID_SUPPRESSED", "STALE_SUPPRESSED"}:
        return event_row, None

    next_state = fresh_state_template(state)
    next_state.update({
        "record_type": "ALERT",
        "alert_key": alert_key,
        "control_name": None,
        "control_value": None,
        "product_id": product_id,
        "product_name": event.get("product_name") or next_state.get("product_name"),
        "warehouse_id": warehouse_id,
        "alert_type": ALERT_TYPE,
        "inventory_status": event.get("inventory_status"),
        "current_alert_event_id": alert_event_id,
        "source_event_id": source_event_id,
        "source_event_timestamp": ensure_utc(event.get("event_timestamp")),
        "source_silver_version": int(silver_version),
        "source_dq_run_id": dq_run_id,
        "source_sequence_number": event.get("sequence_number"),
        "source_kafka_partition": event.get("kafka_partition"),
        "source_kafka_offset": event.get("kafka_offset"),
        "source_silver_processed_timestamp": ensure_utc(
            event.get("silver_processed_timestamp")
        ),
        "last_seen_at": ensure_utc(event.get("event_timestamp")) or detected_at,
        "last_transition_type": transition_type,
        "updated_at": detected_at,
    })

    if transition_type == "OPENED":
        next_state.update({
            "alert_status": "OPEN",
            "opened_at": ensure_utc(event.get("event_timestamp")) or detected_at,
            "opened_alert_event_id": alert_event_id,
            "opened_source_event_id": source_event_id,
            "resolved_at": None,
            "notification_status": notification_status,
            "notification_message_id": None,
            "notification_attempts": 0,
            "next_retry_at": None,
            "last_error": event_row.get("last_error"),
        })
    elif transition_type == "REOBSERVED":
        next_state["alert_status"] = "OPEN"
    else:
        next_state.update({
            "alert_status": "RESOLVED",
            "resolved_at": ensure_utc(event.get("event_timestamp")) or detected_at,
            "notification_status": "NOT_REQUIRED",
            "next_retry_at": None,
            "last_error": None,
        })

    return event_row, next_state


def mark_events_applied(event_rows: list[dict[str, Any]]) -> None:
    if not event_rows:
        return
    applied_at = utc_now()
    updates = []
    for row in event_rows:
        item = dict(row)
        item["processing_status"] = "APPLIED"
        item["state_applied_at"] = applied_at
        item["updated_at"] = applied_at
        updates.append(item)
    merge_rows(
        updates,
        ALERT_EVENTS_TABLE,
        ["alert_event_id"],
        EVENT_SPEC,
        EVENT_SCHEMA,
        update_existing=True,
    )


def apply_fixed_prepared_event(
    event_row: dict[str, Any],
    existing_state: dict[str, Any] | None,
    applied_at: datetime,
) -> dict[str, Any] | None:
    transition = event_row.get("transition_type")
    if transition in {"INVALID_SUPPRESSED", "STALE_SUPPRESSED", "BOOTSTRAP_SUPPRESSED"}:
        return None

    replay_event = {
        "event_id": event_row.get("source_event_id"),
        "event_timestamp": event_row.get("source_event_timestamp"),
        "silver_processed_timestamp": event_row.get("source_silver_processed_timestamp"),
        "kafka_partition": event_row.get("kafka_partition"),
        "kafka_offset": event_row.get("kafka_offset"),
    }
    if existing_state and event_is_stale(replay_event, existing_state):
        return None

    state = fresh_state_template(existing_state)
    state.update({
        "record_type": "ALERT",
        "alert_key": event_row.get("alert_key"),
        "control_name": None,
        "control_value": None,
        "product_id": event_row.get("product_id"),
        "product_name": event_row.get("product_name") or state.get("product_name"),
        "warehouse_id": event_row.get("warehouse_id"),
        "alert_type": event_row.get("alert_type"),
        "inventory_status": event_row.get("inventory_status"),
        "current_alert_event_id": event_row.get("alert_event_id"),
        "source_event_id": event_row.get("source_event_id"),
        "source_event_timestamp": event_row.get("source_event_timestamp"),
        "source_silver_version": event_row.get("source_silver_version"),
        "source_dq_run_id": event_row.get("source_dq_run_id"),
        "source_sequence_number": event_row.get("source_sequence_number"),
        "source_kafka_partition": event_row.get("kafka_partition"),
        "source_kafka_offset": event_row.get("kafka_offset"),
        "source_silver_processed_timestamp": event_row.get(
            "source_silver_processed_timestamp"
        ),
        "last_seen_at": event_row.get("source_event_timestamp") or applied_at,
        "last_transition_type": transition,
        "updated_at": applied_at,
    })

    if transition == "OPENED":
        state.update({
            "alert_status": "OPEN",
            "opened_at": event_row.get("source_event_timestamp") or applied_at,
            "opened_alert_event_id": event_row.get("alert_event_id"),
            "opened_source_event_id": event_row.get("source_event_id"),
            "resolved_at": None,
            "notification_status": event_row.get("notification_status"),
            "notification_message_id": event_row.get("notification_message_id"),
            "notification_attempts": event_row.get("notification_attempts") or 0,
            "next_retry_at": event_row.get("next_retry_at"),
            "last_error": event_row.get("last_error"),
        })
    elif transition == "REOBSERVED":
        state["alert_status"] = "OPEN"
    else:
        state.update({
            "alert_status": "RESOLVED",
            "resolved_at": event_row.get("source_event_timestamp") or applied_at,
            "notification_status": "NOT_REQUIRED",
            "next_retry_at": None,
        })
    return state


def recover_prepared_events() -> int:
    rows = (
        spark.table(ALERT_EVENTS_TABLE)
        .filter(F.col("processing_status") == F.lit("PREPARED"))
        .orderBy(
            F.col("source_event_timestamp").asc_nulls_last(),
            F.col("source_silver_processed_timestamp").asc_nulls_last(),
            F.col("kafka_partition").asc_nulls_last(),
            F.col("kafka_offset").asc_nulls_last(),
            F.col("source_event_id").asc(),
        )
        .limit(MAX_PREPARED_RECOVERY_PER_CYCLE)
        .collect()
    )
    if not rows:
        return 0

    event_rows = [row.asDict(recursive=True) for row in rows]
    keys = sorted({row["alert_key"] for row in event_rows if row.get("alert_key")})
    states = state_rows_for_keys(keys)
    state_updates: dict[str, dict[str, Any]] = {}
    applied_at = utc_now()

    for event_row in event_rows:
        key = event_row.get("alert_key")
        current = state_updates.get(key) or states.get(key)
        updated = apply_fixed_prepared_event(event_row, current, applied_at)
        if updated is not None and key:
            state_updates[key] = updated

    merge_rows(
        list(state_updates.values()),
        ALERT_STATE_TABLE,
        ["alert_key"],
        STATE_SPEC,
        STATE_SCHEMA,
        update_existing=True,
    )
    mark_events_applied(event_rows)
    print("Recovered PREPARED alert events:", len(event_rows))
    return len(event_rows)


def detect_new_transitions(
    normalized_df: DataFrame,
    silver_version: int,
    dq_run_id: str,
) -> dict[str, int]:
    null_ids = normalized_df.filter(F.col("event_id").isNull()).limit(1).collect()
    if null_ids:
        raise RuntimeError("Relevant Silver inventory event has null event_id.")

    processed_ids = spark.table(ALERT_EVENTS_TABLE).select("source_event_id")
    settle_cutoff = utc_now() - timedelta(seconds=EVENT_TIME_SETTLE_SECONDS)
    candidates_df = (
        normalized_df
        .join(processed_ids, normalized_df.event_id == processed_ids.source_event_id, "left_anti")
        .filter(F.col("event_timestamp") <= F.lit(settle_cutoff))
        .orderBy(
            F.col("event_timestamp").asc_nulls_last(),
            F.col("silver_processed_timestamp").asc_nulls_last(),
            F.col("kafka_partition").asc_nulls_last(),
            F.col("kafka_offset").asc_nulls_last(),
            F.col("event_id").asc(),
        )
        .limit(MAX_CANDIDATE_EVENTS_PER_CYCLE + 1)
    )
    collected = candidates_df.collect()
    truncated = len(collected) > MAX_CANDIDATE_EVENTS_PER_CYCLE
    collected = collected[:MAX_CANDIDATE_EVENTS_PER_CYCLE]
    if not collected:
        return {"prepared": 0, "opened": 0, "reobserved": 0, "resolved": 0, "suppressed": 0, "truncated": 0}

    events = [row.asDict(recursive=True) for row in collected]
    keys = sorted({
        make_alert_key(str(item.get("product_id") or "").strip(), str(item.get("warehouse_id") or "").strip())
        for item in events
    })
    states = state_rows_for_keys(keys)
    planned_events: list[dict[str, Any]] = []
    state_updates: dict[str, dict[str, Any]] = {}
    detected_at = utc_now()

    counters = {"prepared": 0, "opened": 0, "reobserved": 0, "resolved": 0, "suppressed": 0, "truncated": int(truncated)}
    for event in events:
        product_id = str(event.get("product_id") or "").strip()
        warehouse_id = str(event.get("warehouse_id") or "").strip()
        key = make_alert_key(product_id, warehouse_id)
        current_state = state_updates.get(key) or states.get(key)
        event_row, next_state = build_transition(
            event,
            current_state,
            silver_version,
            dq_run_id,
            detected_at,
        )
        planned_events.append(event_row)
        if next_state is not None:
            state_updates[key] = next_state

        counters["prepared"] += 1
        transition = event_row["transition_type"]
        if transition == "OPENED":
            counters["opened"] += 1
        elif transition == "REOBSERVED":
            counters["reobserved"] += 1
        elif transition == "RESOLVED":
            counters["resolved"] += 1
        elif transition in {"INVALID_SUPPRESSED", "STALE_SUPPRESSED"}:
            counters["suppressed"] += 1

    # Phase 1: deterministic events are persisted as PREPARED.
    merge_rows(
        planned_events,
        ALERT_EVENTS_TABLE,
        ["source_event_id"],
        EVENT_SPEC,
        EVENT_SCHEMA,
        update_existing=False,
    )
    # Phase 2: apply the resulting current states.
    merge_rows(
        list(state_updates.values()),
        ALERT_STATE_TABLE,
        ["alert_key"],
        STATE_SPEC,
        STATE_SCHEMA,
        update_existing=True,
    )
    # Phase 3: only after state succeeds, mark the events APPLIED.
    mark_events_applied(planned_events)
    return counters


print("Recoverable transition planner initialized.")

# %% [code cell 7]
# OCI user-principal authentication, Draft-only Agent, Function dispatch, retries, and audit
import io
from pathlib import Path
from urllib.parse import urlsplit

import oci
import requests
from oci.exceptions import RequestException, ServiceError

_OCI_AUTH_CONTEXT = None
_FUNCTION_CLIENT = None
ALLOWED_DRAFT_KEYS = {"status", "alert_event_id", "title", "body"}


def iso_utc(value: datetime | None) -> str:
    normalized = ensure_utc(value)
    if normalized is None:
        return ""
    return normalized.isoformat().replace("+00:00", "Z")


def _endpoint_audit_value(value: str) -> str | None:
    if not value:
        return None
    if STORE_ENDPOINTS_IN_AUDIT:
        return value
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _load_oci_auth_context() -> dict[str, Any]:
    global _OCI_AUTH_CONTEXT
    if _OCI_AUTH_CONTEXT is not None:
        return _OCI_AUTH_CONTEXT

    if DISPATCH_MODE == "OFF":
        raise RuntimeError("OCI auth context is not required while DISPATCH_MODE=OFF.")

    config_path = Path(OCI_CONFIG_FILE).expanduser()
    if not config_path.is_file():
        raise RuntimeError("OCI_CONFIG_FILE does not exist or is not readable.")

    config = oci.config.from_file(
        file_location=str(config_path),
        profile_name=OCI_PROFILE,
    )
    config["region"] = REGION

    key_file = str(config.get("key_file", "")).strip()
    if not key_file:
        raise RuntimeError("The selected OCI profile does not contain key_file.")
    key_path = Path(key_file).expanduser()
    if not key_path.is_absolute():
        key_path = (config_path.parent / key_path).resolve()
        config["key_file"] = str(key_path)
    if not key_path.is_file():
        raise RuntimeError("The OCI key_file does not exist or is not readable.")

    security_token_file = str(config.get("security_token_file", "")).strip()
    if security_token_file:
        token_path = Path(security_token_file).expanduser()
        if not token_path.is_absolute():
            token_path = (config_path.parent / token_path).resolve()
        if not token_path.is_file():
            raise RuntimeError("security_token_file is configured but not readable.")
        private_key = oci.signer.load_private_key_from_file(str(key_path))
        token = token_path.read_text(encoding="utf-8").strip()
        signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
        requests_signer = signer
        function_signer = signer
        auth_mode = "SECURITY_TOKEN"
    else:
        oci.config.validate_config(config)
        requests_signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            pass_phrase=config.get("pass_phrase"),
        )
        function_signer = None
        auth_mode = "API_KEY"

    _OCI_AUTH_CONTEXT = {
        "config": config,
        "requests_signer": requests_signer,
        "function_signer": function_signer,
        "auth_mode": auth_mode,
    }
    print("OCI user-principal configuration loaded; credential values were not printed.")
    print("OCI auth mode:", auth_mode)
    return _OCI_AUTH_CONTEXT


def _notification_function_client():
    global _FUNCTION_CLIENT
    if _FUNCTION_CLIENT is not None:
        return _FUNCTION_CLIENT
    if DISPATCH_MODE != "LIVE":
        raise RuntimeError("Notification Function client is only used in LIVE mode.")

    auth = _load_oci_auth_context()
    endpoint = urlsplit(NOTIFICATION_FUNCTION_INVOKE_ENDPOINT)
    service_endpoint = f"{endpoint.scheme}://{endpoint.netloc}"
    kwargs = {
        "config": auth["config"],
        "service_endpoint": service_endpoint,
        "timeout": (10, FUNCTION_TIMEOUT_SECONDS),
    }
    if auth["function_signer"] is not None:
        kwargs["signer"] = auth["function_signer"]
    _FUNCTION_CLIENT = oci.functions.FunctionsInvokeClient(**kwargs)
    return _FUNCTION_CLIENT


def build_agent_text(event: dict[str, Any]) -> str:
    values = {
        "ACTION": "COMPOSE_STOCKOUT_EMAIL",
        "ALERT_EVENT_ID": event["alert_event_id"],
        "ALERT_TYPE": ALERT_TYPE,
        "PRODUCT_ID": event.get("product_id") or "",
        "PRODUCT_NAME": event.get("product_name") or "",
        "WAREHOUSE_ID": event.get("warehouse_id") or "",
        "INVENTORY_STATUS": "OUT_OF_STOCK",
        "EVENT_TIME_UTC": iso_utc(event.get("source_event_timestamp")),
        "SOURCE_EVENT_ID": event.get("source_event_id") or "",
        "QUANTITY": "" if event.get("quantity") is None else str(event.get("quantity")),
        "SOURCE_SILVER_VERSION": str(event.get("source_silver_version") or ""),
    }
    return "\n".join(f"{key}={value}" for key, value in values.items())


def _flatten_response_strings(value: Any) -> list[str]:
    strings: list[str] = []
    if isinstance(value, str):
        strings.append(value)
    elif isinstance(value, dict):
        for item in value.values():
            strings.extend(_flatten_response_strings(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            strings.extend(_flatten_response_strings(item))
    return strings


def _parse_json_object(text: str) -> dict[str, Any] | None:
    candidate = str(text).strip()
    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s*```$", "", candidate).strip()
    try:
        parsed = json.loads(candidate)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(candidate[start:end + 1])
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _extract_draft(agent_response: Any) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    if isinstance(agent_response, dict):
        candidates.append(agent_response)
    for text in _flatten_response_strings(agent_response):
        parsed = _parse_json_object(text)
        if parsed is not None:
            candidates.append(parsed)
    matches = [item for item in candidates if item.get("status") == "DRAFT_READY"]
    if not matches:
        raise RuntimeError("No DRAFT_READY JSON object was found in the Agent response.")
    return matches[-1]


def _validate_draft(draft: dict[str, Any], expected_alert_event_id: str) -> dict[str, Any]:
    unexpected = sorted(set(draft.keys()) - ALLOWED_DRAFT_KEYS)
    if unexpected:
        raise RuntimeError("Agent draft contains unexpected fields: " + ", ".join(unexpected))
    if draft.get("status") != "DRAFT_READY":
        raise RuntimeError("Agent draft status is not DRAFT_READY.")
    if str(draft.get("alert_event_id")) != str(expected_alert_event_id):
        raise RuntimeError("Agent alert_event_id does not match the requested event.")

    title = draft.get("title")
    body = draft.get("body")
    if not isinstance(title, str) or not title.strip():
        raise RuntimeError("Agent title is missing.")
    title = title.strip()
    if "\n" in title or "\r" in title:
        raise RuntimeError("Agent title must be one line.")
    if REQUIRED_TITLE_PREFIX and not title.startswith(REQUIRED_TITLE_PREFIX):
        raise RuntimeError("Agent title does not use the governed prefix.")
    if len(title) > MAX_DRAFT_TITLE_CHARS:
        raise RuntimeError("Agent title exceeds MAX_DRAFT_TITLE_CHARS.")
    if not isinstance(body, str) or not body.strip():
        raise RuntimeError("Agent body is missing.")
    body = body.strip()
    if len(body) > MAX_DRAFT_BODY_CHARS:
        raise RuntimeError("Agent body exceeds MAX_DRAFT_BODY_CHARS.")

    return {
        "status": "DRAFT_READY",
        "alert_event_id": expected_alert_event_id,
        "title": title,
        "body": body,
        "sha256": hashlib.sha256(
            json.dumps(
                {"title": title, "body": body},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }


def _stored_draft(event: dict[str, Any]) -> dict[str, Any] | None:
    if event.get("draft_status") != "DRAFT_READY":
        return None
    if not event.get("draft_title") or not event.get("draft_body"):
        return None
    return _validate_draft(
        {
            "status": "DRAFT_READY",
            "alert_event_id": event["alert_event_id"],
            "title": event["draft_title"],
            "body": event["draft_body"],
        },
        event["alert_event_id"],
    )


def invoke_draft_agent(event: dict[str, Any]) -> dict[str, Any]:
    session_key = f"stockout-draft-{event['alert_event_id']}"
    input_text = build_agent_text(event)
    request_body = {
        "isStreamEnabled": False,
        "sessionKey": session_key,
        "trace": bool(AGENT_TRACE),
        "input": [
            {
                "role": "User",
                "content": [{"type": "INPUT_TEXT", "text": input_text}],
            }
        ],
    }

    try:
        auth = _load_oci_auth_context()
        response = requests.post(
            url=AGENT_CHAT_URL,
            auth=auth["requests_signer"],
            json=request_body,
            headers={"Content-Type": "application/json"},
            timeout=(10, AGENT_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RequestException as exc:
        return {
            "success": False,
            "retryable": True,
            "http_status": None,
            "result_code": "AGENT_REQUEST_EXCEPTION",
            "response": "",
            "opc_request_id": None,
            "session_key": session_key,
            "draft": None,
            "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
        }

    http_status = int(response.status_code)
    opc_request_id = response.headers.get("opc-request-id")
    try:
        response_value: Any = response.json()
    except Exception:
        response_value = response.text
    stored_response = (
        json.dumps(response_value, ensure_ascii=False, default=str)
        if not isinstance(response_value, str)
        else response_value
    )[:MAX_AGENT_RESPONSE_CHARS]

    if not 200 <= http_status < 300:
        retryable = http_status == 429 or http_status >= 500
        return {
            "success": False,
            "retryable": retryable,
            "http_status": http_status,
            "result_code": f"AGENT_HTTP_{http_status}",
            "response": stored_response,
            "opc_request_id": opc_request_id,
            "session_key": session_key,
            "draft": None,
            "error": stored_response[:1000] or f"Agent returned HTTP {http_status}.",
        }

    try:
        draft = _validate_draft(
            _extract_draft(response_value),
            event["alert_event_id"],
        )
    except Exception as exc:
        return {
            "success": False,
            "retryable": False,
            "http_status": http_status,
            "result_code": "AGENT_DRAFT_VALIDATION_FAILED",
            "response": stored_response,
            "opc_request_id": opc_request_id,
            "session_key": session_key,
            "draft": None,
            "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
        }

    return {
        "success": True,
        "retryable": False,
        "http_status": http_status,
        "result_code": "DRAFT_READY",
        "response": stored_response,
        "opc_request_id": opc_request_id,
        "session_key": session_key,
        "draft": draft,
        "error": None,
    }


def _read_function_response(response: Any) -> Any:
    stream = response.data
    if hasattr(stream, "raw") and hasattr(stream.raw, "read"):
        raw = stream.raw.read()
    elif hasattr(stream, "read"):
        raw = stream.read()
    else:
        raw = stream
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if isinstance(raw, dict):
        return raw
    return json.loads(str(raw))


def invoke_notification_function(
    event: dict[str, Any],
    draft: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "alert_event_id": event["alert_event_id"],
        "title": draft["title"],
        "body": draft["body"],
    }
    try:
        response = _notification_function_client().invoke_function(
            function_id=NOTIFICATION_FUNCTION_OCID,
            invoke_function_body=io.BytesIO(
                json.dumps(payload, ensure_ascii=False).encode("utf-8")
            ),
            fn_invoke_type="sync",
            fn_intent="httprequest",
            retry_strategy=oci.retry.NoneRetryStrategy(),
        )
        value = _read_function_response(response)
    except ServiceError as exc:
        if exc.status == 429:
            classification = "FAILED"
            certainty = "CONFIRMED_NOT_SENT"
            retryable = True
        elif exc.status is not None and 400 <= exc.status < 500:
            classification = "FAILED"
            certainty = "CONFIRMED_NOT_SENT"
            retryable = False
        else:
            classification = "UNKNOWN"
            certainty = "AMBIGUOUS"
            retryable = bool(UNKNOWN_RETRY_ENABLED)
        return {
            "classification": classification,
            "delivery_certainty": certainty,
            "retryable": retryable,
            "http_status": exc.status,
            "message_id": None,
            "result_code": exc.code or "FUNCTION_SERVICE_ERROR",
            "response": "",
            "error": exc.message,
            "opc_request_id": exc.request_id,
        }
    except (RequestException, TimeoutError, OSError) as exc:
        return {
            "classification": "UNKNOWN",
            "delivery_certainty": "AMBIGUOUS",
            "retryable": bool(UNKNOWN_RETRY_ENABLED),
            "http_status": None,
            "message_id": None,
            "result_code": "FUNCTION_REQUEST_EXCEPTION",
            "response": "",
            "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
            "opc_request_id": None,
        }
    except Exception as exc:
        return {
            "classification": "UNKNOWN",
            "delivery_certainty": "AMBIGUOUS",
            "retryable": bool(UNKNOWN_RETRY_ENABLED),
            "http_status": None,
            "message_id": None,
            "result_code": "FUNCTION_RESPONSE_EXCEPTION",
            "response": "",
            "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
            "opc_request_id": None,
        }

    stored_response = json.dumps(
        value,
        ensure_ascii=False,
        default=str,
    )[:MAX_FUNCTION_RESPONSE_CHARS]
    status = str(value.get("status") or "") if isinstance(value, dict) else ""
    certainty = str(value.get("delivery_certainty") or "") if isinstance(value, dict) else ""
    message_id = value.get("message_id") if isinstance(value, dict) else None
    opc_request_id = value.get("opc_request_id") if isinstance(value, dict) else None

    if status == "NOTIFICATION_SENT" and certainty == "CONFIRMED_SENT" and message_id:
        classification = "SENT"
        retryable = False
        error = None
    elif status == "VALIDATED_NO_PUBLISH":
        classification = "FAILED"
        certainty = "CONFIRMED_NOT_SENT"
        retryable = False
        error = "Notification Function publishing is disabled."
    elif status in {
        "NOTIFICATION_NOT_SENT",
        "NOTIFICATION_REJECTED",
        "CONFIGURATION_ERROR",
        "NOTIFICATION_FAILED",
    } and certainty != "AMBIGUOUS":
        classification = "FAILED"
        certainty = certainty or "CONFIRMED_NOT_SENT"
        retryable = status == "NOTIFICATION_NOT_SENT"
        error = str(value.get("message") or value.get("error_code") or status)[:1000]
    else:
        classification = "UNKNOWN"
        certainty = certainty or "AMBIGUOUS"
        retryable = bool(UNKNOWN_RETRY_ENABLED)
        error = str(value.get("message") or value.get("error_code") or status or "Unconfirmed Function result")[:1000]

    return {
        "classification": classification,
        "delivery_certainty": certainty,
        "retryable": retryable,
        "http_status": 200,
        "message_id": message_id,
        "result_code": status or "FUNCTION_RESULT_UNCONFIRMED",
        "response": stored_response,
        "error": error,
        "opc_request_id": opc_request_id,
    }


def get_event(alert_event_id: str) -> dict[str, Any] | None:
    rows = (
        spark.table(ALERT_EVENTS_TABLE)
        .filter(F.col("alert_event_id") == F.lit(alert_event_id))
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError(f"Duplicate alert event rows for {alert_event_id}")
    return row_dict(rows[0]) if rows else None


def get_state(alert_key: str) -> dict[str, Any] | None:
    rows = (
        spark.table(ALERT_STATE_TABLE)
        .filter(F.col("alert_key") == F.lit(alert_key))
        .filter(F.coalesce(F.col("record_type"), F.lit("ALERT")) == F.lit("ALERT"))
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError(f"Duplicate alert state rows for {alert_key}")
    return row_dict(rows[0]) if rows else None


def update_event(alert_event_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    current = get_event(alert_event_id)
    if current is None:
        raise RuntimeError(f"Alert event not found: {alert_event_id}")
    current.update(changes)
    current["updated_at"] = utc_now()
    merge_rows(
        [current],
        ALERT_EVENTS_TABLE,
        ["alert_event_id"],
        EVENT_SPEC,
        EVENT_SCHEMA,
        update_existing=True,
    )
    return current


def update_state_notification_summary(
    alert_key: str,
    *,
    status: str,
    attempts: int,
    message_id: str | None,
    next_retry_at: datetime | None,
    error: str | None,
    notified_at: datetime | None,
) -> None:
    state = get_state(alert_key)
    if state is None or state.get("alert_status") != "OPEN":
        return
    state.update({
        "notification_status": status,
        "notification_attempts": int(attempts),
        "notification_message_id": message_id,
        "next_retry_at": next_retry_at,
        "last_error": error,
        "last_notified_at": notified_at or state.get("last_notified_at"),
        "updated_at": utc_now(),
    })
    merge_rows(
        [state],
        ALERT_STATE_TABLE,
        ["alert_key"],
        STATE_SPEC,
        STATE_SCHEMA,
        update_existing=True,
    )


def retry_delay_minutes(attempt_number: int) -> int:
    index = min(max(int(attempt_number) - 1, 0), len(RETRY_DELAYS_MINUTES) - 1)
    return int(RETRY_DELAYS_MINUTES[index])


def recover_expired_dispatch_leases() -> int:
    now = utc_now()
    rows = (
        spark.table(ALERT_EVENTS_TABLE)
        .filter(F.col("notification_status") == F.lit("PROCESSING"))
        .filter(F.col("dispatch_lease_expires_at") < F.lit(now))
        .collect()
    )
    for row in rows:
        event = row.asDict(recursive=True)
        attempts = int(event.get("notification_attempts") or 0)
        stage = str(event.get("dispatch_stage") or "CLAIMED")
        ambiguous = stage == "FUNCTION_INVOKING"
        retryable = (
            bool(UNKNOWN_RETRY_ENABLED and attempts < MAX_NOTIFICATION_ATTEMPTS)
            if ambiguous
            else attempts < MAX_NOTIFICATION_ATTEMPTS
        )
        next_retry = (
            now + timedelta(minutes=retry_delay_minutes(attempts))
            if retryable else None
        )
        status = "UNKNOWN" if ambiguous else "FAILED"
        certainty = "AMBIGUOUS" if ambiguous else "CONFIRMED_NOT_SENT"
        error = (
            "Dispatch lease expired after Function invocation was marked; outcome is ambiguous."
            if ambiguous
            else "Dispatch lease expired before Function invocation; no email side effect was attempted."
        )
        update_event(event["alert_event_id"], {
            "notification_status": status,
            "retryable": retryable,
            "next_retry_at": next_retry,
            "dispatch_owner": None,
            "dispatch_stage": None,
            "dispatch_started_at": None,
            "dispatch_lease_expires_at": None,
            "last_error": error,
        })
        dispatch_id = make_dispatch_id(event["alert_event_id"], attempts)
        dispatch_rows = (
            spark.table(ALERT_DISPATCH_TABLE)
            .filter(F.col("dispatch_id") == F.lit(dispatch_id))
            .limit(1)
            .collect()
        )
        if dispatch_rows:
            dispatch = dispatch_rows[0].asDict(recursive=True)
            dispatch.update({
                "dispatch_status": status,
                "delivery_certainty": certainty,
                "completed_at": now,
                "error_message": error,
                "updated_at": now,
            })
            merge_rows(
                [dispatch],
                ALERT_DISPATCH_TABLE,
                ["dispatch_id"],
                DISPATCH_SPEC,
                DISPATCH_SCHEMA,
                update_existing=True,
            )
    if rows:
        print("Expired dispatch leases recovered:", len(rows))
    return len(rows)


def event_is_still_actionable(event: dict[str, Any]) -> bool:
    if event.get("transition_type") != "OPENED":
        return False
    state = get_state(event["alert_key"])
    if state is None or state.get("alert_status") != "OPEN":
        return False
    opened_alert_event_id = state.get("opened_alert_event_id")
    if opened_alert_event_id:
        return str(opened_alert_event_id) == str(event.get("alert_event_id"))
    state_opened = ensure_utc(state.get("opened_at"))
    event_time = ensure_utc(event.get("source_event_timestamp"))
    if state_opened and event_time and state_opened > event_time:
        return False
    return True


def claim_event_for_dispatch(event: dict[str, Any]) -> dict[str, Any] | None:
    current = get_event(event["alert_event_id"])
    if current is None:
        return None
    allowed_statuses = {"PENDING", "FAILED"}
    if UNKNOWN_RETRY_ENABLED:
        allowed_statuses.add("UNKNOWN")
    if current.get("notification_status") not in allowed_statuses:
        return None
    if not bool(current.get("retryable")):
        return None
    attempts = int(current.get("notification_attempts") or 0)
    if attempts >= MAX_NOTIFICATION_ATTEMPTS:
        return None
    next_retry_at = ensure_utc(current.get("next_retry_at"))
    if next_retry_at and next_retry_at > utc_now():
        return None

    owner = f"{WORKFLOW_RUN_ID or WORKFLOW_JOB_NAME}-{uuid.uuid4().hex}"
    started_at = utc_now()
    current.update({
        "notification_status": "PROCESSING",
        "notification_attempts": attempts + 1,
        "dispatch_owner": owner,
        "dispatch_stage": "CLAIMED",
        "dispatch_started_at": started_at,
        "dispatch_lease_expires_at": started_at + timedelta(minutes=PROCESSING_LEASE_MINUTES),
        "next_retry_at": None,
        "last_error": None,
        "updated_at": started_at,
    })
    merge_rows(
        [current],
        ALERT_EVENTS_TABLE,
        ["alert_event_id"],
        EVENT_SPEC,
        EVENT_SCHEMA,
        update_existing=True,
    )
    verified = get_event(event["alert_event_id"])
    if (
        verified
        and verified.get("dispatch_owner") == owner
        and verified.get("notification_status") == "PROCESSING"
    ):
        return verified
    return None


def _complete_without_function(
    claimed: dict[str, Any],
    dispatch: dict[str, Any],
    *,
    event_status: str,
    dispatch_status: str,
    error: str | None,
    retryable: bool,
    agent_result: dict[str, Any] | None,
) -> None:
    completed_at = utc_now()
    attempt_number = int(claimed["notification_attempts"])
    next_retry_at = (
        completed_at + timedelta(minutes=retry_delay_minutes(attempt_number))
        if retryable and attempt_number < MAX_NOTIFICATION_ATTEMPTS else None
    )
    event_changes = {
        "notification_status": event_status,
        "retryable": bool(next_retry_at is not None),
        "next_retry_at": next_retry_at,
        "dispatch_owner": None,
        "dispatch_stage": None,
        "dispatch_started_at": None,
        "dispatch_lease_expires_at": None,
        "last_error": error,
    }
    if agent_result is not None and not agent_result.get("success"):
        event_changes["draft_status"] = "DRAFT_FAILED"
    updated_event = update_event(claimed["alert_event_id"], event_changes)
    dispatch.update({
        "dispatch_stage": (
            "DRAFT_ONLY_COMPLETED"
            if dispatch_status == "DRAFT_ONLY"
            else "AGENT_DRAFT_FAILED"
        ),
        "dispatch_status": dispatch_status,
        "delivery_certainty": "CONFIRMED_NOT_SENT",
        "completed_at": completed_at,
        "http_status": (agent_result or {}).get("http_status"),
        "agent_http_status": (agent_result or {}).get("http_status"),
        "agent_result_code": (agent_result or {}).get("result_code"),
        "agent_response": (agent_result or {}).get("response"),
        "agent_opc_request_id": (agent_result or {}).get("opc_request_id"),
        "error_message": error,
        "updated_at": completed_at,
    })
    merge_rows(
        [dispatch],
        ALERT_DISPATCH_TABLE,
        ["dispatch_id"],
        DISPATCH_SPEC,
        DISPATCH_SCHEMA,
        update_existing=True,
    )
    update_state_notification_summary(
        updated_event["alert_key"],
        status=event_status,
        attempts=attempt_number,
        message_id=None,
        next_retry_at=next_retry_at,
        error=error,
        notified_at=None,
    )


def dispatch_pending_events() -> dict[str, int]:
    counters = {
        "selected": 0,
        "sent": 0,
        "draft_only": 0,
        "failed": 0,
        "unknown": 0,
        "suppressed": 0,
    }
    if DISPATCH_MODE == "OFF":
        return counters

    now = utc_now()
    statuses = ["PENDING", "FAILED"] + (["UNKNOWN"] if UNKNOWN_RETRY_ENABLED else [])
    due_rows = (
        spark.table(ALERT_EVENTS_TABLE)
        .filter(F.col("processing_status") == F.lit("APPLIED"))
        .filter(F.col("notification_required") == F.lit(True))
        .filter(F.col("notification_status").isin(statuses))
        .filter(F.col("retryable") == F.lit(True))
        .filter(F.col("notification_attempts") < F.lit(MAX_NOTIFICATION_ATTEMPTS))
        .filter(F.col("next_retry_at").isNull() | (F.col("next_retry_at") <= F.lit(now)))
        .orderBy(F.col("detected_at").asc(), F.col("alert_event_id").asc())
        .limit(MAX_DISPATCHES_PER_CYCLE)
        .collect()
    )

    for row in due_rows:
        event = row.asDict(recursive=True)
        counters["selected"] += 1
        if not event_is_still_actionable(event):
            update_event(event["alert_event_id"], {
                "notification_status": "SUPPRESSED",
                "retryable": False,
                "next_retry_at": None,
                "last_error": "Alert resolved or replaced before dispatch.",
            })
            counters["suppressed"] += 1
            continue

        claimed = claim_event_for_dispatch(event)
        if claimed is None:
            continue

        attempt_number = int(claimed["notification_attempts"])
        dispatch_id = make_dispatch_id(claimed["alert_event_id"], attempt_number)
        attempted_at = utc_now()
        dispatch = {
            "dispatch_id": dispatch_id,
            "alert_event_id": claimed["alert_event_id"],
            "alert_key": claimed["alert_key"],
            "attempt_number": attempt_number,
            "dispatch_stage": "CLAIMED",
            "agent_session_key": f"stockout-draft-{claimed['alert_event_id']}",
            "agent_endpoint": _endpoint_audit_value(AGENT_CHAT_URL),
            "function_endpoint": _endpoint_audit_value(
                NOTIFICATION_FUNCTION_INVOKE_ENDPOINT
            ),
            "dispatch_status": "STARTED",
            "delivery_certainty": "PENDING",
            "request_payload_json": json.dumps(
                {
                    "alert_event_id": claimed["alert_event_id"],
                    "source_event_id": claimed["source_event_id"],
                    "product_id": claimed.get("product_id"),
                    "warehouse_id": claimed.get("warehouse_id"),
                    "source_silver_version": claimed.get("source_silver_version"),
                    "dispatch_mode": DISPATCH_MODE,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "attempted_at": attempted_at,
            "completed_at": None,
            "http_status": None,
            "agent_http_status": None,
            "function_http_status": None,
            "notification_message_id": None,
            "agent_result_code": None,
            "function_result_code": None,
            "draft_title": None,
            "draft_body_sha256": None,
            "agent_response": None,
            "function_response": None,
            "error_message": None,
            "opc_request_id": None,
            "agent_opc_request_id": None,
            "function_opc_request_id": None,
            "updated_at": attempted_at,
        }
        merge_rows(
            [dispatch],
            ALERT_DISPATCH_TABLE,
            ["dispatch_id"],
            DISPATCH_SPEC,
            DISPATCH_SCHEMA,
            update_existing=False,
        )

        draft = _stored_draft(claimed)
        agent_result = None
        if draft is None:
            agent_result = invoke_draft_agent(claimed)
            if not agent_result["success"]:
                _complete_without_function(
                    claimed,
                    dispatch,
                    event_status="FAILED",
                    dispatch_status="FAILED",
                    error=agent_result["error"],
                    retryable=bool(agent_result["retryable"]),
                    agent_result=agent_result,
                )
                counters["failed"] += 1
                continue
            draft = agent_result["draft"]
            draft_at = utc_now()
            claimed = update_event(claimed["alert_event_id"], {
                "dispatch_stage": "DRAFT_READY",
                "draft_status": "DRAFT_READY",
                "draft_title": draft["title"],
                "draft_body": draft["body"],
                "draft_sha256": draft["sha256"],
                "draft_created_at": draft_at,
                "agent_session_key": agent_result["session_key"],
                "agent_http_status": agent_result["http_status"],
                "agent_opc_request_id": agent_result["opc_request_id"],
            })
            dispatch.update({
                "dispatch_stage": "DRAFT_READY",
                "agent_session_key": agent_result["session_key"],
                "agent_http_status": agent_result["http_status"],
                "agent_result_code": agent_result["result_code"],
                "draft_title": draft["title"],
                "draft_body_sha256": draft["sha256"],
                "agent_response": agent_result["response"],
                "agent_opc_request_id": agent_result["opc_request_id"],
                "updated_at": draft_at,
            })
            merge_rows(
                [dispatch],
                ALERT_DISPATCH_TABLE,
                ["dispatch_id"],
                DISPATCH_SPEC,
                DISPATCH_SCHEMA,
                update_existing=True,
            )
        else:
            dispatch.update({
                "dispatch_stage": "DRAFT_READY_REUSED",
                "draft_title": draft["title"],
                "draft_body_sha256": draft["sha256"],
                "agent_result_code": "STORED_DRAFT_REUSED",
                "updated_at": utc_now(),
            })
            merge_rows(
                [dispatch],
                ALERT_DISPATCH_TABLE,
                ["dispatch_id"],
                DISPATCH_SPEC,
                DISPATCH_SCHEMA,
                update_existing=True,
            )

        if DISPATCH_MODE == "DRAFT_ONLY":
            _complete_without_function(
                claimed,
                dispatch,
                event_status="DRAFT_ONLY",
                dispatch_status="DRAFT_ONLY",
                error=None,
                retryable=False,
                agent_result=agent_result,
            )
            counters["draft_only"] += 1
            continue

        function_started_at = utc_now()
        claimed = update_event(claimed["alert_event_id"], {
            "dispatch_stage": "FUNCTION_INVOKING",
            "function_status": "INVOKING",
            "function_http_status": None,
            "function_opc_request_id": None,
        })
        dispatch.update({
            "dispatch_stage": "FUNCTION_INVOKING",
            "function_result_code": "INVOKING",
            "updated_at": function_started_at,
        })
        merge_rows(
            [dispatch],
            ALERT_DISPATCH_TABLE,
            ["dispatch_id"],
            DISPATCH_SPEC,
            DISPATCH_SCHEMA,
            update_existing=True,
        )

        function_result = invoke_notification_function(claimed, draft)
        completed_at = utc_now()
        classification = function_result["classification"]
        attempts_exhausted = attempt_number >= MAX_NOTIFICATION_ATTEMPTS
        retryable = bool(function_result["retryable"] and not attempts_exhausted)
        next_retry_at = (
            completed_at + timedelta(minutes=retry_delay_minutes(attempt_number))
            if retryable else None
        )

        if classification == "SENT":
            event_status = "SENT"
            dispatch_status = "SUCCESS"
            counters["sent"] += 1
        elif classification == "FAILED":
            event_status = "FAILED"
            dispatch_status = "FAILED"
            counters["failed"] += 1
        else:
            event_status = "UNKNOWN"
            dispatch_status = "UNKNOWN"
            counters["unknown"] += 1

        updated_event = update_event(claimed["alert_event_id"], {
            "notification_status": event_status,
            "retryable": retryable,
            "next_retry_at": next_retry_at,
            "last_notification_at": completed_at if classification == "SENT" else None,
            "notification_message_id": function_result.get("message_id"),
            "dispatch_owner": None,
            "dispatch_stage": None,
            "dispatch_started_at": None,
            "dispatch_lease_expires_at": None,
            "function_status": function_result.get("result_code"),
            "function_http_status": function_result.get("http_status"),
            "function_opc_request_id": function_result.get("opc_request_id"),
            "last_error": function_result.get("error"),
        })

        dispatch.update({
            "dispatch_stage": "COMPLETED",
            "dispatch_status": dispatch_status,
            "delivery_certainty": function_result.get("delivery_certainty"),
            "completed_at": completed_at,
            "http_status": function_result.get("http_status"),
            "function_http_status": function_result.get("http_status"),
            "notification_message_id": function_result.get("message_id"),
            "function_result_code": function_result.get("result_code"),
            "function_response": function_result.get("response"),
            "error_message": function_result.get("error"),
            "opc_request_id": function_result.get("opc_request_id"),
            "function_opc_request_id": function_result.get("opc_request_id"),
            "updated_at": completed_at,
        })
        merge_rows(
            [dispatch],
            ALERT_DISPATCH_TABLE,
            ["dispatch_id"],
            DISPATCH_SPEC,
            DISPATCH_SCHEMA,
            update_existing=True,
        )
        update_state_notification_summary(
            updated_event["alert_key"],
            status=event_status,
            attempts=attempt_number,
            message_id=function_result.get("message_id"),
            next_retry_at=next_retry_at,
            error=function_result.get("error"),
            notified_at=completed_at if classification == "SENT" else None,
        )
        print(
            "Dispatch result:",
            "alert_event_id=", claimed["alert_event_id"],
            "attempt=", attempt_number,
            "status=", event_status,
            "function_status=", function_result.get("result_code"),
        )

    return counters


if DISPATCH_MODE != "OFF":
    _load_oci_auth_context()
print("Draft-only Agent and Notification Function dispatch helpers initialized.")

# %% [code cell 8]
# One governed detector cycle

def run_detector_cycle(batch_id: int) -> dict[str, Any]:
    cycle_started_at = utc_now()
    print("\n============================================================")
    print("Inventory alert cycle batch_id:", batch_id)
    print("Cycle started UTC            :", cycle_started_at.isoformat())

    recovered_prepared = recover_prepared_events()
    recovered_leases = recover_expired_dispatch_leases()

    silver_version = latest_delta_version(SILVER_TABLE)
    dq_run_id = find_successful_dq_run_for_silver_version(silver_version)
    detection_skipped = False
    transition_stats = {
        "prepared": 0,
        "opened": 0,
        "reobserved": 0,
        "resolved": 0,
        "suppressed": 0,
        "truncated": 0,
    }

    if REQUIRE_SUCCESSFUL_DQ_CYCLE and dq_run_id is None:
        detection_skipped = True
        print(
            "No successful Silver/DQ cycle yet contains Silver version",
            silver_version,
            "- detection deferred without changing state.",
        )
    else:
        dq_run_id = dq_run_id or ""
        pinned_silver = read_delta_version(SILVER_TABLE, silver_version)
        normalized = normalize_inventory_events(pinned_silver)
        bootstrap_if_needed(normalized, silver_version, dq_run_id)
        transition_stats = detect_new_transitions(
            normalized,
            silver_version,
            dq_run_id,
        )

    dispatch_stats = dispatch_pending_events()
    cycle_completed_at = utc_now()
    control = load_control()
    # Do not create the bootstrap control row merely because DQ is not ready.
    # bootstrap_if_needed() is the only path that establishes the first control
    # row, after historical events and baseline state have been safely handled.
    if control is not None:
        control.update({
            "pipeline_name": PIPELINE_NAME,
            "transformation_version": TRANSFORMATION_VERSION,
            "last_batch_id": int(batch_id),
            "last_cycle_started_at_utc": cycle_started_at.isoformat(),
            "last_cycle_completed_at_utc": cycle_completed_at.isoformat(),
            "last_source_silver_version": int(silver_version),
            "last_source_dq_run_id": dq_run_id,
            "last_detection_skipped": bool(detection_skipped),
            "last_transition_stats": transition_stats,
            "last_dispatch_stats": dispatch_stats,
            "last_recovered_prepared": int(recovered_prepared),
            "last_recovered_expired_leases": int(recovered_leases),
            "dispatch_mode": DISPATCH_MODE,
            "execution_mode": EXECUTION_MODE,
        })
        write_control(control)

    result = {
        "batch_id": int(batch_id),
        "silver_version": int(silver_version),
        "dq_run_id": dq_run_id,
        "detection_skipped": detection_skipped,
        "transition_stats": transition_stats,
        "dispatch_stats": dispatch_stats,
        "dispatch_mode": DISPATCH_MODE,
        "execution_mode": EXECUTION_MODE,
        "duration_seconds": (cycle_completed_at - cycle_started_at).total_seconds(),
    }
    print("Cycle result:", json.dumps(result, ensure_ascii=False, default=str))
    return result

# %% [code cell 9]
# Execute one cycle or start the dedicated rate-timer Structured Streaming Workflow
if EXECUTION_MODE == "RUN_ONCE":
    print("Running one governed inventory-alert cycle and exiting...")
    run_once_result = run_detector_cycle(0)
    print("INVENTORY_ALERT_RUN_ONCE_SUCCESS")
    print(json.dumps(run_once_result, ensure_ascii=False, default=str, indent=2))

else:
    existing_queries = [
        query for query in spark.streams.active if query.name == QUERY_NAME
    ]
    if existing_queries:
        raise RuntimeError(
            f"An active streaming query named {QUERY_NAME!r} already exists in this Spark session."
        )

    if RUN_INITIAL_CYCLE:
        print("Running initial inventory-alert cycle before timer stream start...")
        run_detector_cycle(-1)

    def process_timer_microbatch(timer_batch_df: DataFrame, batch_id: int) -> None:
        trigger_rows = int(timer_batch_df.count())
        print("Timer trigger rows:", trigger_rows)
        if trigger_rows == 0:
            return
        run_detector_cycle(batch_id)

    timer_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .option("numPartitions", 1)
        .load()
    )

    inventory_alert_query = (
        timer_stream.writeStream
        .queryName(QUERY_NAME)
        .outputMode("append")
        .foreachBatch(process_timer_microbatch)
        .option("checkpointLocation", TIMER_CHECKPOINT_LOCATION)
        .trigger(processingTime=POLL_INTERVAL)
        .start()
    )

    print("StreamCommerce inventory stockout alert Workflow started.")
    print("Query ID       :", inventory_alert_query.id)
    print("Run ID         :", inventory_alert_query.runId)
    print("Trigger source : Spark rate timer")
    print("Poll interval  :", POLL_INTERVAL)
    print("Checkpoint     :", TIMER_CHECKPOINT_LOCATION)
    print("Dispatch mode  :", DISPATCH_MODE)
    print("Waiting until the Workflow task is stopped or fails...")

    inventory_alert_query.awaitTermination()
