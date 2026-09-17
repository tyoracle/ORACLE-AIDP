"""Source export generated from the sanitized AIDP notebook."""

# %% [code cell 1]
from typing import Any

def _workflow_parameter(name: str, default: str) -> str:
    """
    Read an AIDP Workflow parameter when oidlUtils is available.
    Fall back to the supplied default for interactive notebook runs.
    """
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
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


# Non-secret runtime configuration
KAFKA_BOOTSTRAP = _workflow_parameter(
    "KAFKA_BOOTSTRAP",
    "kafka.example.invalid:9092",
)

KAFKA_TOPICS_CSV = _workflow_parameter(
    "KAFKA_TOPICS",
    (
        "customer_events,orders,payments,inventory_updates,"
        "product_catalog_updates,shipment_events"
    ),
)

KAFKA_CREDENTIAL_NAME = _workflow_parameter(
    "KAFKA_CREDENTIAL_NAME",
    "kafka",
)

BRONZE_TABLE = _workflow_parameter(
    "BRONZE_TABLE",
    "default.default.streamcommerce_bronze_events",
)

VOLUME_FQN = _workflow_parameter(
    "VOLUME_FQN",
    "default.default.streamcommerce_vol",
)

CHECKPOINT_LOCATION = _workflow_parameter(
    "CHECKPOINT_LOCATION",
    (
        "/Volumes/default/default/streamcommerce_vol/"
        "checkpoints/streamcommerce_bronze_v1"
    ),
)

QUERY_NAME = _workflow_parameter(
    "QUERY_NAME",
    "streamcommerce_kafka_to_bronze",
)

TRIGGER_INTERVAL = _workflow_parameter(
    "TRIGGER_INTERVAL",
    "30 seconds",
)

STARTING_OFFSETS = _workflow_parameter(
    "STARTING_OFFSETS",
    "earliest",
)

MAX_OFFSETS_PER_TRIGGER = _workflow_parameter(
    "MAX_OFFSETS_PER_TRIGGER",
    "10000",
)

AUTO_CREATE_OBJECTS = _as_bool(
    _workflow_parameter("AUTO_CREATE_OBJECTS", "false")
)

DISABLE_TLS_HOSTNAME_VERIFICATION = _as_bool(
    _workflow_parameter(
        "DISABLE_TLS_HOSTNAME_VERIFICATION",
        "false",
    )
)

TOPICS = [
    topic.strip()
    for topic in KAFKA_TOPICS_CSV.split(",")
    if topic.strip()
]

if not TOPICS:
    raise ValueError("KAFKA_TOPICS resolved to an empty list.")

if STARTING_OFFSETS not in {"earliest", "latest"}:
    raise ValueError(
        "STARTING_OFFSETS must be either 'earliest' or 'latest'."
    )

try:
    int(MAX_OFFSETS_PER_TRIGGER)
except ValueError as exc:
    raise ValueError(
        "MAX_OFFSETS_PER_TRIGGER must be an integer represented as text."
    ) from exc

spark.conf.set("spark.sql.session.timeZone", "UTC")

print("Runtime configuration loaded.")
print("Kafka bootstrap       :", KAFKA_BOOTSTRAP)
print("Kafka topics          :", ", ".join(TOPICS))
print("Credential name       :", KAFKA_CREDENTIAL_NAME)
print("Bronze table          :", BRONZE_TABLE)
print("Checkpoint location   :", CHECKPOINT_LOCATION)
print("Query name            :", QUERY_NAME)
print("Trigger interval      :", TRIGGER_INTERVAL)
print("Starting offsets      :", STARTING_OFFSETS)
print("Max offsets/trigger   :", MAX_OFFSETS_PER_TRIGGER)
print("Auto-create objects   :", AUTO_CREATE_OBJECTS)
print(
    "TLS hostname verify   :",
    "DISABLED" if DISABLE_TLS_HOSTNAME_VERIFICATION else "ENABLED",
)

# %% [code cell 2]
_aidputils = globals().get("aidputils")

if _aidputils is None:
    raise RuntimeError(
        "The AIDP Credential Store utility is not available in this "
        "notebook execution context. Do not run 'import aidputils'. "
        "Run this file as an AIDP Notebook task on a runtime that "
        "supports Credential Store resolution."
    )

KAFKA_USERNAME = _aidputils.secrets.get(
    name=KAFKA_CREDENTIAL_NAME,
    key="username",
)

KAFKA_PASSWORD = _aidputils.secrets.get(
    name=KAFKA_CREDENTIAL_NAME,
    key="password",
)

if KAFKA_USERNAME is None or KAFKA_PASSWORD is None:
    raise RuntimeError(
        "Credential Store returned an empty Kafka username or password."
    )

KAFKA_USERNAME = str(KAFKA_USERNAME).strip()
KAFKA_PASSWORD = str(KAFKA_PASSWORD)

if not KAFKA_USERNAME or not KAFKA_PASSWORD:
    raise RuntimeError(
        "Credential Store returned a blank Kafka username or password."
    )

print("Kafka SASL credentials resolved from AIDP Credential Store.")
print("Kafka username        : [hidden]")
print("Kafka password        : [hidden]")

# %% [code cell 3]

import os

EXPECTED_COLUMNS = [
    "source_topic",
    "kafka_partition",
    "kafka_offset",
    "message_key",
    "raw_json",
    "kafka_timestamp",
    "kafka_timestamp_type",
    "ingestion_timestamp",
    "ingest_date",
]

volume_parts = VOLUME_FQN.split(".")

if len(volume_parts) != 3:
    raise ValueError(
        "VOLUME_FQN must have the form catalog.schema.volume; "
        f"received {VOLUME_FQN!r}."
    )

VOLUME_PATH = (
    f"/Volumes/{volume_parts[0]}/"
    f"{volume_parts[1]}/{volume_parts[2]}"
)

# AIDP exposes managed volumes through the /Volumes FUSE path.
# Create the Volume once in Master Catalog; do not create it on every job run.
if not os.path.isdir(VOLUME_PATH):
    raise RuntimeError(
        f"Managed Volume path is not available: {VOLUME_PATH}. "
        "Create a Managed Volume in Master Catalog under "
        f"{volume_parts[0]}.{volume_parts[1]} with the name "
        f"{volume_parts[2]!r}, then restart the Workflow."
    )

if not CHECKPOINT_LOCATION.startswith(VOLUME_PATH.rstrip("/") + "/"):
    raise RuntimeError(
        "CHECKPOINT_LOCATION must be a child of the configured managed "
        f"Volume path {VOLUME_PATH}; received {CHECKPOINT_LOCATION!r}."
    )

if AUTO_CREATE_OBJECTS:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
        (
            source_topic           STRING,
            kafka_partition        INT,
            kafka_offset           BIGINT,
            message_key            STRING,
            raw_json               STRING,
            kafka_timestamp        TIMESTAMP,
            kafka_timestamp_type   INT,
            ingestion_timestamp    TIMESTAMP,
            ingest_date            DATE
        )
        USING DELTA
        PARTITIONED BY (ingest_date)
    """)

try:
    existing_columns = spark.table(BRONZE_TABLE).columns
except Exception as exc:
    raise RuntimeError(
        f"Bronze table {BRONZE_TABLE} does not exist or is inaccessible. "
        "Create it in a one-time setup step, or set "
        "AUTO_CREATE_OBJECTS=true so this Workflow creates only the table."
    ) from exc

if existing_columns != EXPECTED_COLUMNS:
    raise RuntimeError(
        "The existing Bronze table schema does not match this notebook.\n"
        f"Expected: {EXPECTED_COLUMNS}\n"
        f"Actual  : {existing_columns}"
    )

print("Managed Volume path verified:", VOLUME_PATH)
print("Bronze table verified       :", BRONZE_TABLE)
print("Checkpoint location         :", CHECKPOINT_LOCATION)

# %% [code cell 4]
import socket

kafka_host, kafka_port_text = KAFKA_BOOTSTRAP.rsplit(":", 1)
kafka_port = int(kafka_port_text)

resolved_addresses = sorted({
    item[4][0]
    for item in socket.getaddrinfo(
        kafka_host,
        kafka_port,
        type=socket.SOCK_STREAM,
    )
})

if not resolved_addresses:
    raise RuntimeError(
        f"Kafka bootstrap hostname did not resolve: {kafka_host}"
    )

with socket.create_connection(
    (kafka_host, kafka_port),
    timeout=10,
):
    pass

print("Kafka hostname resolved:", resolved_addresses)
print(f"Kafka TCP connection succeeded: {kafka_host}:{kafka_port}")

# %% [code cell 5]
from pyspark.sql import functions as F

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "subscribe": ",".join(TOPICS),
    "startingOffsets": STARTING_OFFSETS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "failOnDataLoss": "false",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.scram."
        "ScramLoginModule required "
        f'username="{KAFKA_USERNAME}" '
        f'password="{KAFKA_PASSWORD}";'
    ),
    "maxOffsetsPerTrigger": MAX_OFFSETS_PER_TRIGGER,
}

if DISABLE_TLS_HOSTNAME_VERIFICATION:
    kafka_options[
        "kafka.ssl.endpoint.identification.algorithm"
    ] = ""

raw_kafka_stream = (
    spark.readStream
         .format("kafka")
         .options(**kafka_options)
         .load()
)

bronze_stream = (
    raw_kafka_stream
    .select(
        F.col("topic").alias("source_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").cast("int").alias(
            "kafka_timestamp_type"
        ),
        F.current_timestamp().alias("ingestion_timestamp"),
    )
    .withColumn(
        "ingest_date",
        F.to_date("ingestion_timestamp"),
    )
)

print("Kafka source and Bronze transformation initialized.")
print("Secret Kafka options are not printed.")

# %% [code cell 6]
existing_queries = [
    query
    for query in spark.streams.active
    if query.name == QUERY_NAME
]

if existing_queries:
    raise RuntimeError(
        f"An active streaming query named {QUERY_NAME!r} already exists "
        "in this Spark session. Stop the existing query before starting "
        "another run."
    )

print("Starting Kafka -> Bronze streaming query...")

bronze_query = (
    bronze_stream.writeStream
    .queryName(QUERY_NAME)
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .trigger(
        processingTime=TRIGGER_INTERVAL,
    )
    .toTable(BRONZE_TABLE)
)

print("Streaming query started.")
print("Query ID              :", bronze_query.id)
print("Run ID                :", bronze_query.runId)
print("Query name            :", bronze_query.name)
print("Checkpoint            :", CHECKPOINT_LOCATION)
print("Bronze table          :", BRONZE_TABLE)
print("Waiting until the Workflow task is stopped or the query fails...")

# Expected behavior for an AIDP Workflow Streaming task:
# this call blocks for the lifetime of the streaming job.
bronze_query.awaitTermination()
