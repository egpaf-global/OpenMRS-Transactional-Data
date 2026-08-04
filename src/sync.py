"""
Synchronization utilities.

Maintains:
    - sync_state
    - sync_history
"""

import uuid
from datetime import datetime

from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

from src.settings import (
    SYNC_STATE_TABLE,
    SYNC_HISTORY_TABLE,
    TARGET_SCHEMA
)

spark = SparkSession.builder.getOrCreate()


# ============================================================
# Create Tables
# ============================================================

def create_sync_state_table():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SYNC_STATE_TABLE}
        (
            location_id INT,
            location_name STRING,
            table_name STRING,

            last_transaction_id BIGINT,
            last_transaction_site_datetime TIMESTAMP,

            last_sync_started_at TIMESTAMP,
            last_sync_completed_at TIMESTAMP,

            records_received BIGINT,

            status STRING,
            error_message STRING,

            created_at TIMESTAMP,
            updated_at TIMESTAMP

        )
        USING DELTA
    """)

    print(f"{SYNC_STATE_TABLE} verified.")


def create_sync_history_table():

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SYNC_HISTORY_TABLE}
        (
            run_id STRING,

            location_id INT,
            location_name STRING,
            table_name STRING,

            last_transaction_id BIGINT,
            last_transaction_site_datetime TIMESTAMP,

            sync_started_at TIMESTAMP,
            sync_completed_at TIMESTAMP,

            duration_seconds DOUBLE,

            records_received BIGINT,

            status STRING,
            error_message STRING

        )
        USING DELTA
    """)

    print(f"{SYNC_HISTORY_TABLE} verified.")


def create_sync_tables():

    create_sync_state_table()
    create_sync_history_table()


# ============================================================
# Read Checkpoint
# ============================================================

def get_sync_state(location_id, table_name):

    rows = (
        spark.table(SYNC_STATE_TABLE)
        .filter(
            (col("location_id") == location_id)
            & (col("table_name") == table_name)
        )
        .limit(1)
        .collect()
    )

    if not rows:
        return None

    return rows[0].asDict()


# ============================================================
# Update Checkpoint
# ============================================================

def update_sync_state(
    location_id,
    location_name,
    table_name,
    last_transaction_id,
    last_transaction_site_datetime,
    records_received,
    sync_started_at,
    sync_completed_at,
    status,
    error_message=None
):

    now = datetime.utcnow()

    schema = StructType([
        StructField("location_id", IntegerType(), False),
        StructField("location_name", StringType(), True),
        StructField("table_name", StringType(), False),
        StructField("last_transaction_id", LongType(), True),
        StructField("last_transaction_site_datetime", TimestampType(), True),
        StructField("last_sync_started_at", TimestampType(), True),
        StructField("last_sync_completed_at", TimestampType(), True),
        StructField("records_received", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    source = spark.createDataFrame(
        [
            (
                location_id,
                location_name,
                table_name,
                last_transaction_id,
                last_transaction_site_datetime,
                sync_started_at,
                sync_completed_at,
                records_received,
                status,
                error_message,
                now,
                now,
            )
        ],
        schema=schema,
    )

    delta = DeltaTable.forName(spark, SYNC_STATE_TABLE)

    (
        delta.alias("target")
        .merge(
            source.alias("source"),
            """
            target.location_id = source.location_id
            AND target.table_name = source.table_name
            """,
        )
        .whenMatchedUpdate(
            set={
                "location_name": "source.location_name",
                "last_transaction_id": "source.last_transaction_id",
                "last_transaction_site_datetime": "source.last_transaction_site_datetime",
                "last_sync_started_at": "source.last_sync_started_at",
                "last_sync_completed_at": "source.last_sync_completed_at",
                "records_received": "source.records_received",
                "status": "source.status",
                "error_message": "source.error_message",
                "updated_at": "source.updated_at",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


# ============================================================
# History
# ============================================================

def write_sync_history(
    location_id,
    location_name,
    table_name,
    last_transaction_id,
    last_transaction_site_datetime,
    records_received,
    sync_started_at,
    sync_completed_at,
    status,
    error_message=None,
):

    duration = (
        sync_completed_at - sync_started_at
    ).total_seconds()

    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("location_id", IntegerType(), False),
        StructField("location_name", StringType(), True),
        StructField("table_name", StringType(), False),
        StructField("last_transaction_id", LongType(), True),
        StructField("last_transaction_site_datetime", TimestampType(), True),
        StructField("sync_started_at", TimestampType(), True),
        StructField("sync_completed_at", TimestampType(), True),
        StructField("duration_seconds", StringType(), True),
        StructField("records_received", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
    ])

    df = spark.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                location_id,
                location_name,
                table_name,
                last_transaction_id,
                last_transaction_site_datetime,
                sync_started_at,
                sync_completed_at,
                str(duration),
                records_received,
                status,
                error_message,
            )
        ],
        schema=schema,
    )

    (
        df.write
        .mode("append")
        .format("delta")
        .saveAsTable(SYNC_HISTORY_TABLE)
    )

def reset_sync_tables():
    """
    Drops all transaction and synchronization tables, then recreates
    the synchronization metadata tables.

    Intended for development and testing only.
    """

    tables_to_drop = [
        f"{TARGET_SCHEMA}.patient",
        f"{TARGET_SCHEMA}.encounter",
        f"{TARGET_SCHEMA}.patient_program",
        f"{TARGET_SCHEMA}.order",
        f"{TARGET_SCHEMA}.drug_order",
        f"{TARGET_SCHEMA}.observation",
        SYNC_STATE_TABLE,
        SYNC_HISTORY_TABLE,
    ]

    for table in tables_to_drop:
        print(f"Dropping {table}...")
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"✓ Dropped {table}")

    print("All transaction and synchronization tables dropped.")

    create_sync_tables()

    print("Synchronization tables recreated successfully.")