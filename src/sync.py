"""
Synchronization utilities.

Maintains:

    - sync_state
    - sync_history

Spark session handling:

    - Databricks provides the Spark session.
    - This module does NOT create a new Spark session.
    - The notebook must call configure_spark(spark).
"""

import uuid
from datetime import datetime

from delta.tables import DeltaTable

from pyspark.sql.functions import col

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
)

from src.settings import (
    SYNC_STATE_TABLE,
    SYNC_HISTORY_TABLE,
    TARGET_SCHEMA,
)


# ============================================================
# Spark Session
# ============================================================

spark = None


def configure_spark(session):
    """
    Configure this module to use the Spark session supplied
    by the Databricks notebook.
    """

    global spark

    if session is None:
        raise ValueError(
            "A valid Spark session must be supplied."
        )

    spark = session

    print(
        "✓ sync.py configured with Databricks Spark session"
    )


def _require_spark():
    """
    Ensure the module has been configured with a Spark session.
    """

    if spark is None:
        raise RuntimeError(
            "Spark session has not been configured. "
            "Call src.sync.configure_spark(spark) "
            "from the Databricks notebook first."
        )

    return spark


# ============================================================
# Create Tables
# ============================================================

def create_sync_state_table():

    session = _require_spark()

    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {SYNC_STATE_TABLE}
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
        """
    )

    print(
        f"✓ {SYNC_STATE_TABLE} verified."
    )


def create_sync_history_table():

    session = _require_spark()

    session.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {SYNC_HISTORY_TABLE}
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
        """
    )

    print(
        f"✓ {SYNC_HISTORY_TABLE} verified."
    )


def create_sync_tables():

    print()
    print(
        "Creating/verifying synchronization tables..."
    )

    create_sync_state_table()
    create_sync_history_table()

    print(
        "✓ Synchronization tables ready."
    )


# ============================================================
# Read Checkpoint
# ============================================================

def get_sync_state(
    location_id,
    table_name,
):

    session = _require_spark()

    rows = (
        session.table(
            SYNC_STATE_TABLE
        )
        .filter(
            (col("location_id") == location_id)
            &
            (col("table_name") == table_name)
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
    error_message=None,
):

    session = _require_spark()

    now = datetime.utcnow()

    schema = StructType(
        [
            StructField(
                "location_id",
                IntegerType(),
                False,
            ),

            StructField(
                "location_name",
                StringType(),
                True,
            ),

            StructField(
                "table_name",
                StringType(),
                False,
            ),

            StructField(
                "last_transaction_id",
                LongType(),
                True,
            ),

            StructField(
                "last_transaction_site_datetime",
                TimestampType(),
                True,
            ),

            StructField(
                "last_sync_started_at",
                TimestampType(),
                True,
            ),

            StructField(
                "last_sync_completed_at",
                TimestampType(),
                True,
            ),

            StructField(
                "records_received",
                LongType(),
                True,
            ),

            StructField(
                "status",
                StringType(),
                True,
            ),

            StructField(
                "error_message",
                StringType(),
                True,
            ),

            StructField(
                "created_at",
                TimestampType(),
                True,
            ),

            StructField(
                "updated_at",
                TimestampType(),
                True,
            ),
        ]
    )

    source = session.createDataFrame(
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

    delta = DeltaTable.forName(
        session,
        SYNC_STATE_TABLE,
    )

    (
        delta.alias("target")
        .merge(
            source.alias("source"),
            """
            target.location_id =
                source.location_id
            AND
            target.table_name =
                source.table_name
            """,
        )
        .whenMatchedUpdate(
            set={
                "location_name":
                    "source.location_name",

                "last_transaction_id":
                    "source.last_transaction_id",

                "last_transaction_site_datetime":
                    "source.last_transaction_site_datetime",

                "last_sync_started_at":
                    "source.last_sync_started_at",

                "last_sync_completed_at":
                    "source.last_sync_completed_at",

                "records_received":
                    "source.records_received",

                "status":
                    "source.status",

                "error_message":
                    "source.error_message",

                "updated_at":
                    "source.updated_at",
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
        StructField(
            "last_transaction_site_datetime",
            TimestampType(),
            True
        ),
        StructField(
            "sync_started_at",
            TimestampType(),
            True
        ),
        StructField(
            "sync_completed_at",
            TimestampType(),
            True
        ),
        StructField(
            "duration_seconds",
            DoubleType(),
            True
        ),
        StructField(
            "records_received",
            LongType(),
            True
        ),
        StructField(
            "status",
            StringType(),
            True
        ),
        StructField(
            "error_message",
            StringType(),
            True
        ),
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
                duration,
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

    print(
        f"✓ Sync history written: "
        f"{table_name} | "
        f"{records_received:,} records | "
        f"{duration:.2f}s | "
        f"Status={status}"
    )


# ============================================================
# Development Reset
# ============================================================

def reset_sync_tables():
    """
    DEVELOPMENT / TESTING ONLY.

    Drops transaction and synchronization tables
    and recreates the synchronization tables.

    DO NOT call this during a production synchronization.
    """

    session = _require_spark()

    tables_to_drop = [
        f"{TARGET_SCHEMA}.patient",
        f"{TARGET_SCHEMA}.encounter",
        f"{TARGET_SCHEMA}.patient_program",
        f"{TARGET_SCHEMA}.order",
        f"{TARGET_SCHEMA}.drug_order",
        f"{TARGET_SCHEMA}.observation",
        f"{TARGET_SCHEMA}.patient_state",
        SYNC_STATE_TABLE,
        SYNC_HISTORY_TABLE,
    ]

    print()
    print(
        "=" * 70
    )
    print(
        "RESETTING TRANSACTION SYNCHRONIZATION TABLES"
    )
    print(
        "=" * 70
    )

    for table in tables_to_drop:

        print(
            f"Dropping {table}..."
        )

        session.sql(
            f"DROP TABLE IF EXISTS {table}"
        )

        print(
            f"✓ Dropped {table}"
        )

    print()
    print(
        "All transaction and synchronization "
        "tables dropped."
    )

    create_sync_tables()

    print(
        "✓ Synchronization tables recreated."
    )


# ============================================================
# Metadata Reset
# ============================================================

def reset_metadata_tables():
    """
    DEVELOPMENT / TESTING ONLY.

    Drops metadata Delta tables and removes their
    corresponding synchronization entries.

    Does NOT affect transaction tables.
    """

    session = _require_spark()

    metadata_types = [
        "encounter_type",
        "order_type",
        "program",
        "program_workflow",
        "relationship_type",
        "drug",
        "program_workflow_state",
        "location",
        "concept_name",
        "arv_drug",
    ]

    print()
    print(
        "=" * 70
    )
    print(
        "RESETTING METADATA TABLES"
    )
    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # Drop metadata tables
    # --------------------------------------------------------

    for meta_table in metadata_types:

        full_table_name = (
            f"{TARGET_SCHEMA}.{meta_table}"
        )

        print(
            f"Dropping {full_table_name}..."
        )

        session.sql(
            f"DROP TABLE IF EXISTS "
            f"{full_table_name}"
        )

        print(
            f"✓ Dropped {full_table_name}"
        )

    # --------------------------------------------------------
    # Reset sync state
    # --------------------------------------------------------

    if session.catalog.tableExists(
        SYNC_STATE_TABLE
    ):

        print()
        print(
            f"Cleaning checkpoint state "
            f"in {SYNC_STATE_TABLE}..."
        )

        quoted_tables = ", ".join(
            [
                f"'{table}'"
                for table in metadata_types
            ]
        )

        session.sql(
            f"""
            DELETE FROM {SYNC_STATE_TABLE}
            WHERE table_name IN (
                {quoted_tables}
            )
            """
        )

        print(
            "✓ Checkpoint states cleared."
        )

    # --------------------------------------------------------
    # Reset sync history
    # --------------------------------------------------------

    if session.catalog.tableExists(
        SYNC_HISTORY_TABLE
    ):

        print(
            f"Cleaning history state "
            f"in {SYNC_HISTORY_TABLE}..."
        )

        quoted_tables = ", ".join(
            [
                f"'{table}'"
                for table in metadata_types
            ]
        )

        session.sql(
            f"""
            DELETE FROM {SYNC_HISTORY_TABLE}
            WHERE table_name IN (
                {quoted_tables}
            )
            """
        )

        print(
            "✓ Sync history cleared."
        )

    print()
    print(
        "✓ Metadata reset completed successfully."
    )