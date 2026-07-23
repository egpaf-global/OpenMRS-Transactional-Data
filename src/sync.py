"""
Synchronization utilities.

Maintains:
    control.sync_state
    control.sync_history
"""

import uuid
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()


def create_control_schema():

    spark.sql("""

        CREATE SCHEMA IF NOT EXISTS control

    """)

    print("Control schema verified.")


def create_sync_state_table():

    spark.sql("""

        CREATE TABLE IF NOT EXISTS control.sync_state
        (

            location_id INT,

            location_name STRING,

            table_name STRING,

            last_transaction_id BIGINT,

            last_transaction_datetime TIMESTAMP,

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

    print("sync_state verified.")

def create_sync_history_table():

    spark.sql("""

        CREATE TABLE IF NOT EXISTS control.sync_history
        (

            run_id STRING,

            location_id INT,

            location_name STRING,

            table_name STRING,

            last_transaction_id BIGINT,

            last_transaction_datetime TIMESTAMP,

            sync_started_at TIMESTAMP,

            sync_completed_at TIMESTAMP,

            duration_seconds DOUBLE,

            records_received BIGINT,

            status STRING,

            error_message STRING

        )

        USING DELTA

    """)

    print("sync_history verified.")

def create_sync_tables():

    create_control_schema()

    create_sync_state_table()

    create_sync_history_table()

def get_sync_state(location_id, table_name):

    df = spark.sql(f"""

        SELECT *

        FROM control.sync_state

        WHERE location_id = {location_id}

        AND table_name = '{table_name}'

    """)

    rows = df.collect()

    if len(rows) == 0:

        return None

    return rows[0].asDict()

def update_sync_state(
        location_id,
        location_name,
        table_name,
        last_transaction_id,
        last_transaction_datetime,
        records_received,
        sync_started_at,
        sync_completed_at,
        status,
        error_message=None):

    now = datetime.utcnow()

    df = spark.createDataFrame([

        Row(

            location_id=location_id,

            location_name=location_name,

            table_name=table_name,

            last_transaction_id=last_transaction_id,

            last_transaction_datetime=last_transaction_datetime,

            last_sync_started_at=sync_started_at,

            last_sync_completed_at=sync_completed_at,

            records_received=records_received,

            status=status,

            error_message=error_message,

            created_at=now,

            updated_at=now

        )

    ])

    delta = DeltaTable.forName(
        spark,
        "control.sync_state"
    )

    (
        delta.alias("target")

        .merge(
            df.alias("source"),
            """
            target.location_id = source.location_id
            AND target.table_name = source.table_name
            """
        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()
    )

def write_sync_history(
        location_id,
        location_name,
        table_name,
        last_transaction_id,
        last_transaction_datetime,
        records_received,
        sync_started_at,
        sync_completed_at,
        status,
        error_message=None):

    duration = (
        sync_completed_at - sync_started_at
    ).total_seconds()

    df = spark.createDataFrame([

        {

            "run_id": str(uuid.uuid4()),

            "location_id": location_id,

            "location_name": location_name,

            "table_name": table_name,

            "last_transaction_id": last_transaction_id,

            "last_transaction_datetime": last_transaction_datetime,

            "sync_started_at": sync_started_at,

            "sync_completed_at": sync_completed_at,

            "duration_seconds": duration,

            "records_received": records_received,

            "status": status,

            "error_message": error_message

        }

    ])

    (
        df.write
          .mode("append")
          .format("delta")
          .saveAsTable("control.sync_history")
    )