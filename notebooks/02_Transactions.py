# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC

# COMMAND ----------

# --------------------------------------------------
# Transaction Synchronization Notebook
# --------------------------------------------------

import importlib
from datetime import datetime, timezone

import src.auth
import src.transactions
import src.storage
import src.sync
import src.settings

# --------------------------------------------------
# Reload during development
# --------------------------------------------------

importlib.reload(src.settings)
importlib.reload(src.auth)
importlib.reload(src.transactions)
importlib.reload(src.storage)
importlib.reload(src.sync)

from pyspark.sql import SparkSession

from src.settings import (
    TARGET_SCHEMA,
    TRANSACTION_TABLES,
    INITIAL_TRANSACTION_ID,
    INITIAL_TRANSACTION_SITE_DATETIME
)

from src.auth import login
from src.transactions import get_transactions
from src.storage import save_transactions
from src.sync import (
    get_sync_state,
    update_sync_state,
    write_sync_history
)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = SparkSession.builder.getOrCreate()

print("=" * 70)
print("Glaser360 Transaction Synchronization")
print("=" * 70)
print(f"Target Schema : {TARGET_SCHEMA}")

# --------------------------------------------------
# Login
# --------------------------------------------------

token = login()

print("Authentication successful.")

# --------------------------------------------------
# Read Locations
# --------------------------------------------------

locations = (
    spark.table(f"{TARGET_SCHEMA}.location")
         .select("location_id", "name")
         .where("""
             location_id IN (
                865
             )
         """)
         .orderBy("location_id")
         .toLocalIterator()
)

# --------------------------------------------------
# Synchronize
# --------------------------------------------------

for location in locations:

    location_id = location.location_id
    location_name = location.name

    print()
    print("=" * 70)
    print(f"Location : {location_name} ({location_id})")
    print("=" * 70)

    # --------------------------------------------------
    # Process every table completely before moving on
    # --------------------------------------------------

    for table in TRANSACTION_TABLES:

        table_name = table["table"]

        print()
        print("-" * 70)
        print(f"Synchronizing table: {table_name}")
        print("-" * 70)

        # --------------------------------------------------
        # Read checkpoint ONCE
        # --------------------------------------------------

        state = get_sync_state(
            location_id=location_id,
            table_name=table_name
        )

        if state:

            last_transaction_id = (
                state.get("last_transaction_id")
                or INITIAL_TRANSACTION_ID
            )

            last_transaction_site_datetime = (
                state.get("last_transaction_site_datetime")
                or INITIAL_TRANSACTION_SITE_DATETIME
            )

        else:

            last_transaction_id = INITIAL_TRANSACTION_ID
            last_transaction_site_datetime = (
                INITIAL_TRANSACTION_SITE_DATETIME
            )

        print(
            f"Starting checkpoint -> "
            f"Transaction={last_transaction_id}, "
            f"Datetime={last_transaction_site_datetime}"
        )

        batch_number = 1
        total_records = 0

        # --------------------------------------------------
        # Keep downloading until API returns empty
        # --------------------------------------------------

        while True:

            sync_started_at = datetime.now(timezone.utc)

            try:

                print()
                print(f"Batch {batch_number}")

                response = get_transactions(
                    token=token,
                    table_name=table_name,
                    location_id=location_id,
                    last_transaction_id=last_transaction_id,
                    last_transaction_site_datetime=last_transaction_site_datetime
                )

                records = response.get("records", [])

                record_count = response.get(
                    "record_count",
                    len(records)
                )

                # ------------------------------------------
                # Finished with this table
                # ------------------------------------------

                if record_count == 0:

                    print(
                        f"No more records for {table_name} "
                        f"(Total synchronized: {total_records:,})"
                    )
                    break

                print(f"Retrieved {record_count:,} records")

                # ------------------------------------------
                # Save
                # ------------------------------------------

                save_transactions(
                    table_name=table_name,
                    records=records
                )

                total_records += record_count

                # ------------------------------------------
                # Advance checkpoint
                # ------------------------------------------

                last_transaction_id = response.get(
                    "last_transaction_id",
                    last_transaction_id
                )

                last_transaction_site_datetime = response.get(
                    "last_transaction_site_datetime",
                    last_transaction_site_datetime
                )

                sync_completed_at = datetime.now(timezone.utc)

                update_sync_state(

                    location_id=location_id,
                    location_name=location_name,
                    table_name=table_name,

                    last_transaction_id=last_transaction_id,
                    last_transaction_site_datetime=last_transaction_site_datetime,

                    records_received=record_count,

                    sync_started_at=sync_started_at,
                    sync_completed_at=sync_completed_at,

                    status="SUCCESS"

                )

                write_sync_history(

                    location_id=location_id,
                    location_name=location_name,
                    table_name=table_name,

                    last_transaction_id=last_transaction_id,
                    last_transaction_site_datetime=last_transaction_site_datetime,

                    records_received=record_count,

                    sync_started_at=sync_started_at,
                    sync_completed_at=sync_completed_at,

                    status="SUCCESS"

                )

                print(
                    f"✓ Batch {batch_number} complete "
                    f"({record_count:,} records)"
                )

                batch_number += 1

            except Exception as ex:

                sync_completed_at = datetime.now(timezone.utc)

                print("✗ Synchronization failed")
                print(str(ex))

                write_sync_history(

                    location_id=location_id,
                    location_name=location_name,
                    table_name=table_name,

                    last_transaction_id=last_transaction_id,
                    last_transaction_site_datetime=last_transaction_site_datetime,

                    records_received=0,

                    sync_started_at=sync_started_at,
                    sync_completed_at=sync_completed_at,

                    status="FAILED",
                    error_message=str(ex)

                )

                # Stop processing this table on failure
                break

        print(
            f"Finished {table_name}. "
            f"Total records synchronized: {total_records:,}"
        )

print()
print("=" * 70)
print("Transaction synchronization completed.")
print("=" * 70)
