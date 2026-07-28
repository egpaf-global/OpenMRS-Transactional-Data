# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# --------------------------------------------------
# Imports
# --------------------------------------------------

import importlib
from datetime import datetime, timezone

import src.auth
import src.transactions
import src.storage
import src.sync
import src.settings

importlib.reload(src.auth)
importlib.reload(src.transactions)
importlib.reload(src.storage)
importlib.reload(src.sync)
importlib.reload(src.settings)

from pyspark.sql import SparkSession

from src.auth import login
from src.transactions import get_transactions
from src.storage import save_transactions
from src.sync import (
    get_sync_state,
    update_sync_state,
    write_sync_history
)

from src.settings import (
    CATALOG,
    SCHEMA
)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = SparkSession.builder.getOrCreate()

print(f"Target: {CATALOG}.{SCHEMA}")

# --------------------------------------------------
# Login
# --------------------------------------------------

token = login()

print("Authentication successful.")

# --------------------------------------------------
# Read Locations
# --------------------------------------------------

locations = (
    spark.table(f"{CATALOG}.{SCHEMA}.location")
         .select("location_id", "name")
         .toLocalIterator()
)

# --------------------------------------------------
# Transaction Tables
# --------------------------------------------------

transaction_tables = [
    "patient",
    "encounter",
    "obs",
    "patient_program",
    "orders",
    "drug_order"
]

# --------------------------------------------------
# Synchronize
# --------------------------------------------------

for location in locations:

    location_id = location.location_id
    location_name = location.name

    print("\n===================================================")
    print(f"Location : {location_name} ({location_id})")
    print("===================================================")

    for table_name in transaction_tables:

        print(f"\nSynchronizing {table_name}...")

        start_time = datetime.now(timezone.utc)

        try:

            # ------------------------------------------
            # Read last checkpoint
            # ------------------------------------------

            state = get_sync_state(
                location_id=location_id,
                table_name=table_name
            )

            if state is None:
                last_transaction_id = 0
                last_transaction_datetime = None
            else:
                last_transaction_id = state["last_transaction_id"]
                last_transaction_datetime = state["last_transaction_datetime"]

            # ------------------------------------------
            # Download Transactions
            # ------------------------------------------

            response = get_transactions(
                token=token,
                table_name=table_name,
                location_id=location_id,
                last_transaction_id=last_transaction_id,
                last_transaction_datetime=last_transaction_datetime
            )

            records = response.get("records", [])

            if records:
                save_transactions(
                    table_name=table_name,
                    records=records
                )

            end_time = datetime.now(timezone.utc)

            # ------------------------------------------
            # Update Sync State
            # ------------------------------------------

            update_sync_state(
                location_id=location_id,
                location_name=location_name,
                table_name=table_name,
                last_transaction_id=response.get("last_transaction_id"),
                last_transaction_datetime=response.get("last_transaction_datetime"),
                records_received=response.get("record_count", 0),
                sync_started_at=start_time,
                sync_completed_at=end_time,
                status="SUCCESS"
            )

            # ------------------------------------------
            # Write Sync History
            # ------------------------------------------

            write_sync_history(
                location_id=location_id,
                location_name=location_name,
                table_name=table_name,
                last_transaction_id=response.get("last_transaction_id"),
                last_transaction_datetime=response.get("last_transaction_datetime"),
                records_received=response.get("record_count", 0),
                sync_started_at=start_time,
                sync_completed_at=end_time,
                status="SUCCESS"
            )

            print(f"✓ {table_name}: {response.get('record_count', 0)} records")

        except Exception as ex:

            end_time = datetime.now(timezone.utc)

            print(f"✗ {table_name} failed")
            print(ex)

            write_sync_history(
                location_id=location_id,
                location_name=location_name,
                table_name=table_name,
                last_transaction_id=None,
                last_transaction_datetime=None,
                records_received=0,
                sync_started_at=start_time,
                sync_completed_at=end_time,
                status="FAILED",
                error_message=str(ex)
            )

print("\nTransaction synchronization completed.")
