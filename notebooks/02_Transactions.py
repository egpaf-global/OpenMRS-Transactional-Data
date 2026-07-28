# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# --------------------------------------------------
# Imports
# --------------------------------------------------

import importlib

import src.auth
import src.transactions
import src.storage
import src.sync

importlib.reload(src.auth)
importlib.reload(src.transactions)
importlib.reload(src.storage)
importlib.reload(src.sync)

from pyspark.sql import SparkSession

from src.auth import login
from src.transactions import get_transactions
from src.storage import save_transactions
from src.sync import (
    get_sync_state,
    update_sync_state,
    write_sync_history
)

from datetime import datetime

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = SparkSession.builder.getOrCreate()

# --------------------------------------------------
# Login
# --------------------------------------------------

token = login()

print("Authentication successful")

# --------------------------------------------------
# Read Locations
# --------------------------------------------------

locations = (
    spark.table("bronze.location")
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

    print(f"\n===================================================")
    print(f"Location : {location_name} ({location_id})")
    print("===================================================")

    for table_name in transaction_tables:

        print(f"\nSynchronizing {table_name}")

        start_time = datetime.utcnow()

        try:

            # ------------------------------------------
            # Read last checkpoint
            # ------------------------------------------

            state = get_sync_state(
                location_id,
                table_name
            )

            if state is None:

                last_transaction_id = 0
                last_transaction_datetime = None

            else:

                last_transaction_id = state["last_transaction_id"]
                last_transaction_datetime = state["last_transaction_datetime"]

            # ------------------------------------------
            # Download
            # ------------------------------------------

            response = get_transactions(

                table_name=table_name,

                location_id=location_id,

                last_transaction_id=last_transaction_id,

                last_transaction_datetime=last_transaction_datetime,

                token=token

            )

            records = response["records"]

            # ------------------------------------------
            # Save Bronze Table
            # ------------------------------------------

            save_transactions(
                table_name,
                records
            )

            end_time = datetime.utcnow()

            # ------------------------------------------
            # Update Sync State
            # ------------------------------------------

            update_sync_state(

                location_id=location_id,

                location_name=location_name,

                table_name=table_name,

                last_transaction_id=response["last_transaction_id"],

                last_transaction_datetime=response["last_transaction_datetime"],

                records_received=response["record_count"],

                sync_started_at=start_time,

                sync_completed_at=end_time,

                status="SUCCESS"

            )

            # ------------------------------------------
            # Write History
            # ------------------------------------------

            write_sync_history(

                location_id=location_id,

                location_name=location_name,

                table_name=table_name,

                last_transaction_id=response["last_transaction_id"],

                last_transaction_datetime=response["last_transaction_datetime"],

                records_received=response["record_count"],

                sync_started_at=start_time,

                sync_completed_at=end_time,

                status="SUCCESS"

            )

            print(f"✓ {table_name} complete")

        except Exception as ex:

            end_time = datetime.utcnow()

            print(f"✗ {table_name} failed")
            print(str(ex))

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
