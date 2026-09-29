# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC

# COMMAND ----------

# ============================================================
# Glaser360 Transaction Synchronization
# ============================================================
#
# This notebook:
#   1. Uses the Spark session supplied by Databricks
#   2. Loads locations from the metadata location table
#   3. Processes each location
#   4. Processes each transaction table
#   5. Reads the saved synchronization checkpoint
#   6. Downloads transaction batches
#   7. Saves transactions using Delta MERGE/UPSERT
#   8. Updates the synchronization checkpoint
#   9. Writes synchronization history
#   10. Continues until the API returns no records
#
# IMPORTANT:
#   - Do NOT create SparkSession.builder.getOrCreate() here.
#   - Databricks provides the `spark` variable automatically.
#   - storage.py and sync.py receive this Spark session through
#     configure_spark().
# ============================================================


# ============================================================
# Imports
# ============================================================

import importlib
from datetime import datetime, timezone


# ============================================================
# Import Application Modules
# ============================================================

import src.auth
import src.transactions
import src.storage
import src.sync
import src.settings


# ============================================================
# Reload During Development
# ============================================================
#
# This allows changes to application modules to be picked up
# without restarting the notebook.
#
# Do NOT reload pyspark itself.
# ============================================================

importlib.reload(src.settings)
importlib.reload(src.auth)
importlib.reload(src.transactions)
importlib.reload(src.storage)
importlib.reload(src.sync)


# ============================================================
# Application Imports
# ============================================================

from src.settings import (
    TARGET_SCHEMA,
    TRANSACTION_TABLES,
    INITIAL_TRANSACTION_ID,
    INITIAL_TRANSACTION_SITE_DATETIME,
)

from src.auth import login

from src.transactions import (
    get_transactions,
)

from src.storage import (
    save_transactions,
)

from src.sync import (
    get_sync_state,
    update_sync_state,
    write_sync_history,
)


# ============================================================
# Spark Session
# ============================================================
#
# IMPORTANT:
#
# Databricks automatically provides `spark`.
#
# DO NOT do:
#
#     from pyspark.sql import SparkSession
#     spark = SparkSession.builder.getOrCreate()
#
# The Databricks notebook session should be used directly.
# ============================================================

print()
print("=" * 80)
print("Spark Session")
print("=" * 80)

try:
    spark
except NameError:
    raise RuntimeError(
        "Databricks did not provide a Spark session. "
        "Detach and reattach the notebook to the compute, "
        "then run the notebook again."
    )

if spark is None:
    raise RuntimeError(
        "The Databricks Spark session is None. "
        "Detach and reattach the notebook to the compute, "
        "then run the notebook again."
    )

print(f"Spark session available: {spark}")


# ============================================================
# Validate Spark Session
# ============================================================

try:

    test_result = (
        spark.sql(
            "SELECT 1 AS test"
        )
        .collect()
    )

    if not test_result:
        raise RuntimeError(
            "Spark session test returned no result."
        )

    if test_result[0]["test"] != 1:
        raise RuntimeError(
            "Spark session test returned an unexpected result."
        )

    print("✓ Spark session test successful.")

except Exception as e:

    raise RuntimeError(
        "\n"
        "Spark session is not usable.\n"
        f"Error: {e}\n\n"
        "Recommended action:\n"
        "1. Detach the notebook from the compute.\n"
        "2. Reattach it to the compute.\n"
        "3. Run the notebook again from the first cell."
    )


# ============================================================
# Configure Application Modules
# ============================================================
#
# storage.py and sync.py do not create their own Spark sessions.
#
# We explicitly provide the Databricks Spark session here.
# ============================================================

print()
print("Configuring application modules...")

src.storage.configure_spark(spark)

print("✓ storage.py configured.")

src.sync.configure_spark(spark)

print("✓ sync.py configured.")


# ============================================================
# Header
# ============================================================

print()
print("=" * 80)
print("Glaser360 Transaction Synchronization")
print("=" * 80)

print(
    f"Target Schema : {TARGET_SCHEMA}"
)

print(
    f"Transaction Tables : "
    f"{len(TRANSACTION_TABLES)}"
)

print("=" * 80)


# ============================================================
# Login
# ============================================================

print()
print("Authenticating...")

token = login()

print("✓ Authentication successful.")


# ============================================================
# Read Locations
# ============================================================
#
# We use toLocalIterator() rather than collect() so that the
# entire location table does not need to be loaded into the
# driver at once.
#
# We also avoid count() here because count() causes another
# Spark action and is unnecessary for synchronization.
# ============================================================

print()
print("=" * 80)
print("Loading Locations")
print("=" * 80)

locations_df = (
    spark.table(
        f"{TARGET_SCHEMA}.location"
    )
    .select(
        "location_id",
        "name",
    )
    .orderBy(
        "location_id",
        ascending=False,
    )
)

print(
    "✓ Location table loaded."
)

locations = locations_df.toLocalIterator()


# ============================================================
# Synchronization Counters
# ============================================================

locations_processed = 0
locations_failed = 0

overall_started_at = datetime.now(timezone.utc)


# ============================================================
# Process Locations
# ============================================================

for location in locations:

    locations_processed += 1

    location_id = location.location_id
    location_name = location.name

    # --------------------------------------------------------
    # Location Header
    # --------------------------------------------------------

    print()
    print()
    print("=" * 80)
    print(
        f"Location : {location_name} "
        f"({location_id})"
    )
    print("=" * 80)

    location_started_at = datetime.now(timezone.utc)

    location_failed = False


    # ========================================================
    # Process Transaction Tables
    # ========================================================

    for table in TRANSACTION_TABLES:

        table_name = table["table"]

        print()
        print("-" * 80)
        print(
            f"Synchronizing table: {table_name}"
        )
        print("-" * 80)


        # ====================================================
        # Read Existing Checkpoint
        # ====================================================

        try:

            state = get_sync_state(
                location_id=location_id,
                table_name=table_name,
            )

        except Exception as ex:

            print()
            print(
                f"✗ Failed to read checkpoint "
                f"for '{table_name}'"
            )

            print(
                f"Error: {ex}"
            )

            location_failed = True

            break


        # ====================================================
        # Determine Starting Checkpoint
        # ====================================================

        if state:

            last_transaction_id = (
                state.get(
                    "last_transaction_id"
                )
                or INITIAL_TRANSACTION_ID
            )

            last_transaction_site_datetime = (
                state.get(
                    "last_transaction_site_datetime"
                )
                or INITIAL_TRANSACTION_SITE_DATETIME
            )

        else:

            last_transaction_id = (
                INITIAL_TRANSACTION_ID
            )

            last_transaction_site_datetime = (
                INITIAL_TRANSACTION_SITE_DATETIME
            )


        # ====================================================
        # Print Starting Checkpoint
        # ====================================================

        print()
        print(
            "Starting checkpoint:"
        )

        print(
            f"  Transaction ID : "
            f"{last_transaction_id}"
        )

        print(
            f"  Transaction Date: "
            f"{last_transaction_site_datetime}"
        )


        # ====================================================
        # Batch Counters
        # ====================================================

        batch_number = 1
        total_records = 0

        table_started_at = datetime.now(
            timezone.utc
        )


        # ====================================================
        # Download Until Empty
        # ====================================================
        #
        # The API determines how much data is returned.
        #
        # We continue requesting batches until the API returns
        # zero records.
        # ====================================================

        while True:

            sync_started_at = (
                datetime.now(timezone.utc)
            )

            print()
            print(
                f"Batch {batch_number}"
            )

            print(
                f"Checkpoint:"
            )

            print(
                f"  Transaction ID : "
                f"{last_transaction_id}"
            )

            print(
                f"  Transaction Date: "
                f"{last_transaction_site_datetime}"
            )


            # =================================================
            # Download Transaction Batch
            # =================================================

            try:

                response = get_transactions(
                    token=token,
                    table_name=table_name,
                    location_id=location_id,
                    last_transaction_id=(
                        last_transaction_id
                    ),
                    last_transaction_site_datetime=(
                        last_transaction_site_datetime
                    ),
                )

            except Exception as ex:

                sync_completed_at = (
                    datetime.now(timezone.utc)
                )

                print()
                print(
                    f"✗ Failed to download "
                    f"'{table_name}'"
                )

                print(
                    f"Error: {ex}"
                )


                # --------------------------------------------
                # Write Failure History
                # --------------------------------------------

                try:

                    write_sync_history(

                        location_id=location_id,
                        location_name=location_name,
                        table_name=table_name,

                        last_transaction_id=(
                            last_transaction_id
                        ),

                        last_transaction_site_datetime=(
                            last_transaction_site_datetime
                        ),

                        records_received=0,

                        sync_started_at=(
                            sync_started_at
                        ),

                        sync_completed_at=(
                            sync_completed_at
                        ),

                        status="FAILED",

                        error_message=str(ex),
                    )

                except Exception as history_error:

                    print(
                        "⚠ Failed to write "
                        "synchronization history:"
                    )

                    print(
                        str(history_error)
                    )


                location_failed = True

                break


            # =================================================
            # Validate API Response
            # =================================================

            if not isinstance(response, dict):

                print()
                print(
                    f"✗ Unexpected response type: "
                    f"{type(response)}"
                )

                location_failed = True

                break


            # =================================================
            # Extract Records
            # =================================================

            records = response.get(
                "records",
                [],
            )


            if records is None:

                records = []


            if not isinstance(records, list):

                print()
                print(
                    "✗ API response contains an "
                    "invalid records structure."
                )

                print(
                    f"Received type: "
                    f"{type(records)}"
                )

                location_failed = True

                break


            record_count = response.get(
                "record_count",
                len(records),
            )


            # =================================================
            # No More Records
            # =================================================

            if not records or record_count == 0:

                table_duration = (
                    datetime.now(timezone.utc)
                    - table_started_at
                ).total_seconds()

                print()
                print(
                    "✓ No more records."
                )

                print(
                    f"  Table              : "
                    f"{table_name}"
                )

                print(
                    f"  Total synchronized : "
                    f"{total_records:,}"
                )

                print(
                    f"  Batches             : "
                    f"{batch_number - 1:,}"
                )

                print(
                    f"  Duration            : "
                    f"{table_duration:,.2f} seconds"
                )

                break


            # =================================================
            # Report Retrieved Records
            # =================================================

            print()
            print(
                f"Retrieved "
                f"{record_count:,} records."
            )


            # =================================================
            # Save Transaction Records
            # =================================================

            try:

                saved_count = save_transactions(
                    table_name=table_name,
                    records=records,
                )

            except Exception as ex:

                sync_completed_at = (
                    datetime.now(timezone.utc)
                )

                print()
                print(
                    f"✗ Failed to save "
                    f"'{table_name}'"
                )

                print(
                    f"Error: {ex}"
                )


                # --------------------------------------------
                # Write Failure History
                # --------------------------------------------

                try:

                    write_sync_history(

                        location_id=location_id,
                        location_name=location_name,
                        table_name=table_name,

                        last_transaction_id=(
                            last_transaction_id
                        ),

                        last_transaction_site_datetime=(
                            last_transaction_site_datetime
                        ),

                        records_received=0,

                        sync_started_at=(
                            sync_started_at
                        ),

                        sync_completed_at=(
                            sync_completed_at
                        ),

                        status="FAILED",

                        error_message=str(ex),
                    )

                except Exception as history_error:

                    print(
                        "⚠ Failed to write "
                        "synchronization history:"
                    )

                    print(
                        str(history_error)
                    )


                location_failed = True

                break


            # =================================================
            # Update Total
            # =================================================

            total_records += record_count


            # =================================================
            # Advance Checkpoint
            # =================================================
            #
            # IMPORTANT:
            #
            # The checkpoint is only advanced AFTER the
            # records have successfully been saved.
            #
            # This prevents losing transactions if a save
            # operation fails.
            # =================================================

            new_transaction_id = response.get(
                "last_transaction_id",
                last_transaction_id,
            )

            new_transaction_site_datetime = (
                response.get(
                    "last_transaction_site_datetime",
                    last_transaction_site_datetime,
                )
            )


            # =================================================
            # Validate Checkpoint Progress
            # =================================================

            if (
                new_transaction_id
                == last_transaction_id
                and
                new_transaction_site_datetime
                == last_transaction_site_datetime
            ):

                print()
                print(
                    "⚠ WARNING: API returned records "
                    "but did not advance the checkpoint."
                )

                print(
                    "Stopping this table to prevent "
                    "an infinite synchronization loop."
                )

                location_failed = True

                break


            # =================================================
            # Update Local Checkpoint
            # =================================================

            last_transaction_id = (
                new_transaction_id
            )

            last_transaction_site_datetime = (
                new_transaction_site_datetime
            )


            sync_completed_at = (
                datetime.now(timezone.utc)
            )


            # =================================================
            # Update Sync State
            # =================================================

            try:

                update_sync_state(

                    location_id=location_id,
                    location_name=location_name,
                    table_name=table_name,

                    last_transaction_id=(
                        last_transaction_id
                    ),

                    last_transaction_site_datetime=(
                        last_transaction_site_datetime
                    ),

                    records_received=(
                        record_count
                    ),

                    sync_started_at=(
                        sync_started_at
                    ),

                    sync_completed_at=(
                        sync_completed_at
                    ),

                    status="SUCCESS",

                )

            except Exception as ex:

                print()
                print(
                    "✗ Failed to update "
                    "synchronization checkpoint."
                )

                print(
                    f"Error: {ex}"
                )

                location_failed = True

                break


            # =================================================
            # Write Sync History
            # =================================================

            try:

                write_sync_history(

                    location_id=location_id,
                    location_name=location_name,
                    table_name=table_name,

                    last_transaction_id=(
                        last_transaction_id
                    ),

                    last_transaction_site_datetime=(
                        last_transaction_site_datetime
                    ),

                    records_received=(
                        record_count
                    ),

                    sync_started_at=(
                        sync_started_at
                    ),

                    sync_completed_at=(
                        sync_completed_at
                    ),

                    status="SUCCESS",

                )

            except Exception as ex:

                print()
                print(
                    "⚠ WARNING: Failed to write "
                    "sync history."
                )

                print(
                    f"Error: {ex}"
                )


            # =================================================
            # Batch Summary
            # =================================================

            batch_duration = (
                sync_completed_at
                - sync_started_at
            ).total_seconds()


            print()
            print(
                f"✓ Batch {batch_number} complete"
            )

            print(
                f"  Records this batch : "
                f"{record_count:,}"
            )

            print(
                f"  Total synchronized : "
                f"{total_records:,}"
            )

            print(
                f"  New transaction ID : "
                f"{last_transaction_id}"
            )

            print(
                f"  New transaction date: "
                f"{last_transaction_site_datetime}"
            )

            print(
                f"  Duration           : "
                f"{batch_duration:,.2f} seconds"
            )


            # =================================================
            # Next Batch
            # =================================================

            batch_number += 1


        # ====================================================
        # Table Summary
        # ====================================================

        print()
        print(
            f"Finished table: {table_name}"
        )

        print(
            f"Total records synchronized: "
            f"{total_records:,}"
        )


        # ====================================================
        # Stop Remaining Tables If Failure
        # ====================================================

        if location_failed:

            print()
            print(
                f"✗ Location processing stopped "
                f"because '{table_name}' failed."
            )

            break


    # ========================================================
    # Location Summary
    # ========================================================

    location_duration = (
        datetime.now(timezone.utc)
        - location_started_at
    ).total_seconds()


    print()
    print("=" * 80)

    if location_failed:

        locations_failed += 1

        print(
            f"✗ Location FAILED: "
            f"{location_name} ({location_id})"
        )

    else:

        print(
            f"✓ Location COMPLETED: "
            f"{location_name} ({location_id})"
        )

    print(
        f"Duration: "
        f"{location_duration:,.2f} seconds"
    )

    print("=" * 80)


# ============================================================
# Overall Summary
# ============================================================

overall_completed_at = (
    datetime.now(timezone.utc)
)

overall_duration = (
    overall_completed_at
    - overall_started_at
).total_seconds()


print()
print()
print("=" * 80)
print("TRANSACTION SYNCHRONIZATION COMPLETED")
print("=" * 80)

print(
    f"Target Schema       : "
    f"{TARGET_SCHEMA}"
)

print(
    f"Locations processed : "
    f"{locations_processed:,}"
)

print(
    f"Locations failed    : "
    f"{locations_failed:,}"
)

print(
    f"Locations successful: "
    f"{locations_processed - locations_failed:,}"
)

print(
    f"Total duration      : "
    f"{overall_duration:,.2f} seconds"
)

print("=" * 80)


# ============================================================
# Final Status
# ============================================================

if locations_failed > 0:

    print()
    print(
        "⚠ Synchronization completed "
        "with failures."
    )

else:

    print()
    print(
        "✓ All locations synchronized successfully."
    )
