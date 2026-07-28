# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
import importlib
import json
import time
from datetime import timedelta

import src.auth
import src.metadata
import src.settings
import src.storage

# Reload modules during development
importlib.reload(src.settings)
importlib.reload(src.auth)
importlib.reload(src.metadata)
importlib.reload(src.storage)

from src.auth import login
from src.metadata import get_metadata
from src.storage import (
    save_metadata,
    get_table_names
)

from src.settings import (
    CATALOG,
    SCHEMA
)

print("=" * 90)
print("OPENMRS METADATA INGESTION")
print("=" * 90)
print(f"Target Catalog : {CATALOG}")
print(f"Target Schema  : {SCHEMA}")
print("=" * 90)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

LIMIT = 1000

# --------------------------------------------------
# Login
# --------------------------------------------------

print("\nAuthenticating...")

token = login()

print("Authentication successful.")

# --------------------------------------------------
# Metadata Types
# --------------------------------------------------

metadata_types = [
    "encounter_type",
    "order_type",
    "program",
    "program_workflow",
    "relationship_type",
    "drug",
    "program_workflow_state",
    "location",
    "concept_name"
]

summary = []

overall_start = time.time()

# --------------------------------------------------
# Download Metadata
# --------------------------------------------------

for metadata_type in metadata_types:

    table_start = time.time()

    print("\n" + "=" * 90)
    print(f"Processing: {metadata_type}")
    print("=" * 90)

    offset = 0
    total_saved = 0
    batch = 1

    while True:

        try:

            response = get_metadata(
                metadata_type=metadata_type,
                token=token,
                limit=LIMIT,
                offset=offset
            )

        except Exception as e:

            print(f"FAILED retrieving {metadata_type}")
            print(str(e))
            break

        # --------------------------------------------------
        # Validate Response
        # --------------------------------------------------

        if isinstance(response, str):
            try:
                response = json.loads(response)
            except json.JSONDecodeError:
                print("Invalid JSON response.")
                break

        if not isinstance(response, dict):
            print(f"Unexpected response type: {type(response)}")
            break

        data = response.get("data", [])

        if not isinstance(data, list):
            print("Response does not contain a valid data array.")
            break

        if not data:
            break

        # --------------------------------------------------
        # Save Metadata
        # --------------------------------------------------

        try:

            save_metadata(metadata_type, data)

        except Exception:

            print("\nFAILED while saving metadata")

            if len(data):

                print("\nFirst Record:")

                print(data[0])

            raise

        returned = response.get("returned_records", len(data))
        total = response.get("total_records", total_saved + returned)
        has_more = response.get("has_more", False)

        total_saved += returned

        percent = (total_saved / total) * 100 if total else 100

        print(
            f"Batch {batch:>2} | "
            f"Fetched: {returned:>5,} | "
            f"Progress: {total_saved:>6,}/{total:<6,} "
            f"({percent:6.2f}%)"
        )

        if not has_more:
            break

        offset += returned
        batch += 1

    elapsed = timedelta(seconds=int(time.time() - table_start))

    summary.append({
        "table": metadata_type,
        "records": total_saved,
        "batches": batch,
        "time": elapsed
    })

    print(
        f"\nCompleted {metadata_type} "
        f"({total_saved:,} records) "
        f"in {elapsed}"
    )

# --------------------------------------------------
# Summary
# --------------------------------------------------

overall_elapsed = timedelta(seconds=int(time.time() - overall_start))

print("\n")
print("=" * 90)
print("INGESTION SUMMARY")
print("=" * 90)

grand_total = 0

print(
    f"{'Metadata Type':30}"
    f"{'Records':>15}"
    f"{'Batches':>12}"
    f"{'Duration':>15}"
)

print("-" * 90)

for item in summary:

    grand_total += item["records"]

    print(
        f"{item['table']:30}"
        f"{item['records']:>15,}"
        f"{item['batches']:>12}"
        f"{str(item['time']):>15}"
    )

print("-" * 90)

print(
    f"{'TOTAL':30}"
    f"{grand_total:>15,}"
)

print(f"\nOverall Duration : {overall_elapsed}")

print("\nTables Available:")

for table in sorted(get_table_names()):
    print(f"  ✓ {table}")

print("\nMetadata ingestion completed successfully.")

print("=" * 90)
