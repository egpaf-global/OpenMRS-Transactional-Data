# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
import importlib

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

print(f"Target: {CATALOG}.{SCHEMA}")

# --------------------------------------------------
# Login
# --------------------------------------------------

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

# --------------------------------------------------
# Download Metadata
# --------------------------------------------------

for metadata_type in metadata_types:

    print(f"\nDownloading {metadata_type}...")

    data = get_metadata(metadata_type, token)

    if not data:
        print(f"No records returned for {metadata_type}")
        continue

    try:
        save_metadata(metadata_type, data)
        print(f"{metadata_type} completed.")

    except Exception as e:
        print(f"\nFAILED: {metadata_type}")
        print(type(data))
        print(f"Records: {len(data)}")

        if len(data):
            print("First record:")
            print(data[0])

        raise

    if not data:
        print(f"No records returned for {metadata_type}")
        continue

    save_metadata(metadata_type, data)

print("\nMetadata ingestion completed.")

# --------------------------------------------------
# Summary
# --------------------------------------------------

print(f"\nTables in {CATALOG}.{SCHEMA}:")

for table in get_table_names():
    print(f" - {table}")
