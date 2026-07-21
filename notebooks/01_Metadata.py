# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
import importlib
import src.auth
importlib.reload(src.auth)
from src.auth import login
from src.metadata import get_metadata
from src.storage import save_metadata

# --------------------------------------------------
# Login
# --------------------------------------------------


token = login()

print("Authentication successful")

# --------------------------------------------------
# Metadata types to ingest
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
# Download and save each metadata type
# --------------------------------------------------

for metadata_type in metadata_types:

    print(f"Downloading {metadata_type}...")

    data = get_metadata(metadata_type, token)  # Expecting a list, no change here, but ensure save_metadata can handle list.

    #print(type(data))
    #print(data)
    save_metadata(metadata_type, data)  # Ensure save_metadata can handle list of dicts or modify accordingly.

    print(f"{metadata_type} completed")

print("Metadata ingestion complete.")
