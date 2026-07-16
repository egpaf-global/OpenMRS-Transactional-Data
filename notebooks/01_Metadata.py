# Databricks notebook source
# DBTITLE 1,Cell 1
import importlib
import src.auth
importlib.reload(src.auth)
from src.auth import login
from src.metadata import get_metadata
#from src.storage import save_metadata

# --------------------------------------------------
# Login
# --------------------------------------------------


token = login()

print("Authentication successful")

# --------------------------------------------------
# Metadata types to ingest
# --------------------------------------------------

metadata_types = [
    "concept_name",
    "encounter_type",
    "order_type",
    "program",
    "program_workflow",
    "relationship_type",
    "drug", 
    "program_workflow_state",
    "location"
]

# --------------------------------------------------
# Download and save each metadata type
# --------------------------------------------------

for metadata_type in metadata_types:

    print(f"Downloading {metadata_type}...")

    data = get_metadata(metadata_type, token)

    #save_metadata(metadata_type, data)

    print(f"{metadata_type} completed")

print("Metadata ingestion complete.")
