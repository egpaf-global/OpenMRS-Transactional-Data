# Databricks notebook source
# DBTITLE 1,Cell 1
from src.auth import login
from src.metadata import get_metadata
from config.credentials import EMAIL, PASSWORD
#from src.storage import save_metadata

# --------------------------------------------------
# Login
# --------------------------------------------------


token = login(EMAIL, PASSWORD)

print("Authentication successful")

# --------------------------------------------------
# Metadata types to ingest
# --------------------------------------------------

metadata_types = [
    "location",
    "program",
    "encounter_type"
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
