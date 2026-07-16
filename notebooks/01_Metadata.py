# Databricks notebook source
# DBTITLE 1,Cell 1
# Databricks notebook source

from src.auth import login
from src.metadata import get_metadata
#from src.storage import save_metadata

# --------------------------------------------------
# Login
# --------------------------------------------------

EMAIL = "admin@openmrsg360.org"
PASSWORD = "g360!MalawiD4ta@2006"

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