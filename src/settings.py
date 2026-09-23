"""
Application configuration.

All application-wide configuration should be defined here.
"""

# ============================================================
# API Configuration
# ============================================================

BASE_URL = "http://3.134.109.192:3012"

API_VERSION = "v1"

LOGIN_ENDPOINT = f"/api/{API_VERSION}/login"
METADATA_ENDPOINT = f"/api/{API_VERSION}/openmrs/metadata"
TRANSACTION_ENDPOINT = f"/api/{API_VERSION}/openmrs/transactions"


# ============================================================
# Databricks Unity Catalog
# ============================================================

CATALOG = "programsdev"
SCHEMA = "malawi"

TARGET_SCHEMA = f"{CATALOG}.{SCHEMA}"

SYNC_STATE_TABLE = f"{TARGET_SCHEMA}.sync_state"
SYNC_HISTORY_TABLE = f"{TARGET_SCHEMA}.sync_history"


# ============================================================
# Transaction Tables
# ============================================================

TRANSACTION_TABLES = [

    {
        "table": "patient",
        "keys": ["patient_id", "site_id"],
    },

    {
        "table": "encounter",
        "keys": ["encounter_id", "site_id"],
    },

    {
        "table": "patient_program",
        "keys": ["patient_program_id", "site_id"],
    },

    {
        "table": "order",
        "keys": ["order_id", "site_id"],
    },

    {
        "table": "drug_order",
        "keys": ["order_id", "site_id"],
    },

    {
        "table": "patient_state",
        "keys": ["patient_state_id", "site_id"],
    },

    {
        "table": "observation",
        "keys": ["obs_id", "site_id"],
    },
]


# ============================================================
# Synchronisation
# ============================================================

DEFAULT_BATCH_SIZE = 10000

REQUEST_TIMEOUT = 120

MAX_RETRIES = 3

RETRY_DELAY_SECONDS = 5


# ============================================================
# Initial Checkpoint
# ============================================================

INITIAL_TRANSACTION_LOCATION_ID = 0

INITIAL_TRANSACTION_ID = 0

INITIAL_TRANSACTION_SITE_DATETIME = "1900-01-01T00:00:00Z"


# ============================================================
# Delta Configuration
# ============================================================

ENABLE_SCHEMA_MERGE = True

ENABLE_OPTIMIZE_WRITE = True

ENABLE_AUTO_COMPACT = True


# ============================================================
# Logging
# ============================================================

LOG_PROGRESS_EVERY = 10000

PRINT_API_REQUESTS = False

PRINT_BATCH_SUMMARY = True