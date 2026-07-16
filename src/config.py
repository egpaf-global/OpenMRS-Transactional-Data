"""
Glaser360 Configuration
"""

# =============================================================================
# API CONFIGURATION
# =============================================================================

BASE_URL = "http://3.134.109.192:3012"

API_VERSION = "v1"

LOGIN_ENDPOINT = f"/api/{API_VERSION}/login"

TRANSACTION_ENDPOINT = f"/api/{API_VERSION}/openmrs/transactions"

METADATA_ENDPOINT = f"/api/{API_VERSION}/openmrs/metadata"


# =============================================================================
# DELTA TABLES
# =============================================================================

BRONZE_DATABASE = "bronze"

SILVER_DATABASE = "silver"

GOLD_DATABASE = "gold"


# =============================================================================
# INGESTION SETTINGS
# =============================================================================

DEFAULT_BATCH_SIZE = 100

DEFAULT_TIMEOUT = 120

DEFAULT_TRANSACTION_TYPE = "encounter"


# =============================================================================
# DEFAULT SYNC VALUES
# =============================================================================

INITIAL_SYNC_ID = 0

INITIAL_SYNC_DATETIME = "1900-01-01T00:00:00Z"