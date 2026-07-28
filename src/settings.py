"""
Application configuration.
"""

# ============================================
# API Configuration
# ============================================

BASE_URL = "http://3.134.109.192:3012"

API_VERSION = "v1"

LOGIN_ENDPOINT = f"/api/{API_VERSION}/login"
METADATA_ENDPOINT = f"/api/{API_VERSION}/openmrs/metadata"
TRANSACTION_ENDPOINT = f"/api/{API_VERSION}/openmrs/transactions"


# ============================================
# Databricks Unity Catalog
# ============================================

CATALOG = "programsdev"
SCHEMA = "malawi"


# ============================================
# Synchronisation
# ============================================

DEFAULT_PAGE_SIZE = 10000
REQUEST_TIMEOUT = 120

INITIAL_SYNC_ID = 0
INITIAL_SYNC_DATETIME = "1900-01-01T00:00:00Z"