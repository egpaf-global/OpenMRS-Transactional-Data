"""
Transaction API utilities.
"""

import time
from datetime import datetime

from .api import get
from .settings import (
    TRANSACTION_ENDPOINT,
    DEFAULT_BATCH_SIZE,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    PRINT_API_REQUESTS
)

# Mapping between transaction type and the array returned by the API
RECORD_KEYS = {
    "patient": "patients",
    "encounter": "encounters",
    "observation": "observations",
    "patient_program": "patient_programs",
    "order": "orders",
    "drug_order": "drug_orders",
}


def parse_datetime(value):
    """
    Convert an ISO-8601 timestamp returned by the API into a Python datetime.

    Returns the original value if it is already a datetime or None.
    """

    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):

        value = value.strip()

        if not value:
            return None

        # Handle UTC "Z"
        value = value.replace("Z", "+00:00")

        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass

        # Handle timestamps without timezone
        formats = (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        )

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue

    raise RuntimeError(f"Unable to parse datetime '{value}'")


def get_transactions(
    token,
    table_name,
    location_id,
    last_transaction_id=0,
    last_transaction_site_datetime=None,
    batch_size=None
):
    """
    Download incremental transactions from the OpenMRS API.

    Returns:

        {
            "records": [...],
            "record_count": n,
            "last_transaction_id": x,
            "last_transaction_site_datetime": datetime,
            "has_more": bool
        }
    """

    if batch_size is None:
        batch_size = DEFAULT_BATCH_SIZE

    # Ensure outgoing parameter is a string for the API
    if isinstance(last_transaction_site_datetime, datetime):
        request_datetime = (
            last_transaction_site_datetime
            .isoformat()
            .replace("+00:00", "Z")
        )
    else:
        request_datetime = last_transaction_site_datetime

    params = {
        "type": table_name,
        "location_id": location_id,
        "last_sync_id": last_transaction_id,
        "last_sync_transaction_datetime": request_datetime,
        "limit": batch_size
    }

    if PRINT_API_REQUESTS:
        print(f"GET {TRANSACTION_ENDPOINT}")
        print(params)

    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = get(
                TRANSACTION_ENDPOINT,
                token,
                params=params
            )

            if response is None:
                raise RuntimeError("API returned an empty response.")

            if not isinstance(response, dict):
                raise RuntimeError(
                    f"Unexpected API response type: {type(response)}"
                )

            if PRINT_API_REQUESTS:
                print("API Response:")
                print(response)

            if not response.get("success", False):
                raise RuntimeError(
                    response.get(
                        "message",
                        "API returned success=False."
                    )
                )

            data = response.get("data")

            if data is None:
                raise RuntimeError(
                    "API response does not contain 'data'."
                )

            record_key = RECORD_KEYS.get(table_name)

            if record_key is None:
                raise RuntimeError(
                    f"No record mapping defined for '{table_name}'."
                )

            records = data.get(record_key)

            if records is None:

                available = ", ".join(data.keys())

                raise RuntimeError(
                    f"Expected '{record_key}' in API response. "
                    f"Available keys: {available}"
                )

            last_sync_datetime = parse_datetime(
                data.get(
                    "last_sync_transaction_datetime",
                    last_transaction_site_datetime
                )
            )

            record_count = len(records)

            return {

                "records": records,

                "record_count": record_count,

                "last_transaction_id": data.get(
                    "last_sync_id",
                    last_transaction_id
                ),

                "last_transaction_site_datetime": last_sync_datetime,

                "has_more": record_count == batch_size

            }

        except Exception as ex:

            last_exception = ex

            print(
                f"Attempt {attempt}/{MAX_RETRIES} failed "
                f"for '{table_name}': {ex}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)

    raise RuntimeError(
        f"Failed to download '{table_name}' after "
        f"{MAX_RETRIES} attempts."
    ) from last_exception