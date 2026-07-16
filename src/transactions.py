from .api import get
from .config import TRANSACTION_ENDPOINT

def get_transactions(
        token,
        transaction_type,
        location_id,
        last_sync_id,
        last_sync_datetime,
        limit
):

    endpoint = (
        f"{TRANSACTION_ENDPOINT}"
        f"?type={transaction_type}"
        f"&location_id={location_id}"
        f"&last_sync_id={last_sync_id}"
        f"&last_sync_transaction_datetime={last_sync_datetime}"
        f"&limit={limit}"
    )

    return get(endpoint, token)