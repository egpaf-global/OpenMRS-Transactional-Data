from .api import get
from .config import TRANSACTION_ENDPOINT


def get_transactions(
    token,
    transaction_type,
    location_id,
    last_sync_id=0,
    last_sync_datetime=None,
    limit=1000
):

    params = {
        "type": transaction_type,
        "location_id": location_id,
        "last_sync_id": last_sync_id,
        "last_sync_transaction_datetime": last_sync_datetime,
        "limit": limit
    }

    return get(
        TRANSACTION_ENDPOINT,
        token,
        params=params
    )