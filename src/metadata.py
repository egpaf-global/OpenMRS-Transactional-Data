import requests

from src.settings import (
    BASE_URL,
    METADATA_ENDPOINT,
    REQUEST_TIMEOUT
)


def get_metadata(metadata_type, token, limit=1000, offset=0):

    response = requests.get(
        f"{BASE_URL}{METADATA_ENDPOINT}",
        headers={
            "Authorization": f"Bearer {token}"
        },
        params={
            "type": metadata_type,
            "limit": limit,
            "offset": offset
        },
        timeout=REQUEST_TIMEOUT
    )

    response.raise_for_status()

    return response.json()