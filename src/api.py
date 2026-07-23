import requests

from src.settings import BASE_URL


def get(endpoint, token, params=None):
    """
    Generic GET request.

    Args:
        endpoint (str): API endpoint.
        token (str): JWT token.
        params (dict): Optional query parameters.

    Returns:
        dict: JSON response.
    """

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    response = requests.get(
        f"{BASE_URL}{endpoint}",
        headers=headers,
        params=params,
        timeout=300
    )

    response.raise_for_status()

    payload = response.json()

    # Validate common API response format
    if isinstance(payload, dict):
        if payload.get("success") is False:
            raise Exception(payload.get("message", "API request failed"))

    return payload