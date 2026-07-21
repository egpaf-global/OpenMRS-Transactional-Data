import requests

from src.settings import BASE_URL

def get(endpoint, token):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(
        f"{BASE_URL}{endpoint}",
        headers=headers
    )

    response.raise_for_status()

    return response.json()