import requests
from src.config import BASE_URL, LOGIN_ENDPOINT

def login(email, password):

    response = requests.post(
        f"{BASE_URL}{LOGIN_ENDPOINT}",
        json={
            "email": email,
            "password": password
        }
    )

    response.raise_for_status()

    return response.json()["token"]