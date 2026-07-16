import requests
from src.settings import BASE_URL, LOGIN_ENDPOINT
from config.credentials import EMAIL, PASSWORD


def login():

    response = requests.post(
        f"{BASE_URL}{LOGIN_ENDPOINT}",
        json={
            "email": EMAIL,
            "password": PASSWORD
        }
    )

    response.raise_for_status()

    return response.json()["token"]