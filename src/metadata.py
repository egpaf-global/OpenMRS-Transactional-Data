from src.api import get
from src.config import METADATA_ENDPOINT

def get_metadata(metadata_type, token):

    endpoint = f"{METADATA_ENDPOINT}?type={metadata_type}"

    response = get(endpoint, token)

    return response["data"]