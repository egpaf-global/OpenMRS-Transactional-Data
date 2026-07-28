from src.api import get
from src.settings import METADATA_ENDPOINT


#function to get metadata
def get_metadata(metadata_type, token):

    endpoint = f"{METADATA_ENDPOINT}?type={metadata_type}"

    response = get(endpoint, token)

    return response["data"]