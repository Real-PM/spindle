import json
from configparser import ConfigParser

import requests
from loguru import logger

config = ConfigParser()
config.read("config.ini")

DISCOGS_API_KEY = config["DISCOGS"]["Consumer_Key"]
DISCOGS_SECRET = config["DISCOGS"]["Consumer_Secret"]
BASE_URL = "https://api.discogs.com"

headers = {
    "User-Agent": "jayco_dev_organizer/0.1",
}


def get_discogs_artist_info(
    artist_name: str,
    baseurl: str = BASE_URL,
    headers: dict = headers,
    api_key: str = DISCOGS_API_KEY,
    secret: str = DISCOGS_SECRET,
):
    """
    Retrieves information about a specific artist from the Discogs API.

    Parameters:
    baseurl (str): The base URL of the Discogs API.
    headers (dict): A dictionary of headers to be included in the request.
    key (str): The consumer key for the Discogs API.
    secret (str): The consumer secret for the Discogs API.
    artist_name (str): The name of the artist to retrieve information for.

    Returns:
    dict: A JSON object containing information about the artist if the request is successful, otherwise None.
    """
    params = {
        "q": artist_name,
        "type": "artist",
        "key": api_key,
        "secret": secret,
    }

    response = requests.get(f"{baseurl}/database/search", headers=headers, params=params)
    if response.status_code == 200:
        logger.debug(f"Discogs Response: {response.json()}")
        logger.info(f"Retrieved artist info for {artist_name}")
        return response.json()
    else:
        logger.error(f"Failed to retrieve artist info for {artist_name}")
        return None


def get_discogs_artist_id(result: json):
    """
    Retrieves the Discogs ID of the artist from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing artist information.

    Returns:
    str: The Discogs ID of the artist, or None if the ID is not found.
    """
    try:
        artist_id = result["results"][0]["id"]
        logger.info(f"Retrieved Discogs ID for {result['results'][0]['title']}: {artist_id}")
        return artist_id
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve Discogs ID for {result['results'][0]['title']}: {e}")
        return None
