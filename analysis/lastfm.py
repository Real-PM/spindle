import json
import os

import requests
from dotenv import load_dotenv
from loguru import logger

from db import TEST_DB_PATH
from db.database import Database

load_dotenv()

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "")
LASTFM_SHARED_SECRET = os.getenv("LASTFM_SHARED_SECRET", "")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME", "")
LASTFM_APP_NAME = os.getenv("LASTFM_APP_NAME", "")

# Request timeout in seconds (connect, read)
REQUEST_TIMEOUT = 30

database = Database(TEST_DB_PATH)


def get_artist_info(artist_name):
    """
    Retrieves information about a specific artist from the Last.fm API.

    Parameters:
    artist_name (str): The name of the artist to retrieve information for.

    Returns:
    dict: A JSON object containing information about the artist if the request is successful, otherwise None.
    """
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&autocorrect=1&artist={artist_name}&api_key={LASTFM_API_KEY}&format=json"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout:
        logger.warning(f"Request timeout for artist {artist_name}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for artist {artist_name}: {e}")
        return None
    if response.status_code == 200:
        logger.debug(f"last_fm Response: {response.json()}")
        logger.info(f"Retrieved artist info for {artist_name}")
        return response.json()
    else:
        logger.error(f"Failed to retrieve artist info for {artist_name}")
        return None


def get_artist_mbid(result: dict | None) -> str | None:
    """
    Retrieves the MusicBrainz ID (MBID) of the artist from the given JSON `result` object.

    Parameters:
    result: The JSON object containing artist information.

    Returns:
    str: The MusicBrainz ID (MBID) of the artist, or None if the MBID is not found.
    """
    if result is None:
        return None
    try:
        mbid = result["artist"]["mbid"]
        artist_name = result["artist"].get("name", "Unknown")
        logger.info(f"Retrieved MBID for {artist_name}: {mbid}")
        return mbid
    except (KeyError, TypeError) as e:
        artist_name = result.get("artist", {}).get("name", "Unknown")
        logger.error(f"Failed to retrieve MBID for {artist_name}: {e}")
        return None


def get_artist_tags(result: dict | None) -> list[str]:
    """
    Retrieves the tags from the given JSON `result` object.

    Parameters:
    result: The JSON object containing artist information.

    Returns:
    list: A list of tags associated with the artist, or an empty list if no tags are found.
    """
    if result is None:
        return []
    try:
        tags = result["artist"]["tags"]["tag"]
        tag_list = [tag["name"] for tag in tags]
        artist_name = result["artist"].get("name", "Unknown")
        logger.info(f"Retrieved tags for {artist_name}: {tag_list}")
        return tag_list
    except (KeyError, TypeError) as e:
        artist_name = result.get("artist", {}).get("name", "Unknown")
        logger.error(f"Failed to retrieve tags for {artist_name}: {e}")
        return []


def get_similar_artists(result):
    """Extract similar artists from Last.fm API response.

    Args:
        result (dict): Last.fm API response containing artist info

    Returns:
        list: List of similar artist names
    """
    try:
        similar_artists = []
        if "artist" in result and "similar" in result["artist"]:
            for artist in result["artist"]["similar"]["artist"]:
                similar_artists.append(artist["name"])
        return similar_artists

    except Exception as e:
        logger.error(f"Failed to retrieve similar artists for {result['artist']['name']}: {e}")
        return []


def get_current_mbids_from_db(database: Database):
    """
    Get all MusicBrainz IDs (MBIDs) from the artists table in the database.

    Parameters:
    database (Database): The database object to query.

    Returns:
    list: A list of MusicBrainz IDs (MBIDs) from the database.
    """
    logger.debug("Starting to get MBIDs from db.")
    database.connect()
    query = "SELECT musicbrainz_id FROM artists"
    results = database.execute_select_query(query)
    mbid_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting MBIDs from db.")
    return mbid_list


def get_genres_from_db(database: Database):
    """
    Get all genres from the genres table in the database.

    Parameters:
    database (Database): The database object to query.

    Returns:
    list: A list of genres from the database.
    """
    logger.debug("Starting to get genres from db.")
    database.connect()
    query = "SELECT genre FROM genres"
    results = database.execute_select_query(query)
    genre_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting genres from db.")
    return genre_list


def get_last_fm_track_data(
    artist: str | None = None,
    track: str | None = None,
    mbid: str | None = None,
) -> dict | None:
    """
    Retrieves information about a specific track from the Last.fm API.

    Prefers MBID lookup for precise matching. Falls back to artist+track
    lookup if MBID is not provided.

    Args:
        artist: The name of the artist (required if mbid not provided)
        track: The name of the track (required if mbid not provided)
        mbid: MusicBrainz ID for precise track lookup (preferred)

    Returns:
        dict: JSON response containing track info, or None on failure
    """
    if mbid:
        url = (
            f"http://ws.audioscrobbler.com/2.0/?method=track.getInfo"
            f"&api_key={LASTFM_API_KEY}&mbid={mbid}&format=json"
        )
        lookup_desc = f"MBID {mbid}"
    elif artist and track:
        url = (
            f"http://ws.audioscrobbler.com/2.0/?method=track.getInfo"
            f"&api_key={LASTFM_API_KEY}&artist={artist}&track={track}"
            f"&autocorrect=1&format=json"
        )
        lookup_desc = f"{artist} - {track}"
    else:
        logger.error("get_last_fm_track_data requires either mbid or artist+track")
        return None

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout:
        logger.warning(f"Request timeout for {lookup_desc}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {lookup_desc}: {e}")
        return None
    if response.status_code == 200:
        result = response.json()
        # Check for API error response (e.g., track not found)
        if "error" in result:
            logger.warning(
                f"Last.fm API error for {lookup_desc}: {result.get('message', 'Unknown error')}"
            )
            return None
        logger.debug(f"last_fm Response: {result}")
        logger.info(f"Retrieved track info for {lookup_desc}")
        return result
    else:
        logger.error(
            f"Failed to retrieve track info for {lookup_desc} (HTTP {response.status_code})"
        )
        return None


def get_track_mbid(result: json):
    """
    Retrieves the MusicBrainz ID (MBID) of the track from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing track information.

    Returns:
    str: The MusicBrainz ID (MBID) of the track, or None if the MBID is not found.
    """
    try:
        mbid = result["track"]["mbid"]
        logger.info(f"Retrieved MBID for {result['track']['name']}: {mbid}")
        return mbid
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve MBID for {result['track']['name']}: {e}")
        return None


def get_track_tags(result: json):
    """
    Retrieves the tags from the given JSON `result` object.

    Parameters:
    result (json): The JSON object containing track information.

    Returns:
    list: A list of tags associated with the track, or an empty list if no tags are found.
    """
    try:
        tags = result["track"]["toptags"]["tag"]
        tag_list = [tag["name"] for tag in tags]
        logger.info(f"Retrieved tags for {result['track']['name']}: {tag_list}")
        return tag_list
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to retrieve tags for {result['track']['name']}: {e}")
        return []


def get_track_list_from_db(database: Database):
    """
    Get all tracks from the tracks table in the database.

    Parameters:
    database (Database): The database object to query.

    Returns:
    list: A list of tracks from the database.
    """
    logger.debug("Starting to get tracks from db.")
    database.connect()
    query = """SELECT td.id, a.artist, td.title
    FROM track_data td
    INNER JOIN artists a ON td.artist_id = a.id"""
    results = database.execute_select_query(query)
    track_list = [(result[0], result[1], result[2]) for result in results]
    database.close()
    logger.debug("Finished getting tracks from db.")
    return track_list
