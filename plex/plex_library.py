import csv
import sys

from loguru import logger
from plexapi.myplex import MyPlexAccount

from . import (
    PLEX_PASSWORD,
    PLEX_SERVER_NAME,
    PLEX_TEST_SERVER_NAME,
    PLEX_USER,
)


def plex_connect(test: bool = True):
    """
    Connects to a Plex server using the credentials from environment.

    Args:
        test: If True, connect to test server. If False, connect to production.

    Returns:
        PlexServer: The connected Plex server object.
    """
    server_name = PLEX_TEST_SERVER_NAME if test else PLEX_SERVER_NAME
    account = MyPlexAccount(PLEX_USER, PLEX_PASSWORD)
    try:
        server = account.resource(server_name).connect()
        logger.info(f"Connected to Plex Server: {server_name}")
        return server
    except Exception as e:
        logger.error(f"Error connecting to Plex server {server_name}: {e}")
        sys.exit()


def get_music_library(server, library):
    """
    Finds the music library in the provided Plex server.
    :param server: PLex Server object
    :param library: String representing the name of the music library
    :return: Music library object
    """
    try:
        music_library = server.library.section(library)
        logger.debug("Retrieved Plex server music library")
        return music_library
    except Exception as e:
        logger.error(f"Error retrieving music library: {e}")
        sys.exit()


def get_all_tracks(music_library):
    """
    Retrieve all tracks from the music library.
    :param music_library: Plex library object
    :return: list of track objects
    """
    try:
        tracks = music_library.searchTracks()
        library_size = len(tracks)
        logger.debug(f"Retrieved {library_size} tracks from Plex library")
        return tracks, library_size
    except Exception as e:
        logger.error(f"Error retrieving tracks from music library: {e}")
        sys.exit()


def get_all_tracks_limit(music_library, limit=50):
    """
    Retrieve all tracks from the music library.
    :param music_library: Plex library object
    :return: list of track objects
    """
    try:
        tracks = music_library.searchTracks(limit=limit)
        library_size = len(tracks)
        logger.debug(f"Retrieved {library_size} tracks from Plex library")
        return tracks, library_size
    except Exception as e:
        logger.error(f"Error retrieving tracks from music library: {e}")
        sys.exit()


def get_tracks_since_date(music_library, since_date: str):
    """
    Retrieve tracks added to the library since a specific date.

    Args:
        music_library: Plex library object
        since_date: Date string in 'YYYY-MM-DD' format

    Returns:
        tuple: (list of track objects, count)
    """
    try:
        # Plex uses addedAt filter with format 'YYYY-MM-DD'
        tracks = music_library.searchTracks(filters={"addedAt>>": since_date})
        library_size = len(tracks)
        logger.info(f"Retrieved {library_size} tracks added since {since_date}")
        return tracks, library_size
    except Exception as e:
        logger.error(f"Error retrieving tracks since {since_date}: {e}")
        return [], 0


def extract_track_data(track, filepath_prefix: str):
    """
    Extract Plex track data from a track object. Return a dict with selected data
    along with a server_id for ratingKey and a stripped filepath.

    Args:
        track: Plex track object
        filepath_prefix: string to be stripped from the location[0] field

    Returns:
        dict with track metadata
    """
    genre_list = []
    for genre in track.genres:
        genre_list.append(genre.tag)
    added_date = track.addedAt.strftime("%Y-%m-%d")
    filepath = None
    for media in track.media:
        for part in media.parts:
            filepath = part.file
    logger.debug(f"Original location: {track.locations[0]}")
    logger.debug(f"Filepath prefix: {filepath_prefix}")
    stripped_location = track.locations[0].replace(filepath_prefix, "")
    logger.debug(f"Stripped location: {stripped_location}")

    # Use originalTitle for compilation tracks (contains actual track artist),
    # fall back to album artist for regular albums
    artist = track.originalTitle if track.originalTitle else track.artist().title

    track_data = {
        "title": track.title,
        "artist": artist,
        "album": track.album().title,
        "genre": genre_list,
        "added_date": added_date,
        "filepath": filepath,
        "location": stripped_location,
        "plex_id": int(track.ratingKey),
    }
    return track_data


def listify_track_data(tracks, filepath_prefix: str):
    """
    Lists the track data from the provided list of tracks.

    Parameters:
    tracks (list): A list of track objects to extract data from.

    Returns:
    list: A list of dictionaries containing the track data.
    """
    track_list = []
    lib_size = len(tracks)
    i = 1
    for track in tracks:
        track_data = extract_track_data(track, filepath_prefix)
        track_list.append(track_data)
        logger.debug(f"Added {track.title} - {track.ratingKey}. {i} of {lib_size}")
        i += 1
    logger.info(f"Made a list of all track data: {lib_size} in all")
    return track_list


def export_track_data(track_data, filename):
    """
    Exports the track data to a CSV file.

    Parameters:
    track_data (list): A list of dictionaries containing track data.

    Returns:
    None
    """
    with open(filename, "a") as csvfile:
        fieldnames = [
            "title",
            "artist",
            "album",
            "genre",
            "added_date",
            "filepath",
            "location",
            "plex_id",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for element in track_data:
            writer.writerow(element)
    logger.info("Exported all track data to csv!")
