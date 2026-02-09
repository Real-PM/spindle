"""
Plex playlist management.

Functions for creating and managing playlists on a Plex server
based on track selections from the database.
"""

import random

from loguru import logger
from plexapi.playlist import Playlist
from plexapi.server import PlexServer


def create_playlist(
    server: PlexServer,
    name: str,
    plex_ids: list[int],
    replace_existing: bool = False,
) -> Playlist | None:
    """
    Create a Plex playlist from a list of track plex_ids.

    Args:
        server: Connected PlexServer instance
        name: Name for the playlist
        plex_ids: List of Plex track ratingKeys (plex_id from database)
        replace_existing: If True, delete existing playlist with same name.
            If False and playlist exists, returns None.

    Returns:
        The created Playlist object, or None on error
    """
    if not plex_ids:
        logger.warning("Cannot create playlist with empty track list")
        return None

    # Check for existing playlist
    existing = get_playlist_by_name(server, name)
    if existing:
        if replace_existing:
            logger.info(f"Deleting existing playlist: {name}")
            existing.delete()
        else:
            logger.warning(
                f"Playlist '{name}' already exists. Use replace_existing=True to overwrite."
            )
            return None

    # Convert plex_ids to track objects
    tracks = fetch_tracks_by_ids(server, plex_ids)
    if not tracks:
        logger.error("No valid tracks found for playlist")
        return None

    # Create the playlist
    try:
        playlist = Playlist.create(server, title=name, items=tracks)
        logger.info(f"Created playlist '{name}' with {len(tracks)} tracks")
        return playlist
    except Exception as e:
        logger.error(f"Error creating playlist '{name}': {e}")
        return None


def fetch_tracks_by_ids(server: PlexServer, plex_ids: list[int]) -> list:
    """
    Fetch Plex track objects from a list of plex_ids.

    Args:
        server: Connected PlexServer instance
        plex_ids: List of Plex track ratingKeys

    Returns:
        List of Plex track objects (items that failed to fetch are skipped)
    """
    tracks = []
    failed = 0

    for plex_id in plex_ids:
        try:
            track = server.fetchItem(plex_id)
            tracks.append(track)
        except Exception as e:
            logger.debug(f"Could not fetch track {plex_id}: {e}")
            failed += 1

    if failed > 0:
        logger.warning(f"Failed to fetch {failed}/{len(plex_ids)} tracks")

    return tracks


def get_playlist_by_name(server: PlexServer, name: str) -> Playlist | None:
    """
    Find a playlist by name.

    Args:
        server: Connected PlexServer instance
        name: Playlist name to search for

    Returns:
        Playlist if found, None otherwise
    """
    try:
        playlists = server.playlists()
        for playlist in playlists:
            if playlist.title == name:
                return playlist
        return None
    except Exception as e:
        logger.error(f"Error searching for playlist '{name}': {e}")
        return None


def delete_playlist(server: PlexServer, name: str) -> bool:
    """
    Delete a playlist by name.

    Args:
        server: Connected PlexServer instance
        name: Name of playlist to delete

    Returns:
        True if deleted, False if not found or error
    """
    playlist = get_playlist_by_name(server, name)
    if playlist:
        try:
            playlist.delete()
            logger.info(f"Deleted playlist: {name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting playlist '{name}': {e}")
            return False
    else:
        logger.warning(f"Playlist '{name}' not found")
        return False


def find_similar_tracks(
    server: PlexServer,
    plex_ids: list[int],
    sample_size: int = 10,
    max_distance: float = 0.25,
    limit_per_track: int = 10,
) -> list[dict]:
    """Find sonically similar tracks using Plex's neural-network analysis.

    Randomly samples up to `sample_size` tracks from the input list, calls
    sonicallySimilar() on each, and returns deduplicated results excluding
    any tracks already in the input list.

    Args:
        server: Connected PlexServer instance.
        plex_ids: List of Plex track ratingKeys to find similar tracks for.
        sample_size: Max number of tracks to sample from plex_ids.
        max_distance: Sonic distance threshold (lower = more similar).
        limit_per_track: Max similar tracks to fetch per seed track.

    Returns:
        List of dicts with keys: plex_id, title, artist, album.
    """
    if not plex_ids:
        return []

    # Sample seed tracks
    sampled_ids = random.sample(plex_ids, min(sample_size, len(plex_ids)))
    seed_tracks = fetch_tracks_by_ids(server, sampled_ids)

    if not seed_tracks:
        return []

    input_set = set(plex_ids)
    seen: set[int] = set()
    results: list[dict] = []

    for track in seed_tracks:
        try:
            similar = track.sonicallySimilar(limit=limit_per_track, maxDistance=max_distance)
        except Exception as e:
            logger.warning("sonicallySimilar failed for '{}': {}", track.title, e)
            continue

        for sim in similar:
            rid = int(sim.ratingKey)
            if rid in input_set or rid in seen:
                continue
            seen.add(rid)
            results.append(
                {
                    "plex_id": rid,
                    "title": sim.title or "",
                    "artist": sim.grandparentTitle or "",
                    "album": sim.parentTitle or "",
                }
            )

    logger.info("Found {} similar tracks from {} seed tracks", len(results), len(seed_tracks))
    return results


def add_to_playlist(
    server: PlexServer,
    name: str,
    plex_ids: list[int],
) -> Playlist | None:
    """
    Add tracks to an existing playlist.

    Args:
        server: Connected PlexServer instance
        name: Name of existing playlist
        plex_ids: List of Plex track ratingKeys to add

    Returns:
        Updated Playlist object, or None if playlist not found
    """
    playlist = get_playlist_by_name(server, name)
    if not playlist:
        logger.warning(f"Playlist '{name}' not found")
        return None

    tracks = fetch_tracks_by_ids(server, plex_ids)
    if not tracks:
        logger.warning("No valid tracks to add")
        return playlist

    try:
        playlist.addItems(tracks)
        logger.info(f"Added {len(tracks)} tracks to playlist '{name}'")
        return playlist
    except Exception as e:
        logger.error(f"Error adding tracks to playlist '{name}': {e}")
        return None
