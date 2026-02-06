"""
Spotify API client for fetching audio features (BPM, energy, etc.)

Requires SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env
Get credentials at: https://developer.spotify.com/dashboard
"""

import base64
import os
from time import sleep

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")

# Request timeout in seconds
REQUEST_TIMEOUT = 30

# Cache for access token
_token_cache = {"token": None, "expires_at": 0}


def get_access_token() -> str | None:
    """Get Spotify access token using client credentials flow.

    Returns:
        Access token string, or None on failure
    """
    import time

    # Return cached token if still valid
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        logger.error("Spotify credentials not configured (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)")
        return None

    auth_string = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    auth_b64 = base64.b64encode(auth_string.encode()).decode()

    try:
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            headers={
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            data = response.json()
            _token_cache["token"] = data["access_token"]
            _token_cache["expires_at"] = time.time() + data["expires_in"]
            logger.debug("Obtained Spotify access token")
            return _token_cache["token"]
        else:
            logger.error(f"Failed to get Spotify token: {response.status_code} {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Spotify auth request failed: {e}")
        return None


def search_track(artist: str, title: str) -> dict | None:
    """Search for a track on Spotify by artist and title.

    Args:
        artist: Artist name
        title: Track title

    Returns:
        Spotify track object, or None if not found
    """
    token = get_access_token()
    if not token:
        return None

    query = f"artist:{artist} track:{title}"

    try:
        response = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"q": query, "type": "track", "limit": 1},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            data = response.json()
            tracks = data.get("tracks", {}).get("items", [])
            if tracks:
                track = tracks[0]
                logger.debug(f"Found Spotify track: {track['name']} by {track['artists'][0]['name']}")
                return track
            else:
                logger.debug(f"No Spotify match for: {artist} - {title}")
                return None
        elif response.status_code == 429:
            # Rate limited
            retry_after = int(response.headers.get("Retry-After", 30))
            logger.warning(f"Spotify rate limited, waiting {retry_after}s")
            sleep(retry_after)
            return search_track(artist, title)  # Retry
        else:
            logger.warning(f"Spotify search failed: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Spotify search request failed: {e}")
        return None


def search_track_by_isrc(isrc: str) -> dict | None:
    """Search for a track on Spotify by ISRC.

    Args:
        isrc: International Standard Recording Code

    Returns:
        Spotify track object, or None if not found
    """
    token = get_access_token()
    if not token:
        return None

    try:
        response = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"q": f"isrc:{isrc}", "type": "track", "limit": 1},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            data = response.json()
            tracks = data.get("tracks", {}).get("items", [])
            if tracks:
                track = tracks[0]
                logger.debug(f"Found Spotify track by ISRC {isrc}: {track['name']}")
                return track
            else:
                logger.debug(f"No Spotify match for ISRC: {isrc}")
                return None
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            logger.warning(f"Spotify rate limited, waiting {retry_after}s")
            sleep(retry_after)
            return search_track_by_isrc(isrc)
        else:
            logger.warning(f"Spotify ISRC search failed: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Spotify ISRC search request failed: {e}")
        return None


def get_spotify_id_from_musicbrainz(mbid: str) -> str | None:
    """Look up Spotify ID via MusicBrainz external links.

    Args:
        mbid: MusicBrainz recording ID

    Returns:
        Spotify track ID, or None if not linked
    """
    try:
        response = requests.get(
            f"https://musicbrainz.org/ws/2/recording/{mbid}",
            params={"inc": "url-rels", "fmt": "json"},
            headers={"User-Agent": "MusicOrganizer/1.0 (github.com/user/music_organizer)"},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            data = response.json()
            relations = data.get("relations", [])
            for rel in relations:
                url = rel.get("url", {}).get("resource", "")
                if "open.spotify.com/track/" in url:
                    # Extract Spotify ID from URL
                    spotify_id = url.split("/track/")[-1].split("?")[0]
                    logger.debug(f"Found Spotify ID via MusicBrainz: {spotify_id}")
                    return spotify_id
            logger.debug(f"No Spotify link in MusicBrainz for MBID {mbid}")
            return None
        else:
            logger.debug(f"MusicBrainz lookup failed for {mbid}: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"MusicBrainz request failed: {e}")
        return None


def get_audio_features(spotify_id: str) -> dict | None:
    """Get audio features for a Spotify track.

    Args:
        spotify_id: Spotify track ID

    Returns:
        Dict with audio features, or None on failure
    """
    token = get_access_token()
    if not token:
        return None

    try:
        response = requests.get(
            f"https://api.spotify.com/v1/audio-features/{spotify_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            features = response.json()
            logger.debug(f"Got audio features for {spotify_id}: tempo={features.get('tempo')}")
            return features
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            logger.warning(f"Spotify rate limited, waiting {retry_after}s")
            sleep(retry_after)
            return get_audio_features(spotify_id)
        else:
            logger.warning(f"Spotify audio features failed: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Spotify audio features request failed: {e}")
        return None


def get_audio_features_batch(spotify_ids: list[str]) -> dict[str, dict]:
    """Get audio features for multiple tracks (up to 100).

    Args:
        spotify_ids: List of Spotify track IDs (max 100)

    Returns:
        Dict mapping spotify_id to audio features
    """
    if not spotify_ids:
        return {}

    token = get_access_token()
    if not token:
        return {}

    # API limit is 100 per request
    spotify_ids = spotify_ids[:100]

    try:
        response = requests.get(
            "https://api.spotify.com/v1/audio-features",
            headers={"Authorization": f"Bearer {token}"},
            params={"ids": ",".join(spotify_ids)},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            data = response.json()
            results = {}
            for features in data.get("audio_features", []):
                if features:  # Can be null for unavailable tracks
                    results[features["id"]] = features
            logger.debug(f"Got audio features for {len(results)}/{len(spotify_ids)} tracks")
            return results
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            logger.warning(f"Spotify rate limited, waiting {retry_after}s")
            sleep(retry_after)
            return get_audio_features_batch(spotify_ids)
        else:
            logger.warning(f"Spotify batch audio features failed: {response.status_code}")
            return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"Spotify batch request failed: {e}")
        return {}


def lookup_track_and_features(
    artist: str,
    title: str,
    mbid: str | None = None,
    isrc: str | None = None,
) -> tuple[str | None, dict | None]:
    """Look up a track on Spotify and get its audio features.

    Tries multiple lookup methods in order:
    1. MBID -> MusicBrainz -> Spotify link
    2. ISRC -> Spotify search
    3. Artist + title -> Spotify search

    Args:
        artist: Artist name
        title: Track title
        mbid: Optional MusicBrainz recording ID
        isrc: Optional ISRC code

    Returns:
        Tuple of (spotify_id, audio_features) or (None, None) if not found
    """
    spotify_id = None

    # Try MBID -> MusicBrainz -> Spotify link
    if mbid:
        spotify_id = get_spotify_id_from_musicbrainz(mbid)

    # Try ISRC search
    if not spotify_id and isrc:
        track = search_track_by_isrc(isrc)
        if track:
            spotify_id = track["id"]

    # Fall back to artist + title search
    if not spotify_id:
        track = search_track(artist, title)
        if track:
            spotify_id = track["id"]

    if not spotify_id:
        return None, None

    # Get audio features
    features = get_audio_features(spotify_id)
    return spotify_id, features


def extract_useful_features(features: dict) -> dict:
    """Extract the most useful features from Spotify audio features.

    Args:
        features: Full Spotify audio features response

    Returns:
        Dict with selected features
    """
    if not features:
        return {}

    return {
        "spotify_id": features.get("id"),
        "bpm": round(features.get("tempo", 0)) if features.get("tempo") else None,
        "energy": features.get("energy"),
        "danceability": features.get("danceability"),
        "valence": features.get("valence"),  # "happiness"
        "acousticness": features.get("acousticness"),
        "instrumentalness": features.get("instrumentalness"),
        "key": features.get("key"),
        "mode": features.get("mode"),  # 0=minor, 1=major
        "time_signature": features.get("time_signature"),
    }
