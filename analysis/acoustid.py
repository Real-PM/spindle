"""
AcoustID API integration for resolving AcousticID to MusicBrainz Recording ID.

AcoustID is a fingerprint-based audio identification service. When Picard tags
a file using fingerprint matching, it embeds the AcousticID. We can use this
to look up the corresponding MusicBrainz Recording ID (MBID).

API docs: https://acoustid.org/webservice
Requires: ACOUSTID_API_KEY environment variable (get one at https://acoustid.org/api-key)
"""

import os
from time import sleep

import requests
from loguru import logger

# AcoustID API endpoint
LOOKUP_URL = "https://api.acoustid.org/v2/lookup"

# Rate limiting
REQUEST_DELAY = 0.34  # ~3 requests per second (API limit)


def get_api_key() -> str | None:
    """Get AcoustID API key from environment."""
    return os.getenv("ACOUSTID_API_KEY")


def lookup_mbid_by_acoustid(acoustid: str, api_key: str | None = None) -> str | None:
    """
    Look up MusicBrainz Recording ID from an AcousticID.

    Args:
        acoustid: The AcousticID to look up
        api_key: AcoustID API key (uses env var if not provided)

    Returns:
        MusicBrainz Recording ID if found, None otherwise
    """
    if not api_key:
        api_key = get_api_key()

    if not api_key:
        logger.warning("ACOUSTID_API_KEY not configured - cannot resolve AcousticID to MBID")
        return None

    params = {
        "client": api_key,
        "trackid": acoustid,
        "meta": "recordings",
    }

    try:
        response = requests.get(LOOKUP_URL, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if data.get("status") != "ok":
                logger.warning(f"AcoustID API error: {data.get('error', {}).get('message', 'unknown')}")
                return None

            results = data.get("results", [])
            if not results:
                logger.debug(f"No results for AcousticID {acoustid}")
                return None

            # Get the first result with recordings
            for result in results:
                recordings = result.get("recordings", [])
                if recordings:
                    # Return the first recording's MBID
                    mbid = recordings[0].get("id")
                    if mbid:
                        logger.debug(f"Resolved AcousticID {acoustid[:8]}... to MBID {mbid}")
                        return mbid

            logger.debug(f"No recordings found for AcousticID {acoustid}")
            return None

        elif response.status_code == 429:
            logger.warning("AcoustID rate limit exceeded")
            return None
        else:
            logger.error(f"AcoustID API error {response.status_code}")
            return None

    except requests.RequestException as e:
        logger.error(f"AcoustID request failed: {e}")
        return None


def bulk_lookup_mbid(
    acoustids: list[str],
    api_key: str | None = None,
) -> dict[str, str]:
    """
    Look up MusicBrainz Recording IDs for multiple AcousticIDs.

    Note: AcoustID API doesn't have a true bulk endpoint, so this makes
    individual requests with rate limiting.

    Args:
        acoustids: List of AcousticIDs to look up
        api_key: AcoustID API key (uses env var if not provided)

    Returns:
        Dict mapping AcousticID -> MBID for successful lookups
    """
    if not api_key:
        api_key = get_api_key()

    if not api_key:
        logger.warning("ACOUSTID_API_KEY not configured - cannot resolve AcousticIDs to MBIDs")
        return {}

    results = {}
    total = len(acoustids)

    logger.info(f"Looking up MBIDs for {total} AcousticIDs")

    for i, acoustid in enumerate(acoustids):
        mbid = lookup_mbid_by_acoustid(acoustid, api_key)
        if mbid:
            results[acoustid] = mbid

        # Progress logging
        if (i + 1) % 50 == 0:
            logger.info(f"AcoustID progress: {i + 1}/{total} ({len(results)} resolved)")

        # Rate limiting
        if i < total - 1:  # Don't sleep after last request
            sleep(REQUEST_DELAY)

    logger.info(f"AcoustID lookup complete: {len(results)}/{total} resolved to MBIDs")
    return results


def resolve_acoustids_to_mbids(
    tracks: list[tuple[int, str]],
    api_key: str | None = None,
) -> dict[int, str]:
    """
    Resolve AcousticIDs to MBIDs for a list of tracks.

    Args:
        tracks: List of (track_id, acoustid) tuples
        api_key: AcoustID API key (uses env var if not provided)

    Returns:
        Dict mapping track_id -> MBID for successful lookups
    """
    if not api_key:
        api_key = get_api_key()

    if not api_key:
        logger.warning("ACOUSTID_API_KEY not configured - skipping AcousticID resolution")
        return {}

    results = {}
    total = len(tracks)

    logger.info(f"Resolving {total} AcousticIDs to MBIDs")

    for i, (track_id, acoustid) in enumerate(tracks):
        mbid = lookup_mbid_by_acoustid(acoustid, api_key)
        if mbid:
            results[track_id] = mbid

        # Progress logging
        if (i + 1) % 50 == 0:
            logger.info(f"AcoustID resolution progress: {i + 1}/{total} ({len(results)} resolved)")

        # Rate limiting
        if i < total - 1:
            sleep(REQUEST_DELAY)

    logger.info(f"AcoustID resolution complete: {len(results)}/{total} tracks resolved")
    return results
