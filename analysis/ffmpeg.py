import json
import os
import subprocess as s

from dotenv import load_dotenv
from loguru import logger

from db.database import Database

load_dotenv()

# Path mapping configuration from environment
MUSIC_PATH_PREFIX_PLEX = os.getenv("MUSIC_PATH_PREFIX_PLEX", "")
MUSIC_PATH_PREFIX_LOCAL = os.getenv("MUSIC_PATH_PREFIX_LOCAL", "")
MUSIC_PATH_PREFIX_PLEX_TEST = os.getenv("MUSIC_PATH_PREFIX_PLEX_TEST", "")
MUSIC_PATH_PREFIX_LOCAL_TEST = os.getenv("MUSIC_PATH_PREFIX_LOCAL_TEST", "")


def check_ffprobe_available() -> bool:
    """
    Check if ffprobe is installed and accessible.

    Returns:
        True if ffprobe is available, False otherwise
    """
    try:
        result = s.run(["ffprobe", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            logger.debug("ffprobe is available")
            return True
        else:
            logger.warning("ffprobe returned non-zero exit code")
            return False
    except FileNotFoundError:
        logger.error("ffprobe not found - please install ffmpeg")
        return False
    except Exception as e:
        logger.error(f"Error checking ffprobe availability: {e}")
        return False


def validate_path_mapping(use_test: bool = False) -> dict:
    """
    Validate that path mapping is configured and paths are accessible.

    Args:
        use_test: If True, validate test path mapping; otherwise validate production

    Returns:
        dict with keys:
            'configured': bool - env vars are set
            'accessible': bool - local path exists and is readable
            'plex_prefix': str - the Plex path prefix
            'local_prefix': str - the local path prefix
            'sample_file_ok': bool - at least one file is accessible (if accessible=True)
            'errors': list[str] - any error messages
    """
    result = {
        "configured": False,
        "accessible": False,
        "plex_prefix": "",
        "local_prefix": "",
        "sample_file_ok": False,
        "errors": [],
    }

    if use_test:
        plex_prefix = MUSIC_PATH_PREFIX_PLEX_TEST
        local_prefix = MUSIC_PATH_PREFIX_LOCAL_TEST
        env_name = "test"
    else:
        plex_prefix = MUSIC_PATH_PREFIX_PLEX
        local_prefix = MUSIC_PATH_PREFIX_LOCAL
        env_name = "production"

    result["plex_prefix"] = plex_prefix
    result["local_prefix"] = local_prefix

    # Check if env vars are configured
    if not plex_prefix:
        result["errors"].append(
            f"MUSIC_PATH_PREFIX_PLEX{'_TEST' if use_test else ''} not configured"
        )
    if not local_prefix:
        result["errors"].append(
            f"MUSIC_PATH_PREFIX_LOCAL{'_TEST' if use_test else ''} not configured"
        )

    if not plex_prefix or not local_prefix:
        logger.warning(f"Path mapping not configured for {env_name}")
        return result

    result["configured"] = True

    # Check if local path exists
    if os.path.isdir(local_prefix):
        result["accessible"] = True
        logger.info(f"Local path accessible: {local_prefix}")

        # Try to find at least one audio file to verify read access
        for root, _dirs, files in os.walk(local_prefix):
            for f in files:
                if f.lower().endswith((".mp3", ".flac", ".m4a")):
                    test_file = os.path.join(root, f)
                    if os.access(test_file, os.R_OK):
                        result["sample_file_ok"] = True
                        logger.debug(f"Sample file accessible: {test_file}")
                        break
            if result["sample_file_ok"]:
                break

        if not result["sample_file_ok"]:
            result["errors"].append(f"No readable audio files found in {local_prefix}")
            logger.warning(f"No readable audio files found in {local_prefix}")
    else:
        result["errors"].append(f"Local path not accessible: {local_prefix}")
        logger.warning(f"Local path not accessible: {local_prefix} - is the mount available?")

    return result


def map_plex_path_to_local(plex_path: str, use_test: bool = False) -> str | None:
    """
    Map a Plex-stored filepath to a locally accessible path.

    Args:
        plex_path: Path as stored in database (from Plex)
        use_test: If True, use test path mapping; otherwise use production

    Returns:
        Local path if mapping succeeds, None if path cannot be mapped

    Example:
        Plex path: /volume1/media/music/test-library/Artist/Album/track.flac
        Local path: /mnt/synology-temp/music/test-library/Artist/Album/track.flac
    """
    if use_test:
        prefix_from = MUSIC_PATH_PREFIX_PLEX_TEST
        prefix_to = MUSIC_PATH_PREFIX_LOCAL_TEST
    else:
        prefix_from = MUSIC_PATH_PREFIX_PLEX
        prefix_to = MUSIC_PATH_PREFIX_LOCAL

    if not prefix_from or not prefix_to:
        logger.debug(f"Path mapping not configured (use_test={use_test})")
        return None

    if not plex_path:
        return None

    if plex_path.startswith(prefix_from):
        local_path = plex_path.replace(prefix_from, prefix_to, 1)
        return local_path
    else:
        logger.debug(f"Path does not match expected prefix '{prefix_from}': {plex_path[:80]}...")
        return None


def verify_path_accessible(filepath: str) -> bool:
    """
    Check if a file path exists and is readable.

    Args:
        filepath: Full path to the file

    Returns:
        True if file exists and is readable, False otherwise
    """
    if not filepath:
        return False
    return os.path.isfile(filepath) and os.access(filepath, os.R_OK)


def _get_tag_safe(track_info: dict, tag_names: list[str]) -> str | None:
    """
    Safely extract a tag from ffprobe output, trying multiple name variants.

    Different tagging tools and formats use different tag names:
    - MusicBrainz Picard: 'MusicBrainz Track Id', 'MusicBrainz Artist Id'
    - Some formats: 'MUSICBRAINZ_TRACKID', 'MUSICBRAINZ_ARTISTID'
    - Others: 'musicbrainz_trackid', 'musicbrainz_artistid'

    Args:
        track_info: Dict from ffprobe JSON output
        tag_names: List of tag name variants to try (case-insensitive)

    Returns:
        Tag value if found, None otherwise
    """
    if not track_info:
        return None

    try:
        tags = track_info.get("format", {}).get("tags", {})
        if not tags:
            return None

        # Build case-insensitive lookup
        tags_lower = {k.lower(): v for k, v in tags.items()}

        for name in tag_names:
            value = tags_lower.get(name.lower())
            if value:
                return value.strip()

        return None
    except Exception as e:
        logger.debug(f"Error extracting tag {tag_names}: {e}")
        return None


# Tag name variants for MusicBrainz IDs
# Different taggers and formats use different names for the same data
TRACK_MBID_TAGS = [
    "MusicBrainz Track Id",
    "MUSICBRAINZ_TRACKID",
    "musicbrainz_trackid",
    "MusicBrainz Recording Id",
    "MUSICBRAINZ_RECORDINGID",
    "musicbrainz_recordingid",
    "MusicBrainz Release Track Id",  # Picard uses this
    "MUSICBRAINZ_RELEASETRACKID",
    "musicbrainz_releasetrackid",
]

ARTIST_MBID_TAGS = [
    "MusicBrainz Artist Id",
    "MUSICBRAINZ_ARTISTID",
    "musicbrainz_artistid",
]

ARTIST_NAME_TAGS = [
    "artist",
    "ARTIST",
    "Artist",
    "album_artist",
    "ALBUM_ARTIST",
]

ACOUSTID_TAGS = [
    "Acoustid Id",  # Picard's format
    "ACOUSTID_ID",
    "acoustid_id",
    "Acoustid_Id",
]


def ffmpeg_get_info(filepath: str) -> dict | None:
    """
    Get audio file metadata using ffprobe.

    Args:
        filepath: Path to audio file

    Returns:
        Dict with ffprobe JSON output, or None on error
    """
    if not filepath:
        return None

    logger.debug(f"Getting ffprobe info for {filepath}")
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        filepath,
    ]

    try:
        result = s.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.debug(f"ffprobe error for {filepath}: {result.stderr}")
            return None

        return json.loads(result.stdout)
    except Exception as e:
        logger.debug(f"Exception running ffprobe for {filepath}: {e}")
        return None


def ffmpeg_get_mbtid(track_info: dict) -> str | None:
    """
    Extract MusicBrainz Track/Recording ID from ffprobe output.

    Args:
        track_info: Dict from ffprobe JSON output

    Returns:
        MusicBrainz ID if found, None otherwise
    """
    mbid = _get_tag_safe(track_info, TRACK_MBID_TAGS)

    if mbid:
        logger.debug(f"Found track MBID: {mbid}")

    return mbid


def ffmpeg_get_artist_mbid(track_info: dict) -> str | None:
    """
    Extract MusicBrainz Artist ID from ffprobe output.

    Args:
        track_info: Dict from ffprobe JSON output

    Returns:
        MusicBrainz Artist ID if found, None otherwise
    """
    mbid = _get_tag_safe(track_info, ARTIST_MBID_TAGS)

    if mbid:
        logger.debug(f"Found artist MBID: {mbid}")

    return mbid


def ffmpeg_get_artist_name(track_info: dict) -> str | None:
    """
    Extract artist name from ffprobe output.

    Args:
        track_info: Dict from ffprobe JSON output

    Returns:
        Artist name if found, None otherwise
    """
    return _get_tag_safe(track_info, ARTIST_NAME_TAGS)


def ffmpeg_get_acoustid(track_info: dict) -> str | None:
    """
    Extract AcousticID from ffprobe output.

    AcousticID is a fingerprint-based identifier that Picard embeds when
    it finds a match via acoustic fingerprinting.

    Args:
        track_info: Dict from ffprobe JSON output

    Returns:
        AcousticID if found, None otherwise
    """
    acoustid = _get_tag_safe(track_info, ACOUSTID_TAGS)

    if acoustid:
        logger.debug(f"Found AcousticID: {acoustid}")

    return acoustid


def ffmpeg_get_track_artist_and_artist_mbid(track_info: dict) -> tuple[str | None, str | None]:
    """
    Extract artist name and MusicBrainz Artist ID from ffprobe output.

    Args:
        track_info: Dict from ffprobe JSON output

    Returns:
        Tuple of (artist_name, artist_mbid), either may be None
    """
    artist_name = ffmpeg_get_artist_name(track_info)
    artist_mbid = ffmpeg_get_artist_mbid(track_info)

    return artist_name, artist_mbid


def convert_m4a_to_wav(input_file):
    """Convert an .m4a file to .wav in a persistent temp directory with logging."""
    temp_dir = "temp"
    output_file = os.path.join(temp_dir, os.path.splitext(os.path.basename(input_file))[0] + ".wav")

    logger.info(f"Starting conversion: {input_file} -> {output_file}")
    logger.debug("Generating ffmpeg command for conversion.")

    cmd = ["ffmpeg", "-i", input_file, "-acodec", "pcm_s16le", "-ar", "44100", output_file, "-y"]

    logger.debug(f"Running command: {' '.join(cmd)}")

    try:
        result = s.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"ffmpeg error processing {input_file}: {result.stderr}")
            raise Exception(f"ffmpeg error: {result.stderr}")

        logger.info(f"Successfully converted {input_file} -> {output_file}")
        return output_file

    except Exception:
        logger.exception(f"Exception occurred while converting {input_file}")
        return None


def cleanup_temp_file(file_path: str):
    """
    Clean up temporary files after processing.
    Args:
        file_path:

    Returns:

    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed temporary file: {file_path}")
        else:
            logger.warning(f"Temporary file not found for cleanup: {file_path}")
    except Exception as e:
        logger.error(f"Error removing temporary file {file_path}: {e}")


def process_mbid_from_files(
    database: Database,
    use_test_paths: bool = False,
    batch_size: int = 100,
    limit: int | None = None,
    include_researched: bool = False,
) -> dict:
    """
    Extract MusicBrainz IDs and AcousticIDs from audio files and update database.

    Queries tracks that need metadata extraction (missing MBID or AcousticID),
    maps their paths to local filesystem, extracts IDs using ffprobe, and
    updates the database.

    Args:
        database: Database connection
        use_test_paths: If True, use test path mapping; otherwise use production
        batch_size: Log progress every N tracks
        limit: Optional limit on number of tracks to process (for testing)
        include_researched: If True, process all tracks missing MBID/AcousticID.
            If False (default), only process tracks where researched_at IS NULL.

    Returns:
        Dict with stats:
            'total': int - tracks queried
            'accessible': int - files that could be accessed
            'inaccessible': int - files that couldn't be accessed
            'mbid': dict - MBID extraction stats (extracted, updated, errors)
            'acoustid': dict - AcousticID extraction stats (extracted, updated, errors)
            'skipped': bool - True if skipped due to config/environment issues
    """
    stats = {
        "total": 0,
        "accessible": 0,
        "inaccessible": 0,
        "mbid": {
            "extracted": 0,
            "updated": 0,
            "errors": 0,
        },
        "acoustid": {
            "extracted": 0,
            "updated": 0,
            "errors": 0,
        },
        "skipped": False,
    }

    # Validate environment first
    if not check_ffprobe_available():
        logger.warning("ffprobe not available - skipping metadata extraction from files")
        stats["skipped"] = True
        return stats

    path_validation = validate_path_mapping(use_test=use_test_paths)
    if not path_validation["configured"]:
        logger.warning("Path mapping not configured - skipping metadata extraction from files")
        stats["skipped"] = True
        return stats

    if not path_validation["accessible"]:
        logger.warning(
            f"Music path not accessible: {path_validation['local_prefix']} - "
            "skipping metadata extraction from files"
        )
        stats["skipped"] = True
        return stats

    # Query tracks that need MBID or AcousticID extraction
    database.connect()
    query = """
        SELECT id, filepath, musicbrainz_id, acoustid
        FROM track_data
        WHERE filepath IS NOT NULL AND filepath != ''
          AND (
            (musicbrainz_id IS NULL OR musicbrainz_id = '')
            OR (acoustid IS NULL OR acoustid = '')
          )
    """
    if not include_researched:
        query += " AND researched_at IS NULL"
    if limit:
        query += f" LIMIT {limit}"

    tracks = database.execute_select_query(query)
    database.close()

    if not tracks:
        logger.info("No tracks need metadata extraction")
        return stats

    stats["total"] = len(tracks)
    logger.info(f"Processing {stats['total']} tracks for MBID/AcousticID extraction")

    # Process each track
    for i, (track_id, plex_path, existing_mbid, existing_acoustid) in enumerate(tracks):
        # Map Plex path to local path
        local_path = map_plex_path_to_local(plex_path, use_test=use_test_paths)

        if not local_path or not verify_path_accessible(local_path):
            stats["inaccessible"] += 1
            continue

        stats["accessible"] += 1

        # Extract metadata from file
        track_info = ffmpeg_get_info(local_path)
        if not track_info:
            continue

        # Extract and update MBID if needed
        needs_mbid = not existing_mbid or existing_mbid == ''
        if needs_mbid:
            mbid = ffmpeg_get_mbtid(track_info)
            if mbid:
                stats["mbid"]["extracted"] += 1
                try:
                    database.execute_query(
                        "UPDATE track_data SET musicbrainz_id = ? WHERE id = ?",
                        (mbid, track_id)
                    )
                    stats["mbid"]["updated"] += 1
                except Exception as e:
                    logger.error(f"Error updating track {track_id} with MBID {mbid}: {e}")
                    stats["mbid"]["errors"] += 1

        # Extract and update AcousticID if needed
        needs_acoustid = not existing_acoustid or existing_acoustid == ''
        if needs_acoustid:
            acoustid = ffmpeg_get_acoustid(track_info)
            if acoustid:
                stats["acoustid"]["extracted"] += 1
                try:
                    database.execute_query(
                        "UPDATE track_data SET acoustid = ? WHERE id = ?",
                        (acoustid, track_id)
                    )
                    stats["acoustid"]["updated"] += 1
                except Exception as e:
                    logger.error(f"Error updating track {track_id} with AcousticID: {e}")
                    stats["acoustid"]["errors"] += 1

        # Progress logging
        if (i + 1) % batch_size == 0:
            logger.info(
                f"Progress: {i + 1}/{stats['total']} tracks processed, "
                f"{stats['mbid']['extracted']} MBIDs, {stats['acoustid']['extracted']} AcousticIDs"
            )

    logger.info(
        f"Metadata extraction complete: {stats['total']} tracks, "
        f"{stats['accessible']} accessible, {stats['mbid']['updated']} MBIDs updated, "
        f"{stats['acoustid']['updated']} AcousticIDs updated"
    )

    return stats


def process_artist_mbid_from_files(
    database: Database,
    use_test_paths: bool = False,
    include_researched: bool = False,
) -> dict:
    """
    Extract MusicBrainz Artist IDs from audio files and update artists table.

    Samples one track per artist (that doesn't have an MBID) to extract artist MBIDs.

    Args:
        database: Database connection
        use_test_paths: If True, use test path mapping; otherwise use production
        include_researched: If True, process all artists missing MBID.
            If False (default), only sample from tracks where researched_at IS NULL.

    Returns:
        Dict with stats:
            'total': int - artists queried
            'extracted': int - artist MBIDs found
            'updated': int - database rows updated
            'errors': int - database update errors
            'skipped': bool - True if skipped due to config/environment issues
    """
    stats = {
        "total": 0,
        "extracted": 0,
        "updated": 0,
        "errors": 0,
        "skipped": False,
    }

    # Validate environment first
    if not check_ffprobe_available():
        logger.warning("ffprobe not available - skipping artist MBID extraction from files")
        stats["skipped"] = True
        return stats

    path_validation = validate_path_mapping(use_test=use_test_paths)
    if not path_validation["configured"] or not path_validation["accessible"]:
        logger.warning("Path mapping not configured/accessible - skipping artist MBID extraction")
        stats["skipped"] = True
        return stats

    # Query artists without MBIDs, with a sample track for each
    database.connect()
    query = """
        SELECT a.id, a.artist, MIN(td.filepath) as sample_filepath
        FROM artists a
        JOIN track_data td ON td.artist_id = a.id
        WHERE (a.musicbrainz_id IS NULL OR a.musicbrainz_id = '')
        AND td.filepath IS NOT NULL AND td.filepath != ''
    """
    if not include_researched:
        query += " AND td.researched_at IS NULL"
    query += " GROUP BY a.id, a.artist"
    artists = database.execute_select_query(query)
    database.close()

    if not artists:
        logger.info("No artists without MBIDs found")
        return stats

    stats["total"] = len(artists)
    logger.info(f"Processing {stats['total']} artists for MBID extraction")

    for artist_id, artist_name, plex_path in artists:
        # Map Plex path to local path
        local_path = map_plex_path_to_local(plex_path, use_test=use_test_paths)

        if not local_path or not verify_path_accessible(local_path):
            continue

        # Extract artist MBID from file
        track_info = ffmpeg_get_info(local_path)
        if not track_info:
            continue

        artist_mbid = ffmpeg_get_artist_mbid(track_info)
        if not artist_mbid:
            continue

        stats["extracted"] += 1

        # Update database
        try:
            update_query = "UPDATE artists SET musicbrainz_id = ? WHERE id = ?"
            database.execute_query(update_query, (artist_mbid, artist_id))
            stats["updated"] += 1
            logger.debug(f"Updated artist '{artist_name}' with MBID {artist_mbid}")
        except Exception as e:
            logger.error(f"Error updating artist {artist_id} with MBID {artist_mbid}: {e}")
            stats["errors"] += 1

    logger.info(
        f"Artist MBID extraction complete: {stats['total']} artists, "
        f"{stats['extracted']} MBIDs found, {stats['updated']} updated"
    )

    return stats


def refresh_mbid_for_artists(
    database: Database,
    artist_names: list[str],
    use_test_paths: bool = False,
    dry_run: bool = False,
) -> dict:
    """Re-extract MBIDs and AcousticIDs from files for specific artists.

    Use after Picard has updated file tags. Unlike process_mbid_from_files(),
    this will update tracks even if they already have MBIDs.

    Args:
        database: Database connection
        artist_names: List of artist names to refresh
        use_test_paths: Use test path mapping
        dry_run: If True, log changes but don't update database

    Returns:
        dict with stats: artists_requested, artists_found, artists_not_found,
        tracks (total, accessible, inaccessible, extracted, missing, updated, unchanged, errors),
        artist_mbids (updated, unchanged, errors), acoustids (extracted, updated, unchanged, errors),
        dry_run
    """
    from db.db_functions import get_artist_names_found, get_tracks_by_artist_name

    stats = {
        "artists_requested": len(artist_names),
        "artists_found": 0,
        "artists_not_found": [],
        "tracks": {
            "total": 0,
            "accessible": 0,
            "inaccessible": 0,
            "extracted": 0,
            "missing": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        },
        "artist_mbids": {
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        },
        "acoustids": {
            "extracted": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        },
        "dry_run": dry_run,
        "skipped": False,
    }

    if not artist_names:
        logger.warning("No artist names provided")
        return stats

    # Validate environment first
    if not check_ffprobe_available():
        logger.warning("ffprobe not available - skipping MBID refresh")
        stats["skipped"] = True
        return stats

    path_validation = validate_path_mapping(use_test=use_test_paths)
    if not path_validation["configured"]:
        logger.warning("Path mapping not configured - skipping MBID refresh")
        stats["skipped"] = True
        return stats

    if not path_validation["accessible"]:
        logger.warning(
            f"Music path not accessible: {path_validation['local_prefix']} - skipping MBID refresh"
        )
        stats["skipped"] = True
        return stats

    # Check which artists exist
    found_artists = get_artist_names_found(database, artist_names)
    stats["artists_found"] = len(found_artists)

    # Identify missing artists (case-insensitive comparison)
    found_lower = {a.lower() for a in found_artists}
    stats["artists_not_found"] = [a for a in artist_names if a.lower() not in found_lower]

    if stats["artists_not_found"]:
        logger.warning(f"Artists not found in database: {stats['artists_not_found']}")

    if not found_artists:
        logger.warning("None of the requested artists were found in the database")
        return stats

    # Get all tracks for the specified artists
    tracks = get_tracks_by_artist_name(database, artist_names)
    stats["tracks"]["total"] = len(tracks)

    if not tracks:
        logger.info("No tracks found for the specified artists")
        return stats

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Processing {stats['tracks']['total']} tracks "
        f"for {stats['artists_found']} artists"
    )

    # Track artist MBIDs we discover (one per artist)
    artist_mbid_updates: dict[
        int, tuple[str, str | None, str | None]
    ] = {}  # artist_id -> (name, old_mbid, new_mbid)

    # Process each track
    for (
        track_id,
        plex_path,
        artist_name,
        existing_track_mbid,
        artist_id,
        existing_artist_mbid,
        existing_acoustid,
    ) in tracks:
        # Map Plex path to local path
        local_path = map_plex_path_to_local(plex_path, use_test=use_test_paths)

        if not local_path or not verify_path_accessible(local_path):
            stats["tracks"]["inaccessible"] += 1
            continue

        stats["tracks"]["accessible"] += 1

        # Extract metadata from file
        track_info = ffmpeg_get_info(local_path)
        if not track_info:
            stats["tracks"]["missing"] += 1
            continue

        # Extract track MBID
        new_track_mbid = ffmpeg_get_mbtid(track_info)
        if not new_track_mbid:
            stats["tracks"]["missing"] += 1
            continue

        stats["tracks"]["extracted"] += 1

        # Compare track MBID
        if new_track_mbid == existing_track_mbid:
            stats["tracks"]["unchanged"] += 1
        else:
            # Log the change
            old_display = existing_track_mbid or "NULL"
            logger.info(
                f"{'[DRY RUN] ' if dry_run else ''}Track id={track_id} MBID: "
                f"{old_display} → {new_track_mbid}"
            )

            if not dry_run:
                try:
                    update_query = "UPDATE track_data SET musicbrainz_id = ? WHERE id = ?"
                    database.execute_query(update_query, (new_track_mbid, track_id))
                    stats["tracks"]["updated"] += 1
                except Exception as e:
                    logger.error(f"Error updating track {track_id}: {e}")
                    stats["tracks"]["errors"] += 1
            else:
                stats["tracks"]["updated"] += 1

        # Extract and update AcousticID
        new_acoustid = ffmpeg_get_acoustid(track_info)
        if new_acoustid:
            stats["acoustids"]["extracted"] += 1

            if new_acoustid == existing_acoustid:
                stats["acoustids"]["unchanged"] += 1
            else:
                old_display = existing_acoustid or "NULL"
                logger.info(
                    f"{'[DRY RUN] ' if dry_run else ''}Track id={track_id} AcousticID: "
                    f"{old_display} → {new_acoustid}"
                )

                if not dry_run:
                    try:
                        update_query = "UPDATE track_data SET acoustid = ? WHERE id = ?"
                        database.execute_query(update_query, (new_acoustid, track_id))
                        stats["acoustids"]["updated"] += 1
                    except Exception as e:
                        logger.error(f"Error updating track {track_id} acoustid: {e}")
                        stats["acoustids"]["errors"] += 1
                else:
                    stats["acoustids"]["updated"] += 1

        # Extract artist MBID (only need one per artist)
        if artist_id not in artist_mbid_updates:
            new_artist_mbid = ffmpeg_get_artist_mbid(track_info)
            if new_artist_mbid:
                artist_mbid_updates[artist_id] = (
                    artist_name,
                    existing_artist_mbid,
                    new_artist_mbid,
                )

    # Process artist MBID updates
    for artist_id, (artist_name, old_mbid, new_mbid) in artist_mbid_updates.items():
        if new_mbid == old_mbid:
            stats["artist_mbids"]["unchanged"] += 1
        else:
            old_display = old_mbid or "NULL"
            logger.info(
                f'{"[DRY RUN] " if dry_run else ""}Artist "{artist_name}" (id={artist_id}): '
                f"{old_display} → {new_mbid}"
            )

            if not dry_run:
                try:
                    update_query = "UPDATE artists SET musicbrainz_id = ? WHERE id = ?"
                    database.execute_query(update_query, (new_mbid, artist_id))
                    stats["artist_mbids"]["updated"] += 1
                except Exception as e:
                    logger.error(f"Error updating artist {artist_id}: {e}")
                    stats["artist_mbids"]["errors"] += 1
            else:
                stats["artist_mbids"]["updated"] += 1

    logger.info(
        f"{'[DRY RUN] ' if dry_run else ''}Metadata refresh complete: "
        f"{stats['tracks']['total']} tracks, {stats['tracks']['updated']} MBID updates, "
        f"{stats['acoustids']['updated']} AcousticID updates, "
        f"{stats['artist_mbids']['updated']} artist MBID updates"
    )

    return stats
