"""
Pipeline orchestration functions for the music organizer.

Provides high-level functions to run the full pipeline or incremental updates.
"""

import os
import tempfile

from loguru import logger

import db.db_functions as dbf
import db.db_update as dbu
from analysis.ffmpeg import (
    process_artist_mbid_from_files,
    process_mbid_from_files,
    refresh_mbid_for_artists,
    validate_path_mapping,
)
from db.database import Database
from db.db_functions import add_acoustid_column
from plex.plex_library import (
    export_track_data,
    get_all_tracks,
    get_tracks_since_date,
    listify_track_data,
)


def validate_environment(
    database: Database,
    use_test: bool = True,
) -> dict:
    """
    Validate all prerequisites before running pipeline.

    Args:
        database: Database connection object
        use_test: Whether to validate test or production paths

    Returns:
        dict with keys: 'database_ok', 'paths_ok', 'ffprobe_ok', 'errors'
    """
    errors = []
    result = {
        "database_ok": False,
        "paths_ok": False,
        "ffprobe_ok": False,
        "errors": errors,
    }

    # Check database connection
    try:
        database.connect()
        database.execute_select_query("SELECT 1")
        result["database_ok"] = True
        database.close()
    except Exception as e:
        errors.append(f"Database connection failed: {e}")

    # Check path mapping
    path_validation = validate_path_mapping(use_test=use_test)
    if path_validation["configured"] and path_validation["accessible"]:
        result["paths_ok"] = True
        result["ffprobe_ok"] = path_validation.get("sample_file_ok", False)
    else:
        if not path_validation["configured"]:
            errors.append("Path mapping not configured")
        if not path_validation["accessible"]:
            errors.append(f"Path not accessible: {path_validation.get('local_prefix', 'unknown')}")

    return result


def insert_new_tracks(
    database: Database,
    track_data: list[dict],
    filepath_prefix: str = "",
) -> int:
    """
    Insert new tracks into the database, handling duplicates gracefully.

    Args:
        database: Database connection object
        track_data: List of track data dictionaries
        filepath_prefix: Prefix to strip from file paths

    Returns:
        Number of tracks inserted
    """
    if not track_data:
        return 0

    # Export to temp CSV (using existing insert_tracks function)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        csv_path = f.name

    try:
        export_track_data(track_data, csv_path)

        # Get existing plex_ids to avoid duplicates
        database.connect()
        existing_plex_ids = database.execute_select_query(
            "SELECT plex_id FROM track_data WHERE plex_id IS NOT NULL"
        )
        existing_ids = {row[0] for row in existing_plex_ids}
        database.close()

        # Filter out tracks that already exist
        new_tracks = [t for t in track_data if t["plex_id"] not in existing_ids]

        if not new_tracks:
            logger.info("No new tracks to insert (all already exist)")
            return 0

        # Re-export only new tracks
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            new_csv_path = f.name
        export_track_data(new_tracks, new_csv_path)

        # Insert new tracks
        dbf.insert_tracks(database, new_csv_path)
        os.unlink(new_csv_path)

        logger.info(f"Inserted {len(new_tracks)} new tracks")
        return len(new_tracks)

    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)


def add_new_artists(database: Database) -> int:
    """
    Add any new artists from track_data to the artists table.

    Args:
        database: Database connection object

    Returns:
        Number of new artists added
    """
    database.connect()

    # Find artists in track_data that aren't in artists table
    new_artists = database.execute_select_query("""
        SELECT DISTINCT td.artist
        FROM track_data td
        LEFT JOIN artists a ON td.artist = a.artist
        WHERE a.id IS NULL
    """)

    count = 0
    for (artist,) in new_artists:
        database.execute_query("INSERT INTO artists (artist) VALUES (?)", (artist,))
        count += 1
        logger.info(f"Added new artist: {artist}")

    # Update artist_id for tracks without it (SQLite compatible - no UPDATE...JOIN)
    database.execute_query("""
        UPDATE track_data SET artist_id = (
            SELECT a.id FROM artists a WHERE a.artist = track_data.artist
        ) WHERE artist_id IS NULL
    """)

    database.close()
    logger.info(f"Added {count} new artists")
    return count


def run_incremental_update(
    database: Database,
    music_library,
    filepath_prefix: str = "",
    use_test_paths: bool = True,
    since_date: str | None = None,
    skip_ffprobe: bool = False,
    skip_lastfm: bool = False,
    skip_bpm: bool = False,
    retry_bpm: bool = False,
    rate_limit_delay: float = 0.25,
) -> dict:
    """
    Process only tracks added since last update (or since_date).

    Args:
        database: Database connection object
        music_library: Plex music library object
        filepath_prefix: Prefix to strip from Plex file paths
        use_test_paths: Whether to use test path mappings
        since_date: Optional override for cutoff date (default: last history entry).
                   Format: 'YYYY-MM-DD'
        skip_ffprobe: Skip file-based MBID extraction
        skip_lastfm: Skip Last.fm enrichment
        skip_bpm: Skip BPM enrichment
        retry_bpm: If True, re-attempt BPM for all tracks with NULL BPM
            (including previously researched tracks). Default False.
        rate_limit_delay: Seconds between Last.fm API calls

    Returns:
        dict with stats from processing
    """
    stats = {
        "since_date": None,
        "new_tracks": 0,
        "new_artists": 0,
        "mbid_extraction": {},
        "lastfm_artist": {},
        "lastfm_stub": {},
        "lastfm_track": {},
        "bpm_essentia": {},
        "tracks_researched": 0,
    }

    # Determine cutoff date — use the newest track we already have
    if since_date:
        cutoff = since_date
    else:
        cutoff = dbf.get_latest_added_date(database)
        if cutoff:
            cutoff = cutoff.strftime("%Y-%m-%d") if hasattr(cutoff, 'strftime') else cutoff

    stats["since_date"] = cutoff

    if not cutoff:
        logger.warning("No history found and no since_date provided. Running full import.")
        # Fall through - get_tracks_since_date with None will get all tracks
        cutoff = "1970-01-01"  # Get all tracks

    logger.info(f"Running incremental update for tracks since {cutoff}")

    # Get new tracks from Plex
    new_tracks, count = get_tracks_since_date(music_library, cutoff)
    logger.info(f"Found {count} tracks added since {cutoff}")

    if count == 0:
        logger.info("No new tracks to process")
        return stats

    # Extract and insert new tracks
    track_data = listify_track_data(new_tracks, filepath_prefix)
    stats["new_tracks"] = insert_new_tracks(database, track_data, filepath_prefix)

    if stats["new_tracks"] == 0:
        logger.info("No new tracks inserted (all duplicates)")
        return stats

    # Add new artists
    stats["new_artists"] = add_new_artists(database)

    # Extract genres from new tracks
    genre_list = dbu.populate_genres_table_from_track_data(database)
    if genre_list:
        dbu.insert_genres_if_not_exists(database, genre_list)
        dbu.populate_track_genre_table(database)

    # Ensure acoustid column exists (migration is idempotent)
    add_acoustid_column(database)

    # MBID extraction (processes tracks without MBID)
    if not skip_ffprobe:
        logger.info("Running MBID extraction from files...")
        stats["mbid_extraction"]["tracks"] = process_mbid_from_files(
            database, use_test_paths=use_test_paths, include_researched=retry_bpm
        )
        stats["mbid_extraction"]["artists"] = process_artist_mbid_from_files(
            database, use_test_paths=use_test_paths, include_researched=retry_bpm
        )

    # Last.fm enrichment - targeted processing to avoid re-processing all records
    if not skip_lastfm:
        # 1. Find primary artists needing full enrichment (have tracks, no similar_artists)
        incomplete_primary = dbf.get_primary_artists_without_similar(database)
        primary_ids = [a[0] for a in incomplete_primary]
        logger.info(f"Found {len(primary_ids)} primary artists needing full enrichment")

        # 2. Full enrichment for primary artists (MBID + genres + similar)
        if primary_ids:
            stats["lastfm_artist"] = dbu.enrich_artists_full(
                database, artist_ids=primary_ids, rate_limit_delay=rate_limit_delay
            )
        else:
            stats["lastfm_artist"] = {"total": 0, "processed": 0, "skipped": "no primary artists"}

        # 3. Find stub artists needing core enrichment (no tracks, no MBID)
        incomplete_stubs = dbf.get_stub_artists_without_mbid(database)
        stub_ids = [a[0] for a in incomplete_stubs]
        logger.info(f"Found {len(stub_ids)} stub artists needing core enrichment")

        # 4. Core enrichment for stubs (MBID + genres only - no similar artists)
        if stub_ids:
            stats["lastfm_stub"] = dbu.enrich_artists_core(
                database, artist_ids=stub_ids, rate_limit_delay=rate_limit_delay
            )
        else:
            stats["lastfm_stub"] = {"total": 0, "processed": 0, "skipped": "no stub artists"}

        # 5. Track enrichment
        logger.info("Running Last.fm track enrichment...")
        stats["lastfm_track"] = dbu.process_lastfm_track_data(
            database,
            rate_limit_delay=rate_limit_delay,
        )

    # BPM enrichment (processes tracks without BPM)
    if not skip_bpm:
        logger.info("Running Essentia BPM analysis...")
        stats["bpm_essentia"] = dbu.process_bpm_essentia(
            database,
            use_test_paths=use_test_paths,
            batch_size=25,
            rest_between_batches=10.0,
            include_researched=retry_bpm,
        )
        stats["tracks_researched"] = dbu.mark_tracks_researched(database)

    # Record in history
    dbf.update_history(database, stats["new_tracks"])
    logger.info(f"Incremental update complete. {stats['new_tracks']} new tracks processed.")

    return stats


def run_full_pipeline(
    database: Database,
    music_library,
    filepath_prefix: str = "",
    use_test_paths: bool = True,
    skip_ffprobe: bool = False,
    skip_lastfm: bool = False,
    skip_bpm: bool = False,
    rate_limit_delay: float = 0.25,
) -> dict:
    """
    Execute the complete pipeline from Plex extraction to BPM enrichment.

    Args:
        database: Database connection object
        music_library: Plex music library object
        filepath_prefix: Prefix to strip from Plex file paths
        use_test_paths: Whether to use test path mappings
        skip_ffprobe: Skip file-based MBID extraction
        skip_lastfm: Skip Last.fm enrichment
        skip_bpm: Skip BPM enrichment
        rate_limit_delay: Seconds between Last.fm API calls

    Returns:
        dict with stats from each phase
    """
    stats = {
        "total_tracks": 0,
        "total_artists": 0,
        "mbid_extraction": {},
        "lastfm_artist": {},
        "lastfm_track": {},
        "bpm_essentia": {},
    }

    # Get all tracks from Plex
    tracks, count = get_all_tracks(music_library)
    logger.info(f"Retrieved {count} tracks from Plex")

    if count == 0:
        logger.warning("No tracks in library")
        return stats

    stats["total_tracks"] = count

    # Extract and insert tracks
    track_data = listify_track_data(tracks, filepath_prefix)
    insert_new_tracks(database, track_data, filepath_prefix)

    # Populate artists table
    dbf.populate_artists_table(database)
    dbf.populate_artist_id_column(database)

    database.connect()
    stats["total_artists"] = database.execute_select_query("SELECT COUNT(*) FROM artists")[0][0]
    database.close()

    # Extract genres from tracks
    genre_list = dbu.populate_genres_table_from_track_data(database)
    if genre_list:
        dbu.insert_genres_if_not_exists(database, genre_list)
        dbu.populate_track_genre_table(database)

    # Ensure acoustid column exists (migration is idempotent)
    add_acoustid_column(database)

    # MBID extraction
    if not skip_ffprobe:
        logger.info("Running MBID extraction from files...")
        stats["mbid_extraction"]["tracks"] = process_mbid_from_files(
            database, use_test_paths=use_test_paths, include_researched=True
        )
        stats["mbid_extraction"]["artists"] = process_artist_mbid_from_files(
            database, use_test_paths=use_test_paths, include_researched=True
        )

    # Last.fm enrichment
    if not skip_lastfm:
        logger.info("Running Last.fm artist enrichment...")
        dbu.insert_last_fm_artist_data(database, rate_limit_delay=rate_limit_delay)

        logger.info("Running Last.fm track enrichment...")
        stats["lastfm_track"] = dbu.process_lastfm_track_data(
            database,
            rate_limit_delay=rate_limit_delay,
        )

    # BPM enrichment
    if not skip_bpm:
        logger.info("Running Essentia BPM analysis...")
        stats["bpm_essentia"] = dbu.process_bpm_essentia(
            database,
            use_test_paths=use_test_paths,
            batch_size=25,
            rest_between_batches=10.0,
            include_researched=True,
        )
        dbu.mark_tracks_researched(database)

    # Record in history
    dbf.update_history(database, stats["total_tracks"])
    logger.info(f"Full pipeline complete. {stats['total_tracks']} tracks processed.")

    return stats


def refresh_metadata_for_artists(
    database: Database,
    artist_names: list[str],
    use_test_paths: bool = True,
    dry_run: bool = False,
) -> dict:
    """Refresh metadata for specific artists after Picard tagging.

    Re-extracts MBIDs from files and updates database. Use after manually
    tagging files with Picard.

    Args:
        database: Database connection
        artist_names: List of artist names to refresh
        use_test_paths: Use test path mapping
        dry_run: If True, show what would change without updating

    Returns:
        dict with extraction stats
    """
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Refreshing metadata for artists: {artist_names}")

    stats = refresh_mbid_for_artists(
        database=database,
        artist_names=artist_names,
        use_test_paths=use_test_paths,
        dry_run=dry_run,
    )

    if stats["skipped"]:
        logger.warning("Metadata refresh was skipped due to environment issues")
    elif stats["artists_found"] == 0:
        logger.warning("No matching artists found in database")
    else:
        logger.info(
            f"{'[DRY RUN] ' if dry_run else ''}Metadata refresh complete: "
            f"{stats['artists_found']}/{stats['artists_requested']} artists found, "
            f"{stats['tracks']['updated']} track MBIDs updated, "
            f"{stats['acoustids']['updated']} AcousticIDs updated, "
            f"{stats['artist_mbids']['updated']} artist MBIDs updated"
        )

    return stats
