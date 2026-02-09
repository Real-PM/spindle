import json
import os
import re
from time import sleep

from loguru import logger

import analysis.bpm as bpm_analysis
import analysis.lastfm as lastfm
from analysis.ffmpeg import (
    map_plex_path_to_local,
    validate_path_mapping,
    verify_path_accessible,
)
from analysis.genre_normalize import normalize_genre

from . import TEST_DB_PATH
from .database import Database

database = Database(TEST_DB_PATH)


def populate_genres_table_from_track_data(database: Database):
    logger.debug("Starting to populate genres table from track data.")
    database.connect()
    query = "SELECT genre FROM track_data"
    results = database.execute_select_query(query)
    genre_list = []

    for result in results:
        genre_str = result[0]
        if genre_str != "[]":
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                genre_list.extend(genres)
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    genre_list = list(set(genre_list))
    logger.info(f"Extracted genres: {genre_list}")
    database.close()
    logger.debug("Finished populating genres table from track data.")
    return genre_list


def insert_genres_if_not_exists(database: Database, genre_list: list):
    logger.debug("Starting to insert genres if not exists.")
    database.connect()

    existing_genres_query = "SELECT genre FROM genres"
    existing_genres = database.execute_select_query(existing_genres_query)
    existing_genres_set = {genre[0] for genre in existing_genres}

    new_genres = [genre for genre in genre_list if genre not in existing_genres_set]

    for genre in new_genres:
        database.execute_query("INSERT INTO genres (genre) VALUES (?)", (genre,))
        logger.info(f"Inserted new genre: {genre}")

    database.close()
    logger.debug("Finished inserting genres if not exists.")
    return new_genres


def populate_track_genre_table(database: Database):
    logger.debug("Starting to populate track genre table.")
    database.connect()
    query = "SELECT id, genre FROM track_data"
    results = database.execute_select_query(query)

    for result in results:
        track_id = result[0]
        genre_str = result[1]
        if genre_str != "[]":
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                for genre in genres:
                    genre_id_query = "SELECT id FROM genres WHERE genre = ?"
                    genre_id_result = database.execute_select_query(genre_id_query, (genre,))
                    if genre_id_result:
                        genre_id = genre_id_result[0][0]
                        database.execute_query(
                            "INSERT INTO track_genres (track_id, genre_id) VALUES (?, ?)",
                            (track_id, genre_id),
                        )
                        logger.info(
                            f"Inserted track-genre pair: track_id={track_id}, genre_id={genre_id}"
                        )
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    database.close()
    logger.debug("Finished populating track genre table.")
    return None


def update_track_genre_table(database: Database, cutoff: str = None):
    logger.debug("Starting to update track genre table.")
    database.connect()
    query_wo_cutoff = "SELECT id, genre FROM track_data"
    query_w_cutoff = "SELECT id, genre FROM track_data WHERE added_date > ?"

    if cutoff is None:
        results = database.execute_select_query(query_wo_cutoff)
    else:
        try:
            cutoff_date = re.sub(r"(\d{2})(\d{2})(\d{4})", r"\3-\1-\2", cutoff)
            results = database.execute_select_query(query_w_cutoff, (cutoff_date,))
        except Exception as e:
            logger.error(f"There was an error querying db with cutoff: {e}")
            results = []

    for result in results:
        track_id = result[0]
        genre_str = result[1]
        if genre_str != "[]":
            try:
                genre_str = genre_str.strip("[]").replace("'", "")
                genres = [genre.strip() for genre in genre_str.split(",")]
                for genre in genres:
                    genre_id_query = "SELECT id FROM genres WHERE genre = ?"
                    genre_id_result = database.execute_select_query(genre_id_query, (genre,))
                    if genre_id_result:
                        genre_id = genre_id_result[0][0]
                        database.execute_query(
                            "INSERT INTO track_genres (track_id, genre_id) VALUES (?, ?)",
                            (track_id, genre_id),
                        )
                        logger.info(
                            f"Inserted track-genre pair: track_id={track_id}, genre_id={genre_id}"
                        )
            except Exception as e:
                logger.error(f"Error processing genre string: {e} - genre_str: {genre_str}")

    database.close()
    logger.debug("Finished updating track genre table.")
    return None


def get_artists_from_db(database: Database):
    """
    Get all artists from artists table in the database. Return a list of artist names.
    :param database:
    :return:
    """
    logger.debug("Starting to get artists from db.")
    database.connect()
    query = "SELECT artist FROM artists"
    results = database.execute_select_query(query)
    artist_list = [result[0] for result in results]
    database.close()
    logger.debug("Finished getting artists from db.")
    return artist_list


def check_mbid_and_insert(database: Database, lastfm_json: json, mbid_list: list):
    """
    Check if the MBID is in the database and insert it in artists.musicbrainz_id if it is not.
    :param database:
    :param lastfm_json:
    :param mbid_list:
    :return:
    """
    database.connect()
    mbid = lastfm.get_artist_mbid(lastfm_json)
    if mbid not in mbid_list:
        artist = lastfm_json["artist"]["name"]
        database.execute_query(
            "UPDATE artists SET musicbrainz_id = ? WHERE artist = ?", (mbid, artist)
        )
        logger.info(f"Inserted MBID for {artist}: {mbid}")


def check_tags_and_insert(database: Database, lastfm_json: json, genre_list: list):
    database.connect()
    tags = lastfm.get_artist_tags(lastfm_json)
    for tag in tags:
        if tag.lower() not in [g.lower() for g in genre_list]:  # Case-insensitive check
            database.execute_query("INSERT INTO genres (genre) VALUES (?)", (tag,))
            logger.info(f"Inserted new genre: {tag}")
            genre_list.append(tag)  # Add to list to prevent duplicates
    database.close()


def _ensure_genre_alias(database: Database, raw_genre_id: int, raw_genre: str) -> None:
    """Create a genre_aliases entry for a raw genre if the table exists.

    Maps the raw genre to its normalized canonical form. If the canonical
    genre doesn't exist yet, inserts it into the genres table first.

    Args:
        database: Database connection (must already be connected)
        raw_genre_id: The genre ID of the raw genre
        raw_genre: The raw genre string (for normalization)
    """
    # Check if genre_aliases table exists
    check = database.execute_select_query(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='genre_aliases'"
    )
    if not check or check[0][0] == 0:
        return  # Table doesn't exist yet, skip

    # Check if alias already exists for this raw genre
    existing = database.execute_select_query(
        "SELECT id FROM genre_aliases WHERE raw_genre_id = ?", (raw_genre_id,)
    )
    if existing:
        return  # Already mapped

    # Normalize the genre
    canonical = normalize_genre(raw_genre)
    if not canonical:
        return

    # Find or create canonical genre
    canonical_row = database.execute_select_query(
        "SELECT id FROM genres WHERE LOWER(genre) = LOWER(?)", (canonical,)
    )
    if canonical_row:
        canonical_id = canonical_row[0][0]
    else:
        # Insert the canonical genre
        database.execute_query("INSERT INTO genres (genre) VALUES (?)", (canonical,))
        canonical_row = database.execute_select_query(
            "SELECT id FROM genres WHERE genre = ?", (canonical,)
        )
        if not canonical_row:
            return
        canonical_id = canonical_row[0][0]

    # Insert alias mapping
    database.execute_query(
        "INSERT OR IGNORE INTO genre_aliases (raw_genre_id, canonical_genre_id) VALUES (?, ?)",
        (raw_genre_id, canonical_id),
    )


def _process_artist_mbid_and_genres(
    database: Database,
    artist_id: int,
    artist_name: str,
    artist_info: dict,
) -> dict:
    """Process MBID and genres for a single artist (internal helper).

    Args:
        database: Database connection (must already be connected)
        artist_id: Artist ID in database
        artist_name: Artist name for logging
        artist_info: Last.fm API response for artist

    Returns:
        dict with 'mbid_updated' (bool) and 'genres_added' (int)
    """
    result = {"mbid_updated": False, "genres_added": 0}

    # Update MusicBrainz ID if available
    mbid = lastfm.get_artist_mbid(artist_info)
    if mbid:
        logger.debug(f"MBID for {artist_name}: {mbid}")
        database.execute_query(
            "UPDATE artists SET musicbrainz_id = ? WHERE id = ?", (mbid, artist_id)
        )
        result["mbid_updated"] = True

    # Process genres
    genres = lastfm.get_artist_tags(artist_info)
    for genre in genres:
        genre = genre.lower()
        try:
            # Insert genre if not exists - SQLite compatible
            database.execute_query(
                """
                INSERT OR IGNORE INTO genres (genre)
                SELECT ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM genres
                    WHERE LOWER(genre) = LOWER(?)
                )
            """,
                (genre, genre),
            )

            # Get genre ID
            genre_id = database.execute_select_query(
                "SELECT id FROM genres WHERE LOWER(genre) = LOWER(?)", (genre,)
            )[0][0]

            # Maintain genre_aliases for normalization
            _ensure_genre_alias(database, genre_id, genre)

            # Insert genre relationship if not exists
            database.execute_query(
                """
                INSERT OR IGNORE INTO artist_genres (artist_id, genre_id)
                SELECT ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM artist_genres
                    WHERE artist_id = ? AND genre_id = ?
                )
            """,
                (artist_id, genre_id, artist_id, genre_id),
            )

            result["genres_added"] += 1
            logger.debug(f"Processed genre for {artist_name}: {genre}")
        except Exception as e:
            logger.error(f"Error processing genre {genre} for {artist_name}: {e}")

    return result


def _process_similar_artists(
    database: Database,
    artist_id: int,
    artist_name: str,
    artist_info: dict,
) -> int:
    """Process similar artists for a single artist (internal helper).

    Args:
        database: Database connection (must already be connected)
        artist_id: Artist ID in database
        artist_name: Artist name for logging
        artist_info: Last.fm API response for artist

    Returns:
        Number of similar artists added
    """
    similar_artists = lastfm.get_similar_artists(artist_info)
    logger.debug(f"Similar artists for {artist_name}: {similar_artists}")

    added = 0
    for similar_artist in similar_artists:
        if not similar_artist:
            continue

        try:
            # Insert similar artist if not exists
            database.execute_query(
                """
                INSERT OR IGNORE INTO artists (artist)
                SELECT ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM artists
                    WHERE LOWER(artist) = LOWER(?)
                )
            """,
                (similar_artist, similar_artist),
            )

            # Get similar artist ID
            similar_artist_id = database.execute_select_query(
                "SELECT id FROM artists WHERE LOWER(artist) = LOWER(?)",
                (similar_artist,),
            )[0][0]

            # Insert similar artist relationship if not exists
            database.execute_query(
                """
                INSERT OR IGNORE INTO similar_artists (artist_id, similar_artist_id)
                SELECT ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM similar_artists
                    WHERE artist_id = ? AND similar_artist_id = ?
                )
            """,
                (artist_id, similar_artist_id, artist_id, similar_artist_id),
            )

            added += 1
            logger.debug(f"Processed similar artist: {artist_name} -> {similar_artist}")
        except Exception as e:
            logger.error(f"Error processing similar artist {similar_artist} for {artist_name}: {e}")

    return added


def enrich_artists_core(
    database: Database,
    artist_ids: list[int] | None = None,
    rate_limit_delay: float = 0.25,
) -> dict:
    """Enrich artists with MBID and genres only. Does NOT fetch similar artists.

    Use this for stub artists (added via similar_artists) to complete their data
    without expanding the artist graph.

    Args:
        database: Database connection object
        artist_ids: Optional list of artist IDs to process. If None, processes all.
        rate_limit_delay: Seconds between API calls. Default 0.25 (4 req/s).

    Returns:
        dict with stats: {'total': int, 'processed': int, 'mbid_updated': int, 'genres_added': int, 'failed': int}
    """
    stats = {
        "total": 0,
        "processed": 0,
        "mbid_updated": 0,
        "genres_added": 0,
        "failed": 0,
    }

    logger.info("Starting core artist enrichment (MBID + genres only)")
    logger.info(f"Rate limit delay: {rate_limit_delay}s ({1 / rate_limit_delay:.1f} req/s)")
    database.connect()

    try:
        # Build query based on whether artist_ids is provided
        if artist_ids is not None:
            if not artist_ids:  # Empty list = nothing to process
                logger.info("No artists to enrich (empty list)")
                database.close()
                return stats
            placeholders = ",".join(["?"] * len(artist_ids))
            query = f"SELECT id, artist FROM artists WHERE id IN ({placeholders})"
            artists = database.execute_select_query(query, tuple(artist_ids))
        else:
            artists = database.execute_select_query("SELECT id, artist FROM artists")

        stats["total"] = len(artists)
        logger.info(f"Found {stats['total']} artists to enrich (core)")

        for i, (artist_id, artist_name) in enumerate(artists):
            try:
                if i > 0:
                    sleep(rate_limit_delay)

                # Keep connection alive during long-running loops
                database.ensure_connection()

                artist_info = lastfm.get_artist_info(artist_name)

                # Mark enrichment attempted regardless of success
                database.execute_query(
                    "UPDATE artists SET enrichment_attempted_at = datetime('now') WHERE id = ?",
                    (artist_id,),
                )

                if not artist_info:
                    logger.warning(f"Failed to retrieve artist info for {artist_name}")
                    stats["failed"] += 1
                    continue

                result = _process_artist_mbid_and_genres(
                    database, artist_id, artist_name, artist_info
                )

                stats["processed"] += 1
                if result["mbid_updated"]:
                    stats["mbid_updated"] += 1
                stats["genres_added"] += result["genres_added"]

                if (i + 1) % 50 == 0:
                    logger.info(
                        f"Core enrichment progress: {i + 1}/{stats['total']} artists, "
                        f"{stats['mbid_updated']} MBIDs, {stats['genres_added']} genres"
                    )

            except Exception as e:
                logger.error(f"Error processing artist {artist_name}: {e}")
                stats["failed"] += 1

    except Exception as e:
        logger.error(f"Error in core artist enrichment: {e}")
        raise
    finally:
        database.close()

    logger.info(
        f"Core enrichment complete: {stats['processed']} artists, "
        f"{stats['mbid_updated']} MBIDs updated, {stats['genres_added']} genres added"
    )
    return stats


def enrich_artists_full(
    database: Database,
    artist_ids: list[int] | None = None,
    rate_limit_delay: float = 0.25,
) -> dict:
    """Enrich artists with MBID, genres, AND similar artists.

    Use this for primary artists (have tracks in library). Creates new stub
    artists in similar_artists table.

    Args:
        database: Database connection object
        artist_ids: Optional list of artist IDs to process. If None, processes all.
        rate_limit_delay: Seconds between API calls. Default 0.25 (4 req/s).

    Returns:
        dict with stats: {'total': int, 'processed': int, 'mbid_updated': int, 'genres_added': int, 'similar_added': int, 'failed': int}
    """
    stats = {
        "total": 0,
        "processed": 0,
        "mbid_updated": 0,
        "genres_added": 0,
        "similar_added": 0,
        "failed": 0,
    }

    logger.info("Starting full artist enrichment (MBID + genres + similar artists)")
    logger.info(f"Rate limit delay: {rate_limit_delay}s ({1 / rate_limit_delay:.1f} req/s)")
    database.connect()

    try:
        # Build query based on whether artist_ids is provided
        if artist_ids is not None:
            if not artist_ids:  # Empty list = nothing to process
                logger.info("No artists to enrich (empty list)")
                database.close()
                return stats
            placeholders = ",".join(["?"] * len(artist_ids))
            query = f"SELECT id, artist FROM artists WHERE id IN ({placeholders})"
            artists = database.execute_select_query(query, tuple(artist_ids))
        else:
            artists = database.execute_select_query("SELECT id, artist FROM artists")

        stats["total"] = len(artists)
        logger.info(f"Found {stats['total']} artists to enrich (full)")

        for i, (artist_id, artist_name) in enumerate(artists):
            try:
                if i > 0:
                    sleep(rate_limit_delay)

                # Keep connection alive during long-running loops
                database.ensure_connection()

                artist_info = lastfm.get_artist_info(artist_name)

                # Mark enrichment attempted regardless of success
                database.execute_query(
                    "UPDATE artists SET enrichment_attempted_at = datetime('now') WHERE id = ?",
                    (artist_id,),
                )

                if not artist_info:
                    logger.warning(f"Failed to retrieve artist info for {artist_name}")
                    stats["failed"] += 1
                    continue

                # Process MBID and genres
                result = _process_artist_mbid_and_genres(
                    database, artist_id, artist_name, artist_info
                )
                stats["processed"] += 1
                if result["mbid_updated"]:
                    stats["mbid_updated"] += 1
                stats["genres_added"] += result["genres_added"]

                # Process similar artists
                similar_count = _process_similar_artists(
                    database, artist_id, artist_name, artist_info
                )
                stats["similar_added"] += similar_count

                if (i + 1) % 50 == 0:
                    logger.info(
                        f"Full enrichment progress: {i + 1}/{stats['total']} artists, "
                        f"{stats['mbid_updated']} MBIDs, {stats['similar_added']} similar"
                    )

            except Exception as e:
                logger.error(f"Error processing artist {artist_name}: {e}")
                stats["failed"] += 1

    except Exception as e:
        logger.error(f"Error in full artist enrichment: {e}")
        raise
    finally:
        database.close()

    logger.info(
        f"Full enrichment complete: {stats['processed']} artists, "
        f"{stats['mbid_updated']} MBIDs, {stats['genres_added']} genres, "
        f"{stats['similar_added']} similar artists"
    )
    return stats


def insert_last_fm_artist_data(
    database: Database,
    artist_ids: list[int] | None = None,
    rate_limit_delay: float = 0.25,
) -> dict:
    """Legacy wrapper - calls enrich_artists_full() for all artists.

    Maintained for backward compatibility.

    Args:
        database: Database connection object
        artist_ids: Optional list of artist IDs to process. If None, processes all.
        rate_limit_delay: Seconds between API calls. Default 0.25 (4 req/s).

    Returns:
        dict with stats from enrich_artists_full()
    """
    logger.debug("insert_last_fm_artist_data called - delegating to enrich_artists_full()")
    return enrich_artists_full(database, artist_ids, rate_limit_delay)


def insert_lastfm_track_data(
    database: Database,
    db_track_data: tuple[int, str, str, str | None],
) -> bool:
    """
    Process a single track's Last.fm data.

    Retrieves Last.fm data using MBID (preferred) or artist+track fallback.
    Updates MBID if not already set, and inserts track genres.

    Args:
        database: Database connection (should already be connected)
        db_track_data: Tuple of (track_id, artist_name, track_title, existing_mbid)

    Returns:
        bool: True if track was processed successfully, False otherwise
    """
    track_id, artist, title, existing_mbid = db_track_data

    try:
        # Prefer MBID lookup for precision, fall back to artist+track
        lfm_track_data = lastfm.get_last_fm_track_data(
            artist=artist,
            track=title,
            mbid=existing_mbid,
        )
        if not lfm_track_data:
            return False

        logger.debug(f"Received Last.fm data for {title}: {lfm_track_data}")

        # Update MBID if we don't have one yet
        if not existing_mbid:
            track_mbid = lastfm.get_track_mbid(lfm_track_data)
            if track_mbid:
                database.execute_query(
                    "UPDATE track_data SET musicbrainz_id = ? WHERE id = ?",
                    (track_mbid, track_id),
                )
                logger.info(f"Updated MBID for {title}: {track_mbid}")

        # Process track genres (always, regardless of MBID status)
        track_genres = lastfm.get_track_tags(lfm_track_data)
        for genre in track_genres:
            genre = genre.lower()
            try:
                # Insert genre if not exists - SQLite compatible
                database.execute_query(
                    """
                    INSERT OR IGNORE INTO genres (genre)
                    SELECT ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM genres
                        WHERE LOWER(genre) = LOWER(?)
                    )
                """,
                    (genre, genre),
                )

                # Get genre ID
                genre_id = database.execute_select_query(
                    "SELECT id FROM genres WHERE LOWER(genre) = LOWER(?)", (genre,)
                )[0][0]

                # Maintain genre_aliases for normalization
                _ensure_genre_alias(database, genre_id, genre)

                # Insert genre relationship if not exists
                database.execute_query(
                    """
                    INSERT OR IGNORE INTO track_genres (track_id, genre_id)
                    SELECT ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM track_genres
                        WHERE track_id = ? AND genre_id = ?
                    )
                """,
                    (track_id, genre_id, track_id, genre_id),
                )

                logger.debug(f"Processed genre for {title}: {genre}")
            except Exception as e:
                logger.error(f"Error processing genre {genre} for {title}: {e}")

        return True

    except Exception as e:
        logger.error(f"Error processing track {title}: {e}")
        return False


def process_lastfm_track_data(
    database: Database,
    rate_limit_delay: float = 0.25,
    limit: int | None = None,
) -> dict:
    """
    Fetch track-level data from Last.fm API for all tracks.

    This is Phase 6 in the pipeline - gets track MBIDs and track-specific genres
    which may differ from artist-level genres.

    Uses existing MBID (from ffprobe/Phase 4) for precise lookup when available,
    falls back to artist+track lookup otherwise.

    Tracks are marked with lastfm_attempted_at timestamp after each query attempt,
    regardless of whether data was found. This prevents re-querying tracks that
    Last.fm doesn't have data for.

    Args:
        database: Database connection object
        rate_limit_delay: Seconds between API calls. Default 0.25 (4 req/s).
            Last.fm allows ~5 req/s averaged over 5 minutes.
        limit: Optional limit on number of tracks to process (for testing)

    Returns:
        dict with stats: {'total': int, 'processed': int, 'updated': int, 'skipped': int, 'failed': int}
    """
    logger.info("Starting Last.fm track data enrichment (Phase 6)")
    logger.info(f"Rate limit delay: {rate_limit_delay}s ({1 / rate_limit_delay:.1f} req/s)")

    stats = {
        "total": 0,
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }

    database.connect()

    # Query tracks that haven't been attempted yet
    query = """
        SELECT td.id, a.artist, td.title, td.musicbrainz_id
        FROM track_data td
        INNER JOIN artists a ON td.artist_id = a.id
        WHERE td.lastfm_attempted_at IS NULL
    """

    if limit:
        query += f" LIMIT {limit}"

    tracks = database.execute_select_query(query)
    stats["total"] = len(tracks)

    if stats["total"] == 0:
        logger.info("No tracks found needing Last.fm enrichment")
        database.close()
        return stats

    logger.info(f"Found {stats['total']} tracks to process")

    # Estimate time
    estimated_seconds = stats["total"] * rate_limit_delay
    estimated_hours = estimated_seconds / 3600
    logger.info(f"Estimated time: {estimated_hours:.1f} hours at {1 / rate_limit_delay:.1f} req/s")

    for i, track_data in enumerate(tracks):
        track_id, artist, title, existing_mbid = track_data

        # Rate limiting
        if i > 0:  # Skip delay on first request
            sleep(rate_limit_delay)

        # Keep connection alive during long-running loops
        database.ensure_connection()

        stats["processed"] += 1

        # Log lookup method
        lookup_method = "MBID" if existing_mbid else "artist+track"
        logger.debug(f"[{i + 1}/{stats['total']}] {artist} - {title} (via {lookup_method})")

        # Process track (pass full tuple including MBID)
        success = insert_lastfm_track_data(database, track_data)

        # Mark as attempted regardless of success (prevents re-querying)
        database.execute_query(
            "UPDATE track_data SET lastfm_attempted_at = datetime('now') WHERE id = ?",
            (track_id,),
        )

        if success:
            stats["updated"] += 1
        else:
            stats["failed"] += 1

        # Progress logging every 100 tracks
        if (i + 1) % 100 == 0:
            elapsed_pct = (i + 1) / stats["total"] * 100
            logger.info(
                f"Progress: {i + 1}/{stats['total']} ({elapsed_pct:.1f}%), "
                f"{stats['updated']} updated, {stats['failed']} failed"
            )

    database.close()

    logger.info(
        f"Last.fm track enrichment complete: {stats['total']} tracks, "
        f"{stats['updated']} updated, {stats['failed']} failed"
    )

    if stats["total"] > 0:
        success_rate = stats["updated"] / stats["total"] * 100
        logger.info(f"Success rate: {success_rate:.1f}%")

    return stats


def process_bpm_essentia(
    database: Database,
    use_test_paths: bool = False,
    batch_size: int = 25,
    limit: int | None = None,
    rest_between_batches: float = 10.0,
    include_researched: bool = False,
) -> dict:
    """
    Analyze BPM locally using Essentia for tracks without BPM data.

    Analyzes the actual audio files using Essentia's RhythmExtractor2013
    algorithm.

    Args:
        database: Database connection object
        use_test_paths: If True, use test path mapping; otherwise use production
        batch_size: Number of tracks to process before logging progress and resting.
            Smaller batches = more frequent rest periods. Default 25.
        limit: Optional limit on number of tracks to process (for testing)
        rest_between_batches: Seconds to pause between batches for CPU thermal
            management. Audio analysis is CPU-intensive; insufficient rest can
            cause system overheating. Default 10 seconds is conservative.
        include_researched: If True, process all tracks with NULL BPM (including
            previously researched ones). If False (default), only process tracks
            where researched_at IS NULL (new tracks only).

    Returns:
        Dict with stats:
            'total': int - tracks queried (without BPM)
            'accessible': int - files that could be accessed
            'inaccessible': int - files that couldn't be accessed
            'analyzed': int - tracks successfully analyzed
            'failed': int - tracks where analysis failed
            'updated': int - database rows updated
            'errors': int - database update errors
            'skipped': bool - True if skipped due to config/environment issues
    """
    stats = {
        "total": 0,
        "accessible": 0,
        "inaccessible": 0,
        "analyzed": 0,
        "failed": 0,
        "updated": 0,
        "errors": 0,
        "skipped": False,
    }

    # Check Essentia availability
    if not bpm_analysis.check_essentia_available():
        logger.warning("Essentia not available - skipping local BPM analysis")
        stats["skipped"] = True
        return stats

    # Validate path mapping
    path_validation = validate_path_mapping(use_test=use_test_paths)
    if not path_validation["configured"]:
        logger.warning("Path mapping not configured - skipping local BPM analysis")
        stats["skipped"] = True
        return stats

    if not path_validation["accessible"]:
        logger.warning(
            f"Music path not accessible: {path_validation['local_prefix']} - "
            "skipping local BPM analysis"
        )
        stats["skipped"] = True
        return stats

    # Query tracks without BPM
    database.connect()
    query = """
        SELECT id, filepath
        FROM track_data
        WHERE (bpm IS NULL OR bpm = 0)
        AND filepath IS NOT NULL AND filepath != ''
    """
    if not include_researched:
        query += " AND researched_at IS NULL"
    if limit:
        query += f" LIMIT {limit}"

    tracks = database.execute_select_query(query)
    database.close()

    if not tracks:
        logger.info("No tracks without BPM found")
        return stats

    stats["total"] = len(tracks)
    logger.info(
        f"Starting Essentia BPM analysis: {stats['total']} tracks, "
        f"batch_size={batch_size}, rest={rest_between_batches}s"
    )

    # Process tracks in batches
    for i, (track_id, plex_path) in enumerate(tracks):
        # Log before processing each track (helps identify crash point)
        logger.debug(f"[{i + 1}/{stats['total']}] Processing track_id={track_id}")

        # Map Plex path to local path
        local_path = map_plex_path_to_local(plex_path, use_test=use_test_paths)

        if not local_path or not verify_path_accessible(local_path):
            logger.debug("  Skipped: file not accessible")
            stats["inaccessible"] += 1
            continue

        stats["accessible"] += 1

        # Log the file being analyzed (this is where CPU-intensive work happens)
        filename = os.path.basename(local_path) if local_path else "unknown"
        logger.debug(f"  Analyzing: {filename}")

        # Analyze BPM (in subprocess to isolate potential SEGV crashes)
        bpm_value = bpm_analysis.get_bpm_essentia_safe(local_path)

        if bpm_value is None:
            logger.debug("  Failed: no BPM detected")
            stats["failed"] += 1
            continue

        stats["analyzed"] += 1
        logger.debug(f"  BPM: {bpm_value:.1f}")

        # Update database
        try:
            bpm_int = round(bpm_value)
            database.execute_query(
                "UPDATE track_data SET bpm = ? WHERE id = ?", (bpm_int, track_id)
            )
            stats["updated"] += 1
        except Exception as e:
            logger.error(f"Error updating track {track_id} with BPM {bpm_value}: {e}")
            stats["errors"] += 1

        # Progress logging and rest between batches
        if (i + 1) % batch_size == 0:
            logger.info(
                f"Batch complete: {i + 1}/{stats['total']} tracks, "
                f"{stats['analyzed']} analyzed, {stats['updated']} updated. "
                f"Resting {rest_between_batches}s for CPU cooldown..."
            )
            if rest_between_batches > 0:
                sleep(rest_between_batches)
            logger.debug("Rest complete, resuming processing")

    # Final summary
    logger.info(
        f"Essentia BPM analysis complete: {stats['total']} tracks, "
        f"{stats['accessible']} accessible, {stats['analyzed']} analyzed, "
        f"{stats['updated']} updated"
    )

    if stats["total"] > 0:
        coverage_pct = stats["analyzed"] / stats["total"] * 100
        logger.info(f"Analysis success rate: {coverage_pct:.1f}%")

    return stats


def mark_tracks_researched(database: Database) -> int:
    """Mark all un-researched tracks as researched.

    Sets researched_at = now for any track where it's currently NULL.
    Call this after BPM analysis completes to prevent re-processing
    on the next incremental run.

    Args:
        database: Database connection object

    Returns:
        Number of tracks marked as researched
    """
    database.connect()
    database.execute_query(
        "UPDATE track_data SET researched_at = datetime('now') WHERE researched_at IS NULL"
    )
    result = database.execute_select_query("SELECT changes()")
    count = result[0][0] if result else 0
    database.close()
    logger.info(f"Marked {count} tracks as researched")
    return count
