import csv
import datetime

from loguru import logger

from . import TEST_DB_PATH
from .database import Database

database = Database(TEST_DB_PATH)


def insert_tracks(database: Database, csv_file):
    database.connect()
    query = """
    INSERT INTO track_data (title, artist, album, genre, added_date, filepath, location, plex_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                values = (
                    row["title"],
                    row["artist"],
                    row["album"],
                    row["genre"],
                    row["added_date"],
                    row["filepath"],
                    row["location"],
                    row["plex_id"],
                )
                database.execute_query(query, values)
                logger.info(f"Inserted track record for {row['plex_id']}")
            except Exception as e:
                logger.error(f"Error inserting track record: {e}")
                logger.debug(e)
                continue


def get_id_location(database: Database, cutoff=None):
    """
    Query the database for the id and location of each track. Replace the beginning of the location
    Args:
        database: Database object
        cutoff: String representing the date to use as a cutoff for the query in 'mmddyyyy' format

    Returns:
        list: List of tuples containing id, Test_Server_id, and updated location
    """
    database.connect()
    query_wo_cutoff = "SELECT id, plex_id, location FROM track_data"
    query_w_cutoff = "SELECT id, plex_id, location FROM track_data WHERE added_date > ?"

    if cutoff is None:
        results = database.execute_select_query(query_wo_cutoff)
        logger.info("Queried db without cutoff")
    else:
        try:
            # Convert cutoff from 'mmddyyyy' to 'yyyy-mm-dd'
            cutoff_date = datetime.datetime.strptime(cutoff, "%m%d%Y").strftime("%Y-%m-%d")
            results = database.execute_select_query(query_w_cutoff, (cutoff_date,))
            logger.info("Queried db with cutoff")
        except ValueError:
            logger.error("Invalid date format. Please use 'mmddyyyy'")
            results = []
    return results


def export_results(results: list, file_path: str = "output/id_location.csv"):
    """
    Export the results of a query to a CSV file. 'results' is a list of tuples.
    :param results: List of tuples containing the data to be written to CSV
    :param file_path: Path to the CSV file
    :return: None
    """
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "plex_id", "location"])
        writer.writerows(results)
    logger.info(f"id_location results exported to {file_path}")
    return None


def populate_artists_table(database: Database):
    """

    :param database:
    :return:
    """
    database.connect()
    query = """
    SELECT DISTINCT artist FROM track_data
    """
    artists = database.execute_select_query(query)
    for artist in artists:
        database.execute_query("INSERT INTO artists (artist) VALUES (?)", (artist[0],))
        logger.info(
            f"Inserted {artist[0]} into artists table; {artists.index(artist) + 1} of {len(artists)}"
        )
    logger.debug("Populated artists table")


def add_artist_id_column(database: Database):
    """
    Replaces the artist column in the track_data table with the artist id from the artists table.
    Should only be called once at the beginning of the program.
    Returns:

    """
    database.connect()
    # SQLite doesn't support ADD COLUMN with FOREIGN KEY in ALTER TABLE
    # Check if column exists first
    check_query = "SELECT COUNT(*) FROM pragma_table_info('track_data') WHERE name = 'artist_id'"
    result = database.execute_select_query(check_query)
    if result and result[0][0] > 0:
        logger.info("artist_id column already exists in track_data")
        return None

    query = "ALTER TABLE track_data ADD COLUMN artist_id INTEGER REFERENCES artists(id)"
    result = database.execute_query(query)
    logger.debug("Added artist_id column to track_data table")
    return result


def populate_artist_id_column(database: Database):
    """
    Populates the artist_id column in the track_data table with the artist id from the artists table.
    Should only be called once at the beginning of the program.
    Returns:

    """
    database.connect()
    query = """
    SELECT id, artist
    FROM artists
    """
    artists = database.execute_select_query(query)
    logger.debug("Queried DB for id and artist")
    update_query = "UPDATE track_data SET artist_id = ? WHERE artist = ?"

    for artist in artists:
        params = (artist[0], artist[1])
        database.execute_query(update_query, params)
        logger.info(
            f"Updated {artist[1]} in track_data table; {artists.index(artist) + 1} of {len(artists)}"
        )
    logger.debug("Updated artist_id column in track_data table")


def add_enrichment_attempted_column(database: Database) -> bool:
    """Add enrichment_attempted_at column to artists table.

    This column tracks when an artist was last processed for enrichment,
    preventing re-processing of artists that Last.fm doesn't recognize
    (e.g., "feat." artists that return no similar artists).

    Args:
        database: Database connection

    Returns:
        True if column was added, False if it already exists or error occurred
    """
    database.connect()

    # Check if column already exists using SQLite pragma
    check_query = """
        SELECT COUNT(*)
        FROM pragma_table_info('artists')
        WHERE name = 'enrichment_attempted_at'
    """
    result = database.execute_select_query(check_query)

    if result and result[0][0] > 0:
        logger.info("enrichment_attempted_at column already exists in artists")
        database.close()
        return False

    # Add the column
    try:
        alter_query = "ALTER TABLE artists ADD COLUMN enrichment_attempted_at TEXT"
        database.execute_query(alter_query)
        logger.info("Added enrichment_attempted_at column to artists table")
        database.close()
        return True
    except Exception as e:
        logger.error(f"Failed to add enrichment_attempted_at column: {e}")
        database.close()
        return False


def add_lastfm_attempted_column(database: Database) -> bool:
    """Add lastfm_attempted_at column to track_data table.

    This column tracks when a track was last queried for Last.fm enrichment,
    preventing re-querying tracks that Last.fm doesn't have data for.

    Args:
        database: Database connection

    Returns:
        True if column was added, False if it already exists or error occurred
    """
    database.connect()

    # Check if column already exists using SQLite pragma
    check_query = """
        SELECT COUNT(*)
        FROM pragma_table_info('track_data')
        WHERE name = 'lastfm_attempted_at'
    """
    result = database.execute_select_query(check_query)

    if result and result[0][0] > 0:
        logger.info("lastfm_attempted_at column already exists in track_data")
        database.close()
        return False

    # Add the column
    try:
        alter_query = "ALTER TABLE track_data ADD COLUMN lastfm_attempted_at TEXT"
        database.execute_query(alter_query)
        logger.info("Added lastfm_attempted_at column to track_data table")
        database.close()
        return True
    except Exception as e:
        logger.error(f"Failed to add lastfm_attempted_at column: {e}")
        database.close()
        return False


def add_researched_at_column(database: Database) -> bool:
    """Add researched_at column to track_data table.

    This column tracks when a track was last run through the full pipeline
    (including BPM analysis). Allows incremental runs to skip tracks that
    have already been researched, while still allowing explicit retries.

    On first add, backfills all existing tracks with the current timestamp
    so only truly new tracks are processed on the next incremental run.

    Args:
        database: Database connection

    Returns:
        True if column was added, False if it already exists or error occurred
    """
    database.connect()

    check_query = """
        SELECT COUNT(*)
        FROM pragma_table_info('track_data')
        WHERE name = 'researched_at'
    """
    result = database.execute_select_query(check_query)

    if result and result[0][0] > 0:
        logger.info("researched_at column already exists in track_data")
        database.close()
        return False

    try:
        alter_query = "ALTER TABLE track_data ADD COLUMN researched_at TEXT"
        database.execute_query(alter_query)
        logger.info("Added researched_at column to track_data table")

        # Backfill: mark all existing tracks as researched so only new tracks
        # are picked up by the next incremental run
        backfill_query = "UPDATE track_data SET researched_at = datetime('now')"
        database.execute_query(backfill_query)
        count_result = database.execute_select_query("SELECT changes()")
        backfill_count = count_result[0][0] if count_result else 0
        logger.info(f"Backfilled researched_at for {backfill_count} existing tracks")

        database.close()
        return True
    except Exception as e:
        logger.error(f"Failed to add researched_at column: {e}")
        database.close()
        return False


def add_spotify_columns(database: Database) -> bool:
    """Add Spotify-related columns to track_data table.

    Adds columns for Spotify ID and audio features (energy, danceability, etc.)
    These provide richer data for playlist generation.

    Args:
        database: Database connection

    Returns:
        True if any columns were added, False if all already exist
    """
    database.connect()

    columns_to_add = [
        ("spotify_id", "TEXT"),
        ("spotify_bpm", "INTEGER"),  # Spotify's tempo, separate from our bpm
        ("energy", "REAL"),
        ("danceability", "REAL"),
        ("valence", "REAL"),
        ("acousticness", "REAL"),
        ("instrumentalness", "REAL"),
        ("spotify_key", "INTEGER"),
        ("spotify_mode", "INTEGER"),
        ("time_signature", "INTEGER"),
        ("spotify_attempted_at", "TEXT"),
    ]

    added_any = False
    for col_name, col_type in columns_to_add:
        check_query = f"""
            SELECT COUNT(*)
            FROM pragma_table_info('track_data')
            WHERE name = '{col_name}'
        """
        result = database.execute_select_query(check_query)

        if result and result[0][0] > 0:
            continue  # Column exists

        try:
            alter_query = f"ALTER TABLE track_data ADD COLUMN {col_name} {col_type}"
            database.execute_query(alter_query)
            logger.info(f"Added {col_name} column to track_data table")
            added_any = True
        except Exception as e:
            logger.error(f"Failed to add {col_name} column: {e}")

    database.close()
    return added_any


def add_acoustid_column(database: Database) -> bool:
    """Add acoustid column to track_data table.

    AcousticID is a fingerprint-based identifier that Picard embeds when
    it finds a match via acoustic fingerprinting. Storing this saves a step
    if we later need fingerprint-based matching.

    Args:
        database: Database connection

    Returns:
        True if column was added, False if it already exists or error occurred
    """
    database.connect()

    # Check if column already exists using SQLite pragma
    check_query = """
        SELECT COUNT(*)
        FROM pragma_table_info('track_data')
        WHERE name = 'acoustid'
    """
    result = database.execute_select_query(check_query)

    if result and result[0][0] > 0:
        logger.info("acoustid column already exists in track_data")
        database.close()
        return False

    # Add the column
    try:
        alter_query = "ALTER TABLE track_data ADD COLUMN acoustid TEXT"
        database.execute_query(alter_query)
        logger.info("Added acoustid column to track_data table")
        database.close()
        return True
    except Exception as e:
        logger.error(f"Failed to add acoustid column: {e}")
        database.close()
        return False


def get_last_update_date(database: Database):
    """Get the date of the last pipeline run from history table."""
    database.connect()
    query = "SELECT MAX(tx_date) FROM history"
    result = database.execute_select_query(query)
    if result and result[0][0]:
        return result[0][0]
    return None


def get_latest_added_date(database: Database):
    database.connect()
    query = "SELECT MAX(added_date) FROM track_data"
    result = database.execute_select_query(query)
    result = result[0][0]
    return result


def update_history(database: Database, import_size: int):
    """
    Update the history table with the date of the last update, the number of records added, and the date of the last library update.
    Args:
        database:

    Returns:

    """
    database.connect()
    max_date = get_latest_added_date(database)
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    query = """
    INSERT INTO history (tx_date, records, latest_entry) VALUES (?, ?, ?)
    """
    database.execute_query(query, (today, import_size, max_date))


def get_primary_artists_without_similar(database: Database) -> list[tuple[int, str]]:
    """Find artists with tracks that haven't been enriched yet.

    These are "primary" artists (have tracks in the library) that need full
    Last.fm enrichment including similar artist discovery.

    Uses enrichment_attempted_at column to avoid re-processing artists that
    Last.fm doesn't recognize (e.g., "feat." artists).

    Args:
        database: Database connection object

    Returns:
        List of (artist_id, artist_name) tuples for artists needing enrichment
    """
    database.connect()
    query = """
        SELECT DISTINCT a.id, a.artist
        FROM artists a
        INNER JOIN track_data td ON a.id = td.artist_id
        WHERE a.enrichment_attempted_at IS NULL
    """
    results = database.execute_select_query(query)
    database.close()
    return results


def get_stub_artists_without_mbid(database: Database) -> list[tuple[int, str]]:
    """Find stub artists that haven't been enriched yet.

    These are "stub" artists added via similar_artists relationships that need
    MBID and genre enrichment, but should NOT have their similar artists fetched
    (to prevent infinite graph expansion).

    Uses enrichment_attempted_at column to avoid re-processing artists that
    Last.fm doesn't recognize.

    Args:
        database: Database connection object

    Returns:
        List of (artist_id, artist_name) tuples for stub artists needing enrichment
    """
    database.connect()
    query = """
        SELECT a.id, a.artist
        FROM artists a
        LEFT JOIN track_data td ON a.id = td.artist_id
        WHERE td.id IS NULL
          AND a.enrichment_attempted_at IS NULL
    """
    results = database.execute_select_query(query)
    database.close()
    return results


def get_tracks_by_artist_name(
    database: Database,
    artist_names: list[str],
) -> list[tuple[int, str, str, str | None, int, str | None, str | None]]:
    """Get all tracks for specified artists.

    Args:
        database: Database connection
        artist_names: List of artist names to match (case-insensitive)

    Returns:
        List of (track_id, filepath, artist_name, track_mbid, artist_id, artist_mbid, acoustid) tuples
    """
    if not artist_names:
        return []

    database.connect()
    placeholders = ",".join(["?"] * len(artist_names))
    query = f"""
        SELECT td.id, td.filepath, a.artist, td.musicbrainz_id, a.id, a.musicbrainz_id, td.acoustid
        FROM track_data td
        INNER JOIN artists a ON td.artist_id = a.id
        WHERE LOWER(a.artist) IN ({placeholders})
          AND td.filepath IS NOT NULL AND td.filepath != ''
    """
    params = tuple(name.lower() for name in artist_names)
    results = database.execute_select_query(query, params)
    database.close()
    return results


def add_genre_normalization_tables(database: Database) -> bool:
    """Add genre normalization and grouping tables (idempotent).

    Creates three new tables:
    - genre_aliases: maps every raw genre_id to its canonical genre_id
    - genre_groups: named groups of genres (e.g. "Rock", "Electronic")
    - genre_group_members: many-to-many linking groups to genres

    Does NOT modify existing genres, track_genres, or artist_genres tables.

    Args:
        database: Database connection

    Returns:
        True if any tables were created, False if all already exist
    """
    database.connect()

    tables_to_create = {
        "genre_aliases": """
            CREATE TABLE IF NOT EXISTS genre_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_genre_id INTEGER NOT NULL,
                canonical_genre_id INTEGER NOT NULL,
                FOREIGN KEY (raw_genre_id) REFERENCES genres(id) ON DELETE CASCADE,
                FOREIGN KEY (canonical_genre_id) REFERENCES genres(id) ON DELETE CASCADE,
                UNIQUE (raw_genre_id)
            )
        """,
        "genre_groups": """
            CREATE TABLE IF NOT EXISTS genre_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                description TEXT,
                sort_order INTEGER DEFAULT 0
            )
        """,
        "genre_group_members": """
            CREATE TABLE IF NOT EXISTS genre_group_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                genre_id INTEGER NOT NULL,
                FOREIGN KEY (group_id) REFERENCES genre_groups(id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE,
                UNIQUE (group_id, genre_id)
            )
        """,
    }

    created_any = False
    for table_name, ddl in tables_to_create.items():
        # Check if table exists
        check_query = (
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
        )
        result = database.execute_select_query(check_query, (table_name,))
        if result and result[0][0] > 0:
            logger.info(f"{table_name} table already exists")
            continue

        try:
            database.execute_query(ddl)
            logger.info(f"Created {table_name} table")
            created_any = True
        except Exception as e:
            logger.error(f"Failed to create {table_name} table: {e}")

    # Create indexes for performance
    indexes = [
        "CREATE INDEX IF NOT EXISTS ix_genre_aliases_canonical ON genre_aliases (canonical_genre_id)",
        "CREATE INDEX IF NOT EXISTS ix_genre_group_members_group ON genre_group_members (group_id)",
        "CREATE INDEX IF NOT EXISTS ix_genre_group_members_genre ON genre_group_members (genre_id)",
    ]
    for idx_sql in indexes:
        try:
            database.execute_query(idx_sql)
        except Exception as e:
            logger.error(f"Failed to create index: {e}")

    database.close()
    return created_any


def get_artist_names_found(
    database: Database,
    artist_names: list[str],
) -> list[str]:
    """Check which artist names exist in database (case-insensitive).

    Args:
        database: Database connection
        artist_names: List of artist names to check

    Returns:
        List of artist names that were found (in their database casing)
    """
    if not artist_names:
        return []

    database.connect()
    placeholders = ",".join(["?"] * len(artist_names))
    query = f"""
        SELECT DISTINCT a.artist
        FROM artists a
        WHERE LOWER(a.artist) IN ({placeholders})
    """
    params = tuple(name.lower() for name in artist_names)
    results = database.execute_select_query(query, params)
    database.close()
    return [r[0] for r in results]
