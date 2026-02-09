"""
Playlist query functions.

Foundation functions for building smart playlists. Each function returns
a list of plex_ids that can be passed directly to plex.playlists.create_playlist().

Functions are designed to be composed via set operations:
    rock_ids = set(get_tracks_by_genre(db, "rock"))
    uptempo_ids = set(get_tracks_by_bpm_range(db, 120, 150))
    playlist = list(rock_ids & uptempo_ids)

The build_playlist_query() function provides a UI-friendly interface that
composes the smaller functions based on provided parameters.
"""

import random

from db.database import Database


def get_tracks_by_title(db: Database, title: str) -> list[int]:
    """
    Get tracks matching a title (case-insensitive, partial match).

    Args:
        db: Database connection
        title: Title substring to search for

    Returns:
        List of plex_ids for matching tracks
    """
    query = """
        SELECT plex_id FROM track_data
        WHERE LOWER(title) LIKE LOWER(?)
        AND plex_id IS NOT NULL
    """
    pattern = f"%{title}%"
    db.connect()
    rows = db.execute_select_query(query, (pattern,))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_bpm_range(
    db: Database,
    min_bpm: int,
    max_bpm: int,
) -> list[int]:
    """
    Get tracks within a BPM range.

    Args:
        db: Database connection
        min_bpm: Minimum BPM (inclusive)
        max_bpm: Maximum BPM (inclusive)

    Returns:
        List of plex_ids for matching tracks
    """
    query = """
        SELECT plex_id FROM track_data
        WHERE bpm BETWEEN ? AND ?
        AND plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query, (min_bpm, max_bpm))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_genre(db: Database, genre: str) -> list[int]:
    """
    Get tracks matching a genre (case-insensitive, partial match).

    Uses "effective genres" - checks track's direct genres first,
    falls back to artist genres if track has none.

    Args:
        db: Database connection
        genre: Genre to match (e.g., "rock", "electronic")

    Returns:
        List of plex_ids for matching tracks
    """
    query = """
        SELECT DISTINCT td.plex_id
        FROM track_data td
        LEFT JOIN track_genres tg ON td.id = tg.track_id
        LEFT JOIN genres g1 ON tg.genre_id = g1.id
        LEFT JOIN artist_genres ag ON td.artist_id = ag.artist_id
        LEFT JOIN genres g2 ON ag.genre_id = g2.id
        WHERE td.plex_id IS NOT NULL
        AND (
            LOWER(g1.genre) LIKE LOWER(?)
            OR (g1.genre IS NULL AND LOWER(g2.genre) LIKE LOWER(?))
        )
    """
    pattern = f"%{genre}%"
    db.connect()
    rows = db.execute_select_query(query, (pattern, pattern))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_genres(db: Database, genres: list[str]) -> list[int]:
    """
    Get tracks matching any of the specified genres.

    Args:
        db: Database connection
        genres: List of genres to match (OR logic)

    Returns:
        List of plex_ids for matching tracks
    """
    if not genres:
        return []

    results = set()
    for genre in genres:
        results.update(get_tracks_by_genre(db, genre))
    return list(results)


def get_tracks_by_artist(db: Database, artist_name: str) -> list[int]:
    """
    Get tracks by a specific artist (case-insensitive, exact match).

    Args:
        db: Database connection
        artist_name: Artist name to match

    Returns:
        List of plex_ids for matching tracks
    """
    query = """
        SELECT plex_id FROM track_data
        WHERE LOWER(artist) = LOWER(?)
        AND plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query, (artist_name,))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_artists(db: Database, artist_names: list[str]) -> list[int]:
    """
    Get tracks by any of the specified artists.

    Args:
        db: Database connection
        artist_names: List of artist names (OR logic)

    Returns:
        List of plex_ids for matching tracks
    """
    if not artist_names:
        return []

    placeholders = ",".join("?" * len(artist_names))
    query = f"""
        SELECT plex_id FROM track_data
        WHERE LOWER(artist) IN ({placeholders})
        AND plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query, tuple(name.lower() for name in artist_names))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_similar_artists(db: Database, artist_name: str) -> list[int]:
    """
    Get tracks by artists similar to the given artist.

    Uses the similar_artists table populated from Last.fm data.

    Args:
        db: Database connection
        artist_name: Name of the seed artist

    Returns:
        List of plex_ids for tracks by similar artists
    """
    query = """
        SELECT DISTINCT td.plex_id
        FROM track_data td
        INNER JOIN artists a ON td.artist_id = a.id
        INNER JOIN similar_artists sa ON a.id = sa.similar_artist_id
        INNER JOIN artists seed ON sa.artist_id = seed.id
        WHERE LOWER(seed.artist) = LOWER(?)
        AND td.plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query, (artist_name,))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_artist_and_similar(db: Database, artist_name: str) -> list[int]:
    """
    Get tracks by an artist AND artists similar to them.

    Convenience function that combines get_tracks_by_artist and
    get_tracks_by_similar_artists.

    Args:
        db: Database connection
        artist_name: Name of the seed artist

    Returns:
        List of plex_ids for tracks by artist and similar artists
    """
    artist_tracks = set(get_tracks_by_artist(db, artist_name))
    similar_tracks = set(get_tracks_by_similar_artists(db, artist_name))
    return list(artist_tracks | similar_tracks)


def get_random_tracks(db: Database, limit: int = 50) -> list[int]:
    """
    Get a random selection of tracks.

    Args:
        db: Database connection
        limit: Maximum number of tracks to return

    Returns:
        List of plex_ids for random tracks
    """
    query = """
        SELECT plex_id FROM track_data
        WHERE plex_id IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?
    """
    db.connect()
    rows = db.execute_select_query(query, (limit,))
    db.close()
    return [row[0] for row in rows]


def get_all_genres(db: Database) -> list[str]:
    """
    Get all unique genre names (for UI dropdowns).

    Returns:
        Sorted list of genre names
    """
    query = "SELECT DISTINCT genre FROM genres ORDER BY genre"
    db.connect()
    rows = db.execute_select_query(query)
    db.close()
    return [row[0] for row in rows]


def get_normalized_genres(db: Database) -> list[str]:
    """Get canonical (normalized) genre names for the UI dropdown.

    Uses genre_aliases to deduplicate — returns only canonical genres
    that have at least one track or artist association. Falls back to
    get_all_genres() if genre_aliases table doesn't exist or is empty.

    Returns:
        Sorted list of canonical genre names
    """
    db.connect()

    # Check if genre_aliases table exists and has data
    check = db.execute_select_query(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='genre_aliases'"
    )
    if not check or check[0][0] == 0:
        db.close()
        return get_all_genres(db)

    alias_count = db.execute_select_query("SELECT COUNT(*) FROM genre_aliases")
    if not alias_count or alias_count[0][0] == 0:
        db.close()
        return get_all_genres(db)

    # Get distinct canonical genres that are referenced by tracks or artists
    query = """
        SELECT DISTINCT g_canonical.genre
        FROM genre_aliases ga
        INNER JOIN genres g_canonical ON ga.canonical_genre_id = g_canonical.id
        INNER JOIN genres g_raw ON ga.raw_genre_id = g_raw.id
        WHERE g_raw.id IN (
            SELECT genre_id FROM track_genres
            UNION
            SELECT genre_id FROM artist_genres
        )
        ORDER BY g_canonical.genre
    """
    rows = db.execute_select_query(query)
    db.close()

    if not rows:
        return get_all_genres(db)

    return [row[0] for row in rows]


def get_all_genre_groups(db: Database) -> list[dict]:
    """Get all genre groups with member counts for UI dropdown.

    Returns:
        List of dicts with keys: name, display_name, description, member_count
        Sorted by sort_order.
    """
    db.connect()

    # Check if table exists
    check = db.execute_select_query(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='genre_groups'"
    )
    if not check or check[0][0] == 0:
        db.close()
        return []

    query = """
        SELECT gg.name, gg.display_name, gg.description,
               COUNT(DISTINCT ggm.genre_id) as member_count
        FROM genre_groups gg
        LEFT JOIN genre_group_members ggm ON gg.id = ggm.group_id
        GROUP BY gg.id, gg.name, gg.display_name, gg.description
        ORDER BY gg.sort_order, gg.display_name
    """
    rows = db.execute_select_query(query)
    db.close()

    return [
        {
            "name": row[0],
            "display_name": row[1],
            "description": row[2] or "",
            "member_count": row[3],
        }
        for row in rows
    ]


def get_tracks_by_genre_group(db: Database, group_name: str) -> list[int]:
    """Get tracks matching any genre in a genre group.

    Uses genre_group_members to expand the group into individual genres,
    then matches via track_genres and artist_genres (with fallback).

    Args:
        db: Database connection
        group_name: The group's name field (e.g. "rock", "electronic")

    Returns:
        List of plex_ids for matching tracks
    """
    query = """
        SELECT DISTINCT td.plex_id
        FROM track_data td
        LEFT JOIN track_genres tg ON td.id = tg.track_id
        LEFT JOIN artist_genres ag ON td.artist_id = ag.artist_id
        INNER JOIN genre_group_members ggm ON (
            ggm.genre_id = tg.genre_id
            OR (tg.genre_id IS NULL AND ggm.genre_id = ag.genre_id)
        )
        INNER JOIN genre_groups gg ON ggm.group_id = gg.id
        WHERE gg.name = ?
        AND td.plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query, (group_name,))
    db.close()
    return [row[0] for row in rows]


def get_tracks_by_genre_groups(db: Database, group_names: list[str]) -> list[int]:
    """Get tracks matching any genre in any of the specified groups (OR logic).

    Args:
        db: Database connection
        group_names: List of group name values

    Returns:
        List of plex_ids for matching tracks
    """
    if not group_names:
        return []

    placeholders = ",".join("?" * len(group_names))
    query = f"""
        SELECT DISTINCT td.plex_id
        FROM track_data td
        LEFT JOIN track_genres tg ON td.id = tg.track_id
        LEFT JOIN artist_genres ag ON td.artist_id = ag.artist_id
        INNER JOIN genre_group_members ggm ON (
            ggm.genre_id = tg.genre_id
            OR (tg.genre_id IS NULL AND ggm.genre_id = ag.genre_id)
        )
        INNER JOIN genre_groups gg ON ggm.group_id = gg.id
        WHERE gg.name IN ({placeholders})
        AND td.plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query, tuple(group_names))
    db.close()
    return [row[0] for row in rows]


def get_all_artists_with_tracks(db: Database) -> list[str]:
    """
    Get all artist names that have tracks in the library (for UI dropdowns).

    Returns:
        Sorted list of artist names
    """
    query = """
        SELECT DISTINCT artist FROM track_data
        WHERE artist IS NOT NULL
        ORDER BY artist
    """
    db.connect()
    rows = db.execute_select_query(query)
    db.close()
    return [row[0] for row in rows]


def get_tracks_without_bpm(db: Database) -> list[int]:
    """
    Get tracks that have no BPM data.

    Useful for finding gaps in the data or excluding incomplete tracks.

    Returns:
        List of plex_ids for tracks without BPM
    """
    query = """
        SELECT plex_id FROM track_data
        WHERE (bpm IS NULL OR bpm = 0)
        AND plex_id IS NOT NULL
    """
    db.connect()
    rows = db.execute_select_query(query)
    db.close()
    return [row[0] for row in rows]


def get_track_count_by_genre(db: Database) -> list[tuple[str, int]]:
    """
    Get track counts per genre (using effective genres).

    Useful for understanding data distribution.

    Returns:
        List of (genre_name, track_count) tuples, sorted by count descending
    """
    query = """
        SELECT g.genre, COUNT(DISTINCT td.id) as track_count
        FROM genres g
        LEFT JOIN track_genres tg ON g.id = tg.genre_id
        LEFT JOIN track_data td ON tg.track_id = td.id
        GROUP BY g.genre
        HAVING track_count > 0
        ORDER BY track_count DESC
    """
    db.connect()
    rows = db.execute_select_query(query)
    db.close()
    return [(row[0], row[1]) for row in rows]


def get_bpm_distribution(db: Database, bucket_size: int = 10) -> list[tuple[int, int]]:
    """
    Get track counts grouped by BPM ranges.

    Args:
        db: Database connection
        bucket_size: Size of each BPM bucket (default 10)

    Returns:
        List of (bucket_start, track_count) tuples
    """
    query = f"""
        SELECT (bpm / {bucket_size}) * {bucket_size} as bucket, COUNT(*) as count
        FROM track_data
        WHERE bpm IS NOT NULL AND bpm > 0
        GROUP BY bucket
        ORDER BY bucket
    """
    db.connect()
    rows = db.execute_select_query(query)
    db.close()
    return [(row[0], row[1]) for row in rows]


def build_playlist_query(
    db: Database,
    title: str | None = None,
    genres: list[str] | None = None,
    genre_groups: list[str] | None = None,
    bpm_range: tuple[int, int] | None = None,
    artists: list[str] | None = None,
    similar_to: str | None = None,
    limit: int | None = None,
    shuffle: bool = True,
) -> list[int]:
    """
    Build a playlist query by composing filters.

    Title, genre/genre_groups, and BPM filters are ANDed (intersection).
    Genre groups and specific genres are unioned first, then ANDed with
    other filters.
    Artists and similar_to are unioned first, then ANDed with other filters.

    Args:
        db: Database connection
        title: Title substring to search for (case-insensitive partial match)
        genres: List of genres to include (OR within, AND with other filters)
        genre_groups: List of genre group names to expand and include
            (OR within, unioned with genres, AND with other filters)
        bpm_range: Tuple of (min_bpm, max_bpm)
        artists: List of specific artists to include
        similar_to: Seed artist — returns tracks by similar artists only,
            NOT the seed artist itself. Use the artists param to include
            the seed artist's tracks.
        limit: Maximum tracks to return (applied after shuffle)
        shuffle: If True, randomize order before applying limit

    Returns:
        List of plex_ids matching all specified criteria

    Example:
        # Uptempo rock playlist using genre groups, max 50 tracks
        plex_ids = build_playlist_query(
            db,
            genre_groups=["rock", "hard_rock"],
            bpm_range=(120, 150),
            limit=50,
        )
    """
    result_set: set[int] | None = None

    # Apply title filter
    if title:
        title_ids = set(get_tracks_by_title(db, title))
        result_set = title_ids if result_set is None else result_set & title_ids

    # Apply genre + genre_groups filter (unioned, then ANDed with other filters)
    genre_pool: set[int] | None = None
    if genres:
        genre_pool = set(get_tracks_by_genres(db, genres))
    if genre_groups:
        group_ids = set(get_tracks_by_genre_groups(db, genre_groups))
        genre_pool = group_ids if genre_pool is None else genre_pool | group_ids
    if genre_pool is not None:
        result_set = genre_pool if result_set is None else result_set & genre_pool

    # Apply BPM range filter
    if bpm_range:
        min_bpm, max_bpm = bpm_range
        bpm_ids = set(get_tracks_by_bpm_range(db, min_bpm, max_bpm))
        result_set = bpm_ids if result_set is None else result_set & bpm_ids

    # Apply artist + similar_to filter (unioned, then ANDed with other filters)
    artist_pool: set[int] | None = None
    if artists:
        artist_pool = set(get_tracks_by_artists(db, artists))
    if similar_to:
        similar_ids = set(get_tracks_by_similar_artists(db, similar_to))
        artist_pool = similar_ids if artist_pool is None else artist_pool | similar_ids
    if artist_pool is not None:
        result_set = artist_pool if result_set is None else result_set & artist_pool

    # Handle empty result
    if result_set is None:
        return []

    result_list = list(result_set)

    # Shuffle before limiting
    if shuffle:
        random.shuffle(result_list)

    # Apply limit
    if limit:
        result_list = result_list[:limit]

    return result_list