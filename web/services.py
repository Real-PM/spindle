"""
Service layer bridging Flask routes and the database query functions.

Provides track detail lookups and dropdown data that the route handlers need
but that don't belong in the generic db/queries module.
"""

from db.database import Database
from db.queries import (
    get_all_artists_with_tracks,
    get_all_genre_groups,
    get_normalized_genres,
)


def get_track_details(db: Database, plex_ids: list[int]) -> list[dict]:
    """
    Fetch track details for the preview table.

    Uses genre inheritance: track-level genres first, falls back to artist genres.

    Args:
        db: Database instance
        plex_ids: List of plex_ids to look up

    Returns:
        List of dicts with keys: plex_id, title, artist, album, bpm, genres
    """
    if not plex_ids:
        return []

    placeholders = ",".join("?" * len(plex_ids))
    query = f"""
        SELECT td.plex_id, td.title, td.artist, td.album, td.bpm,
               GROUP_CONCAT(DISTINCT g.genre) AS genres
        FROM track_data td
        LEFT JOIN track_genres tg ON td.id = tg.track_id
        LEFT JOIN artist_genres ag ON td.artist_id = ag.artist_id AND tg.track_id IS NULL
        LEFT JOIN genres g ON g.id = COALESCE(tg.genre_id, ag.genre_id)
        WHERE td.plex_id IN ({placeholders})
        GROUP BY td.plex_id, td.title, td.artist, td.album, td.bpm
    """
    db.connect()
    rows = db.execute_select_query(query, tuple(plex_ids))
    db.close()

    return [
        {
            "plex_id": row[0],
            "title": row[1] or "",
            "artist": row[2] or "",
            "album": row[3] or "",
            "bpm": int(row[4]) if row[4] else "",
            "genres": (row[5] or "").replace(",", ", "),
        }
        for row in rows
    ]


def search_tracks(db: Database, query: str, limit: int = 15) -> list[dict]:
    """
    Search tracks by title or artist (case-insensitive).

    Args:
        db: Database instance
        query: Search string to match against title and artist
        limit: Maximum results to return

    Returns:
        List of dicts with keys: plex_id, title, artist, album, bpm, genres
    """
    if not query or len(query) < 2:
        return []

    pattern = f"%{query}%"
    sql = """
        SELECT td.plex_id, td.title, td.artist, td.album, td.bpm,
               GROUP_CONCAT(DISTINCT g.genre) AS genres
        FROM track_data td
        LEFT JOIN track_genres tg ON td.id = tg.track_id
        LEFT JOIN artist_genres ag ON td.artist_id = ag.artist_id AND tg.track_id IS NULL
        LEFT JOIN genres g ON g.id = COALESCE(tg.genre_id, ag.genre_id)
        WHERE td.title LIKE ? OR td.artist LIKE ?
        GROUP BY td.plex_id, td.title, td.artist, td.album, td.bpm
        LIMIT ?
    """
    db.connect()
    rows = db.execute_select_query(sql, (pattern, pattern, limit))
    db.close()

    return [
        {
            "plex_id": row[0],
            "title": row[1] or "",
            "artist": row[2] or "",
            "album": row[3] or "",
            "bpm": int(row[4]) if row[4] else "",
            "genres": (row[5] or "").replace(",", ", "),
        }
        for row in rows
    ]


def get_dropdown_data(db: Database) -> dict:
    """
    Fetch data needed to populate filter dropdowns.

    Args:
        db: Database instance

    Returns:
        Dict with keys: genre_groups (list[dict]), genres (list[str]),
        artists (list[str])
    """
    return {
        "genre_groups": get_all_genre_groups(db),
        "genres": get_normalized_genres(db),
        "artists": get_all_artists_with_tracks(db),
    }
