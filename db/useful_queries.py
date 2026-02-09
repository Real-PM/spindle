"""Useful SQL queries and view definitions for the music organizer database.

This module contains reusable SQL queries and DDL statements for common operations.
"""

# =============================================================================
# BASIC QUERIES
# =============================================================================

ALL_TRACK_IDS_FILEPATHS_TITLES = """
SELECT td.id, td.filepath, td.title
FROM track_data td
"""

# =============================================================================
# GENRE INHERITANCE
# =============================================================================
# Tracks can inherit genres from their artist when no track-level genre exists.
# This provides ~97% genre coverage vs ~1% with track-level genres alone.

TRACKS_WITH_EFFECTIVE_GENRES = """
SELECT
    t.id AS track_id,
    t.title,
    t.artist_id,
    a.artist AS artist_name,
    g.id AS genre_id,
    g.genre,
    CASE
        WHEN tg.track_id IS NOT NULL THEN 'track'
        ELSE 'artist'
    END AS genre_source
FROM track_data t
JOIN artists a ON t.artist_id = a.id
LEFT JOIN track_genres tg ON t.id = tg.track_id
LEFT JOIN artist_genres ag ON t.artist_id = ag.artist_id AND tg.track_id IS NULL
JOIN genres g ON g.id = COALESCE(tg.genre_id, ag.genre_id)
ORDER BY t.id, g.genre
"""

# SQLite version: GROUP_CONCAT without ORDER BY clause (SQLite doesn't support ORDER BY in GROUP_CONCAT)
TRACKS_WITH_EFFECTIVE_GENRES_GROUPED = """
SELECT
    t.id AS track_id,
    t.title,
    a.artist AS artist_name,
    GROUP_CONCAT(DISTINCT g.genre, ', ') AS genres,
    CASE
        WHEN EXISTS (SELECT 1 FROM track_genres tg WHERE tg.track_id = t.id) THEN 'track'
        ELSE 'artist'
    END AS genre_source
FROM track_data t
JOIN artists a ON t.artist_id = a.id
LEFT JOIN track_genres tg ON t.id = tg.track_id
LEFT JOIN artist_genres ag ON t.artist_id = ag.artist_id
JOIN genres g ON g.id = COALESCE(tg.genre_id, ag.genre_id)
GROUP BY t.id, t.title, a.artist
ORDER BY t.id
"""

# =============================================================================
# VIEW DEFINITIONS
# =============================================================================

# SQLite doesn't support CREATE OR REPLACE VIEW, so we use DROP IF EXISTS + CREATE
DROP_VIEW_TRACK_EFFECTIVE_GENRES = "DROP VIEW IF EXISTS v_track_effective_genres"

CREATE_VIEW_TRACK_EFFECTIVE_GENRES = """
CREATE VIEW v_track_effective_genres AS
SELECT
    t.id AS track_id,
    t.title,
    t.artist_id,
    a.artist AS artist_name,
    g.id AS genre_id,
    g.genre,
    CASE
        WHEN tg.track_id IS NOT NULL THEN 'track'
        ELSE 'artist'
    END AS genre_source
FROM track_data t
JOIN artists a ON t.artist_id = a.id
LEFT JOIN track_genres tg ON t.id = tg.track_id
LEFT JOIN artist_genres ag ON t.artist_id = ag.artist_id AND tg.track_id IS NULL
JOIN genres g ON g.id = COALESCE(tg.genre_id, ag.genre_id)
"""

DROP_VIEW_TRACK_EFFECTIVE_GENRES_GROUPED = "DROP VIEW IF EXISTS v_track_effective_genres_grouped"

CREATE_VIEW_TRACK_EFFECTIVE_GENRES_GROUPED = """
CREATE VIEW v_track_effective_genres_grouped AS
SELECT
    t.id AS track_id,
    t.title,
    a.artist AS artist_name,
    t.bpm,
    GROUP_CONCAT(DISTINCT g.genre, ', ') AS genres,
    CASE
        WHEN EXISTS (SELECT 1 FROM track_genres tg WHERE tg.track_id = t.id) THEN 'track'
        ELSE 'artist'
    END AS genre_source
FROM track_data t
JOIN artists a ON t.artist_id = a.id
LEFT JOIN track_genres tg ON t.id = tg.track_id
LEFT JOIN artist_genres ag ON t.artist_id = ag.artist_id
JOIN genres g ON g.id = COALESCE(tg.genre_id, ag.genre_id)
GROUP BY t.id, t.title, a.artist, t.bpm
"""
