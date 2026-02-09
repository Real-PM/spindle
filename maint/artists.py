from loguru import logger

import analysis.lastfm as lfm
import db.db_update as dbu
from db.database import Database

db = dbu.database


def maintain_artists_mbid(database: Database):
    """
    Query db for artists without mbid and update them with mbid from lastfm
    Returns:

    """
    database.connect()
    query = """SELECT id, artist FROM artists WHERE artists.musicbrainz_id IS NULL"""
    artists = database.execute_select_query(query)
    for artist_id, artist in artists:
        info = lfm.get_artist_info(artist)
        mbid = lfm.get_artist_mbid(info)
        if mbid:
            update_query = "UPDATE artists SET musicbrainz_id = ? WHERE id = ?"
            database.execute_query(update_query, (mbid, artist_id))
            logger.info(f"Updated {artist} with mbid {mbid}")
        else:
            logger.info(f"Failed to update {artist} with mbid")
    database.close()


def maintain_artist_genres(database: Database):
    """
    Query database for artists without an entry in artist_genres and update them with genres from lastfm
    Args:
        database:

    Returns:

    """
    database.connect()
    query = """SELECT artists.id, artists.artist
FROM artists
LEFT JOIN artist_genres ON artists.id = artist_genres.artist_id
WHERE artist_genres.artist_id IS NULL;
    """
    artists = database.execute_select_query(query)
    for artist_id, artist in artists:
        info = lfm.get_artist_info(artist)
        genres = lfm.get_artist_tags(info)
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

                logger.info(f"Processed genre for {artist}: {genre}")
            except Exception as e:
                logger.error(f"Error processing genre {genre} for {artist}: {e}")
                continue
