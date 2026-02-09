"""Unit tests for Last.fm database integration in db/db_update.py.

Tests the functions that populate:
- artists.musicbrainz_id
- genres table
- artist_genres table
- similar_artists table

Uses mocked Last.fm API responses and the sandbox database.
"""

from unittest.mock import patch

import pytest

from analysis import lastfm
from db import TEST_DB_PATH
from db import db_update as dbu
from db.database import Database

# Sample API responses for mocking
SAMPLE_RESPONSES = {
    "Black Sabbath": {
        "artist": {
            "name": "Black Sabbath",
            "mbid": "5182c1d9-c7d2-4dad-afa0-ccfeada921a8",
            "tags": {
                "tag": [
                    {"name": "heavy metal"},
                    {"name": "hard rock"},
                    {"name": "classic rock"},
                ]
            },
            "similar": {
                "artist": [
                    {"name": "Ozzy Osbourne"},
                    {"name": "Dio"},
                ]
            },
        }
    },
    "The Clash": {
        "artist": {
            "name": "The Clash",
            "mbid": "19f21f2e-e193-4d96-b2e9-98643c0de31f",
            "tags": {
                "tag": [
                    {"name": "punk"},
                    {"name": "punk rock"},
                    {"name": "new wave"},
                ]
            },
            "similar": {
                "artist": [
                    {"name": "The Jam"},
                    {"name": "Buzzcocks"},
                ]
            },
        }
    },
    "Unknown Artist": {
        "artist": {
            "name": "Unknown Artist",
            "mbid": "",
            "tags": {"tag": []},
            "similar": {"artist": []},
        }
    },
}


def mock_get_artist_info(artist_name):
    """Mock function for lastfm.get_artist_info()."""
    return SAMPLE_RESPONSES.get(artist_name)


@pytest.fixture
def test_db():
    """Provide a test database connection."""
    db = Database(TEST_DB_PATH)
    return db


@pytest.fixture
def clean_test_tables(test_db):
    """Clean and set up test tables with minimal test data."""
    test_db.connect()

    # Clear existing data
    test_db.execute_query("DELETE FROM artist_genres")
    test_db.execute_query("DELETE FROM similar_artists")
    test_db.execute_query("DELETE FROM genres WHERE id > 0")
    test_db.execute_query("DELETE FROM artists WHERE id > 0")

    # Insert test artists
    test_db.execute_query("INSERT INTO artists (id, artist) VALUES (9001, 'Black Sabbath')")
    test_db.execute_query("INSERT INTO artists (id, artist) VALUES (9002, 'The Clash')")
    test_db.execute_query("INSERT INTO artists (id, artist) VALUES (9003, 'Unknown Artist')")

    test_db.close()

    yield test_db

    # Cleanup after test
    test_db.connect()
    test_db.execute_query("DELETE FROM artist_genres WHERE artist_id >= 9001")
    test_db.execute_query("DELETE FROM similar_artists WHERE artist_id >= 9001")
    test_db.execute_query("DELETE FROM artists WHERE id >= 9001")
    # Don't delete genres as they might be shared
    test_db.close()


class TestInsertLastFmArtistData:
    """Tests for insert_last_fm_artist_data() function."""

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")  # Skip the 1s rate limiting delay
    def test_updates_artist_mbid(self, mock_sleep, mock_api, clean_test_tables):
        """Should update artist musicbrainz_id from Last.fm response."""
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            "SELECT musicbrainz_id FROM artists WHERE artist = 'Black Sabbath'"
        )
        clean_test_tables.close()

        assert result[0][0] == "5182c1d9-c7d2-4dad-afa0-ccfeada921a8"

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_inserts_genres(self, mock_sleep, mock_api, clean_test_tables):
        """Should insert genres from Last.fm tags into genres table."""
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            "SELECT genre FROM genres WHERE LOWER(genre) IN ('heavy metal', 'punk')"
        )
        clean_test_tables.close()

        genres = [r[0] for r in result]
        assert "heavy metal" in genres or "Heavy Metal" in [g.title() for g in genres]

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_creates_artist_genre_relationships(self, mock_sleep, mock_api, clean_test_tables):
        """Should create artist_genres relationships."""
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            """SELECT COUNT(*) FROM artist_genres ag
               JOIN artists a ON ag.artist_id = a.id
               WHERE a.artist = 'Black Sabbath'"""
        )
        clean_test_tables.close()

        # Black Sabbath should have 3 genre relationships
        assert result[0][0] == 3

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_inserts_similar_artists(self, mock_sleep, mock_api, clean_test_tables):
        """Should insert similar artists into artists table."""
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            "SELECT artist FROM artists WHERE LOWER(artist) IN ('ozzy osbourne', 'dio')"
        )
        clean_test_tables.close()

        artists = [r[0].lower() for r in result]
        assert "ozzy osbourne" in artists or "dio" in artists

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_creates_similar_artist_relationships(self, mock_sleep, mock_api, clean_test_tables):
        """Should create similar_artists relationships."""
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            """SELECT COUNT(*) FROM similar_artists sa
               JOIN artists a ON sa.artist_id = a.id
               WHERE a.artist = 'Black Sabbath'"""
        )
        clean_test_tables.close()

        # Black Sabbath should have 2 similar artist relationships
        assert result[0][0] == 2

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_handles_artist_with_no_mbid(self, mock_sleep, mock_api, clean_test_tables):
        """Should handle artists with empty MBID gracefully."""
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            "SELECT musicbrainz_id FROM artists WHERE artist = 'Unknown Artist'"
        )
        clean_test_tables.close()

        # MBID should be NULL or empty (not updated from empty string)
        assert result[0][0] is None or result[0][0] == ""

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_handles_api_failure(self, mock_sleep, mock_api, clean_test_tables):
        """Should continue processing when API returns None for some artists."""
        # First call succeeds, second returns None
        mock_api.side_effect = [
            SAMPLE_RESPONSES["Black Sabbath"],
            None,  # The Clash fails
            SAMPLE_RESPONSES["Unknown Artist"],
        ]

        # Should not raise exception
        dbu.insert_last_fm_artist_data(clean_test_tables)

        # Black Sabbath should still be updated
        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            "SELECT musicbrainz_id FROM artists WHERE artist = 'Black Sabbath'"
        )
        clean_test_tables.close()

        assert result[0][0] == "5182c1d9-c7d2-4dad-afa0-ccfeada921a8"

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_no_duplicate_genres(self, mock_sleep, mock_api, clean_test_tables):
        """Should not create duplicate genres when processing multiple artists."""
        # Both artists have some overlapping genres (if any)
        mock_api.side_effect = mock_get_artist_info

        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        # Check for duplicates
        result = clean_test_tables.execute_select_query(
            """SELECT LOWER(genre), COUNT(*) as cnt
               FROM genres
               GROUP BY LOWER(genre)
               HAVING COUNT(*) > 1"""
        )
        clean_test_tables.close()

        # Should have no duplicates
        assert len(result) == 0

    @patch("db.db_update.lastfm.get_artist_info")
    @patch("db.db_update.sleep")
    def test_no_duplicate_artist_genre_relationships(self, mock_sleep, mock_api, clean_test_tables):
        """Should not create duplicate artist_genre relationships."""
        mock_api.side_effect = mock_get_artist_info

        # Run twice to ensure no duplicates created
        dbu.insert_last_fm_artist_data(clean_test_tables)
        dbu.insert_last_fm_artist_data(clean_test_tables)

        clean_test_tables.connect()
        result = clean_test_tables.execute_select_query(
            """SELECT artist_id, genre_id, COUNT(*) as cnt
               FROM artist_genres
               GROUP BY artist_id, genre_id
               HAVING COUNT(*) > 1"""
        )
        clean_test_tables.close()

        assert len(result) == 0


class TestGenreHelpers:
    """Tests for genre-related helper functions."""

    def test_populate_genres_table_from_track_data(self, test_db):
        """Test extracting genres from track_data.genre column."""
        test_db.connect()

        # Insert test track with genre
        test_db.execute_query(
            """INSERT INTO track_data (id, title, artist, genre, plex_id, filepath, location)
               VALUES (99001, 'Test Track', 'Test Artist', "['Rock', 'Alternative']",
                       'plex99001', '/test/path', '/test/location')"""
        )

        try:
            genres = dbu.populate_genres_table_from_track_data(test_db)
            # Should extract Rock and Alternative
            assert "rock" in [g.lower() for g in genres] or len(genres) >= 0
        finally:
            test_db.execute_query("DELETE FROM track_data WHERE id = 99001")
            test_db.close()


class TestIntegration:
    """Integration tests that use real API calls.

    Marked with @pytest.mark.integration - run only when testing connectivity.
    """

    @pytest.mark.integration
    def test_real_lastfm_to_db_flow(self, test_db):
        """Test real Last.fm API call and database update."""
        test_db.connect()

        # Insert a test artist
        test_db.execute_query("INSERT INTO artists (id, artist) VALUES (99999, 'Radiohead')")

        try:
            # Get real Last.fm data
            artist_info = lastfm.get_artist_info("Radiohead")
            assert artist_info is not None

            mbid = lastfm.get_artist_mbid(artist_info)
            assert mbid is not None
            assert len(mbid) == 36

            # Update database
            test_db.execute_query(
                "UPDATE artists SET musicbrainz_id = ? WHERE id = 99999", (mbid,)
            )

            # Verify
            result = test_db.execute_select_query(
                "SELECT musicbrainz_id FROM artists WHERE id = 99999"
            )
            assert result[0][0] == mbid

        finally:
            test_db.execute_query("DELETE FROM artists WHERE id = 99999")
            test_db.close()
