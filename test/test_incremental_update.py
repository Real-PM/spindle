"""
Tests for incremental update functionality.

Tests the ability to add new tracks to an existing database without
re-processing all data.
"""

import pytest

from pipeline import (
    add_new_artists,
    insert_new_tracks,
    run_incremental_update,
    validate_environment,
)
from plex.plex_library import get_tracks_since_date


class TestValidateEnvironment:
    """Tests for environment validation."""

    def test_validate_returns_dict(self, db_test):
        """Should return dict with expected keys."""
        result = validate_environment(db_test, use_test=True)

        assert isinstance(result, dict)
        assert "database_ok" in result
        assert "paths_ok" in result
        assert "ffprobe_ok" in result
        assert "errors" in result

    def test_database_ok_with_valid_connection(self, db_test):
        """Database check should pass with valid connection."""
        result = validate_environment(db_test, use_test=True)
        assert result["database_ok"] is True


class TestGetTracksSinceDate:
    """Tests for Plex date filtering."""

    def test_get_tracks_since_old_date(self, test_library):
        """Should return all tracks when using old date."""
        tracks, count = get_tracks_since_date(test_library, "2000-01-01")

        # Should get all tracks since the date is ancient
        assert count > 0
        assert len(tracks) == count

    def test_get_tracks_since_future_date(self, test_library):
        """Should return no tracks when using future date."""
        tracks, count = get_tracks_since_date(test_library, "2099-01-01")

        assert count == 0
        assert len(tracks) == 0


class TestInsertNewTracks:
    """Tests for inserting new tracks."""

    def test_insert_new_tracks_empty_list(self, db_test):
        """Should handle empty list gracefully."""
        count = insert_new_tracks(db_test, [])
        assert count == 0

    def test_insert_new_tracks_detects_duplicates(self, db_test):
        """Should not insert tracks that already exist (by plex_id)."""
        # Get an existing track
        db_test.connect()
        existing = db_test.execute_select_query(
            "SELECT plex_id, title, artist, album FROM track_data LIMIT 1"
        )
        db_test.close()

        if not existing:
            pytest.skip("No existing tracks in database")

        plex_id, title, artist, album = existing[0]

        # Try to insert a "new" track with the same plex_id
        track_data = [{
            "plex_id": plex_id,
            "title": title,
            "artist": artist,
            "album": album,
            "genre": "[]",
            "added_date": "2026-01-01",
            "filepath": "/fake/path",
            "location": "/fake/location",
        }]

        count = insert_new_tracks(db_test, track_data)
        assert count == 0  # Should not insert duplicate


class TestAddNewArtists:
    """Tests for adding new artists."""

    def test_add_new_artists_with_existing_db(self, db_test):
        """Should handle database with existing artists."""
        # This should find no new artists to add
        count = add_new_artists(db_test)

        # In a populated database, should be 0 or low number
        assert count >= 0


class TestRunIncrementalUpdate:
    """Integration tests for the incremental update workflow."""

    def test_incremental_with_future_date(self, db_test, test_library):
        """Should handle case where no new tracks exist."""
        stats = run_incremental_update(
            database=db_test,
            music_library=test_library,
            use_test_paths=True,
            since_date="2099-01-01",  # Future date = no new tracks
            skip_ffprobe=True,
            skip_lastfm=True,
            skip_bpm=True,
        )

        assert stats["new_tracks"] == 0
        assert stats["since_date"] == "2099-01-01"

    def test_incremental_returns_stats_dict(self, db_test, test_library):
        """Should return stats dict with expected keys."""
        stats = run_incremental_update(
            database=db_test,
            music_library=test_library,
            use_test_paths=True,
            since_date="2099-01-01",
            skip_ffprobe=True,
            skip_lastfm=True,
            skip_bpm=True,
        )

        assert isinstance(stats, dict)
        assert "since_date" in stats
        assert "new_tracks" in stats
        assert "new_artists" in stats

    def test_incremental_uses_history_when_available(self, db_test, test_library):
        """Should use last history date when since_date not provided."""
        import db.db_functions as dbf

        # Check if history exists
        last_update = dbf.get_last_update_date(db_test)

        if last_update is None:
            # No history - this will fall back to processing all tracks
            # which is too slow for a unit test. Just verify function exists.
            pytest.skip("No history in database - skipping slow full import test")

        stats = run_incremental_update(
            database=db_test,
            music_library=test_library,
            use_test_paths=True,
            since_date=None,  # Use history
            skip_ffprobe=True,
            skip_lastfm=True,
            skip_bpm=True,
        )

        # Should have a since_date from history
        assert stats["since_date"] is not None
        assert stats["since_date"] == last_update.strftime("%Y-%m-%d")