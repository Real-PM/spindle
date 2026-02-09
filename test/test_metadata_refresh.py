"""
Tests for metadata refresh by artist functionality.

Tests the ability to re-extract MBIDs from files for specific artists
after manual tagging with MusicBrainz Picard.
"""

import pytest

from db.db_functions import add_acoustid_column, get_artist_names_found, get_tracks_by_artist_name
from pipeline import refresh_metadata_for_artists


class TestGetTracksByArtistName:
    """Tests for get_tracks_by_artist_name query function."""

    def test_returns_empty_list_for_empty_input(self, db_test):
        """Should return empty list when no artist names provided."""
        result = get_tracks_by_artist_name(db_test, [])
        assert result == []

    def test_returns_tracks_for_existing_artist(self, db_test):
        """Should return tracks for an artist that exists in database."""
        # First, find an artist that has tracks
        db_test.connect()
        existing = db_test.execute_select_query("""
            SELECT a.artist
            FROM artists a
            INNER JOIN track_data td ON a.id = td.artist_id
            WHERE td.filepath IS NOT NULL AND td.filepath != ''
            LIMIT 1
        """)
        db_test.close()

        if not existing:
            pytest.skip("No artists with tracks in database")

        artist_name = existing[0][0]
        result = get_tracks_by_artist_name(db_test, [artist_name])

        assert len(result) > 0
        # Verify result structure: (track_id, filepath, artist_name, track_mbid, artist_id, artist_mbid, acoustid)
        first_track = result[0]
        assert len(first_track) == 7
        assert isinstance(first_track[0], int)  # track_id
        assert isinstance(first_track[1], str)  # filepath
        assert first_track[2].lower() == artist_name.lower()  # artist_name

    def test_case_insensitive_matching(self, db_test):
        """Should match artist names case-insensitively."""
        # Find an artist
        db_test.connect()
        existing = db_test.execute_select_query("""
            SELECT a.artist
            FROM artists a
            INNER JOIN track_data td ON a.id = td.artist_id
            WHERE td.filepath IS NOT NULL AND td.filepath != ''
            LIMIT 1
        """)
        db_test.close()

        if not existing:
            pytest.skip("No artists with tracks in database")

        artist_name = existing[0][0]

        # Try with different casing
        upper_result = get_tracks_by_artist_name(db_test, [artist_name.upper()])
        lower_result = get_tracks_by_artist_name(db_test, [artist_name.lower()])

        assert len(upper_result) == len(lower_result)
        assert len(upper_result) > 0

    def test_returns_empty_for_nonexistent_artist(self, db_test):
        """Should return empty list for artist not in database."""
        result = get_tracks_by_artist_name(db_test, ["NonexistentArtist12345XYZ"])
        assert result == []

    def test_handles_multiple_artists(self, db_test):
        """Should return tracks for multiple artists."""
        # Find two artists with tracks
        db_test.connect()
        existing = db_test.execute_select_query("""
            SELECT DISTINCT a.artist
            FROM artists a
            INNER JOIN track_data td ON a.id = td.artist_id
            WHERE td.filepath IS NOT NULL AND td.filepath != ''
            LIMIT 2
        """)
        db_test.close()

        if len(existing) < 2:
            pytest.skip("Need at least 2 artists with tracks in database")

        artist_names = [row[0] for row in existing]
        result = get_tracks_by_artist_name(db_test, artist_names)

        assert len(result) > 0


class TestGetArtistNamesFound:
    """Tests for get_artist_names_found helper function."""

    def test_returns_empty_for_empty_input(self, db_test):
        """Should return empty list for empty input."""
        result = get_artist_names_found(db_test, [])
        assert result == []

    def test_returns_found_artists(self, db_test):
        """Should return artists that exist in database."""
        db_test.connect()
        existing = db_test.execute_select_query("SELECT artist FROM artists LIMIT 1")
        db_test.close()

        if not existing:
            pytest.skip("No artists in database")

        artist_name = existing[0][0]
        result = get_artist_names_found(db_test, [artist_name])

        assert len(result) == 1
        assert result[0].lower() == artist_name.lower()

    def test_excludes_nonexistent_artists(self, db_test):
        """Should not return artists that don't exist."""
        result = get_artist_names_found(db_test, ["NonexistentArtist12345XYZ"])
        assert result == []


class TestRefreshMetadataForArtists:
    """Tests for the main refresh_metadata_for_artists function."""

    def test_returns_stats_dict_structure(self, db_test):
        """Should return properly structured stats dict."""
        # Use a non-existent artist so we don't modify anything
        stats = refresh_metadata_for_artists(
            database=db_test,
            artist_names=["NonexistentArtist12345XYZ"],
            use_test_paths=True,
            dry_run=True,
        )

        # Verify top-level structure
        assert isinstance(stats, dict)
        assert "artists_requested" in stats
        assert "artists_found" in stats
        assert "artists_not_found" in stats
        assert "tracks" in stats
        assert "artist_mbids" in stats
        assert "acoustids" in stats
        assert "dry_run" in stats

        # Verify tracks sub-structure
        assert "total" in stats["tracks"]
        assert "accessible" in stats["tracks"]
        assert "inaccessible" in stats["tracks"]
        assert "extracted" in stats["tracks"]
        assert "missing" in stats["tracks"]
        assert "updated" in stats["tracks"]
        assert "unchanged" in stats["tracks"]
        assert "errors" in stats["tracks"]

        # Verify artist_mbids sub-structure
        assert "updated" in stats["artist_mbids"]
        assert "unchanged" in stats["artist_mbids"]
        assert "errors" in stats["artist_mbids"]

        # Verify acoustids sub-structure
        assert "extracted" in stats["acoustids"]
        assert "updated" in stats["acoustids"]
        assert "unchanged" in stats["acoustids"]
        assert "errors" in stats["acoustids"]

    def test_nonexistent_artist_returns_zero_found(self, db_test):
        """Should report 0 found for non-existent artist."""
        stats = refresh_metadata_for_artists(
            database=db_test,
            artist_names=["NonexistentArtist12345XYZ"],
            use_test_paths=True,
            dry_run=True,
        )

        assert stats["artists_requested"] == 1
        assert stats["artists_found"] == 0
        assert "NonexistentArtist12345XYZ" in stats["artists_not_found"]

    def test_dry_run_flag_is_preserved(self, db_test):
        """Should preserve dry_run flag in returned stats."""
        stats_dry = refresh_metadata_for_artists(
            database=db_test,
            artist_names=["NonexistentArtist12345XYZ"],
            use_test_paths=True,
            dry_run=True,
        )
        assert stats_dry["dry_run"] is True

        stats_wet = refresh_metadata_for_artists(
            database=db_test,
            artist_names=["NonexistentArtist12345XYZ"],
            use_test_paths=True,
            dry_run=False,
        )
        assert stats_wet["dry_run"] is False

    def test_empty_artist_list_returns_early(self, db_test):
        """Should handle empty artist list gracefully."""
        stats = refresh_metadata_for_artists(
            database=db_test,
            artist_names=[],
            use_test_paths=True,
            dry_run=True,
        )

        assert stats["artists_requested"] == 0
        assert stats["artists_found"] == 0

    def test_dry_run_does_not_modify_database(self, db_test):
        """Dry run should not modify any database records."""
        # Find an artist with tracks
        db_test.connect()
        existing = db_test.execute_select_query("""
            SELECT a.artist, td.id, td.musicbrainz_id
            FROM artists a
            INNER JOIN track_data td ON a.id = td.artist_id
            WHERE td.filepath IS NOT NULL AND td.filepath != ''
            LIMIT 1
        """)
        db_test.close()

        if not existing:
            pytest.skip("No artists with tracks in database")

        artist_name = existing[0][0]
        track_id = existing[0][1]
        original_mbid = existing[0][2]

        # Run dry run
        refresh_metadata_for_artists(
            database=db_test,
            artist_names=[artist_name],
            use_test_paths=True,
            dry_run=True,
        )

        # Verify MBID unchanged
        db_test.connect()
        after = db_test.execute_select_query(
            "SELECT musicbrainz_id FROM track_data WHERE id = ?",
            (track_id,),
        )
        db_test.close()

        assert after[0][0] == original_mbid


class TestRefreshMetadataIntegration:
    """Integration tests for metadata refresh (require accessible files)."""

    @pytest.mark.integration
    def test_refresh_with_real_artist(self, db_test):
        """Integration test: refresh metadata for a real artist."""
        # Find an artist with accessible tracks
        db_test.connect()
        existing = db_test.execute_select_query("""
            SELECT DISTINCT a.artist
            FROM artists a
            INNER JOIN track_data td ON a.id = td.artist_id
            WHERE td.filepath IS NOT NULL AND td.filepath != ''
            LIMIT 1
        """)
        db_test.close()

        if not existing:
            pytest.skip("No artists with tracks in database")

        artist_name = existing[0][0]

        # Run refresh in dry_run mode to see what would happen
        stats = refresh_metadata_for_artists(
            database=db_test,
            artist_names=[artist_name],
            use_test_paths=True,
            dry_run=True,
        )

        # Should find the artist
        assert stats["artists_found"] == 1
        assert stats["tracks"]["total"] > 0

        # If paths are not accessible (mount not available), that's OK
        # Just verify the function completes without error


class TestAddAcoustidColumn:
    """Tests for the acoustid column migration."""

    def test_migration_is_idempotent(self, db_test):
        """Running migration twice should return False the second time."""
        # First call might add the column or find it exists
        add_acoustid_column(db_test)

        # Second call should always return False (column exists)
        second_result = add_acoustid_column(db_test)
        assert second_result is False

    def test_acoustid_column_exists_after_migration(self, db_test):
        """Column should exist after migration."""
        add_acoustid_column(db_test)

        db_test.connect()
        result = db_test.execute_select_query("""
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'track_data'
              AND COLUMN_NAME = 'acoustid'
        """)
        db_test.close()

        assert result[0][0] == 1
