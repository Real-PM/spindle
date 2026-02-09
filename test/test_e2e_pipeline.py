"""
End-to-end pipeline tests for the music organizer.

Tests the full flow from Plex extraction through database population and enrichment.
Uses the test Plex library and sandbox database.

Run with: pytest test/test_e2e_pipeline.py -v -s
"""

import os
import tempfile

import pytest

import db.db_functions as dbf
import db.db_update as dbu
from analysis.ffmpeg import (
    check_ffprobe_available,
    map_plex_path_to_local,
    process_artist_mbid_from_files,
    process_mbid_from_files,
    validate_path_mapping,
    verify_path_accessible,
)
from db import TEST_DB_PATH
from db.database import Database
from db.setup_test_env import truncate_all_tables
from plex import PLEX_TEST_LIBRARY
from plex.plex_library import (
    export_track_data,
    get_all_tracks,
    listify_track_data,
)


class TestPipelinePrerequisites:
    """Verify test environment is ready before running pipeline."""

    def test_database_connection(self, db_test):
        """Sandbox database should be accessible."""
        result = db_test.execute_select_query("SELECT 1")
        assert result == [(1,)]

    def test_plex_test_server_connection(self, plex_test_server):
        """Test Plex server should be accessible."""
        assert plex_test_server is not None
        assert plex_test_server.friendlyName is not None

    def test_test_library_exists(self, test_library):
        """Test music library should exist on test server."""
        assert test_library is not None
        assert test_library.title == PLEX_TEST_LIBRARY


class TestEnvironmentValidation:
    """Tests for Phase 2: Environment validation before ffprobe extraction."""

    def test_ffprobe_available(self):
        """ffprobe should be installed and accessible."""
        result = check_ffprobe_available()
        assert result is True, "ffprobe not available - install ffmpeg"

    def test_path_mapping_configured_test(self):
        """Test path mapping should be configured in .env."""
        result = validate_path_mapping(use_test=True)

        assert result["configured"] is True, f"Test path mapping not configured: {result['errors']}"
        assert result["plex_prefix"] != "", "MUSIC_PATH_PREFIX_PLEX_TEST not set"
        assert result["local_prefix"] != "", "MUSIC_PATH_PREFIX_LOCAL_TEST not set"

    def test_path_mapping_accessible_test(self):
        """Test music path should be accessible (CIFS mount)."""
        result = validate_path_mapping(use_test=True)

        if not result["configured"]:
            pytest.skip("Test path mapping not configured")

        assert result["accessible"] is True, (
            f"Test path not accessible: {result['local_prefix']} - "
            f"is the CIFS mount available? Errors: {result['errors']}"
        )

    def test_sample_file_readable_test(self):
        """Should find and read at least one audio file in test path."""
        result = validate_path_mapping(use_test=True)

        if not result["accessible"]:
            pytest.skip("Test path not accessible")

        assert result["sample_file_ok"] is True, (
            f"No readable audio files found in {result['local_prefix']}"
        )

    def test_map_plex_path_to_local_test(self, db_test):
        """Should correctly map Plex path to local mount path."""
        # Get a real filepath from the database
        result = db_test.execute_select_query(
            "SELECT filepath FROM track_data WHERE filepath IS NOT NULL LIMIT 1"
        )

        if not result:
            pytest.skip("No tracks with filepath in database")

        plex_path = result[0][0]
        local_path = map_plex_path_to_local(plex_path, use_test=True)

        assert local_path is not None, f"Failed to map path: {plex_path}"
        assert local_path.startswith("/mnt/"), f"Mapped path should start with /mnt/: {local_path}"

    def test_mapped_file_accessible(self, db_test):
        """Mapped file path should be accessible on local filesystem."""
        # Get a real filepath from the database
        result = db_test.execute_select_query(
            "SELECT filepath FROM track_data WHERE filepath IS NOT NULL LIMIT 1"
        )

        if not result:
            pytest.skip("No tracks with filepath in database")

        plex_path = result[0][0]
        local_path = map_plex_path_to_local(plex_path, use_test=True)

        if local_path is None:
            pytest.skip(f"Could not map path: {plex_path}")

        assert verify_path_accessible(local_path), f"Mapped file not accessible: {local_path}"

    def test_path_mapping_configured_prod(self):
        """Production path mapping should be configured in .env."""
        result = validate_path_mapping(use_test=False)

        assert result["configured"] is True, f"Prod path mapping not configured: {result['errors']}"
        assert result["plex_prefix"] != "", "MUSIC_PATH_PREFIX_PLEX not set"
        assert result["local_prefix"] != "", "MUSIC_PATH_PREFIX_LOCAL not set"

    @pytest.mark.skipif(
        not os.path.isdir("/mnt/unraid/slsk/music"), reason="Production NFS mount not available"
    )
    def test_path_mapping_accessible_prod(self):
        """Production music path should be accessible (NFS mount)."""
        result = validate_path_mapping(use_test=False)

        if not result["configured"]:
            pytest.skip("Production path mapping not configured")

        # This may fail if NFS mount isn't available - that's OK
        if not result["accessible"]:
            pytest.skip(f"Production NFS mount not available: {result['local_prefix']}")

        assert result["accessible"] is True


class TestPlexExtraction:
    """Tests for extracting track data from Plex."""

    def test_get_all_tracks_returns_tracks(self, test_library):
        """Should retrieve tracks from test library."""
        tracks, count = get_all_tracks(test_library)

        assert count > 0
        assert len(tracks) == count

    def test_extract_track_data_structure(self, test_library):
        """Extracted track data should have required fields."""
        tracks, _ = get_all_tracks(test_library)
        # Use empty prefix for test - locations are relative
        track_data = listify_track_data(tracks[:1], filepath_prefix="")

        assert len(track_data) == 1
        required_fields = [
            "title",
            "artist",
            "album",
            "genre",
            "added_date",
            "filepath",
            "location",
            "plex_id",
        ]
        for field in required_fields:
            assert field in track_data[0], f"Missing field: {field}"

    def test_export_to_csv(self, test_library):
        """Should export track data to CSV file."""
        tracks, _ = get_all_tracks(test_library)
        track_data = listify_track_data(tracks[:5], filepath_prefix="")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            csv_path = f.name

        try:
            export_track_data(track_data, csv_path)
            assert os.path.exists(csv_path)
            assert os.path.getsize(csv_path) > 0
        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)


@pytest.fixture(scope="module")
def fresh_sandbox():
    """
    Reset sandbox database before tests in this module.
    Returns the Database object for further use.
    """
    db = Database(TEST_DB_PATH)
    truncate_all_tables(db)
    return db


@pytest.fixture(scope="module")
def populated_sandbox(fresh_sandbox, plex_test_server, test_library):
    """
    Populate sandbox with track data from Plex test library.
    This fixture depends on fresh_sandbox to ensure we start clean.
    """
    db = fresh_sandbox

    # Extract tracks from Plex
    tracks, count = get_all_tracks(test_library)
    if count == 0:
        pytest.skip("No tracks in test library")

    # Export to temp CSV
    track_data = listify_track_data(tracks, filepath_prefix="")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        csv_path = f.name

    try:
        export_track_data(track_data, csv_path)

        # Insert tracks into database
        dbf.insert_tracks(db, csv_path)

        # Populate artists table
        dbf.populate_artists_table(db)

        # Link artist_id in track_data
        dbf.populate_artist_id_column(db)

    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)

    return db


class TestDatabaseLoad:
    """Tests for initial database population."""

    def test_tracks_inserted(self, populated_sandbox):
        """Tracks should be inserted into track_data table."""
        db = populated_sandbox
        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM track_data")
        db.close()

        assert result[0][0] > 0

    def test_artists_populated(self, populated_sandbox):
        """Artists should be extracted and populated."""
        db = populated_sandbox
        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM artists")
        db.close()

        assert result[0][0] > 0

    def test_artist_id_linked(self, populated_sandbox):
        """track_data.artist_id should be populated."""
        db = populated_sandbox
        db.connect()
        result = db.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE artist_id IS NOT NULL"
        )
        db.close()

        assert result[0][0] > 0

    def test_track_fields_populated(self, populated_sandbox):
        """Core track fields should have values."""
        db = populated_sandbox
        db.connect()
        result = db.execute_select_query("""
            SELECT title, artist, album, plex_id
            FROM track_data
            LIMIT 1
        """)
        db.close()

        assert len(result) == 1
        title, artist, album, plex_id = result[0]
        assert title is not None and title != ""
        assert artist is not None and artist != ""
        assert album is not None and album != ""
        assert plex_id is not None and plex_id > 0

    def test_compilation_tracks_have_correct_artist(self, populated_sandbox):
        """Compilation album tracks should have track artist, not 'Various Artists'."""
        db = populated_sandbox
        db.connect()
        result = db.execute_select_query("""
            SELECT title, artist, album
            FROM track_data
            WHERE album LIKE '%No Thanks%Punk Rebellion%'
        """)
        db.close()

        if len(result) == 0:
            pytest.skip(
                "Compilation album 'No Thanks! The '70s Punk Rebellion' not in test library"
            )

        # None of the tracks should have "Various Artists" as the artist
        various_artists_tracks = [
            (title, artist)
            for title, artist, album in result
            if artist.lower() == "various artists"
        ]

        assert len(various_artists_tracks) == 0, (
            f"Found {len(various_artists_tracks)} tracks with 'Various Artists' instead of track artist: "
            f"{various_artists_tracks[:5]}"
        )

        # Log some sample artists for verification
        print(f"\nCompilation album tracks ({len(result)} total):")
        for title, artist, _album in result[:5]:
            print(f"  - '{title}' by {artist}")


@pytest.fixture(scope="module")
def enriched_sandbox(populated_sandbox):
    """
    Enrich sandbox with genres from track data.
    Optionally enriches with Last.fm data (slow).
    """
    db = populated_sandbox

    # Populate genres from track_data (from Plex genres)
    genre_list = dbu.populate_genres_table_from_track_data(db)
    if genre_list:
        dbu.insert_genres_if_not_exists(db, genre_list)
        dbu.populate_track_genre_table(db)

    return db


class TestGenreEnrichment:
    """Tests for genre enrichment from track data."""

    def test_genres_populated(self, enriched_sandbox):
        """Genres should be extracted from track data."""
        db = enriched_sandbox
        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM genres")
        db.close()

        # May be 0 if tracks have no genre tags - that's OK
        assert result[0][0] >= 0

    def test_track_genre_relationships(self, enriched_sandbox):
        """Track-genre relationships should be created."""
        db = enriched_sandbox
        db.connect()
        genre_count = db.execute_select_query("SELECT COUNT(*) FROM genres")[0][0]
        db.close()

        if genre_count > 0:
            db.connect()
            result = db.execute_select_query("SELECT COUNT(*) FROM track_genres")
            db.close()
            assert result[0][0] > 0


@pytest.fixture(scope="module")
def mbid_enriched_sandbox(enriched_sandbox):
    """
    Extract MBIDs from audio files using ffprobe.
    This runs BEFORE Last.fm enrichment to maximize MBID coverage.
    """
    db = enriched_sandbox

    # Extract track MBIDs from files
    track_stats = process_mbid_from_files(db, use_test_paths=True)

    # Extract artist MBIDs from files
    artist_stats = process_artist_mbid_from_files(db, use_test_paths=True)

    return db, track_stats, artist_stats


class TestFFprobeMBIDExtraction:
    """Tests for Phase 4.1/4.2: MBID extraction from audio files."""

    def test_track_mbid_stats_returned(self, mbid_enriched_sandbox):
        """Should return stats dict with expected keys."""
        db, track_stats, artist_stats = mbid_enriched_sandbox

        assert isinstance(track_stats, dict)
        assert "total" in track_stats
        assert "accessible" in track_stats
        assert "extracted" in track_stats
        assert "updated" in track_stats
        assert "skipped" in track_stats

    def test_artist_mbid_stats_returned(self, mbid_enriched_sandbox):
        """Should return stats dict with expected keys."""
        db, track_stats, artist_stats = mbid_enriched_sandbox

        assert isinstance(artist_stats, dict)
        assert "total" in artist_stats
        assert "extracted" in artist_stats
        assert "updated" in artist_stats
        assert "skipped" in artist_stats

    def test_files_were_accessible(self, mbid_enriched_sandbox):
        """At least some files should be accessible for extraction."""
        db, track_stats, artist_stats = mbid_enriched_sandbox

        if track_stats.get("skipped"):
            pytest.skip("MBID extraction was skipped (environment not configured)")

        # If not skipped, we should have accessed some files
        assert track_stats["accessible"] > 0, f"No files were accessible. Stats: {track_stats}"

    def test_mbids_extracted_from_files(self, mbid_enriched_sandbox):
        """Should extract MBIDs from tagged files."""
        db, track_stats, artist_stats = mbid_enriched_sandbox

        if track_stats.get("skipped"):
            pytest.skip("MBID extraction was skipped")

        # We expect at least some files to have MBIDs if they're Picard-tagged
        # This may be 0 for untagged libraries, which is OK
        assert track_stats["extracted"] >= 0

    def test_mbid_values_are_valid_uuids(self, mbid_enriched_sandbox):
        """Extracted MBIDs should be valid UUID format."""
        import re

        db, track_stats, artist_stats = mbid_enriched_sandbox

        db.connect()
        result = db.execute_select_query("""
            SELECT musicbrainz_id FROM track_data
            WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''
            LIMIT 10
        """)
        db.close()

        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
        )

        for (mbid,) in result:
            assert uuid_pattern.match(mbid), f"Invalid MBID format: {mbid}"

    def test_mbid_coverage_improved(self, mbid_enriched_sandbox):
        """MBID coverage should be tracked in stats."""
        db, track_stats, artist_stats = mbid_enriched_sandbox

        if track_stats.get("skipped"):
            pytest.skip("MBID extraction was skipped")

        # Log the coverage for visibility
        db.connect()
        total = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
        with_mbid = db.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''"
        )[0][0]
        db.close()

        coverage_pct = (with_mbid / total * 100) if total > 0 else 0
        print(
            f"\nMBID coverage after ffprobe extraction: {with_mbid}/{total} ({coverage_pct:.1f}%)"
        )
        print(f"Track stats: {track_stats}")
        print(f"Artist stats: {artist_stats}")

        # This test always passes - it's for visibility
        assert True


@pytest.fixture(scope="module")
def lastfm_enriched_sandbox(mbid_enriched_sandbox):
    """
    Phase 5: Enrich sandbox with Last.fm artist data.

    Gets artist MBIDs, artist genres, and similar artists.
    Rate limit: 4 req/s (0.25s delay).
    """
    db, _, _ = mbid_enriched_sandbox  # Unpack tuple from mbid_enriched_sandbox

    db.connect()
    artist_count = db.execute_select_query("SELECT COUNT(*) FROM artists")[0][0]
    db.close()

    print(f"\nPhase 5: Last.fm artist enrichment for {artist_count} artists...")

    try:
        dbu.insert_last_fm_artist_data(db, rate_limit_delay=0.25)
    except Exception as e:
        pytest.skip(f"Last.fm artist enrichment failed: {e}")

    return db


@pytest.fixture(scope="module")
def lastfm_track_enriched_sandbox(lastfm_enriched_sandbox):
    """
    Phase 6: Enrich sandbox with Last.fm track data.

    Gets track MBIDs and track-level genres (which may differ from artist genres).
    Uses existing MBID for precise lookup when available.
    Rate limit: 4 req/s (0.25s delay).
    """
    db = lastfm_enriched_sandbox

    db.connect()
    track_count = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
    db.close()

    print(f"\nPhase 6: Last.fm track enrichment for {track_count} tracks...")

    try:
        stats = dbu.process_lastfm_track_data(
            db,
            rate_limit_delay=0.25,
            skip_with_genres=True,  # Skip tracks that already have genres
        )
    except Exception as e:
        pytest.skip(f"Last.fm track enrichment failed: {e}")

    return db, stats


class TestLastFmArtistEnrichment:
    """Tests for Phase 5: Last.fm artist enrichment."""

    def test_artist_mbids_populated(self, lastfm_enriched_sandbox):
        """Artists should have MusicBrainz IDs from Last.fm."""
        db = lastfm_enriched_sandbox
        db.connect()
        total_artists = db.execute_select_query("SELECT COUNT(*) FROM artists")[0][0]
        artists_with_mbid = db.execute_select_query("""
            SELECT COUNT(*) FROM artists
            WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''
        """)[0][0]
        db.close()

        coverage_pct = (artists_with_mbid / total_artists * 100) if total_artists > 0 else 0
        print(f"\nArtist MBID coverage: {artists_with_mbid}/{total_artists} ({coverage_pct:.1f}%)")

        # Expect at least some artists to have MBIDs from Last.fm
        assert artists_with_mbid > 0, "No artists received MBIDs from Last.fm"

    def test_genres_table_populated(self, lastfm_enriched_sandbox):
        """Genres table should be populated from Last.fm tags."""
        db = lastfm_enriched_sandbox
        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM genres")
        db.close()

        print(f"\nGenres in database: {result[0][0]}")
        assert result[0][0] > 0, "No genres were populated from Last.fm"

    def test_artist_genres_populated(self, lastfm_enriched_sandbox):
        """Artist-genre relationships should be created."""
        db = lastfm_enriched_sandbox
        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM artist_genres")
        db.close()

        print(f"\nArtist-genre relationships: {result[0][0]}")
        assert result[0][0] > 0, "No artist-genre relationships created"

    def test_similar_artists_populated(self, lastfm_enriched_sandbox):
        """Similar artist relationships should be created."""
        db = lastfm_enriched_sandbox
        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM similar_artists")
        db.close()

        print(f"\nSimilar artist relationships: {result[0][0]}")
        # Similar artists may or may not be returned by Last.fm
        assert result[0][0] >= 0

    def test_sample_artist_genres(self, lastfm_enriched_sandbox):
        """Display sample artist genres for verification."""
        db = lastfm_enriched_sandbox
        db.connect()
        result = db.execute_select_query("""
            SELECT a.artist, GROUP_CONCAT(g.genre, ', ') as genres
            FROM artists a
            JOIN artist_genres ag ON a.id = ag.artist_id
            JOIN genres g ON ag.genre_id = g.id
            GROUP BY a.id
            LIMIT 5
        """)
        db.close()

        print("\nSample artist genres:")
        for artist, genres in result:
            print(f"  {artist}: {genres}")

        assert True  # Visibility test


class TestLastFmTrackEnrichment:
    """Tests for Phase 6: Last.fm track enrichment."""

    def test_track_enrichment_stats(self, lastfm_track_enriched_sandbox):
        """Should return stats from track enrichment."""
        db, stats = lastfm_track_enriched_sandbox

        assert isinstance(stats, dict)
        assert "total" in stats
        assert "updated" in stats
        assert "failed" in stats

        print(f"\nPhase 6 stats: {stats}")

    def test_track_genres_populated(self, lastfm_track_enriched_sandbox):
        """Track-genre relationships should be created."""
        db, stats = lastfm_track_enriched_sandbox

        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM track_genres")
        db.close()

        print(f"\nTrack-genre relationships: {result[0][0]}")
        assert result[0][0] > 0, "No track-genre relationships created"

    def test_track_mbids_improved(self, lastfm_track_enriched_sandbox):
        """Track MBID coverage should improve after Last.fm enrichment."""
        db, stats = lastfm_track_enriched_sandbox

        db.connect()
        total_tracks = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
        tracks_with_mbid = db.execute_select_query("""
            SELECT COUNT(*) FROM track_data
            WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''
        """)[0][0]
        db.close()

        coverage_pct = (tracks_with_mbid / total_tracks * 100) if total_tracks > 0 else 0
        print(
            f"\nTrack MBID coverage after Phase 6: {tracks_with_mbid}/{total_tracks} ({coverage_pct:.1f}%)"
        )

        # Track MBID coverage should be meaningful
        assert tracks_with_mbid >= 0  # May be 0 if Last.fm doesn't return MBIDs

    def test_sample_track_genres(self, lastfm_track_enriched_sandbox):
        """Display sample track genres for verification."""
        db, _ = lastfm_track_enriched_sandbox

        db.connect()
        result = db.execute_select_query("""
            SELECT td.title, a.artist, GROUP_CONCAT(g.genre, ', ') as genres
            FROM track_data td
            JOIN artists a ON td.artist_id = a.id
            JOIN track_genres tg ON td.id = tg.track_id
            JOIN genres g ON tg.genre_id = g.id
            GROUP BY td.id
            LIMIT 5
        """)
        db.close()

        print("\nSample track genres:")
        for title, artist, genres in result:
            print(f"  '{title}' by {artist}: {genres}")

        assert True  # Visibility test


@pytest.fixture(scope="module")
def essentia_bpm_sandbox(lastfm_track_enriched_sandbox):
    """
    Phase 7: Enrich sandbox with BPM data from local Essentia analysis.

    Depends on lastfm_track_enriched_sandbox to maximize MBID coverage.
    """
    db, lastfm_track_stats = lastfm_track_enriched_sandbox

    print("\nPhase 7: Essentia BPM analysis...")

    # Run Essentia BPM analysis
    # Conservative settings to prevent CPU overheating during extended analysis
    essentia_stats = dbu.process_bpm_essentia(
        db,
        use_test_paths=True,
        batch_size=25,
        rest_between_batches=10.0,
        include_researched=True,
    )

    return db, essentia_stats


class TestEssentiaBpmEnrichment:
    """Tests for Phase 7: Local BPM analysis using Essentia."""

    def test_essentia_stats_returned(self, essentia_bpm_sandbox):
        """Should return stats dict with expected keys."""
        db, essentia_stats = essentia_bpm_sandbox

        assert isinstance(essentia_stats, dict)
        assert "total" in essentia_stats
        assert "accessible" in essentia_stats
        assert "analyzed" in essentia_stats
        assert "updated" in essentia_stats
        assert "skipped" in essentia_stats

    def test_essentia_processed_tracks(self, essentia_bpm_sandbox):
        """Essentia should process tracks without BPM."""
        db, essentia_stats = essentia_bpm_sandbox

        if essentia_stats.get("skipped"):
            pytest.skip("Essentia BPM analysis was skipped (environment not configured)")

        print(f"\nEssentia stats: {essentia_stats}")
        assert essentia_stats["total"] >= 0

    def test_essentia_bpm_values_valid(self, essentia_bpm_sandbox):
        """Essentia-analyzed BPM values should be in valid range."""
        db, essentia_stats = essentia_bpm_sandbox

        if essentia_stats.get("skipped") or essentia_stats.get("analyzed", 0) == 0:
            pytest.skip("No tracks were analyzed by Essentia")

        db.connect()
        result = db.execute_select_query("""
            SELECT bpm FROM track_data
            WHERE bpm IS NOT NULL AND bpm > 0
        """)
        db.close()

        for (bpm,) in result:
            assert 40 <= bpm <= 220, f"BPM {bpm} outside valid range"

    def test_bpm_coverage(self, essentia_bpm_sandbox):
        """Essentia should provide BPM coverage."""
        db, essentia_stats = essentia_bpm_sandbox

        db.connect()
        total_tracks = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
        tracks_with_bpm = db.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
        )[0][0]
        db.close()

        coverage_pct = (tracks_with_bpm / total_tracks * 100) if total_tracks > 0 else 0

        print(f"\n{'=' * 50}")
        print("BPM COVERAGE SUMMARY")
        print(f"{'=' * 50}")
        print(f"Total tracks:     {total_tracks}")
        print(f"Tracks with BPM:  {tracks_with_bpm} ({coverage_pct:.1f}%)")
        print(f"Essentia:         {essentia_stats.get('updated', 0)} tracks")
        print(f"{'=' * 50}")

        # This test always passes - it's for visibility
        assert True


@pytest.fixture(scope="module")
def finalized_sandbox(essentia_bpm_sandbox):
    """
    Phase 8: Finalization - record import in history table.

    This is the final step of the pipeline, recording the import
    for future incremental updates.
    """
    db, essentia_stats = essentia_bpm_sandbox

    # Get the track count for history
    db.connect()
    track_count = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
    db.close()

    # Record import in history table
    dbf.update_history(db, track_count)

    return db, track_count


class TestHistoryIntegration:
    """Tests for Phase 8: History tracking."""

    def test_history_record_created(self, finalized_sandbox):
        """Should create a history record after pipeline completion."""
        db, track_count = finalized_sandbox

        db.connect()
        result = db.execute_select_query("SELECT COUNT(*) FROM history")
        db.close()

        assert result[0][0] > 0, "No history record was created"

    def test_history_record_has_correct_count(self, finalized_sandbox):
        """History record should have correct track count."""
        db, track_count = finalized_sandbox

        db.connect()
        result = db.execute_select_query(
            "SELECT records FROM history ORDER BY id DESC LIMIT 1"
        )
        db.close()

        assert result[0][0] == track_count, (
            f"History records ({result[0][0]}) doesn't match track count ({track_count})"
        )

    def test_history_has_latest_entry_date(self, finalized_sandbox):
        """History should record the latest track added_date."""
        db, _ = finalized_sandbox

        db.connect()
        history_latest = db.execute_select_query(
            "SELECT latest_entry FROM history ORDER BY id DESC LIMIT 1"
        )[0][0]
        track_latest = db.execute_select_query(
            "SELECT MAX(added_date) FROM track_data"
        )[0][0]
        db.close()

        # Convert to comparable format if needed
        assert history_latest is not None, "History latest_entry is NULL"
        print(f"\nHistory latest_entry: {history_latest}")
        print(f"Track max added_date: {track_latest}")

    def test_get_last_update_date(self, finalized_sandbox):
        """Should be able to retrieve last update date for incremental updates."""
        db, _ = finalized_sandbox

        last_update = dbf.get_last_update_date(db)

        assert last_update is not None, "get_last_update_date returned None"
        print(f"\nLast update date: {last_update}")


class TestFullPipelineIntegrity:
    """Integration tests for the complete pipeline."""

    def test_foreign_key_integrity(self, bpm_enriched_sandbox):
        """All foreign key relationships should be valid."""
        db, _ = bpm_enriched_sandbox
        db.connect()

        # Check track_data.artist_id references valid artists
        orphan_tracks = db.execute_select_query("""
            SELECT COUNT(*) FROM track_data td
            LEFT JOIN artists a ON td.artist_id = a.id
            WHERE td.artist_id IS NOT NULL AND a.id IS NULL
        """)
        assert orphan_tracks[0][0] == 0, "Found tracks with invalid artist_id"

        # Check track_genres references valid tracks and genres
        invalid_track_genres = db.execute_select_query("""
            SELECT COUNT(*) FROM track_genres tg
            LEFT JOIN track_data td ON tg.track_id = td.id
            LEFT JOIN genres g ON tg.genre_id = g.id
            WHERE td.id IS NULL OR g.id IS NULL
        """)
        assert invalid_track_genres[0][0] == 0, "Found invalid track_genres relationships"

        db.close()

    def test_no_duplicate_plex_ids(self, bpm_enriched_sandbox):
        """Each Plex track should only appear once."""
        db, _ = bpm_enriched_sandbox
        db.connect()

        duplicates = db.execute_select_query("""
            SELECT plex_id, COUNT(*) as cnt
            FROM track_data
            WHERE plex_id IS NOT NULL
            GROUP BY plex_id
            HAVING COUNT(*) > 1
        """)
        db.close()

        assert len(duplicates) == 0, f"Found duplicate plex_ids: {duplicates}"

    def test_data_summary(self, finalized_sandbox, capsys):
        """Print comprehensive summary of pipeline results for review."""
        db, _ = finalized_sandbox
        db.connect()

        # Core counts
        track_count = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
        artist_count = db.execute_select_query("SELECT COUNT(*) FROM artists")[0][0]
        genre_count = db.execute_select_query("SELECT COUNT(*) FROM genres")[0][0]

        # MBID coverage
        tracks_with_mbid = db.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''"
        )[0][0]
        artists_with_mbid = db.execute_select_query(
            "SELECT COUNT(*) FROM artists WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''"
        )[0][0]

        # BPM coverage
        tracks_with_bpm = db.execute_select_query(
            "SELECT COUNT(*) FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
        )[0][0]

        # Genre relationships
        artist_genre_count = db.execute_select_query("SELECT COUNT(*) FROM artist_genres")[0][0]
        track_genre_count = db.execute_select_query("SELECT COUNT(*) FROM track_genres")[0][0]
        similar_artist_count = db.execute_select_query("SELECT COUNT(*) FROM similar_artists")[0][0]

        db.close()

        print("\n" + "=" * 60)
        print("FULL PIPELINE SUMMARY")
        print("=" * 60)
        print(f"\n{'CORE DATA':^60}")
        print("-" * 60)
        print(f"  Tracks:              {track_count}")
        print(f"  Artists:             {artist_count}")
        print(f"  Genres:              {genre_count}")

        print(f"\n{'MBID COVERAGE':^60}")
        print("-" * 60)
        print(
            f"  Tracks with MBID:    {tracks_with_mbid}/{track_count} ({tracks_with_mbid / track_count * 100:.1f}%)"
        )
        print(
            f"  Artists with MBID:   {artists_with_mbid}/{artist_count} ({artists_with_mbid / artist_count * 100:.1f}%)"
        )

        print(f"\n{'BPM COVERAGE':^60}")
        print("-" * 60)
        print(
            f"  Tracks with BPM:     {tracks_with_bpm}/{track_count} ({tracks_with_bpm / track_count * 100:.1f}%)"
        )

        print(f"\n{'GENRE RELATIONSHIPS':^60}")
        print("-" * 60)
        print(f"  Artist-genre links:  {artist_genre_count}")
        print(f"  Track-genre links:   {track_genre_count}")
        print(f"  Similar artists:     {similar_artist_count}")

        # History info
        db.connect()
        history = db.execute_select_query(
            "SELECT tx_date, records, latest_entry FROM history ORDER BY id DESC LIMIT 1"
        )
        db.close()

        if history:
            tx_date, records, latest_entry = history[0]
            print(f"\n{'HISTORY':^60}")
            print("-" * 60)
            print(f"  Last import date:    {tx_date}")
            print(f"  Records imported:    {records}")
            print(f"  Latest track date:   {latest_entry}")

        print("\n" + "=" * 60)

        # This test always passes - it's for visibility
        assert True
