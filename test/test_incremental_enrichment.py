"""
Tests for incremental enrichment functionality.

Tests the split between core and full artist enrichment to ensure:
- Core enrichment adds MBID + genres but NOT similar artists
- Full enrichment adds MBID + genres + similar artists
- Query functions correctly identify artists needing enrichment
"""

import pytest

import db.db_functions as dbf
import db.db_update as dbu


class TestGetPrimaryArtistsWithoutSimilar:
    """Tests for get_primary_artists_without_similar() query."""

    def test_returns_list(self, db_test):
        """Should return a list of tuples."""
        result = dbf.get_primary_artists_without_similar(db_test)
        assert isinstance(result, list)

    def test_returns_artist_tuples(self, db_test):
        """Should return tuples of (artist_id, artist_name)."""
        result = dbf.get_primary_artists_without_similar(db_test)
        if result:
            assert len(result[0]) == 2
            assert isinstance(result[0][0], int)  # artist_id
            assert isinstance(result[0][1], str)  # artist_name

    def test_excludes_artists_with_similar(self, db_test):
        """Artists with similar_artists records should NOT be in results."""
        # Get artists that DO have similar_artists records
        db_test.connect()
        artists_with_similar = db_test.execute_select_query("""
            SELECT DISTINCT artist_id FROM similar_artists
        """)
        db_test.close()

        if not artists_with_similar:
            pytest.skip("No artists with similar_artists in database")

        # Get the result
        result = dbf.get_primary_artists_without_similar(db_test)
        result_ids = {r[0] for r in result}

        # None of the artists with similar should be in result
        similar_ids = {a[0] for a in artists_with_similar}
        assert result_ids.isdisjoint(similar_ids), "Artists with similar_artists should be excluded"

    def test_only_includes_artists_with_tracks(self, db_test):
        """Only artists linked to track_data should be returned."""
        result = dbf.get_primary_artists_without_similar(db_test)

        if not result:
            pytest.skip("No primary artists without similar found")

        # Verify each returned artist has tracks
        db_test.connect()
        for artist_id, artist_name in result[:5]:  # Check first 5
            track_count = db_test.execute_select_query(
                "SELECT COUNT(*) FROM track_data WHERE artist_id = ?", (artist_id,)
            )[0][0]
            assert track_count > 0, f"Artist {artist_name} should have tracks"
        db_test.close()


class TestGetStubArtistsWithoutMbid:
    """Tests for get_stub_artists_without_mbid() query."""

    def test_returns_list(self, db_test):
        """Should return a list of tuples."""
        result = dbf.get_stub_artists_without_mbid(db_test)
        assert isinstance(result, list)

    def test_returns_artist_tuples(self, db_test):
        """Should return tuples of (artist_id, artist_name)."""
        result = dbf.get_stub_artists_without_mbid(db_test)
        if result:
            assert len(result[0]) == 2
            assert isinstance(result[0][0], int)  # artist_id
            assert isinstance(result[0][1], str)  # artist_name

    def test_excludes_artists_with_tracks(self, db_test):
        """Artists with tracks should NOT be in results (they're not stubs)."""
        # Get artists that have tracks
        db_test.connect()
        artists_with_tracks = db_test.execute_select_query("""
            SELECT DISTINCT artist_id FROM track_data WHERE artist_id IS NOT NULL
        """)
        db_test.close()

        # Get the result
        result = dbf.get_stub_artists_without_mbid(db_test)
        result_ids = {r[0] for r in result}

        # None of the artists with tracks should be in result
        track_artist_ids = {a[0] for a in artists_with_tracks}
        assert result_ids.isdisjoint(track_artist_ids), "Artists with tracks should be excluded"

    def test_excludes_artists_with_mbid(self, db_test):
        """Artists with MBID should NOT be in results."""
        result = dbf.get_stub_artists_without_mbid(db_test)

        if not result:
            pytest.skip("No stub artists without MBID found")

        # Verify each returned artist has no MBID
        db_test.connect()
        for artist_id, artist_name in result[:5]:  # Check first 5
            mbid = db_test.execute_select_query(
                "SELECT musicbrainz_id FROM artists WHERE id = ?", (artist_id,)
            )[0][0]
            assert mbid is None, f"Artist {artist_name} should not have MBID"
        db_test.close()


class TestEnrichArtistsCore:
    """Tests for enrich_artists_core() - MBID + genres only."""

    def test_returns_stats_dict(self, db_test):
        """Should return a dict with expected keys."""
        result = dbu.enrich_artists_core(db_test, artist_ids=[])
        assert isinstance(result, dict)
        assert "total" in result
        assert "processed" in result
        assert "mbid_updated" in result
        assert "genres_added" in result
        assert "failed" in result

    def test_empty_list_returns_zero_total(self, db_test):
        """Empty artist_ids should return total=0."""
        result = dbu.enrich_artists_core(db_test, artist_ids=[])
        assert result["total"] == 0

    @pytest.mark.integration
    def test_does_not_add_similar_artists(self, db_test):
        """Core enrichment should NOT add similar_artists records.

        This is the key behavioral difference from full enrichment.
        """
        # Find an artist without similar_artists that we can test with
        incomplete = dbf.get_primary_artists_without_similar(db_test)
        if not incomplete:
            pytest.skip("No artists without similar_artists to test")

        test_artist_id = incomplete[0][0]

        # Verify no similar_artists before
        db_test.connect()
        before_count = db_test.execute_select_query(
            "SELECT COUNT(*) FROM similar_artists WHERE artist_id = ?", (test_artist_id,)
        )[0][0]
        db_test.close()

        # Run core enrichment (only 1 artist, fast)
        dbu.enrich_artists_core(
            db_test,
            artist_ids=[test_artist_id],
            rate_limit_delay=0.25,
        )

        # Verify STILL no similar_artists after
        db_test.connect()
        after_count = db_test.execute_select_query(
            "SELECT COUNT(*) FROM similar_artists WHERE artist_id = ?", (test_artist_id,)
        )[0][0]
        db_test.close()

        assert after_count == before_count, "Core enrichment should not add similar artists"


class TestEnrichArtistsFull:
    """Tests for enrich_artists_full() - MBID + genres + similar artists."""

    def test_returns_stats_dict(self, db_test):
        """Should return a dict with expected keys."""
        result = dbu.enrich_artists_full(db_test, artist_ids=[])
        assert isinstance(result, dict)
        assert "total" in result
        assert "processed" in result
        assert "mbid_updated" in result
        assert "genres_added" in result
        assert "similar_added" in result
        assert "failed" in result

    def test_empty_list_returns_zero_total(self, db_test):
        """Empty artist_ids should return total=0."""
        result = dbu.enrich_artists_full(db_test, artist_ids=[])
        assert result["total"] == 0

    @pytest.mark.integration
    def test_adds_similar_artists(self, db_test):
        """Full enrichment SHOULD add similar_artists records.

        This is the key behavioral difference from core enrichment.
        Note: This test makes API calls and modifies the database.
        """
        # Find an artist without similar_artists that we can test with
        incomplete = dbf.get_primary_artists_without_similar(db_test)
        if not incomplete:
            pytest.skip("No artists without similar_artists to test")

        test_artist_id, test_artist_name = incomplete[0]

        # Run full enrichment (only 1 artist, fast)
        result = dbu.enrich_artists_full(
            db_test,
            artist_ids=[test_artist_id],
            rate_limit_delay=0.25,
        )

        # If we got any similar artists, they should be recorded
        if result["similar_added"] > 0:
            db_test.connect()
            after_count = db_test.execute_select_query(
                "SELECT COUNT(*) FROM similar_artists WHERE artist_id = ?", (test_artist_id,)
            )[0][0]
            db_test.close()
            assert after_count > 0, "Full enrichment should add similar artists"


class TestInsertLastFmArtistData:
    """Tests for legacy wrapper function."""

    def test_wrapper_returns_stats(self, db_test):
        """Legacy wrapper should return same dict as enrich_artists_full."""
        result = dbu.insert_last_fm_artist_data(db_test, artist_ids=[])
        assert isinstance(result, dict)
        assert "total" in result
        assert "similar_added" in result  # Proof it delegates to full enrichment


class TestIncrementalEnrichmentFlow:
    """Integration tests for the complete incremental enrichment flow."""

    def test_queries_identify_different_artist_sets(self, db_test):
        """Primary and stub queries should return different artists."""
        primary = dbf.get_primary_artists_without_similar(db_test)
        stubs = dbf.get_stub_artists_without_mbid(db_test)

        primary_ids = {a[0] for a in primary}
        stub_ids = {a[0] for a in stubs}

        # These sets should be disjoint (no overlap)
        assert primary_ids.isdisjoint(stub_ids), (
            "Primary artists (with tracks) and stub artists (without tracks) should not overlap"
        )

    def test_subsequent_run_finds_fewer_artists(self, db_test):
        """After enrichment, queries should return fewer artists.

        This validates that the incremental detection logic works.
        Note: This test is slow as it makes API calls.
        """
        # Get initial counts
        primary_before = len(dbf.get_primary_artists_without_similar(db_test))
        stubs_before = len(dbf.get_stub_artists_without_mbid(db_test))

        if primary_before == 0 and stubs_before == 0:
            pytest.skip("No incomplete artists to test with")

        # Run enrichment on just 1 artist to avoid long test time
        if primary_before > 0:
            primary = dbf.get_primary_artists_without_similar(db_test)
            test_artist_id = primary[0][0]
            test_artist_name = primary[0][1]

            result = dbu.enrich_artists_full(
                db_test,
                artist_ids=[test_artist_id],
                rate_limit_delay=0.25,
            )

            # Only verify removal if similar artists were actually added
            # (Some artists don't exist in Last.fm or have no similar artists)
            if result["similar_added"] > 0:
                primary_after = dbf.get_primary_artists_without_similar(db_test)
                primary_after_ids = {a[0] for a in primary_after}
                assert test_artist_id not in primary_after_ids, (
                    f"Enriched primary artist {test_artist_name} should no longer "
                    "appear in incomplete list after similar artists were added"
                )
            else:
                pytest.skip(
                    f"Artist '{test_artist_name}' not found in Last.fm or has no similar artists"
                )
