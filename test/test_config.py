"""
Tests to validate environment configuration is correct.

Run these first to ensure all connections work before running pipeline tests.
"""


class TestDatabaseConfig:
    """Validate database connections."""

    def test_sandbox_connection(self, db_test):
        """Verify sandbox database is accessible."""
        result = db_test.execute_select_query("SELECT 1")
        assert result == [(1,)]

    def test_sandbox_has_tables(self, db_test):
        """Verify sandbox has the expected tables."""
        result = db_test.execute_select_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'sandbox'"
        )
        tables = {row[0] for row in result}
        expected = {"track_data", "artists", "genres"}
        assert expected.issubset(tables), f"Missing tables: {expected - tables}"

    def test_production_connection(self, db_prod):
        """Verify production database is accessible."""
        result = db_prod.execute_select_query("SELECT 1")
        assert result == [(1,)]


class TestPlexConfig:
    """Validate Plex connections."""

    def test_plex_account_valid(self, plex_account):
        """Verify Plex account credentials work."""
        assert plex_account is not None
        assert plex_account.username is not None

    def test_test_server_connection(self, plex_test_server):
        """Verify test Plex server (Schroeder) is accessible."""
        assert plex_test_server is not None
        assert plex_test_server.friendlyName == "Schroeder"

    def test_test_library_exists(self, test_library):
        """Verify test_music library exists on Schroeder."""
        assert test_library is not None
        assert test_library.title == "test_music"

    def test_test_library_has_tracks(self, test_library):
        """Verify test library has some tracks to work with."""
        tracks = test_library.searchTracks()
        assert len(tracks) > 0, "Test library is empty"

    def test_prod_server_connection(self, plex_prod_server):
        """Verify production Plex server (UNRAID) is accessible."""
        # This test will skip if prod server is unavailable (see conftest.py)
        assert plex_prod_server is not None
        assert plex_prod_server.friendlyName == "UNRAID"
