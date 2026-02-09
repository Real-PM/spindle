"""
Tests for the playlist builder web UI.

Uses Flask's test client and the sandbox database.
Plex server is not connected during tests.
"""

import pytest

from db import TEST_DB_PATH
from web import create_app


@pytest.fixture
def app():
    """Create Flask app configured for testing with sandbox DB."""
    app = create_app(db_path=TEST_DB_PATH, testing=True)
    yield app


@pytest.fixture
def client(app):
    """Flask test client."""
    return app.test_client()


class TestHealth:
    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "ok"
        assert data["plex_connected"] is False  # testing mode


class TestIndex:
    def test_page_loads(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert b"Playlist Builder" in response.data

    def test_dropdowns_populated(self, client):
        """Index page should include genre and artist options from sandbox DB."""
        response = client.get("/")
        html = response.data.decode()
        # Should have at least one <option> tag from the database
        assert "<option" in html


class TestPreviewCount:
    def test_count_no_filters(self, client):
        """With no filters, count should return 0 (build_playlist_query returns [] with no filters)."""
        response = client.get("/api/preview-count")
        assert response.status_code == 200
        assert b"0" in response.data

    def test_count_with_bpm_filter(self, client):
        """BPM filter should return a count fragment."""
        response = client.get("/api/preview-count?min_bpm=60&max_bpm=200")
        assert response.status_code == 200
        # Response is an HTML fragment with a number
        assert b"track" in response.data


class TestPreview:
    def test_preview_no_filters(self, client):
        """Preview with no filters should return 'no tracks' message."""
        response = client.post("/api/preview")
        assert response.status_code == 200
        assert b"No tracks" in response.data

    def test_preview_with_filters(self, client):
        """Preview with BPM filter should return a table or no-match message."""
        response = client.post("/api/preview", data={"min_bpm": "60", "max_bpm": "200"})
        assert response.status_code == 200
        # Should contain either a table or the no-match message
        html = response.data.decode()
        assert "<table" in html or "No tracks" in html


class TestTrackSearch:
    def test_empty_query_returns_empty(self, client):
        """Empty query should return empty JSON array."""
        response = client.get("/api/track-search?q=")
        assert response.status_code == 200
        assert response.get_json() == []

    def test_short_query_returns_empty(self, client):
        """Query shorter than 2 chars should return empty JSON array."""
        response = client.get("/api/track-search?q=a")
        assert response.status_code == 200
        assert response.get_json() == []

    def test_returns_json(self, client):
        """Search results should be a JSON array."""
        response = client.get("/api/track-search?q=test")
        assert response.status_code == 200
        data = response.get_json()
        assert isinstance(data, list)

    def test_result_fields(self, client):
        """If results are returned, each should have expected fields."""
        response = client.get("/api/track-search?q=the")
        data = response.get_json()
        if data:
            track = data[0]
            for field in ("plex_id", "title", "artist", "album", "bpm", "genres"):
                assert field in track


class TestCreatePlaylist:
    def test_create_without_name(self, client):
        """Creating a playlist without a name should return an error."""
        response = client.post("/api/create-playlist")
        assert response.status_code == 200
        assert b"name is required" in response.data

    def test_create_without_plex(self, client):
        """Creating a playlist without Plex connected should return an error."""
        response = client.post(
            "/api/create-playlist",
            data={"playlist_name": "Test Playlist", "min_bpm": "100", "max_bpm": "200"},
        )
        assert response.status_code == 200
        # Returns either "no tracks" (if filters match nothing) or "not connected"
        assert b"result-error" in response.data


class TestCreatePlaylistWithExplicitIds:
    def test_explicit_ids_without_plex(self, client):
        """Explicit plex_ids should be accepted (fails gracefully without Plex)."""
        response = client.post(
            "/api/create-playlist",
            data={"playlist_name": "Test", "track_plex_ids": "[1, 2, 3]"},
        )
        assert response.status_code == 200
        assert b"not connected" in response.data

    def test_invalid_json_returns_error(self, client):
        """Invalid JSON in track_plex_ids should return error."""
        response = client.post(
            "/api/create-playlist",
            data={"playlist_name": "Test", "track_plex_ids": "not-json"},
        )
        assert response.status_code == 200
        assert b"Invalid track list" in response.data

    def test_empty_list_returns_error(self, client):
        """Empty plex_ids list should return error."""
        response = client.post(
            "/api/create-playlist",
            data={"playlist_name": "Test", "track_plex_ids": "[]"},
        )
        assert response.status_code == 200
        assert b"empty" in response.data


class TestSimilarTracks:
    def test_without_plex_returns_error(self, client):
        """Without Plex connected, should return connection error message."""
        response = client.post(
            "/api/similar-tracks",
            data={"track_plex_ids": "[1, 2, 3]"},
        )
        assert response.status_code == 200
        assert b"not connected" in response.data

    def test_empty_plex_ids_returns_error(self, client):
        """Empty track_plex_ids should return error message."""
        response = client.post("/api/similar-tracks", data={"track_plex_ids": ""})
        assert response.status_code == 200
        assert b"No tracks provided" in response.data

    def test_missing_plex_ids_returns_error(self, client):
        """Missing track_plex_ids field should return error message."""
        response = client.post("/api/similar-tracks")
        assert response.status_code == 200
        assert b"No tracks provided" in response.data

    def test_invalid_json_returns_error(self, client):
        """Invalid JSON in track_plex_ids should return error message."""
        response = client.post(
            "/api/similar-tracks",
            data={"track_plex_ids": "not-json"},
        )
        assert response.status_code == 200
        assert b"Invalid track list" in response.data

    def test_empty_list_returns_error(self, client):
        """Empty JSON list should return error message."""
        response = client.post(
            "/api/similar-tracks",
            data={"track_plex_ids": "[]"},
        )
        assert response.status_code == 200
        assert b"empty" in response.data
