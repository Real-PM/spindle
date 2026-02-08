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
        assert b"not connected" in response.data
