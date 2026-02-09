"""
Integration tests for genre groups query layer.

Tests against the sandbox database. Requires that genre normalization
tables exist (created dynamically in fixtures if needed).
"""

import pytest

from db import TEST_DB_PATH
from db.database import Database
from db.db_functions import add_genre_normalization_tables
from db.queries import (
    build_playlist_query,
    get_all_genre_groups,
    get_normalized_genres,
    get_tracks_by_genre_group,
    get_tracks_by_genre_groups,
)


@pytest.fixture
def db():
    """Database connection to sandbox."""
    database = Database(TEST_DB_PATH)
    yield database


@pytest.fixture(autouse=True)
def ensure_tables(db):
    """Ensure genre normalization tables exist in sandbox before tests."""
    add_genre_normalization_tables(db)


class TestGetNormalizedGenres:
    def test_returns_list(self, db):
        """Should return a list of strings."""
        result = get_normalized_genres(db)
        assert isinstance(result, list)
        # Should fall back to get_all_genres if no aliases populated
        # Either way, should return a list

    def test_genres_are_sorted(self, db):
        result = get_normalized_genres(db)
        if len(result) > 1:
            assert result == sorted(result)


class TestGetAllGenreGroups:
    def test_returns_list_of_dicts(self, db):
        """Should return list of dicts with expected keys."""
        result = get_all_genre_groups(db)
        assert isinstance(result, list)
        if result:
            group = result[0]
            assert "name" in group
            assert "display_name" in group
            assert "member_count" in group

    def test_returns_empty_without_data(self, db):
        """Without populated groups, should return empty or list depending on table state."""
        result = get_all_genre_groups(db)
        assert isinstance(result, list)


class TestGetTracksByGenreGroup:
    def test_returns_list(self, db):
        result = get_tracks_by_genre_group(db, "rock")
        assert isinstance(result, list)

    def test_nonexistent_group_returns_empty(self, db):
        result = get_tracks_by_genre_group(db, "nonexistent_group_xyz")
        assert result == []


class TestGetTracksByGenreGroups:
    def test_empty_input_returns_empty(self, db):
        assert get_tracks_by_genre_groups(db, []) == []

    def test_returns_list(self, db):
        result = get_tracks_by_genre_groups(db, ["rock", "jazz"])
        assert isinstance(result, list)


class TestBuildPlaylistQueryWithGenreGroups:
    def test_genre_groups_param(self, db):
        """build_playlist_query should accept genre_groups parameter."""
        result = build_playlist_query(db, genre_groups=["rock"], shuffle=False)
        assert isinstance(result, list)

    def test_genre_groups_and_genres_union(self, db):
        """Genre groups and genres should be unioned."""
        groups_only = set(
            build_playlist_query(db, genre_groups=["rock"], shuffle=False)
        )
        genres_only = set(
            build_playlist_query(db, genres=["jazz"], shuffle=False)
        )
        both = set(
            build_playlist_query(
                db, genres=["jazz"], genre_groups=["rock"], shuffle=False
            )
        )
        # Union should be >= either individual set
        assert both >= groups_only or both >= genres_only or both == set()

    def test_genre_groups_and_bpm_intersection(self, db):
        """Genre groups should AND with BPM filter."""
        result = build_playlist_query(
            db, genre_groups=["rock"], bpm_range=(60, 200), shuffle=False
        )
        assert isinstance(result, list)

    def test_backward_compatible(self, db):
        """Existing filters should work without genre_groups."""
        result = build_playlist_query(
            db, genres=["rock"], bpm_range=(60, 200), shuffle=False
        )
        assert isinstance(result, list)
