"""
Pytest configuration and fixtures for music_organizer tests.

Test Strategy:
    E2E tests using the test Plex library (Schroeder/test_music) and sandbox database.
    Tests validate the full pipeline from Plex extraction through database storage.

Supported File Types:
    Music: flac, mp3, m4a

Logging:
    Uses crash-resilient logging (fsync after every write) to ensure logs
    survive system crashes during CPU-intensive operations like BPM analysis.
"""

import os
from datetime import datetime

import pytest
from plexapi.myplex import MyPlexAccount

from config.logging import setup_logging
from db import DB_PATH, TEST_DB_PATH
from db.database import Database
from plex import (
    PLEX_MUSIC_LIBRARY,
    PLEX_PASSWORD,
    PLEX_SERVER_NAME,
    PLEX_TEST_LIBRARY,
    PLEX_TEST_SERVER_NAME,
    PLEX_USER,
)

# File type constants for test assertions
SUPPORTED_AUDIO_EXTENSIONS = {".flac", ".mp3", ".m4a"}

# Ensure logs directory exists
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)


@pytest.fixture(scope="session", autouse=True)
def setup_test_logging():
    """
    Configure crash-resilient logging for the entire test session.

    Logs are written to logs/test_YYYYMMDD_HHMMSS.log with immediate
    flush to disk after every message.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"test_{timestamp}.log")

    setup_logging(
        log_file=log_file,
        level="DEBUG",
        console=True,
        console_level="INFO",
        crash_resilient=True,  # Flush and fsync after every write
    )

    yield log_file  # Makes log path available if needed


@pytest.fixture(scope="function")
def db_test():
    """Database connection to sandbox (test database). Fresh connection per test."""
    database = Database(TEST_DB_PATH)
    database.connect()
    yield database
    if database.connection:
        database.close()


@pytest.fixture(scope="function")
def db_prod():
    """Database connection to production database. Use with caution."""
    database = Database(DB_PATH)
    database.connect()
    yield database
    if database.connection:
        database.close()


@pytest.fixture(scope="session")
def plex_account():
    """Authenticated Plex account."""
    return MyPlexAccount(PLEX_USER, PLEX_PASSWORD)


@pytest.fixture(scope="session")
def plex_test_server(plex_account):
    """Connection to test Plex server (Schroeder)."""
    return plex_account.resource(PLEX_TEST_SERVER_NAME).connect()


@pytest.fixture(scope="session")
def plex_prod_server(plex_account):
    """Connection to production Plex server (UNRAID). Use with caution."""
    try:
        return plex_account.resource(PLEX_SERVER_NAME).connect()
    except Exception as e:
        pytest.skip(f"Production server unavailable: {e}")


@pytest.fixture(scope="session")
def test_library(plex_test_server):
    """Test music library from Schroeder."""
    return plex_test_server.library.section(PLEX_TEST_LIBRARY)


@pytest.fixture(scope="session")
def prod_library(plex_prod_server):
    """Production music library from UNRAID. Use with caution."""
    if plex_prod_server is None:
        pytest.skip("Production server unavailable")
    return plex_prod_server.library.section(PLEX_MUSIC_LIBRARY)
