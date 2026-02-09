import os

from dotenv import load_dotenv

load_dotenv()

# Production Plex server
PLEX_SERVER_NAME = os.getenv("PLEX_SERVER_NAME", "")
PLEX_SERVER_URL = os.getenv("PLEX_SERVER_URL", "")
PLEX_SERVER_TOKEN = os.getenv("PLEX_SERVER_TOKEN", "")
PLEX_MUSIC_LIBRARY = os.getenv("PLEX_MUSIC_LIBRARY", "Music")
PLEX_USER = os.getenv("PLEX_USER", "")
PLEX_PASSWORD = os.getenv("PLEX_PASSWORD", "")

# Test Plex server
PLEX_TEST_SERVER_NAME = os.getenv("PLEX_TEST_SERVER_NAME", "")
PLEX_TEST_SERVER_URL = os.getenv("PLEX_TEST_SERVER_URL", "")
PLEX_TEST_SERVER_TOKEN = os.getenv("PLEX_TEST_SERVER_TOKEN", "")
PLEX_TEST_LIBRARY = os.getenv("PLEX_TEST_LIBRARY", "test_music")

__all__ = [
    "PLEX_SERVER_NAME",
    "PLEX_SERVER_URL",
    "PLEX_SERVER_TOKEN",
    "PLEX_MUSIC_LIBRARY",
    "PLEX_USER",
    "PLEX_PASSWORD",
    "PLEX_TEST_SERVER_NAME",
    "PLEX_TEST_SERVER_URL",
    "PLEX_TEST_SERVER_TOKEN",
    "PLEX_TEST_LIBRARY",
]
