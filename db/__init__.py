import os

from dotenv import load_dotenv

load_dotenv()

# SQLite database paths
DB_PATH = os.getenv("SQLITE_DB_PATH", "data/music_organizer.db")
TEST_DB_PATH = os.getenv("SQLITE_TEST_DB_PATH", "data/sandbox.db")

__all__ = ["DB_PATH", "TEST_DB_PATH"]
