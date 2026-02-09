"""
Reset the sandbox (test) database by deleting all data from tables.

Preserves table schema but removes all data for a fresh test run.
"""

from loguru import logger

from db import TEST_DB_PATH
from db.database import Database

# Tables to clear, in order (respects foreign key constraints)
TABLES_TO_CLEAR = [
    "track_genres",
    "artist_genres",
    "similar_artists",
    "track_data",
    "artists",
    "genres",
    "history",
]


def clear_all_tables(database: Database) -> int:
    """
    Delete all data from tables in the test database.

    Args:
        database: Connected Database instance

    Returns:
        Number of tables cleared
    """
    database.connect()

    # Disable foreign key checks for deletion
    database.execute_query("PRAGMA foreign_keys = OFF")

    cleared = 0
    for table in TABLES_TO_CLEAR:
        try:
            database.execute_query(f"DELETE FROM {table}")
            logger.info(f"Cleared table: {table}")
            cleared += 1
        except Exception as e:
            logger.warning(f"Could not clear {table}: {e}")

    # Re-enable foreign key checks
    database.execute_query("PRAGMA foreign_keys = ON")

    database.close()
    return cleared


if __name__ == "__main__":
    print(f"Resetting sandbox database: {TEST_DB_PATH}")

    db = Database(TEST_DB_PATH)
    count = clear_all_tables(db)

    print(f"Cleared {count} tables")
