#!/usr/bin/env python3
"""
Import JSON data into SQLite database.

This script reads JSON files exported by export_mysql.py and imports them
into a new SQLite database. It creates the schema and imports data in the
correct order to respect foreign key constraints.

Usage:
    python scripts/import_sqlite.py [--input-dir INPUT_DIR] [--db-path DB_PATH]

Tables imported (in order):
    1. artists (no dependencies)
    2. genres (no dependencies)
    3. history (no dependencies)
    4. track_data (depends on artists)
    5. track_genres (depends on track_data, genres)
    6. artist_genres (depends on artists, genres)
    7. similar_artists (depends on artists)
"""

import argparse
import json
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import Database

# Tables to import in order (respects foreign key dependencies)
TABLES_IN_ORDER = [
    "artists",
    "genres",
    "history",
    "track_data",
    "track_genres",
    "artist_genres",
    "similar_artists",
]

# Column mappings for each table (excluding auto-increment id)
TABLE_COLUMNS = {
    "artists": ["id", "artist", "last_fm_id", "discogs_id", "musicbrainz_id", "enrichment_attempted_at"],
    "genres": ["id", "genre"],
    "history": ["id", "tx_date", "records", "latest_entry"],
    "track_data": [
        "id", "title", "artist", "album", "added_date", "filepath", "location",
        "bpm", "genre", "artist_id", "plex_id", "musicbrainz_id", "acoustid"
    ],
    "track_genres": ["id", "track_id", "genre_id"],
    "artist_genres": ["id", "artist_id", "genre_id"],
    "similar_artists": ["id", "artist_id", "similar_artist_id"],
}


BATCH_SIZE = 1000


def import_table(database: Database, table_name: str, json_path: str) -> int:
    """Import a single table from JSON using batch inserts.

    Args:
        database: Database connection
        table_name: Name of table to import
        json_path: Path to JSON file

    Returns:
        Number of rows imported
    """
    if not os.path.exists(json_path):
        print(f"  Skipping {table_name}: no JSON file found")
        return 0

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print(f"  Skipping {table_name}: empty data")
        return 0

    columns = TABLE_COLUMNS.get(table_name)
    if not columns:
        print(f"  Warning: Unknown table {table_name}, skipping")
        return 0

    # Build INSERT statement
    placeholders = ", ".join(["?"] * len(columns))
    column_names = ", ".join(columns)
    insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

    # Convert rows to tuples
    def row_to_tuple(row: dict) -> tuple:
        values = []
        for col in columns:
            value = row.get(col)
            if value == "":
                value = None
            values.append(value)
        return tuple(values)

    imported = 0
    errors = 0
    total_rows = len(data)

    # Process in batches
    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total_rows)
        batch = data[batch_start:batch_end]
        batch_tuples = [row_to_tuple(row) for row in batch]

        try:
            database.execute_many(insert_sql, batch_tuples)
            imported += len(batch_tuples)
        except Exception as e:
            # Batch failed - fall back to individual inserts for this batch
            print(f"    Batch {batch_start}-{batch_end} failed, falling back to individual inserts: {e}")
            for i, row in enumerate(batch):
                try:
                    database.execute_query(insert_sql, row_to_tuple(row))
                    imported += 1
                except Exception as row_error:
                    errors += 1
                    print(f"    Error inserting row {batch_start + i} in {table_name}: {row_error}")

        # Progress update every batch
        print(f"  {table_name}: {imported}/{total_rows} rows ({100 * imported // total_rows}%)")

    if errors:
        print(f"  Imported {imported} rows into {table_name} ({errors} errors)")
    else:
        print(f"  Imported {imported} rows into {table_name}")
    return imported


def main():
    parser = argparse.ArgumentParser(description="Import JSON data into SQLite database")
    parser.add_argument(
        "--input-dir",
        default="data/mysql_export",
        help="Directory containing JSON files (default: data/mysql_export)",
    )
    parser.add_argument(
        "--db-path",
        default="data/music_organizer.db",
        help="Path to SQLite database (default: data/music_organizer.db)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing tables before import",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input_dir):
        print(f"Error: Input directory does not exist: {args.input_dir}")
        print("Run export_mysql.py first to export data from MySQL.")
        sys.exit(1)

    print(f"SQLite database: {args.db_path}")
    print(f"Input directory: {args.input_dir}")
    print()

    # Connect to SQLite database
    database = Database(args.db_path)
    database.connect()

    if args.drop_existing:
        print("Dropping existing tables...")
        database.drop_all_tables()
        database.connect()  # Reconnect after drop_all_tables closes connection

    # Create all tables
    print("Creating tables...")
    database.create_all_tables()
    print()

    # Disable foreign key checks during import
    database.execute_query("PRAGMA foreign_keys = OFF")

    # Import tables in order
    print("Importing data...")
    total_rows = 0
    for table in TABLES_IN_ORDER:
        json_path = os.path.join(args.input_dir, f"{table}.json")
        rows = import_table(database, table, json_path)
        total_rows += rows

    # Re-enable foreign key checks
    database.execute_query("PRAGMA foreign_keys = ON")

    database.close()

    print()
    print(f"Import complete: {total_rows} total rows imported")
    print()
    print("Next steps:")
    print("  1. Verify with: python scripts/verify_migration.py")
    print("  2. Update .env with: SQLITE_DB_PATH=data/music_organizer.db")


if __name__ == "__main__":
    main()
