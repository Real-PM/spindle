#!/usr/bin/env python3
"""
Verify SQLite migration by comparing row counts and spot-checking data.

This script compares the exported JSON files with the SQLite database
to ensure all data was migrated correctly.

Usage:
    python scripts/verify_migration.py [--input-dir INPUT_DIR] [--db-path DB_PATH]
"""

import argparse
import json
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.database import Database

# Tables to verify
TABLES = [
    "artists",
    "genres",
    "history",
    "track_data",
    "track_genres",
    "artist_genres",
    "similar_artists",
]


def count_json_rows(json_path: str) -> int:
    """Count rows in a JSON file."""
    if not os.path.exists(json_path):
        return -1
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return len(data)


def count_sqlite_rows(database: Database, table_name: str) -> int:
    """Count rows in a SQLite table."""
    try:
        result = database.execute_select_query(f"SELECT COUNT(*) FROM {table_name}")
        return result[0][0] if result else -1
    except Exception as e:
        print(f"    Error counting {table_name}: {e}")
        return -1


def spot_check_artists(database: Database, json_path: str) -> bool:
    """Spot check a few artists to verify data integrity."""
    if not os.path.exists(json_path):
        return True

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        return True

    # Check first, middle, and last artist
    samples = [data[0]]
    if len(data) > 1:
        samples.append(data[len(data) // 2])
    if len(data) > 2:
        samples.append(data[-1])

    all_match = True
    for artist in samples:
        result = database.execute_select_query(
            "SELECT artist, musicbrainz_id FROM artists WHERE id = ?",
            (artist["id"],)
        )
        if not result:
            print(f"    Artist ID {artist['id']} not found in SQLite")
            all_match = False
        elif result[0][0] != artist["artist"]:
            print(f"    Artist mismatch: JSON={artist['artist']}, SQLite={result[0][0]}")
            all_match = False

    return all_match


def spot_check_tracks(database: Database, json_path: str) -> bool:
    """Spot check a few tracks to verify data integrity."""
    if not os.path.exists(json_path):
        return True

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        return True

    # Check first, middle, and last track
    samples = [data[0]]
    if len(data) > 1:
        samples.append(data[len(data) // 2])
    if len(data) > 2:
        samples.append(data[-1])

    all_match = True
    for track in samples:
        result = database.execute_select_query(
            "SELECT title, artist, bpm FROM track_data WHERE id = ?",
            (track["id"],)
        )
        if not result:
            print(f"    Track ID {track['id']} not found in SQLite")
            all_match = False
        elif result[0][0] != track["title"]:
            print(f"    Track title mismatch: JSON={track['title']}, SQLite={result[0][0]}")
            all_match = False

    return all_match


def main():
    parser = argparse.ArgumentParser(description="Verify SQLite migration")
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
    args = parser.parse_args()

    if not os.path.exists(args.input_dir):
        print(f"Error: Input directory does not exist: {args.input_dir}")
        print("Run export_mysql.py first to export data from MySQL.")
        sys.exit(1)

    if not os.path.exists(args.db_path):
        print(f"Error: SQLite database does not exist: {args.db_path}")
        print("Run import_sqlite.py first to import data.")
        sys.exit(1)

    print(f"Comparing: {args.input_dir} (JSON) vs {args.db_path} (SQLite)")
    print()

    # Connect to SQLite database
    database = Database(args.db_path)
    database.connect()

    # Compare row counts
    print("Row counts:")
    print("-" * 50)
    print(f"{'Table':<20} {'JSON':>10} {'SQLite':>10} {'Match':>8}")
    print("-" * 50)

    all_match = True
    total_json = 0
    total_sqlite = 0

    for table in TABLES:
        json_path = os.path.join(args.input_dir, f"{table}.json")
        json_count = count_json_rows(json_path)
        sqlite_count = count_sqlite_rows(database, table)

        if json_count >= 0:
            total_json += json_count
        if sqlite_count >= 0:
            total_sqlite += sqlite_count

        match = "✓" if json_count == sqlite_count else "✗"
        if json_count != sqlite_count:
            all_match = False

        json_str = str(json_count) if json_count >= 0 else "N/A"
        sqlite_str = str(sqlite_count) if sqlite_count >= 0 else "N/A"
        print(f"{table:<20} {json_str:>10} {sqlite_str:>10} {match:>8}")

    print("-" * 50)
    total_match = "✓" if total_json == total_sqlite else "✗"
    print(f"{'TOTAL':<20} {total_json:>10} {total_sqlite:>10} {total_match:>8}")
    print()

    # Spot checks
    print("Spot checks:")
    print("-" * 50)

    artists_ok = spot_check_artists(database, os.path.join(args.input_dir, "artists.json"))
    print(f"  Artists: {'✓ OK' if artists_ok else '✗ MISMATCH'}")

    tracks_ok = spot_check_tracks(database, os.path.join(args.input_dir, "track_data.json"))
    print(f"  Tracks: {'✓ OK' if tracks_ok else '✗ MISMATCH'}")

    database.close()

    print()
    if all_match and artists_ok and tracks_ok:
        print("✓ Migration verified successfully!")
        print()
        print("You can now:")
        print("  1. Update .env with: SQLITE_DB_PATH=data/music_organizer.db")
        print("  2. Remove mysql-connector-python from requirements.txt")
        print("  3. Delete the JSON export files if no longer needed")
    else:
        print("✗ Migration verification FAILED - please investigate the mismatches above")
        sys.exit(1)


if __name__ == "__main__":
    main()
