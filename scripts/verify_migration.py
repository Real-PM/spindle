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
import random
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
    with open(json_path, encoding="utf-8") as f:
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


SAMPLE_PERCENT = 5  # Check 5% of records

# Columns to verify for each table (subset of important columns)
TABLE_VERIFY_COLUMNS = {
    "artists": ["id", "artist", "musicbrainz_id", "last_fm_id"],
    "genres": ["id", "genre"],
    "history": ["id", "tx_date", "records"],
    "track_data": ["id", "title", "artist", "album", "bpm", "filepath", "musicbrainz_id"],
    "track_genres": ["id", "track_id", "genre_id"],
    "artist_genres": ["id", "artist_id", "genre_id"],
    "similar_artists": ["id", "artist_id", "similar_artist_id"],
}


def normalize_value(value):
    """Normalize values for comparison (handle empty strings vs None)."""
    if value == "" or value is None:
        return None
    return value


def spot_check_table(
    database: Database, table_name: str, json_path: str, sample_percent: int = SAMPLE_PERCENT
) -> tuple[bool, int, int]:
    """Spot check a percentage of records for a table.

    Args:
        database: Database connection
        table_name: Name of table to check
        json_path: Path to JSON file
        sample_percent: Percentage of records to sample (default 5%)

    Returns:
        Tuple of (all_match, checked_count, mismatch_count)
    """
    if not os.path.exists(json_path):
        return True, 0, 0

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        return True, 0, 0

    columns = TABLE_VERIFY_COLUMNS.get(table_name, ["id"])

    # Sample records (at least 1, at most all)
    sample_size = max(1, len(data) * sample_percent // 100)
    samples = random.sample(data, min(sample_size, len(data)))

    # Build SELECT query
    column_names = ", ".join(columns)
    select_sql = f"SELECT {column_names} FROM {table_name} WHERE id = ?"

    mismatches = 0
    for row in samples:
        result = database.execute_select_query(select_sql, (row["id"],))

        if not result:
            print(f"    {table_name} ID {row['id']} not found in SQLite")
            mismatches += 1
            continue

        # Compare each column
        sqlite_row = result[0]
        for i, col in enumerate(columns):
            json_val = normalize_value(row.get(col))
            sqlite_val = normalize_value(sqlite_row[i])

            if json_val != sqlite_val:
                print(f"    {table_name}.{col} mismatch (id={row['id']}): JSON={json_val!r}, SQLite={sqlite_val!r}")
                mismatches += 1
                break  # Only report first mismatch per row

    return mismatches == 0, len(samples), mismatches


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

    # Spot checks (5% of each table)
    print(f"Spot checks ({SAMPLE_PERCENT}% random sample per table):")
    print("-" * 60)
    print(f"{'Table':<20} {'Checked':>10} {'Errors':>10} {'Result':>10}")
    print("-" * 60)

    spot_checks_ok = True
    total_checked = 0
    total_errors = 0

    for table in TABLES:
        json_path = os.path.join(args.input_dir, f"{table}.json")
        ok, checked, errors = spot_check_table(database, table, json_path)
        total_checked += checked
        total_errors += errors
        if not ok:
            spot_checks_ok = False
        result = "✓ OK" if ok else "✗ FAIL"
        print(f"{table:<20} {checked:>10} {errors:>10} {result:>10}")

    print("-" * 60)
    total_result = "✓ OK" if spot_checks_ok else "✗ FAIL"
    print(f"{'TOTAL':<20} {total_checked:>10} {total_errors:>10} {total_result:>10}")

    database.close()

    print()
    if all_match and spot_checks_ok:
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
