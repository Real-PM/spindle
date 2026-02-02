#!/usr/bin/env python3
"""
Export MySQL database to JSON files for SQLite migration.

This script connects to the MySQL database and exports all 7 tables to JSON files.
Run this BEFORE migrating to SQLite to create a backup of your data.

Usage:
    python scripts/export_mysql.py [--output-dir OUTPUT_DIR]

Tables exported:
    - artists
    - genres
    - track_data
    - track_genres
    - artist_genres
    - similar_artists
    - history
"""

import argparse
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import mysql.connector
    from dotenv import load_dotenv
except ImportError:
    print("Error: mysql-connector-python and python-dotenv are required.")
    print("Install with: pip install mysql-connector-python python-dotenv")
    sys.exit(1)

load_dotenv()

# MySQL configuration from environment
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "music_organizer")

# Tables to export in order (respects foreign key dependencies)
TABLES = [
    "artists",
    "genres",
    "history",
    "track_data",
    "track_genres",
    "artist_genres",
    "similar_artists",
]


def json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def export_table(cursor, table_name: str, output_dir: str) -> int:
    """Export a single table to JSON.

    Args:
        cursor: MySQL cursor
        table_name: Name of table to export
        output_dir: Directory to write JSON files

    Returns:
        Number of rows exported
    """
    # Get column names
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [row[0] for row in cursor.fetchall()]

    # Get all rows
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Convert to list of dicts
    data = []
    for row in rows:
        row_dict = {}
        for col_name, value in zip(columns, row):
            row_dict[col_name] = value
        data.append(row_dict)

    # Write to JSON file
    output_path = os.path.join(output_dir, f"{table_name}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, default=json_serializer, indent=2, ensure_ascii=False)

    print(f"  Exported {len(data)} rows from {table_name}")
    return len(data)


def main():
    parser = argparse.ArgumentParser(description="Export MySQL database to JSON for SQLite migration")
    parser.add_argument(
        "--output-dir",
        default="data/mysql_export",
        help="Directory to write JSON files (default: data/mysql_export)",
    )
    parser.add_argument(
        "--database",
        default=MYSQL_DATABASE,
        help=f"MySQL database name (default: {MYSQL_DATABASE})",
    )
    args = parser.parse_args()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Connecting to MySQL database: {args.database}@{MYSQL_HOST}")
    print(f"Output directory: {args.output_dir}")
    print()

    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=args.database,
        )
        cursor = connection.cursor()

        total_rows = 0
        for table in TABLES:
            try:
                rows = export_table(cursor, table, args.output_dir)
                total_rows += rows
            except mysql.connector.Error as e:
                print(f"  Warning: Could not export {table}: {e}")

        cursor.close()
        connection.close()

        print()
        print(f"Export complete: {total_rows} total rows exported to {args.output_dir}")
        print()
        print("Next steps:")
        print("  1. Run: python scripts/import_sqlite.py")
        print("  2. Verify with: python scripts/verify_migration.py")

    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
