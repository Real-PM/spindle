#!/usr/bin/env python3
"""
Populate genre_groups and genre_group_members tables from curated data.

Reads group definitions from analysis/genre_groups_data.py and inserts them
into the database. Idempotent — uses INSERT OR IGNORE.

Usage:
    python scripts/populate_genre_groups.py              # Production DB
    python scripts/populate_genre_groups.py --dry-run    # Preview only
    python scripts/populate_genre_groups.py --sandbox    # Sandbox DB
"""

import argparse
import sys

sys.path.insert(0, "/mnt/hdd/PycharmProjects/music_organizer_clean")

from loguru import logger

from config import setup_logging

setup_logging("logs/populate_genre_groups.log")

from analysis.genre_groups_data import GENRE_GROUPS
from db import DB_PATH, TEST_DB_PATH
from db.database import Database
from db.db_functions import add_genre_normalization_tables


def populate_groups(db_path: str, dry_run: bool = False) -> dict:
    """Populate genre groups and memberships from curated data.

    Args:
        db_path: Path to SQLite database
        dry_run: If True, only report what would happen

    Returns:
        Dict with stats
    """
    stats = {
        "groups_created": 0,
        "members_linked": 0,
        "genres_not_found": [],
    }

    database = Database(db_path)

    if not dry_run:
        add_genre_normalization_tables(database)

    # Build genre name → id lookup (case-insensitive)
    database.connect()
    genre_rows = database.execute_select_query("SELECT id, genre FROM genres")
    genre_lookup: dict[str, int] = {row[1].lower(): row[0] for row in genre_rows}

    for group_def in GENRE_GROUPS:
        name = group_def["name"]
        display_name = group_def["display_name"]
        description = group_def.get("description", "")
        sort_order = group_def.get("sort_order", 0)
        members = group_def["members"]

        # Find matching genre IDs
        matched_ids: list[tuple[str, int]] = []
        for member_name in members:
            genre_id = genre_lookup.get(member_name.lower())
            if genre_id:
                matched_ids.append((member_name, genre_id))
            else:
                stats["genres_not_found"].append((name, member_name))

        if dry_run:
            logger.info(
                f"[DRY RUN] Group '{display_name}': "
                f"{len(matched_ids)}/{len(members)} genres matched"
            )
            for _missing_group, missing_genre in [
                (g, m) for g, m in stats["genres_not_found"] if g == name
            ]:
                logger.warning(f"  Not found: {missing_genre}")
            stats["groups_created"] += 1
            stats["members_linked"] += len(matched_ids)
            continue

        # Insert group
        database.execute_query(
            """
            INSERT OR IGNORE INTO genre_groups (name, display_name, description, sort_order)
            VALUES (?, ?, ?, ?)
            """,
            (name, display_name, description, sort_order),
        )

        # Get group ID (may already exist)
        result = database.execute_select_query(
            "SELECT id FROM genre_groups WHERE name = ?", (name,)
        )
        if not result:
            logger.error(f"Failed to get ID for group: {name}")
            continue

        group_id = result[0][0]
        stats["groups_created"] += 1

        # Update display_name/description/sort_order in case group already existed
        database.execute_query(
            """
            UPDATE genre_groups
            SET display_name = ?, description = ?, sort_order = ?
            WHERE id = ?
            """,
            (display_name, description, sort_order, group_id),
        )

        # Insert memberships
        for member_name, genre_id in matched_ids:
            database.execute_query(
                """
                INSERT OR IGNORE INTO genre_group_members (group_id, genre_id)
                VALUES (?, ?)
                """,
                (group_id, genre_id),
            )
            stats["members_linked"] += 1
            logger.debug(f"  Linked {member_name} (id={genre_id}) to group {name}")

        logger.info(
            f"Group '{display_name}': {len(matched_ids)}/{len(members)} genres linked"
        )

    database.close()

    # Report missing genres
    if stats["genres_not_found"]:
        logger.warning(f"{len(stats['genres_not_found'])} genre references not found in DB:")
        for group_name, genre_name in stats["genres_not_found"]:
            logger.warning(f"  {group_name} -> {genre_name}")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Populate genre groups in the database")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to the database",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="Run against the sandbox database instead of production",
    )
    args = parser.parse_args()

    db_path = TEST_DB_PATH if args.sandbox else DB_PATH
    logger.info(f"Using database: {db_path}")
    logger.info(f"Dry run: {args.dry_run}")

    stats = populate_groups(db_path, dry_run=args.dry_run)

    print("\nResults:")
    print(f"  Groups created/updated: {stats['groups_created']}")
    print(f"  Genre memberships:      {stats['members_linked']}")
    print(f"  Genres not found:       {len(stats['genres_not_found'])}")
    if stats["genres_not_found"]:
        print("\n  Missing genres:")
        for group_name, genre_name in stats["genres_not_found"]:
            print(f"    {group_name} -> {genre_name}")


if __name__ == "__main__":
    main()
