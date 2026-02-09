#!/usr/bin/env python3
"""
Populate genre_aliases table from normalization engine.

Reads all genres from the database, normalizes them, inserts canonical
genres if they don't exist, and creates genre_aliases mappings.

Only adds rows — never modifies existing data.

Usage:
    python scripts/normalize_genres.py              # Execute against production DB
    python scripts/normalize_genres.py --dry-run    # Preview changes without writing
    python scripts/normalize_genres.py --sandbox    # Execute against sandbox DB
"""

import argparse
import sys

sys.path.insert(0, "/mnt/hdd/PycharmProjects/music_organizer_clean")

from loguru import logger

from config import setup_logging

setup_logging("logs/normalize_genres.log")

from analysis.genre_normalize import build_normalization_map, find_duplicate_clusters
from db import DB_PATH, TEST_DB_PATH
from db.database import Database
from db.db_functions import add_genre_normalization_tables


def get_all_genres_with_ids(database: Database) -> list[tuple[int, str]]:
    """Fetch all (id, genre) pairs from the genres table."""
    database.connect()
    rows = database.execute_select_query("SELECT id, genre FROM genres ORDER BY id")
    database.close()
    return rows


def run_normalization(db_path: str, dry_run: bool = False) -> dict:
    """Run the full normalization pipeline.

    Args:
        db_path: Path to SQLite database
        dry_run: If True, only report what would change without writing

    Returns:
        Dict with stats: canonical_new, aliases_created, clusters_found
    """
    stats = {
        "total_genres": 0,
        "canonical_new": 0,
        "aliases_created": 0,
        "clusters_found": 0,
        "identity_mappings": 0,
    }

    database = Database(db_path)

    # Ensure tables exist
    if not dry_run:
        add_genre_normalization_tables(database)

    # Fetch all existing genres
    genre_rows = get_all_genres_with_ids(database)
    stats["total_genres"] = len(genre_rows)
    logger.info(f"Found {stats['total_genres']} genres in database")

    if not genre_rows:
        logger.info("No genres to normalize")
        return stats

    # Build normalization map
    raw_genres = [row[1] for row in genre_rows]
    norm_map = build_normalization_map(raw_genres)

    # Show duplicate clusters
    clusters = find_duplicate_clusters(raw_genres)
    stats["clusters_found"] = len(clusters)
    logger.info(f"Found {stats['clusters_found']} duplicate clusters")

    if dry_run:
        logger.info("=== DRY RUN - showing clusters ===")
        for canonical, variants in sorted(clusters.items()):
            logger.info(f"  {canonical} <- {variants}")

        # Count how many new canonical genres would be needed
        existing_genres_lower = {g.lower() for g in raw_genres}
        canonical_values = set(norm_map.values())
        new_canonicals = {c for c in canonical_values if c not in existing_genres_lower}
        stats["canonical_new"] = len(new_canonicals)
        if new_canonicals:
            logger.info(f"Would create {len(new_canonicals)} new canonical genres:")
            for c in sorted(new_canonicals):
                logger.info(f"  + {c}")

        # Count aliases
        for raw, canonical in norm_map.items():
            if raw.lower() != canonical:
                stats["aliases_created"] += 1
            else:
                stats["identity_mappings"] += 1

        logger.info(f"Would create {stats['aliases_created']} non-identity aliases")
        logger.info(f"Would create {stats['identity_mappings']} identity aliases (self → self)")
        return stats

    # --- Write mode ---
    database.connect()

    # Build lookup: lowercase genre → genre_id for existing genres
    genre_id_lookup: dict[str, int] = {}
    for gid, gname in genre_rows:
        genre_id_lookup[gname.lower()] = gid

    # Insert new canonical genres that don't exist yet
    canonical_values = set(norm_map.values())
    for canonical in sorted(canonical_values):
        if canonical not in genre_id_lookup:
            database.execute_query(
                "INSERT INTO genres (genre) VALUES (?)", (canonical,)
            )
            # Fetch the new ID
            result = database.execute_select_query(
                "SELECT id FROM genres WHERE genre = ?", (canonical,)
            )
            if result:
                genre_id_lookup[canonical] = result[0][0]
                stats["canonical_new"] += 1
                logger.info(f"Inserted new canonical genre: {canonical} (id={result[0][0]})")

    # Create genre_aliases entries
    for raw_genre, canonical in norm_map.items():
        raw_id = genre_id_lookup.get(raw_genre.lower())
        canonical_id = genre_id_lookup.get(canonical)

        if raw_id is None:
            logger.warning(f"No genre_id found for raw genre: {raw_genre}")
            continue
        if canonical_id is None:
            logger.warning(f"No genre_id found for canonical genre: {canonical}")
            continue

        # Insert alias (skip if already exists via UNIQUE constraint)
        database.execute_query(
            """
            INSERT OR IGNORE INTO genre_aliases (raw_genre_id, canonical_genre_id)
            VALUES (?, ?)
            """,
            (raw_id, canonical_id),
        )
        stats["aliases_created"] += 1

        if raw_genre.lower() == canonical:
            stats["identity_mappings"] += 1

    database.close()

    logger.info(
        f"Normalization complete: {stats['canonical_new']} new canonical genres, "
        f"{stats['aliases_created']} aliases created"
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Normalize genre tags in the database")
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

    stats = run_normalization(db_path, dry_run=args.dry_run)

    print("\nResults:")
    print(f"  Total genres:         {stats['total_genres']}")
    print(f"  Duplicate clusters:   {stats['clusters_found']}")
    print(f"  New canonical genres: {stats['canonical_new']}")
    print(f"  Aliases created:      {stats['aliases_created']}")
    print(f"  Identity mappings:    {stats['identity_mappings']}")


if __name__ == "__main__":
    main()
