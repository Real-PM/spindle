#!/usr/bin/env python3
"""
Incremental update pipeline.

Adds new tracks from Plex (added since last run) and enriches them.
Does NOT re-process existing tracks.

Usage:
    python scripts/run_incremental.py [--since-date YYYY-MM-DD]

If --since-date is not provided, uses the last entry in the history table.
"""

import argparse
import sys
from datetime import datetime

sys.path.insert(0, "/mnt/hdd/PycharmProjects/music_organizer_clean")

from loguru import logger

from config import setup_logging

# Setup logging first
setup_logging("logs/incremental_update.log")

import db.db_functions as dbf
from db import DB_PATH
from db.database import Database
from pipeline import run_incremental_update, validate_environment
from plex import PLEX_MUSIC_LIBRARY
from plex.plex_library import get_music_library, plex_connect


def main():
    parser = argparse.ArgumentParser(description="Run incremental update pipeline")
    parser.add_argument(
        "--since-date",
        type=str,
        default=None,
        help="Only process tracks added after this date (YYYY-MM-DD). Default: last history entry.",
    )
    parser.add_argument(
        "--skip-ffprobe",
        action="store_true",
        help="Skip ffprobe MBID extraction from files",
    )
    parser.add_argument(
        "--skip-lastfm",
        action="store_true",
        help="Skip Last.fm enrichment",
    )
    parser.add_argument(
        "--skip-bpm",
        action="store_true",
        help="Skip BPM enrichment (Essentia local analysis)",
    )
    parser.add_argument(
        "--retry-bpm",
        action="store_true",
        help="Re-attempt BPM for all tracks with NULL BPM, including previously researched tracks",
    )
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"INCREMENTAL UPDATE PIPELINE - {start_time}")
    logger.info("=" * 60)

    # Connect to production database
    logger.info(f"Connecting to production database: {DB_PATH}")
    db = Database(DB_PATH)

    # Run migrations (idempotent)
    logger.info("Running migrations...")
    dbf.add_acoustid_column(db)
    dbf.add_enrichment_attempted_column(db)
    dbf.add_lastfm_attempted_column(db)
    dbf.add_researched_at_column(db)

    # Validate environment
    logger.info("Validating environment...")
    validation = validate_environment(db, use_test=False)

    if validation["errors"]:
        logger.error(f"Environment validation failed: {validation['errors']}")
        print(f"Environment validation failed: {validation['errors']}")
        sys.exit(1)

    # Check what date we're using
    if args.since_date:
        since_date = args.since_date
        logger.info(f"Using provided since_date: {since_date}")
    else:
        since_date = dbf.get_latest_added_date(db)
        if since_date:
            since_date = since_date if isinstance(since_date, str) else since_date.strftime("%Y-%m-%d")
            logger.info(f"Using latest added_date from track_data: {since_date}")
        else:
            logger.warning("No tracks in database - will process ALL tracks")
            since_date = None

    # Connect to Plex
    logger.info("Connecting to Plex server...")
    server = plex_connect(test=False)
    music_library = get_music_library(server, PLEX_MUSIC_LIBRARY)
    logger.info(f"Connected to library: {PLEX_MUSIC_LIBRARY}")

    # Run incremental update
    logger.info("=" * 60)
    logger.info("STARTING INCREMENTAL UPDATE")
    logger.info("=" * 60)

    stats = run_incremental_update(
        database=db,
        music_library=music_library,
        use_test_paths=False,
        since_date=since_date,
        skip_ffprobe=args.skip_ffprobe,
        skip_lastfm=args.skip_lastfm,
        skip_bpm=args.skip_bpm,
        retry_bpm=args.retry_bpm,
        rate_limit_delay=0.25,
    )

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("INCREMENTAL UPDATE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Duration: {duration}")
    logger.info(f"Stats: {stats}")

    # Print summary
    print("\n" + "=" * 60)
    print("INCREMENTAL UPDATE COMPLETE")
    print("=" * 60)
    print(f"Duration: {duration}")
    print(f"Since date: {stats.get('since_date', 'N/A')}")
    print(f"New tracks: {stats.get('new_tracks', 0)}")
    print(f"New artists: {stats.get('new_artists', 0)}")

    if stats.get('mbid_extraction'):
        print(f"MBID extraction: {stats['mbid_extraction']}")
    if stats.get('lastfm_artist'):
        print(f"Last.fm artist enrichment: {stats['lastfm_artist']}")
    if stats.get('lastfm_track'):
        print(f"Last.fm track enrichment: {stats['lastfm_track']}")
    if stats.get('bpm_essentia'):
        print(f"BPM Essentia: {stats['bpm_essentia']}")
    if stats.get('tracks_researched'):
        print(f"Tracks marked researched: {stats['tracks_researched']}")


if __name__ == "__main__":
    main()
