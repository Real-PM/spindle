#!/usr/bin/env python3
"""
Production pipeline runner.

Initializes the production database and runs the full pipeline.
"""

import sys
from datetime import datetime

sys.path.insert(0, "/mnt/hdd/PycharmProjects/music_organizer_clean")

from loguru import logger

from config import setup_logging

# Setup logging first
setup_logging("logs/production_run.log")

from db import DB_PATH
from db.database import Database
from pipeline import run_full_pipeline, validate_environment
from plex import PLEX_MUSIC_LIBRARY
from plex.plex_library import get_all_tracks, get_music_library, plex_connect


def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"PRODUCTION PIPELINE RUN - {start_time}")
    logger.info("=" * 60)

    # Connect to production database
    logger.info(f"Connecting to production database: {DB_PATH}")
    db = Database(DB_PATH)

    # Validate environment first
    logger.info("Validating environment...")
    validation = validate_environment(db, use_test=False)
    logger.info(f"Validation result: {validation}")

    if validation["errors"]:
        logger.error(f"Environment validation failed: {validation['errors']}")
        return

    # Initialize database (drop and recreate all tables)
    logger.info("Initializing database - dropping and recreating all tables...")
    db.connect()
    db.create_all_tables()
    db.close()
    logger.info("Database initialized successfully")

    # Connect to production Plex server
    logger.info("Connecting to production Plex server...")
    server = plex_connect(test=False)
    music_library = get_music_library(server, PLEX_MUSIC_LIBRARY)

    # Get track count for progress estimation
    logger.info("Counting tracks in library...")
    tracks, count = get_all_tracks(music_library)
    logger.info(f"Found {count} tracks in production library")

    # Run the full pipeline
    logger.info("=" * 60)
    logger.info("STARTING FULL PIPELINE")
    logger.info("=" * 60)

    stats = run_full_pipeline(
        database=db,
        music_library=music_library,
        use_test_paths=False,  # Production paths
        skip_ffprobe=False,
        skip_lastfm=False,
        skip_bpm=False,
        rate_limit_delay=0.25,  # ~4 req/sec for Last.fm
    )

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Duration: {duration}")
    logger.info(f"Stats: {stats}")

    # Print summary
    print("\n" + "=" * 60)
    print("PRODUCTION PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Duration: {duration}")
    print(f"Total tracks: {stats.get('total_tracks', 'N/A')}")
    print(f"Total artists: {stats.get('total_artists', 'N/A')}")
    print(f"MBID extraction: {stats.get('mbid_extraction', {})}")
    print(f"Last.fm artist: {stats.get('lastfm_artist', {})}")
    print(f"Last.fm track: {stats.get('lastfm_track', {})}")
    print(f"BPM Essentia: {stats.get('bpm_essentia', {})}")

if __name__ == "__main__":
    main()
