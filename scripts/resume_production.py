#!/usr/bin/env python3
"""
Resume production pipeline from current database state.

Unlike run_production.py, this does NOT drop tables. It queries the database
to find incomplete records and resumes from where it left off.
"""

import sys
from datetime import datetime

sys.path.insert(0, "/mnt/hdd/PycharmProjects/music_organizer_clean")

from loguru import logger

from config import setup_logging

# Setup logging first
setup_logging("logs/resume_production.log")

import db.db_functions as dbf
import db.db_update as dbu
from db import DB_PATH
from db.database import Database


def check_status(db: Database) -> dict:
    """Check current enrichment status."""
    db.connect()

    status = {}

    # Track counts
    status["total_tracks"] = db.execute_select_query("SELECT COUNT(*) FROM track_data")[0][0]
    status["tracks_with_mbid"] = db.execute_select_query(
        "SELECT COUNT(*) FROM track_data WHERE musicbrainz_id IS NOT NULL AND musicbrainz_id != ''"
    )[0][0]
    status["tracks_with_acoustid"] = db.execute_select_query(
        "SELECT COUNT(*) FROM track_data WHERE acoustid IS NOT NULL AND acoustid != ''"
    )[0][0]
    status["tracks_with_bpm"] = db.execute_select_query(
        "SELECT COUNT(*) FROM track_data WHERE bpm IS NOT NULL AND bpm > 0"
    )[0][0]

    # Artist counts
    status["total_artists"] = db.execute_select_query("SELECT COUNT(*) FROM artists")[0][0]
    status["primary_artists"] = db.execute_select_query(
        "SELECT COUNT(DISTINCT a.id) FROM artists a INNER JOIN track_data td ON a.id = td.artist_id"
    )[0][0]

    db.close()

    # These use their own connections
    status["primary_unenriched"] = len(dbf.get_primary_artists_without_similar(db))
    status["stubs_unenriched"] = len(dbf.get_stub_artists_without_mbid(db))

    return status


def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"RESUME PRODUCTION PIPELINE - {start_time}")
    logger.info("=" * 60)

    # Connect to production database (NO table drops!)
    logger.info(f"Connecting to production database: {DB_PATH}")
    db = Database(DB_PATH)

    # Run migrations (idempotent)
    logger.info("Running migrations...")
    dbf.add_acoustid_column(db)
    dbf.add_enrichment_attempted_column(db)
    dbf.add_lastfm_attempted_column(db)
    dbf.add_researched_at_column(db)

    # Check current status
    logger.info("Checking current database status...")
    status = check_status(db)

    logger.info(f"Tracks: {status['total_tracks']} total, {status['tracks_with_mbid']} with MBID, "
                f"{status['tracks_with_acoustid']} with AcousticID, {status['tracks_with_bpm']} with BPM")
    logger.info(f"Artists: {status['total_artists']} total, {status['primary_artists']} primary")
    logger.info(f"Incomplete: {status['primary_unenriched']} primary artists need enrichment, "
                f"{status['stubs_unenriched']} stubs need enrichment")

    # Phase 1: Complete primary artist enrichment
    if status["primary_unenriched"] > 0:
        logger.info("=" * 60)
        logger.info(f"PHASE 1: Artist enrichment ({status['primary_unenriched']} remaining)")
        logger.info("=" * 60)

        incomplete = dbf.get_primary_artists_without_similar(db)
        artist_ids = [a[0] for a in incomplete]

        dbu.enrich_artists_full(db, artist_ids=artist_ids, rate_limit_delay=0.25)
    else:
        logger.info("PHASE 1: Artist enrichment already complete")

    # Phase 2: Stub artist enrichment
    status["stubs_unenriched"] = len(dbf.get_stub_artists_without_mbid(db))
    if status["stubs_unenriched"] > 0:
        logger.info("=" * 60)
        logger.info(f"PHASE 2: Stub artist enrichment ({status['stubs_unenriched']} remaining)")
        logger.info("=" * 60)

        incomplete_stubs = dbf.get_stub_artists_without_mbid(db)
        stub_ids = [a[0] for a in incomplete_stubs]

        dbu.enrich_artists_core(db, artist_ids=stub_ids, rate_limit_delay=0.25)
    else:
        logger.info("PHASE 2: Stub artist enrichment already complete")

    # Phase 3: Track enrichment
    logger.info("=" * 60)
    logger.info("PHASE 3: Last.fm track enrichment")
    logger.info("=" * 60)

    track_stats = dbu.process_lastfm_track_data(db, rate_limit_delay=0.25)
    logger.info(f"Track enrichment: {track_stats}")

    # Phase 4: BPM enrichment (Essentia local analysis)
    logger.info("=" * 60)
    logger.info("PHASE 4: Essentia BPM analysis")
    logger.info("=" * 60)

    bpm_essentia_stats = dbu.process_bpm_essentia(
        db,
        use_test_paths=False,
        batch_size=25,
        rest_between_batches=10.0,
        include_researched=True,
    )
    logger.info(f"Essentia BPM: {bpm_essentia_stats}")

    # Mark all tracks as researched after BPM phase completes
    researched_count = dbu.mark_tracks_researched(db)
    logger.info(f"Marked {researched_count} tracks as researched")

    # Record in history so incremental runs know the cutoff date
    dbf.update_history(db, status["total_tracks"])

    # Final status
    end_time = datetime.now()
    duration = end_time - start_time

    final_status = check_status(db)

    logger.info("=" * 60)
    logger.info("RESUME COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Duration: {duration}")
    logger.info(f"Final tracks with BPM: {final_status['tracks_with_bpm']}/{final_status['total_tracks']}")

    print("\n" + "=" * 60)
    print("RESUME COMPLETE")
    print("=" * 60)
    print(f"Duration: {duration}")
    print(f"Tracks: {final_status['total_tracks']}")
    print(f"  - With MBID: {final_status['tracks_with_mbid']}")
    print(f"  - With AcousticID: {final_status['tracks_with_acoustid']}")
    print(f"  - With BPM: {final_status['tracks_with_bpm']}")


if __name__ == "__main__":
    main()
