#!/usr/bin/env python3
"""
Fetch Spotify audio features for tracks in the library.

This script looks up tracks on Spotify and retrieves audio features
(BPM, energy, danceability, etc.) for playlist generation.

Usage:
    python scripts/fetch_spotify_data.py [--limit N]

Lookup order:
    1. MBID -> MusicBrainz -> Spotify link
    2. Artist + title -> Spotify search

The script marks each track with spotify_attempted_at to avoid re-querying.
"""

import argparse
import sys
from datetime import datetime
from time import sleep

sys.path.insert(0, "/mnt/hdd/PycharmProjects/music_organizer_clean")

from loguru import logger

from config import setup_logging

# Setup logging first
setup_logging("logs/spotify_fetch.log")

import analysis.spotify as spotify
import db.db_functions as dbf
from db import DB_PATH
from db.database import Database


def process_spotify_data(
    database: Database,
    rate_limit_delay: float = 0.1,
    limit: int | None = None,
) -> dict:
    """Fetch Spotify audio features for tracks that need it.

    Args:
        database: Database connection object
        rate_limit_delay: Seconds between API calls. Spotify allows ~30 req/s
            but we're conservative to avoid rate limits.
        limit: Optional limit on number of tracks to process

    Returns:
        dict with stats
    """
    stats = {
        "total": 0,
        "processed": 0,
        "found": 0,
        "not_found": 0,
        "failed": 0,
    }

    logger.info("Starting Spotify audio features fetch")
    logger.info(f"Rate limit delay: {rate_limit_delay}s")

    # Verify credentials
    if not spotify.get_access_token():
        logger.error("Failed to authenticate with Spotify. Check credentials.")
        return stats

    database.connect()

    # Query tracks that haven't been attempted yet
    query = """
        SELECT td.id, a.artist, td.title, td.musicbrainz_id
        FROM track_data td
        INNER JOIN artists a ON td.artist_id = a.id
        WHERE td.spotify_attempted_at IS NULL
    """

    if limit:
        query += f" LIMIT {limit}"

    tracks = database.execute_select_query(query)
    stats["total"] = len(tracks)

    if stats["total"] == 0:
        logger.info("No tracks found needing Spotify lookup")
        database.close()
        return stats

    logger.info(f"Found {stats['total']} tracks to process")

    for i, (track_id, artist, title, mbid) in enumerate(tracks):
        if i > 0:
            sleep(rate_limit_delay)

        stats["processed"] += 1

        logger.debug(f"[{i + 1}/{stats['total']}] {artist} - {title}")

        try:
            # Look up track and get features
            spotify_id, features = spotify.lookup_track_and_features(
                artist=artist,
                title=title,
                mbid=mbid,
            )

            # Mark as attempted regardless of success
            database.execute_query(
                "UPDATE track_data SET spotify_attempted_at = datetime('now') WHERE id = ?",
                (track_id,),
            )

            if spotify_id and features:
                # Extract and store features
                extracted = spotify.extract_useful_features(features)

                database.execute_query(
                    """
                    UPDATE track_data SET
                        spotify_id = ?,
                        spotify_bpm = ?,
                        energy = ?,
                        danceability = ?,
                        valence = ?,
                        acousticness = ?,
                        instrumentalness = ?,
                        spotify_key = ?,
                        spotify_mode = ?,
                        time_signature = ?
                    WHERE id = ?
                    """,
                    (
                        extracted.get("spotify_id"),
                        extracted.get("bpm"),
                        extracted.get("energy"),
                        extracted.get("danceability"),
                        extracted.get("valence"),
                        extracted.get("acousticness"),
                        extracted.get("instrumentalness"),
                        extracted.get("key"),
                        extracted.get("mode"),
                        extracted.get("time_signature"),
                        track_id,
                    ),
                )
                stats["found"] += 1
                logger.debug(f"  Spotify BPM: {extracted.get('bpm')}, energy: {extracted.get('energy')}")
            else:
                stats["not_found"] += 1
                logger.debug("  Not found on Spotify")

        except Exception as e:
            logger.error(f"Error processing track {track_id}: {e}")
            stats["failed"] += 1
            # Still mark as attempted to avoid retrying failed tracks
            database.execute_query(
                "UPDATE track_data SET spotify_attempted_at = datetime('now') WHERE id = ?",
                (track_id,),
            )

        # Progress logging every 100 tracks
        if (i + 1) % 100 == 0:
            elapsed_pct = (i + 1) / stats["total"] * 100
            logger.info(
                f"Progress: {i + 1}/{stats['total']} ({elapsed_pct:.1f}%), "
                f"{stats['found']} found, {stats['not_found']} not found"
            )

    database.close()

    logger.info(
        f"Spotify fetch complete: {stats['total']} tracks, "
        f"{stats['found']} found, {stats['not_found']} not found, {stats['failed']} failed"
    )

    if stats["total"] > 0:
        success_rate = stats["found"] / stats["total"] * 100
        logger.info(f"Success rate: {success_rate:.1f}%")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Fetch Spotify audio features for library")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of tracks to process (for testing)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.1,
        help="Delay between API calls in seconds (default: 0.1)",
    )
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"SPOTIFY DATA FETCH - {start_time}")
    logger.info("=" * 60)

    # Connect to database
    logger.info(f"Connecting to database: {DB_PATH}")
    db = Database(DB_PATH)

    # Run migrations (add Spotify columns if needed)
    logger.info("Running migrations...")
    dbf.add_spotify_columns(db)

    # Process tracks
    stats = process_spotify_data(
        database=db,
        rate_limit_delay=args.rate_limit,
        limit=args.limit,
    )

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("SPOTIFY FETCH COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Duration: {duration}")

    # Print summary
    print("\n" + "=" * 60)
    print("SPOTIFY FETCH COMPLETE")
    print("=" * 60)
    print(f"Duration: {duration}")
    print(f"Total: {stats['total']}")
    print(f"Found: {stats['found']}")
    print(f"Not found: {stats['not_found']}")
    print(f"Failed: {stats['failed']}")
    if stats["total"] > 0:
        print(f"Success rate: {stats['found'] / stats['total'] * 100:.1f}%")


if __name__ == "__main__":
    main()
