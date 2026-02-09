#!/usr/bin/env python3
"""Compare artists between two Plex libraries to find missing artists.

Finds artists that exist in the source library but not in the target library.
Useful for identifying accidentally deleted artists that need to be restored.

Usage:
    python maint/compare_plex_artists.py

Configure the server/library names below or via command line arguments.
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os

from dotenv import load_dotenv
from plexapi.myplex import MyPlexAccount

load_dotenv()

PLEX_USER = os.getenv("PLEX_USER", "")
PLEX_PASSWORD = os.getenv("PLEX_PASSWORD", "")


def connect_to_server(account: MyPlexAccount, server_name: str):
    """Connect to a Plex server by name."""
    try:
        server = account.resource(server_name).connect()
        print(f"Connected to: {server_name}")
        return server
    except Exception as e:
        print(f"Error connecting to {server_name}: {e}")
        sys.exit(1)


def get_artists_from_library(server, library_name: str) -> set[str]:
    """Get all artist names from a Plex music library."""
    try:
        library = server.library.section(library_name)
        artists = library.searchArtists()
        artist_names = {a.title for a in artists}
        print(f"Found {len(artist_names)} artists in '{library_name}'")
        return artist_names
    except Exception as e:
        print(f"Error getting artists from '{library_name}': {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Find artists in source library that are missing from target library"
    )
    parser.add_argument(
        "--source-server",
        default="Schroeder",
        help="Server name containing the backup/source library",
    )
    parser.add_argument(
        "--source-library",
        default="Music - Schroeder",
        help="Source library name (the backup)",
    )
    parser.add_argument(
        "--target-server",
        default=os.getenv("PLEX_SERVER_NAME", ""),
        help="Server name containing the production/target library",
    )
    parser.add_argument(
        "--target-library",
        default=os.getenv("PLEX_MUSIC_LIBRARY", "Music"),
        help="Target library name (production)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (optional, prints to stdout if not specified)",
    )
    args = parser.parse_args()

    if not PLEX_USER or not PLEX_PASSWORD:
        print("Error: PLEX_USER and PLEX_PASSWORD must be set in .env")
        sys.exit(1)

    print(f"Logging in as {PLEX_USER}...")
    account = MyPlexAccount(PLEX_USER, PLEX_PASSWORD)

    # Connect to both servers
    source_server = connect_to_server(account, args.source_server)
    target_server = connect_to_server(account, args.target_server)

    # Get artists from both libraries
    print()
    source_artists = get_artists_from_library(source_server, args.source_library)
    target_artists = get_artists_from_library(target_server, args.target_library)

    # Find artists in source that are missing from target
    missing_artists = source_artists - target_artists
    missing_sorted = sorted(missing_artists, key=str.lower)

    # Output results
    print(f"\n{'='*60}")
    print(f"Artists in '{args.source_library}' missing from '{args.target_library}':")
    print(f"{'='*60}")
    print(f"Total missing: {len(missing_sorted)}")
    print()

    if args.output:
        with open(args.output, "w") as f:
            for artist in missing_sorted:
                f.write(f"{artist}\n")
        print(f"Written to: {args.output}")
    else:
        for artist in missing_sorted:
            print(artist)


if __name__ == "__main__":
    main()
