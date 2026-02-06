# Session Continuity - 2026-02-04

## What Was Accomplished (Session 10)

### Pipeline Progress
- Phase 3 (Last.fm track enrichment): **Complete** - all 36,874 tracks processed
- Phase 4 (AcousticBrainz BPM): **Complete** - 13,239 hits out of 30,203 tracks with MBIDs (43.8% hit rate)
- Phase 5 (Essentia BPM): **Crashed at 53.8%** (12,720/23,635 tracks)

### Essentia Crash on Spoken Word Content
Pipeline crashed with SEGV while analyzing `Pimsleur_-_Spanish_I_-_Lesson_19.mp3` (track_id=21234). Essentia's RhythmExtractor2013 doesn't handle speech-only audio gracefully.

**Fix:** Removed 109 spoken word tracks from database:
- 104 Pimsleur language lessons (filepath match)
- 5 tracks with "Spoken" genre
- 5 tracks with "Comedy" genre (stand-up, not music)

### Resume Point
Restart `scripts/resume_production.py` - will resume Phase 5 at track ~12,720 of ~23,526 (adjusted after deletions).

### Commands to Remember
```bash
# Re-enable system sleep after pipeline completes
sudo systemctl unmask sleep.target suspend.target hibernate.target
```

---

## What Was Accomplished (Session 9)

### SQLite Migration Completed

Successfully migrated from MySQL to SQLite for improved portability and eliminated connection issues.

**Changes:**
- `db/database.py` - Refactored to use SQLite exclusively
- `db/__init__.py` - Now exports `SQLITE_DB_PATH` and `SQLITE_TEST_DB_PATH`
- All SQL updated to use `?` placeholders instead of `%s`
- Added `execute_many()` for batch inserts (1000x faster imports)
- Migration scripts: `scripts/export_mysql.py`, `scripts/import_sqlite.py`, `scripts/verify_migration.py`

### Last.fm Track Enrichment Tracking

Added `lastfm_attempted_at` column to track_data to prevent re-querying tracks that Last.fm doesn't have data for.

**Changes:**
- `db/db_functions.py` - Added `add_lastfm_attempted_column()` migration
- `db/db_update.py` - `process_lastfm_track_data()` now sets timestamp after each attempt
- Query filters by `lastfm_attempted_at IS NULL` instead of checking track_genres

### API Request Timeouts

Added 30-second timeout to all Last.fm API requests to prevent hangs during network issues.

**Changes:**
- `analysis/lastfm.py` - Added `REQUEST_TIMEOUT = 30` and timeout parameter to all `requests.get()` calls

### Incremental Update Script

Created `scripts/run_incremental.py` for adding new tracks to the library without re-processing existing data.

### Spotify Integration (NEW)

Added Spotify API integration for fetching audio features (BPM, energy, danceability, etc.)

**New Files:**
- `analysis/spotify.py` - Spotify API client with:
  - OAuth client credentials flow
  - Track search (by artist+title, ISRC, or MBID→MusicBrainz→Spotify link)
  - Audio features fetching (single and batch)
  - Rate limit handling with automatic retry
- `scripts/fetch_spotify_data.py` - Backfill script for existing library

**New Columns in track_data:**
- `spotify_id` - Spotify track ID
- `spotify_bpm` - BPM from Spotify (separate from our bpm column)
- `energy`, `danceability`, `valence`, `acousticness`, `instrumentalness` - Audio features (0.0-1.0)
- `spotify_key`, `spotify_mode`, `time_signature` - Musical key info
- `spotify_attempted_at` - Tracking column

**Migration:**
- `db/db_functions.py` - Added `add_spotify_columns()` migration

**Configuration Required:**
Add to `.env`:
```
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
```
Get credentials at: https://developer.spotify.com/dashboard

**Usage:**
```bash
# Backfill existing library
python scripts/fetch_spotify_data.py

# With limit for testing
python scripts/fetch_spotify_data.py --limit 100
```

---

## Planned Features

### SQLite Migration (Next Major Feature)

Migrate from MySQL to SQLite to improve portability and eliminate connection issues.

**Motivation:**
- Remote MySQL connections drop during long-running API calls (stale connection errors)
- Lower barrier to entry for other users (no MySQL setup required)
- Single-file database = easy backup/portability
- Data volume (~37k tracks) well within SQLite capacity

**Migration Plan:**
1. Abstract database layer (mostly done via `Database` class)
2. Replace MySQL-specific syntax:
   - Placeholders: `%s` → `?`
   - Auto-increment: `AUTO_INCREMENT` → `AUTOINCREMENT`
   - Any MySQL-specific functions
3. Add auto-reconnect logic to `Database` class (interim fix for MySQL)
4. Write migration script: export MySQL → transform → import SQLite
5. Update `.env` config to support both backends (for transition period)

**Files to Modify:**
- `db/database.py` - Add SQLite support, connection abstraction
- `db/__init__.py` - Config for database backend selection
- `db/db_functions.py` - Verify SQL compatibility
- `db/db_update.py` - Verify SQL compatibility
- New: `scripts/migrate_to_sqlite.py`

### Enrichment Tracking & Connection Keep-Alive (Next Up)

Two fixes to implement after current pipeline run completes:

**1. `enrichment_attempted_at` Column**

Prevents re-processing artists that Last.fm doesn't recognize (e.g., "feat." artists).

- Add `enrichment_attempted_at TIMESTAMP NULL` to `artists` table
- Migration function: `add_enrichment_attempted_column()` (idempotent)
- Update `get_primary_artists_without_similar()` → check `enrichment_attempted_at IS NULL`
- Update `get_stub_artists_without_mbid()` → same
- Update `enrich_artists_full()` and `enrich_artists_core()` → set timestamp after each artist

**2. MySQL Keep-Alive / Auto-Reconnect**

Handles connection drops from hibernation or long API calls.

- Add `ensure_connection()` → ping with reconnect
- Add `execute_with_reconnect()` → wrapper that catches OperationalError and retries
- Call `ensure_connection()` in enrichment loops before each artist/track

**Files to Modify:**
- `db/database.py` - Add keep-alive methods
- `db/db_functions.py` - Add migration, update detection queries
- `db/db_update.py` - Set timestamp after enrichment, use keep-alive in loops

---

### AI-Powered Genre Grouping

Use AI to semantically group entries in the `genres` table by "feel" or mood, enabling broader net for smart playlist generation.

**Motivation:**
- Current genre matching is exact-string based (e.g., "post-punk" won't match "punk")
- Many genres are related by feel but have different names (e.g., "chillwave", "dream pop", "shoegaze")
- Smart playlists could benefit from "give me songs that feel like X" rather than exact genre matching

**Potential Approach:**
1. Export all unique genres from database
2. Send to AI with prompt to cluster by musical feel/mood/energy
3. Store groupings in new table (e.g., `genre_groups` with `group_name`, `genre_id`)
4. Update playlist queries to optionally expand genre matches to include related genres

**Example Groups:**
- "High Energy Rock": punk, post-punk, garage rock, hard rock
- "Atmospheric/Dreamy": shoegaze, dream pop, chillwave, ambient
- "Melancholic": slowcore, sadcore, darkwave, gothic rock

---

## What Was Accomplished (Session 8)

### AcousticID Integration

Extended metadata extraction to capture AcousticID alongside MBID for future BPM lookup enhancement.

**Changes:**
- `process_mbid_from_files()` now extracts both MBID and AcousticID from files
- New `analysis/acoustid.py` module for AcoustID API integration (requires API key)
- `process_bpm_acousticbrainz()` has two phases: MBID direct lookup, then AcousticID fallback
- Added `ACOUSTID_API_KEY` to `.env.example`

**Note:** AcoustID API registration was blocked, so AcousticID→MBID resolution is not currently functional. AcousticIDs are still extracted and stored for future use.

### Resume Production Script

Created `scripts/resume_production.py` to resume pipeline from database state without dropping tables.

**Features:**
- Checks current enrichment status
- Skips completed phases
- Uses existing query functions to find incomplete records
- Handles all phases: artist enrichment, stub enrichment, track enrichment, BPM lookup

---

## What Was Accomplished (Session 7)

### Metadata Refresh by Artist Feature

Added ability to re-extract MBIDs from audio files for specific artists after manual tagging with MusicBrainz Picard.

#### Problem Solved
- Existing `process_mbid_from_files()` only processes tracks WITHOUT MBIDs
- After Picard tagging, tracks may have new/updated MBIDs in file tags
- No way to update database for specific artists without re-processing all files

#### Solution: Targeted Metadata Refresh

**New Query Functions** (`db/db_functions.py`):
- `get_tracks_by_artist_name()` - Get all tracks for specified artists (case-insensitive)
- `get_artist_names_found()` - Check which artists exist in database

**New Refresh Function** (`analysis/ffmpeg.py`):
- `refresh_mbid_for_artists()` - Re-extract MBIDs from files for specific artists
  - Updates tracks even if they already have MBIDs (unlike `process_mbid_from_files`)
  - Also updates artist MBIDs (one sample per artist)
  - Supports dry-run mode for verification
  - Returns detailed stats dict

**Orchestration Function** (`pipeline.py`):
- `refresh_metadata_for_artists()` - High-level function for refreshing metadata

#### Usage Example
```python
from db.database import Database
from pipeline import refresh_metadata_for_artists

db = Database(...)

# After running Picard on The Beatles and Pink Floyd albums:
stats = refresh_metadata_for_artists(
    database=db,
    artist_names=["The Beatles", "Pink Floyd"],
    use_test_paths=True,
    dry_run=True,  # Preview first
)

print(f"Would update {stats['tracks']['updated']} tracks")

# If looks good, run for real:
stats = refresh_metadata_for_artists(
    database=db,
    artist_names=["The Beatles", "Pink Floyd"],
    use_test_paths=True,
    dry_run=False,
)
```

#### Stats Dictionary Structure
```python
stats = {
    "artists_requested": 3,
    "artists_found": 2,
    "artists_not_found": ["Unknown Artist"],
    "tracks": {
        "total": 45,
        "accessible": 42,
        "inaccessible": 3,
        "extracted": 40,
        "missing": 2,
        "updated": 35,
        "unchanged": 5,
        "errors": 0,
    },
    "artist_mbids": {
        "updated": 2,
        "unchanged": 0,
        "errors": 0,
    },
    "acoustids": {
        "extracted": 38,
        "updated": 30,
        "unchanged": 8,
        "errors": 0,
    },
    "dry_run": False,
}
```

#### Tests Added
New test file: `test/test_metadata_refresh.py` with 16 tests:
- `TestGetTracksByArtistName` - 5 tests (empty input, existing artist, case-insensitive, nonexistent, multiple)
- `TestGetArtistNamesFound` - 3 tests
- `TestRefreshMetadataForArtists` - 5 tests (stats structure, nonexistent artist, dry_run flag, empty list, dry_run doesn't modify)
- `TestRefreshMetadataIntegration` - 1 integration test
- `TestAddAcoustidColumn` - 2 tests (idempotent migration, column exists verification)

#### Test Results
All 16 tests passed in 21.09s

### AcousticID Extraction

Extended metadata refresh to also extract and store AcousticID from files. Picard embeds AcousticID when it finds a match via acoustic fingerprinting.

**New tag extraction** (`analysis/ffmpeg.py`):
- `ACOUSTID_TAGS` - Tag name variants for AcousticID
- `ffmpeg_get_acoustid()` - Extract AcousticID from ffprobe output

**Migration function** (`db/db_functions.py`):
- `add_acoustid_column()` - Adds `acoustid VARCHAR(255)` column to track_data (idempotent)
- Called automatically in `run_full_pipeline()` and `run_incremental_update()`

**Updated functions**:
- `get_tracks_by_artist_name()` - Now returns 7-tuple including acoustid
- `refresh_mbid_for_artists()` - Extracts and stores AcousticID alongside MBID
- Stats dict now includes `acoustids` section with `extracted`, `updated`, `unchanged`, `errors`

#### Files Modified
| File | Changes |
|------|---------|
| `db/db_functions.py` | Added `get_tracks_by_artist_name()`, `get_artist_names_found()`, `add_acoustid_column()` |
| `analysis/ffmpeg.py` | Added `refresh_mbid_for_artists()`, `ffmpeg_get_acoustid()`, `ACOUSTID_TAGS` |
| `pipeline.py` | Added `refresh_metadata_for_artists()` orchestration |
| `test/test_metadata_refresh.py` | New test file with 16 tests |

---

## What Was Accomplished (Session 6)

### Incremental Update Pipeline Redesign

Fixed the design flaw where `insert_last_fm_artist_data()` and `process_lastfm_track_data()` processed ALL records every run, defeating the purpose of incremental updates.

#### Problem Solved
- Similar artists added as stubs never got MBID/genres (67% of artists incomplete)
- Fetching similar artists for stub artists would cause infinite graph expansion

#### Solution: Split Enrichment

**New Query Functions** (`db/db_functions.py`):
- `get_primary_artists_without_similar()` - Artists with tracks but no similar_artists records
- `get_stub_artists_without_mbid()` - Artists without tracks and without MBID

**Split Enrichment Functions** (`db/db_update.py`):
- `enrich_artists_core()` - MBID + genres only (for stub artists - prevents infinite expansion)
- `enrich_artists_full()` - MBID + genres + similar artists (for primary artists)
- `insert_last_fm_artist_data()` - Legacy wrapper that calls `enrich_artists_full()`

**Updated Pipeline Flow** (`pipeline.py`):
```python
# 1. Full enrichment for primary artists (have tracks, no similar_artists)
primary_ids = get_primary_artists_without_similar(database)
enrich_artists_full(database, artist_ids=primary_ids)

# 2. Core enrichment for stubs (no tracks, no MBID) - safe, no expansion
stub_ids = get_stub_artists_without_mbid(database)
enrich_artists_core(database, artist_ids=stub_ids)

# 3. Track enrichment (skip_with_genres=True already works correctly)
process_lastfm_track_data(database, skip_with_genres=True)
```

#### Key Design Decisions
- Detection: Use absence of `similar_artists` records to identify artists needing full enrichment
- Stub artists get MBID/genres but NOT similar artists (prevents infinite loop)
- Both functions support `artist_ids` parameter for targeted processing
- Empty list `[]` returns immediately (0 artists), `None` processes all artists

#### Tests Added
New test file: `test/test_incremental_enrichment.py` with 17 tests:
- `TestGetPrimaryArtistsWithoutSimilar` - 4 tests for query correctness
- `TestGetStubArtistsWithoutMbid` - 4 tests for query correctness
- `TestEnrichArtistsCore` - 3 tests (including integration test for no-similar-artists behavior)
- `TestEnrichArtistsFull` - 3 tests (including integration test for similar-artists behavior)
- `TestInsertLastFmArtistData` - 1 test for legacy wrapper
- `TestIncrementalEnrichmentFlow` - 2 tests for complete flow

#### Test Results
- 14 passed, 1 skipped (artist not found in Last.fm), 2 deselected (integration tests)
- Existing `test/test_incremental_update.py` tests: 9 passed, 1 skipped

#### Files Modified
| File | Changes |
|------|---------|
| `db/db_functions.py` | Added `get_primary_artists_without_similar()`, `get_stub_artists_without_mbid()` |
| `db/db_update.py` | Split `insert_last_fm_artist_data()` into `enrich_artists_core()` and `enrich_artists_full()` |
| `pipeline.py` | Updated `run_incremental_update()` with targeted enrichment, added `lastfm_stub` stats |
| `test/test_incremental_enrichment.py` | New test file with 17 tests |

#### Completed Tasks
1. ~~Add query functions to identify incomplete artists~~ ✓
2. ~~Split enrichment into core (MBID+genres) and full (MBID+genres+similar)~~ ✓
3. ~~Update pipeline.py with targeted processing~~ ✓
4. ~~Write comprehensive tests~~ ✓
5. ~~Run tests against sandbox database~~ ✓

---

## What Was Accomplished (Session 5)

### Code Cleanup and Linting

#### Dead Code Removed
- `db/initial_load_pandas.py` - Unused WIP pandas ETL class (never imported)
- `bpm_test.py` - Old test file using removed function
- `process_bpm()` in `db/db_update.py` - Referenced undefined `bpm` module

#### Ruff Linting Completed
- 81 issues auto-fixed with `ruff check . --fix`
- Manual fixes for remaining 10 issues:
  - `analysis/ffmpeg.py`: Changed `stdout=PIPE, stderr=PIPE` to `capture_output=True` (3 locations)
  - `analysis/ffmpeg.py`: Fixed unused loop variable `dirs` → `_dirs`
  - `config/logging.py`: Added `# noqa: SIM115` for intentional open() without context manager
  - `test/test_acousticbrainz.py`: Fixed unused loop variables
  - `test/test_bpm_pipeline.py`: Fixed unused loop variable `key` → `_key`
  - `test/test_e2e_pipeline.py`: Fixed unused loop variable `album` → `_album`
- All ruff checks now pass

### History Integration (Phase 8)
- Added `finalized_sandbox` fixture to e2e pipeline that calls `update_history()`
- New test class `TestHistoryIntegration` with 4 tests:
  - `test_history_record_created` - verifies history record exists
  - `test_history_record_has_correct_count` - verifies track count matches
  - `test_history_has_latest_entry_date` - verifies latest_entry populated
  - `test_get_last_update_date` - verifies function returns valid date
- Updated `test_data_summary` to include history information in output
- Updated `.claude/revised_flow.md` to mark Phase 8.1 as integrated

### Incremental Updates and Orchestration (Phase 9)
- Created new `pipeline.py` module with orchestration functions:
  - `validate_environment()` - validates DB, paths, ffprobe before running
  - `run_full_pipeline()` - complete pipeline from Plex extraction to BPM
  - `run_incremental_update()` - processes only new tracks since last update
  - `insert_new_tracks()` - helper that avoids duplicates by plex_id
  - `add_new_artists()` - adds artists from new tracks to artists table
- Added `get_tracks_since_date()` to `plex/plex_library.py` for Plex filtering
- Created `test/test_incremental_update.py` with tests for new functionality
- Updated `.claude/revised_flow.md` to mark all phases as implemented

#### Completed Tasks
1. ~~Dead code cleanup~~ ✓
2. ~~Ruff linting~~ ✓
3. ~~History integration into pipeline~~ ✓
4. ~~Implement incremental updates~~ ✓
5. ~~Create orchestration functions~~ ✓

#### Bug Discovered During Testing (FIXED in Session 6)
**Last.fm enrichment functions process ALL records, not just new ones**
- ~~`insert_last_fm_artist_data()` in `db/db_update.py` - processes all artists~~
- ~~`process_lastfm_track_data()` in `db/db_update.py` - processes all tracks~~
- ✓ **Fixed**: Split enrichment functions now accept `artist_ids` parameter
- ✓ **Fixed**: Pipeline uses queries to identify only incomplete artists

#### Remaining Work
- **LOW**: Discogs integration (Phase 5.6) - additional genre source

---

## What Was Accomplished (Session 4)

### E2E Test Run - Full Pipeline Validation
**Duration:** 2 hours 24 minutes | **Result:** 44 passed, 2 skipped

#### Pipeline Results Summary
| Metric | Value |
|--------|-------|
| Total tracks | 1,557 |
| Total artists | 336 |
| Total genres | 120 |

#### Coverage Metrics
| Metric | Count | Percentage |
|--------|-------|------------|
| Tracks with MBID | 1,144 / 1,557 | 73.5% |
| Artists with MBID | 94 / 336 | 28.0% |
| Artists with genres | 96 / 336 | 28.6% |
| Tracks with BPM | 1,556 / 1,557 | 99.9% |
| Tracks with genres (direct) | 21 / 1,557 | 1.3% |

#### BPM Sources
| Source | Tracks |
|--------|--------|
| AcousticBrainz | 221 (19.3% hit rate) |
| Essentia (local) | 1,335 |

#### Relationship Tables
| Table | Count |
|-------|-------|
| artist_genres | 472 links |
| track_genres | 105 links |
| similar_artists | 490 links |

### Artist Breakdown Analysis
- **112 artists** (33%) have tracks in the library
- **224 artists** (67%) exist only as similar artist recommendations
- Similar artists are intentionally not enriched to avoid infinite loops

### Genre Inheritance Solution
Added queries and views to `db/useful_queries.py` for genre inheritance:

**Problem:** Only 1.3% of tracks have direct genre tags from Last.fm
**Solution:** Inherit genres from artist when track has no genres
**Result:** 96.7% of tracks now have genre access (1,505 / 1,557)

#### New Queries
- `TRACKS_WITH_EFFECTIVE_GENRES` - Flat result (one row per track-genre)
- `TRACKS_WITH_EFFECTIVE_GENRES_GROUPED` - Grouped with comma-separated genres

#### New Views
- `v_track_effective_genres` - Flat view
- `v_track_effective_genres_grouped` - Grouped view with BPM (useful for playlists)

Example playlist query:
```sql
SELECT * FROM v_track_effective_genres_grouped
WHERE genres LIKE '%rock%' AND bpm BETWEEN 120 AND 140
```

---

## Previous Sessions (2-3) Summary

### Last.fm API Optimization
- Rate limiting increased 4x (0.25s delay, ~4 req/s)
- MBID-first track lookup implemented
- Phase 6 batch processor added

### Pipeline Reordering
- New order: Last.fm (Phases 5, 6) → AcousticBrainz (Phase 7.1) → Essentia (Phase 7.2)

---

## Key Functions

| Function | Location | Description |
|----------|----------|-------------|
| `refresh_metadata_for_artists()` | `pipeline.py` | Re-extract MBIDs/AcousticIDs for specific artists (after Picard) |
| `refresh_mbid_for_artists()` | `analysis/ffmpeg.py` | Low-level metadata refresh with dry-run support |
| `get_tracks_by_artist_name()` | `db/db_functions.py` | Query: tracks by artist name (case-insensitive) |
| `ffmpeg_get_acoustid()` | `analysis/ffmpeg.py` | Extract AcousticID from ffprobe output |
| `add_acoustid_column()` | `db/db_functions.py` | Migration: add acoustid column to track_data |
| `enrich_artists_core()` | `db/db_update.py` | MBID + genres only (for stub artists) |
| `enrich_artists_full()` | `db/db_update.py` | MBID + genres + similar (for primary artists) |
| `insert_last_fm_artist_data()` | `db/db_update.py` | Legacy wrapper → `enrich_artists_full()` |
| `process_lastfm_track_data()` | `db/db_update.py` | Phase 6: Track enrichment |
| `get_last_fm_track_data()` | `analysis/lastfm.py` | MBID-first track lookup |
| `get_primary_artists_without_similar()` | `db/db_functions.py` | Query: primary artists needing enrichment |
| `get_stub_artists_without_mbid()` | `db/db_functions.py` | Query: stub artists needing enrichment |
| `process_bpm_acousticbrainz()` | `db/db_update.py` | Phase 7.1: API BPM lookup |
| `process_bpm_essentia()` | `db/db_update.py` | Phase 7.2: Local BPM analysis |
| `lookup_track_and_features()` | `analysis/spotify.py` | Find track on Spotify, get audio features |
| `get_audio_features_batch()` | `analysis/spotify.py` | Batch fetch audio features (up to 100) |
| `add_spotify_columns()` | `db/db_functions.py` | Migration: add Spotify columns to track_data |
| `add_lastfm_attempted_column()` | `db/db_functions.py` | Migration: add lastfm_attempted_at to track_data |

## Pending Commit

Files modified (Session 7):
- `db/db_functions.py` - Added `get_tracks_by_artist_name()`, `get_artist_names_found()`, `add_acoustid_column()`
- `analysis/ffmpeg.py` - Added `refresh_mbid_for_artists()`, `ffmpeg_get_acoustid()`, `ACOUSTID_TAGS`
- `pipeline.py` - Added `refresh_metadata_for_artists()` orchestration
- `test/test_metadata_refresh.py` - New test file (16 tests)
- `continuity.md` - Updated with Session 7 notes

## Design Decisions Documented

### Split Enrichment Strategy (Session 6)
The infinite loop risk comes from fetching similar artists, not from fetching MBID/genres.

**Solution:** Split enrichment into two modes:
- **Core enrichment** (`enrich_artists_core`): MBID + genres only - safe for any artist
- **Full enrichment** (`enrich_artists_full`): MBID + genres + similar artists - only for primary artists

**Detection logic:**
- Primary artists (have tracks) → need full enrichment → detected by absence of `similar_artists` records
- Stub artists (no tracks) → need core enrichment only → detected by absence of MBID

This allows stub artists to get MBID/genres (improving data quality) without expanding the artist graph infinitely.

### Genre inheritance rationale
Last.fm rarely returns track-level tags (1.3% coverage). Artist-level tags are much more reliable (28.6% of artists have tags). Inheriting artist genres for tracks without their own provides 96.7% effective genre coverage.
