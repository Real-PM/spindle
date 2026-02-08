# CLAUDE.md - Music Organizer Project

## Project Overview

A Python application that extracts music metadata from a Plex server, enriches it with data from external APIs (Last.fm, Discogs, Spotify), performs audio analysis (BPM detection via Essentia), and stores everything in a SQLite database for playlist generation and music discovery.

**Python Version:** 3.13  
**Primary User:** Jay (experienced developer, prefers hands-on understanding of code)

## Environment Information

### Virtual Env
~/virtual-envs/music_organizer_clean/.venv

### Database
SQLite database at `data/music_organizer.db` (path configured in `db/__init__.py`).
Test database: `data/sandbox.db`
---

## Core Development Philosophy

### Plan → Do → Test

Every feature or fix follows this cycle:

1. **Plan:** Discuss the approach before writing code. Outline inputs, outputs, edge cases, and how it fits with existing modules.
2. **Do:** Implement in small, focused commits. Prefer minimal changes that accomplish one thing well.
3. **Test:** Write or update tests before considering the work complete. Run relevant tests to confirm nothing broke.

Never skip the planning step for non-trivial changes. When in doubt, ask clarifying questions.

### Modularity Over Monoliths

- **Small functions:** Each function should do one thing. If a docstring needs "and" to describe it, split it.
- **Composable pipelines:** Functions return values that feed into the next function. Avoid side effects where possible.
- **Clear interfaces:** Functions that will be called from other modules get strict type hints and clear docstrings.
- **Runtime linking:** Build pipelines by composing functions at the call site, not by hardcoding dependencies inside functions.

```python
# Good: composable
mbids = extract_mbids_from_tracks(tracks)
bpm_results = fetch_bpm_for_mbids(mbids)
update_count = write_bpm_to_database(db, bpm_results)

# Avoid: monolithic
process_everything(tracks, db)  # What does this do? Hard to test pieces.
```

---

## Code Standards

### Type Hints

**Strict type hints on all new code.** Legacy code will be linted later, but going forward:

```python
from typing import Optional

def get_bpm_essentia(filepath: str) -> Optional[float]:
    """Analyze BPM locally using Essentia."""
    ...
```

Use `Optional[X]` for values that may be `None`. Use `list[X]` and `dict[K, V]` (Python 3.9+ syntax) rather than `typing.List`.

### Docstrings

Use Google-style docstrings for public functions:

```python
def fetch_bpm_for_tracks(
    tracks: list[tuple[int, str]], 
    use_bulk: bool = True
) -> dict[int, float]:
    """Fetch BPM values for tracks with MusicBrainz IDs.
    
    Args:
        tracks: List of (track_id, mbid) tuples.
        use_bulk: If True, use batch API requests (25 per request).
    
    Returns:
        Dict mapping track_id to BPM value. Tracks without BPM data are omitted.
    
    Raises:
        requests.RequestException: If API is unreachable after retries.
    """
```

### Formatting

Use **ruff** for formatting and linting:

```bash
ruff check . --fix    # Lint and auto-fix
ruff format .         # Format
```

Config in `pyproject.toml`:

```toml
[tool.ruff]
line-length = 100
target-version = "py313"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
ignore = ["E501"]  # Line length handled by formatter
```

### Logging

Use **loguru** via the centralized config:

```python
from config import setup_logging
from loguru import logger

setup_logging("logs/my_module.log")
logger.info("Processing started")
logger.warning("Track {} missing MBID", track_id)
logger.error("API request failed: {}", e)
```

Log levels:
- `debug`: Verbose tracing for development
- `info`: Normal operation milestones
- `warning`: Recoverable issues, missing data, skipped items
- `error`: Failures that affect results but don't crash
- `critical`: Unrecoverable failures

---

## Error Handling Strategy

### External APIs (Last.fm, Discogs, Spotify)

**Graceful degradation:** Don't crash the pipeline because one API call failed.

```python
from loguru import logger
import tenacity

@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    retry=tenacity.retry_if_exception_type(requests.RequestException),
    before_sleep=lambda retry_state: logger.warning(
        "Retry {} for {} after error: {}", 
        retry_state.attempt_number,
        retry_state.fn.__name__,
        retry_state.outcome.exception()
    )
)
def call_external_api(...) -> Optional[dict]:
    ...
```

Pattern for API functions:
1. Retry with exponential backoff (3 attempts, 2s → 4s → 8s)
2. On final failure, log error and return `None` (don't raise)
3. Caller decides whether `None` is acceptable or needs handling

### Database Operations

More strict—database errors often indicate real problems:

```python
def update_bpm(db: Database, track_id: int, bpm: float) -> bool:
    """Update BPM for a track. Returns True on success, False on failure."""
    try:
        db.execute("UPDATE track_data SET bpm = ? WHERE id = ?", (bpm, track_id))
        return True
    except sqlite3.Error as e:
        logger.error("Failed to update BPM for track {}: {}", track_id, e)
        return False
```

---

## Configuration

### Environment Variables (.env)

Migrate from `config.ini` to `.env` files:

```bash
# .env (gitignored)
MYSQL_HOST=localhost
MYSQL_USER=music_user
MYSQL_PASSWORD=secret
MYSQL_DATABASE=music_organizer
MYSQL_PORT=3306

MYSQL_TEST_DATABASE=sandbox

PLEX_SERVER_URL=http://localhost:32400
PLEX_TOKEN=your_token
PLEX_MUSIC_LIBRARY=Music
PLEX_TEST_LIBRARY=Test Music

LASTFM_API_KEY=your_key
LASTFM_SHARED_SECRET=your_secret
LASTFM_USERNAME=your_username

DISCOGS_TOKEN=your_token
```

Load with python-dotenv:

```python
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "music_organizer")
```

Create a `.env.example` with placeholder values for documentation.

### Secrets Management

Current approach: `.env` files in `.gitignore`. This is acceptable for a personal project.

Future consideration: 1Password CLI (`op`) can inject secrets at runtime without storing them in files. Ask if interested in setting this up later.

---

## Testing

### Framework & Structure

- **Framework:** pytest
- **Location:** `test/` directory
- **Naming:** `test_<module>.py` or `<module>_test.py`

```
test/
├── test_lastfm.py
├── test_e2e_pipeline.py    # End-to-end: full pipeline
├── test_incremental_enrichment.py
├── test_incremental_update.py
├── test_metadata_refresh.py
└── conftest.py
```

### Running Tests

```bash
# All tests
pytest

# Specific test file
pytest test/test_e2e_pipeline.py

# With output
pytest -v -s
```

### Test Patterns

**Unit tests** for pure functions:

```python
def test_parse_genre_string():
    result = parse_genre_string("['Rock', 'Alternative']")
    assert result == ["Rock", "Alternative"]
```

**Integration tests** for API calls (use fixtures/mocks or skip if offline):

```python
import pytest

@pytest.mark.integration
def test_lastfm_artist_lookup():
    """Requires network access."""
    result = get_artist_info("The Beatles")
    assert result is None or isinstance(result, dict)
```

**End-to-end tests** are the primary success metric for this project. If e2e tests pass, the system works.

---

## Project Structure

```
music_organizer/
├── analysis/               # External API clients and audio analysis
│   ├── __init__.py
│   ├── bpm.py              # Local BPM analysis (Essentia)
│   ├── discogs.py          # Discogs API client
│   ├── ffmpeg.py           # Audio file metadata extraction
│   ├── lastfm.py           # Last.fm API client
│   └── spotify.py          # Spotify API client
├── config/                 # Configuration utilities
│   ├── __init__.py         # Exports setup_logging
│   └── logging.py          # Centralized loguru setup
├── db/                     # Database layer
│   ├── __init__.py         # Connection factory, reads .env
│   ├── database.py         # Database class/context manager
│   ├── db_functions.py     # Query helpers
│   ├── db_update.py        # Batch update operations
│   └── useful_queries.py   # Ad-hoc query collection
├── maint/                  # Maintenance scripts
├── plex/                   # Plex server interaction
│   └── plex_library.py     # Extract tracks from Plex
├── test/                   # All tests
├── .env                    # Local config (gitignored)
├── .env.example            # Template for .env
├── .gitignore
├── pyproject.toml          # Ruff config, project metadata
├── requirements.txt
└── CLAUDE.md               # This file
```

---

## Key Workflows

### Initial Load Pipeline

```
Plex Server → CSV Export → Pandas Transform → SQLite Load
```

See `Initial_Load_Workflow.MD` for detailed steps. Key modules:
- `plex.plex_library`: Extract tracks from Plex
- `analysis.ffmpeg`: Extract MBIDs and artist info from file metadata
- `analysis.lastfm`: Enrich with genres, similar artists
- `db.db_update`: Batch database operations

### BPM Enrichment Pipeline

```
Essentia local analysis (all tracks without BPM)
```

- `analysis.bpm`: Local audio analysis using Essentia's RhythmExtractor2013

---

## Database Schema

```sql
-- Core tables
track_data (id, title, artist, album, bpm, musicbrainz_id, artist_id, plex_id, filepath, location, genre)
artists (id, artist, musicbrainz_id, last_fm_id, discogs_id)
genres (id, genre)

-- Relationships
track_genres (track_id, genre_id)
artist_genres (artist_id, genre_id)
similar_artists (artist_id, similar_artist_id)
```

Test database: `sandbox` (same schema, smaller dataset)

---

## Dependencies

Key packages:
- `loguru`: Logging
- `requests`: HTTP client for APIs
- `tenacity`: Retry logic with backoff
- `pandas`: Data transformation
- `PlexAPI`: Plex server interaction
- `python-dotenv`: Environment variable loading
- `ruff`: Formatting and linting
- `pytest`: Testing

**Note:** librosa and AcousticBrainz have been removed. Essentia is used for all local BPM analysis. AcousticBrainz shut down in 2022.

---

## Common Commands

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest
pytest test/e2e_test.py -v -s

# Format and lint
ruff check . --fix
ruff format .

# Run e2e pipeline test
pytest test/test_e2e_pipeline.py -v -s
```

---

## Working With Claude

### What to Expect From Me

- I'll ask clarifying questions before implementing non-trivial features
- I'll propose a plan and wait for approval on larger changes
- I'll write tests alongside new code
- I'll use type hints and docstrings on all new functions
- I'll handle errors gracefully with logging, not crashes
- I'll write commit messages when asked (you handle git operations)

### How to Get the Best Results

- Tell me which module or file you're focused on
- Share relevant error messages or test output
- Let me know if you want exploration/options vs. a direct implementation
- Flag if something I wrote doesn't match your mental model

### Current Project State

See `continuity.md` for:
- What was completed in the last session
- Current blockers
- Next steps to resume work

---

## Commit Message Format

When asked to write commit messages:

```
<type>: <short summary>

<body - what and why, not how>

<footer - breaking changes, issue refs>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example:
```
feat: add researched_at flag to skip already-processed tracks

Tracks that have been through the pipeline get stamped with researched_at
so incremental runs only analyze BPM for genuinely new tracks.
```
Do not include "Claude Code" in commit messages.
