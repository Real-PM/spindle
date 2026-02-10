# Spindle

A music metadata enrichment pipeline and playlist builder. Spindle extracts track data from a Plex server, enriches it with external sources (Last.fm, Discogs, Spotify), runs BPM analysis via Essentia, and stores everything in a SQLite database. A web UI lets you filter, preview, and create playlists directly on your Plex server.

## Features

- **Plex integration** — Pull your full music library and push playlists back
- **Metadata enrichment** — Genres, similar artists, and tags from Last.fm and Discogs
- **BPM detection** — Local audio analysis using Essentia
- **Playlist builder UI** — Filter by genre, artist, BPM range, genre groups, and similar artists
- **Editable previews** — Drag-to-reorder, remove, and search-to-add tracks before creating
- **Find Similar** — Discover sonically similar tracks using Plex's neural-network analysis
- **Genre normalization** — Alias mapping and genre groups to tame inconsistent tags

## Requirements

- Python 3.12+
- A Plex server with Plex Pass (required for sonic analysis features)
- API keys for any enrichment sources you want to use (see `.env.example`)

## Setup

### 1. Clone and configure

```bash
git clone <repo-url> spindle
cd spindle
cp .env.example .env
# Edit .env with your Plex URL/token and API keys
```

### 2. Database

Spindle uses SQLite. The database lives at `data/music_organizer.db` by default (configurable via `SQLITE_DB_PATH` in `.env`). The pipeline scripts create and populate the database — no manual schema setup needed.

---

## Running without Docker

### Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the web UI (development)

```bash
flask --app web:create_app run --debug --port 5000
```

Open [http://localhost:5000](http://localhost:5000).

### Run the web UI (production)

```bash
gunicorn --bind 0.0.0.0:5000 --workers 2 "web:create_app()"
```

### Run tests

```bash
pytest
pytest test/test_web.py -v   # just web tests
```

### Lint and format

```bash
ruff check . --fix
ruff format .
```

---

## Running with Docker

### Build and start

```bash
docker compose up -d --build
```

This will:
- Build the image from `Dockerfile` (Python 3.13-slim + gunicorn)
- Mount `./data` as read-only into the container
- Load environment variables from `.env`
- Expose the web UI on port 5000
- Restart automatically unless stopped

### Check status

```bash
docker compose ps
docker compose logs -f web
```

The container includes a health check at `/health`.

### Stop

```bash
docker compose down
```

---

## Pipeline scripts

These scripts populate and enrich the database. Run them outside Docker with your virtual environment activated.

| Script | Purpose |
|--------|---------|
| `scripts/run_production.py` | Full initial load from Plex |
| `scripts/run_incremental.py` | Pick up new tracks since last run |
| `scripts/resume_production.py` | Resume an interrupted production run |
| `scripts/normalize_genres.py` | Build genre alias mappings |
| `scripts/populate_genre_groups.py` | Populate genre group tables |
| `scripts/fetch_spotify_data.py` | Enrich with Spotify audio features |

## Project structure

```
spindle/
├── analysis/       # API clients (Last.fm, Discogs, Spotify) and audio analysis
├── config/         # Logging configuration
├── db/             # Database layer (SQLite connection, queries, updates)
├── plex/           # Plex server interaction and playlist management
├── scripts/        # Pipeline and maintenance scripts
├── web/            # Flask app (routes, templates, static assets)
├── test/           # pytest test suite
├── data/           # SQLite databases (gitignored)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## License

Another  project by [Real PM](https://realpm.net).
