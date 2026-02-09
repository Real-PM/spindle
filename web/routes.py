"""
Route handlers for the playlist builder web UI.
"""

import json

from flask import Blueprint, current_app, jsonify, render_template, request
from loguru import logger

from db.database import Database
from db.queries import build_playlist_query
from plex.playlists import create_playlist, find_similar_tracks
from web.services import get_dropdown_data, get_track_details, search_tracks

bp = Blueprint("main", __name__)


def _get_db() -> Database:
    """Create a Database instance from the app config."""
    return Database(current_app.config["DB_PATH"])


def _parse_filters(req) -> dict:
    """
    Parse filter parameters from the request into build_playlist_query kwargs.

    Args:
        req: Flask request object

    Returns:
        Dict of keyword arguments for build_playlist_query()
    """
    title = req.values.get("title", "").strip() or None
    genre_groups = req.values.getlist("genre_groups") or None
    genres = req.values.getlist("genres") or None
    artists = req.values.getlist("artists") or None
    similar_to = req.values.get("similar_to", "").strip() or None
    min_bpm = req.values.get("min_bpm", type=int)
    max_bpm = req.values.get("max_bpm", type=int)
    bpm_range = (min_bpm, max_bpm) if min_bpm and max_bpm else None
    limit = req.values.get("limit", type=int) or None

    return {
        "title": title,
        "genre_groups": genre_groups,
        "genres": genres,
        "artists": artists,
        "similar_to": similar_to,
        "bpm_range": bpm_range,
        "limit": limit,
    }


@bp.route("/")
def index():
    """Main playlist builder page with filter form."""
    db = _get_db()
    dropdown_data = get_dropdown_data(db)
    return render_template("index.html", **dropdown_data)


@bp.route("/api/preview-count")
def preview_count():
    """Return track count matching current filters (htmx fragment)."""
    db = _get_db()
    filters = _parse_filters(request)
    # Don't shuffle just for counting
    filters["shuffle"] = False
    plex_ids = build_playlist_query(db, **filters)
    return render_template("partials/track_count.html", count=len(plex_ids))


@bp.route("/api/preview", methods=["POST"])
def preview():
    """Return preview table of matching tracks (htmx fragment)."""
    db = _get_db()
    filters = _parse_filters(request)
    logger.debug("Preview filters: {}", filters)
    plex_ids = build_playlist_query(db, **filters)
    logger.debug("Preview matched {} plex_ids", len(plex_ids))
    tracks = get_track_details(db, plex_ids)
    return render_template("partials/track_table.html", tracks=tracks, count=len(tracks))


@bp.route("/api/track-search")
def track_search():
    """Search tracks by title/artist for the add-track autocomplete (JSON)."""
    query = request.args.get("q", "").strip()
    if len(query) < 2:
        return jsonify([])
    db = _get_db()
    results = search_tracks(db, query)
    return jsonify(results)


@bp.route("/api/create-playlist", methods=["POST"])
def create_playlist_route():
    """Create a Plex playlist from explicit track list or current filters (htmx fragment)."""
    name = request.form.get("playlist_name", "").strip()
    if not name:
        return render_template(
            "partials/create_result.html", success=False, message="Playlist name is required."
        )

    # Check for explicit track list first (from edited preview)
    track_plex_ids_raw = request.form.get("track_plex_ids", "").strip()
    if track_plex_ids_raw:
        try:
            plex_ids = json.loads(track_plex_ids_raw)
            if not isinstance(plex_ids, list) or not plex_ids:
                return render_template(
                    "partials/create_result.html",
                    success=False,
                    message="Track list is empty.",
                )
            plex_ids = [int(pid) for pid in plex_ids]
        except (json.JSONDecodeError, ValueError, TypeError):
            return render_template(
                "partials/create_result.html",
                success=False,
                message="Invalid track list data.",
            )
    else:
        # Fall back to filter-based query
        db = _get_db()
        filters = _parse_filters(request)
        plex_ids = build_playlist_query(db, **filters)

    if not plex_ids:
        return render_template(
            "partials/create_result.html",
            success=False,
            message="No tracks match the current filters.",
        )

    if current_app.plex_server is None:
        return render_template(
            "partials/create_result.html",
            success=False,
            message="Plex server is not connected. Check server configuration.",
        )

    replace_existing = request.form.get("replace_existing") == "on"
    playlist = create_playlist(
        current_app.plex_server, name, plex_ids, replace_existing=replace_existing
    )

    if playlist:
        return render_template(
            "partials/create_result.html",
            success=True,
            message=f"Playlist '{name}' created with {len(plex_ids)} tracks.",
        )
    else:
        return render_template(
            "partials/create_result.html",
            success=False,
            message=f"Failed to create playlist '{name}'. It may already exist (enable 'Replace if exists').",
        )


@bp.route("/api/similar-tracks", methods=["POST"])
def similar_tracks():
    """Find sonically similar tracks via Plex API (htmx fragment)."""
    raw = request.form.get("track_plex_ids", "").strip()
    if not raw:
        return render_template(
            "partials/similar_tracks.html",
            tracks=[],
            error="No tracks provided.",
        )

    try:
        plex_ids = json.loads(raw)
        if not isinstance(plex_ids, list) or not plex_ids:
            return render_template(
                "partials/similar_tracks.html",
                tracks=[],
                error="Track list is empty.",
            )
        plex_ids = [int(pid) for pid in plex_ids]
    except (json.JSONDecodeError, ValueError, TypeError):
        return render_template(
            "partials/similar_tracks.html",
            tracks=[],
            error="Invalid track list data.",
        )

    if current_app.plex_server is None:
        return render_template(
            "partials/similar_tracks.html",
            tracks=[],
            error="Plex server is not connected. Check server configuration.",
        )

    tracks = find_similar_tracks(current_app.plex_server, plex_ids)
    return render_template("partials/similar_tracks.html", tracks=tracks, error=None)


@bp.route("/health")
def health():
    """Health check endpoint for Docker."""
    plex_ok = current_app.plex_server is not None
    return {"status": "ok", "plex_connected": plex_ok}
