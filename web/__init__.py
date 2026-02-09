"""
Flask application factory for the Music Organizer playlist builder.
"""

import os

from flask import Flask
from loguru import logger

from config import setup_logging
from db import DB_PATH
from db.database import Database
from plex import PLEX_SERVER_TOKEN, PLEX_SERVER_URL


def create_app(db_path: str | None = None, testing: bool = False) -> Flask:
    """
    Create and configure the Flask application.

    Args:
        db_path: Override database path (defaults to DB_PATH from env).
        testing: If True, skip Plex server connection.

    Returns:
        Configured Flask app instance.
    """
    app = Flask(__name__)
    app.config["TESTING"] = testing

    # Logging
    setup_logging("logs/web.log", level="DEBUG", console_level="INFO")

    # Database — store path so routes can create instances as needed
    app.config["DB_PATH"] = db_path or DB_PATH

    # Plex server — connect once at startup
    app.plex_server = None
    if not testing:
        _init_plex(app)

    # Register routes
    from web.routes import bp

    app.register_blueprint(bp)

    logger.info("Flask app created (db={})", app.config["DB_PATH"])
    return app


def _init_plex(app: Flask) -> None:
    """Attempt to connect to Plex server. Sets app.plex_server or None."""
    if not PLEX_SERVER_URL or not PLEX_SERVER_TOKEN:
        logger.warning("Plex URL/token not configured — playlist creation disabled")
        return

    url = PLEX_SERVER_URL
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        from plexapi.server import PlexServer

        app.plex_server = PlexServer(url, PLEX_SERVER_TOKEN)
        logger.info("Connected to Plex server: {}", PLEX_SERVER_URL)
    except Exception as e:
        logger.error("Could not connect to Plex server: {}", e)
        app.plex_server = None
