"""
Per-request club resolution for FleetNests.

Resolution order (first match wins):
  1. Subdomain: bentley.fleetnests.com  → short_name = "bentley"
  2. URL prefix: /bentley/calendar       → short_name = "bentley"  (not yet wired)
  3. CLUB_SHORT_NAME env var             → single-club dev / legacy mode

Sets on Flask's g for each request:
  g.club          — full club dict from master DB
  g.club_id       — club.id int
  g.vehicle_type  — 'boat' | 'plane'
  g.club_dsn      — DSN string for club DB connection (consumed by db.py)
"""

import os
import logging

from flask import Flask, g, abort
import master_db

log = logging.getLogger(__name__)

# In-process cache: short_name → club dict. Invalidate by restarting the app
# or calling _club_cache.clear() after provisioning a new club.
_club_cache: dict[str, dict] = {}


def _resolve_short_name(host: str) -> str | None:
    """
    Extract a club short_name from the Host header.
    Matches patterns like:
      bentley.fleetnests.com   → "bentley"
      bentley.localhost         → "bentley"
      bentley.fleetnests.local → "bentley"

    Returns None if the host is bare (fleetnests.com, localhost, etc.).
    """
    # Strip port if present
    host_only = host.split(":")[0].lower()
    parts = host_only.split(".")
    # Need at least THREE parts (subdomain.domain.tld) to be a club subdomain.
    # Bare domains like fleetnests.com, localhost have only 1-2 parts.
    if len(parts) >= 3:
        candidate = parts[0]
        # Reject obviously non-club hostnames
        if candidate not in ("www", "api", "superadmin", "admin", "mail"):
            return candidate
    return None


def _build_dsn(club: dict) -> str:
    """
    Construct the DSN for a club database.
    Resolution order:
      1. Env var DB_PASS_{DB_USER_UPPER}  (existing clubs / manual override)
      2. club['db_password'] from master DB (clubs provisioned after schema migration)
      3. DATABASE_URL fallback             (single-club dev mode)
    """
    db_user = club.get("db_user")
    db_name = club.get("db_name")

    # Single-club mode: no per-club DB credentials, use DATABASE_URL directly
    if not db_user:
        return os.environ.get("DATABASE_URL", "")

    # 1. Env var (highest priority — allows manual override)
    env_key = f"DB_PASS_{db_user.upper()}"
    password = os.environ.get(env_key)

    # 2. Master DB stored password (new clubs work without .env change or restart)
    if not password:
        password = club.get("db_password")

    if password:
        host = os.environ.get("PG_HOST", "localhost")
        port = os.environ.get("PG_PORT", "5432")
        return f"postgresql://{db_user}:{password}@{host}:{port}/{db_name}"

    # Fallback: DATABASE_URL (single-club dev)
    return os.environ.get("DATABASE_URL", "")


def _load_club(short_name: str) -> dict | None:
    """Look up club from cache, master DB, or DATABASE_URL (single-club mode)."""
    if short_name in _club_cache:
        return _club_cache[short_name]

    # Single-club mode: no master DB configured, build a synthetic club dict
    if not os.environ.get("MASTER_DATABASE_URL"):
        club = {
            "id": 1,
            "short_name": short_name,
            "name": os.environ.get("CLUB_NAME", "FleetNests"),
            "db_name": None,
            "db_user": None,
            "vehicle_type": os.environ.get("VEHICLE_TYPE", "boat"),
            "is_active": True,
        }
        _club_cache[short_name] = club
        return club

    club = master_db.get_club_by_short_name(short_name)
    if club:
        _club_cache[short_name] = dict(club)
    return _club_cache.get(short_name)


def invalidate_cache(short_name: str = None):
    """Remove a club from the cache (or clear all). Call after provisioning."""
    if short_name:
        _club_cache.pop(short_name, None)
    else:
        _club_cache.clear()


def init_app(app: Flask):
    """Register the before_request hook that resolves club for every request."""

    @app.before_request
    def resolve_club():
        from flask import request

        # Super-admin routes bypass club resolution
        if request.path.startswith("/superadmin"):
            g.club = None
            g.club_id = None
            g.vehicle_type = None
            g.club_dsn = None
            return

        short_name = None

        # 1. Path-based header (Nginx sets X-Club-Short-Name for /sample1/, /sample2/ etc.)
        short_name = request.headers.get("X-Club-Short-Name")

        # 2. Subdomain
        if not short_name:
            host = request.host or ""
            short_name = _resolve_short_name(host)

        # 3. Env var override (dev / single-club mode)
        if not short_name:
            short_name = os.environ.get("CLUB_SHORT_NAME")

        if not short_name:
            # Bare root domain → serve the marketing site instead of 404
            host_only = (request.host or "").split(":")[0].lower()
            if host_only in ("fleetnests.com", "www.fleetnests.com", "localhost", "127.0.0.1"):
                g.is_marketing = True
                g.club = None
                g.club_id = None
                g.vehicle_type = None
                g.club_dsn = None
                return
            log.warning("No club resolved for host=%s path=%s", request.host, request.path)
            abort(404)

        club = _load_club(short_name)
        if not club:
            log.warning("Unknown or inactive club short_name=%s", short_name)
            abort(404)

        g.club = club
        g.club_id = club["id"]
        g.vehicle_type = club["vehicle_type"]
        g.club_dsn = _build_dsn(club)

        # Tell db.py which DSN to use for this request
        import db
        db.set_club_dsn(g.club_dsn)
