"""
Club provisioning workflow for FleetNests.

provision_club() is the single entry point for creating a new club:
  1. Derive db_name and db_user from short_name
  2. Create PostgreSQL user + database
  3. Apply init_club_db.sql
  4. Seed default vehicle row
  5. Seed club_settings with type-appropriate defaults
  6. Insert club row in master clubs table
  7. Invalidate club cache

Requires the calling process to have superuser or CREATEROLE privileges,
or have those privileges granted via the PG_ADMIN_* env vars.
"""

import os
import subprocess
import logging
import master_db
import club_resolver

log = logging.getLogger(__name__)

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "init_club_db.sql")


def _run_psql(cmd_args: list[str], input_text: str = None) -> str:
    """Run a psql command, raise on failure, return stdout."""
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_admin_user = os.environ.get("PG_ADMIN_USER", "postgres")
    pg_admin_pass = os.environ.get("PG_ADMIN_PASSWORD", "")

    env = os.environ.copy()
    if pg_admin_pass:
        env["PGPASSWORD"] = pg_admin_pass

    base_cmd = [
        "psql",
        "-h", pg_host,
        "-p", pg_port,
        "-U", pg_admin_user,
        "-v", "ON_ERROR_STOP=1",
    ]
    result = subprocess.run(
        base_cmd + cmd_args,
        capture_output=True, text=True, env=env,
        input=input_text,
    )
    if result.returncode != 0:
        raise RuntimeError(f"psql failed: {result.stderr.strip()}")
    return result.stdout


def _db_user_exists(db_user: str) -> bool:
    try:
        out = _run_psql(["-c", f"SELECT 1 FROM pg_roles WHERE rolname='{db_user}'",
                         "-t", "postgres"])
        return "1" in out
    except Exception:
        return False


def _db_exists(db_name: str) -> bool:
    try:
        out = _run_psql(["-c", f"SELECT 1 FROM pg_database WHERE datname='{db_name}'",
                         "-t", "postgres"])
        return "1" in out
    except Exception:
        return False


def _default_settings_for_type(vehicle_type: str) -> dict:
    """Return seed club_settings rows appropriate for the vehicle type."""
    common = {
        "approval_required": "false",
        "min_hours":         "2",
        "max_hours":         "6",
        "max_advance_days":  "60",
        "max_consecutive_days": "3",
        "max_pending":       "7",
    }
    if vehicle_type == "boat":
        common.update({
            "marina_phone":  "",
            "weather_zone":  "TXZ206",
            "nws_county":    "TXC091",
        })
    else:  # plane
        common.update({
            "fbo_phone":         "",
            "aviation_station":  "KSAT",
        })
    return common


def provision_club(name: str, short_name: str, vehicle_type: str,
                   contact_email: str = "", timezone: str = "America/Chicago") -> dict:
    """
    Create a new club: database, user, schema, seed data, master record.
    Returns the new club row dict on success.
    Raises RuntimeError on failure.
    """
    short_name = short_name.lower().strip()
    db_name = f"club-{short_name}"
    db_user = f"club_{short_name}_user"

    # Generate a random password for the new DB user
    import secrets
    db_password = secrets.token_urlsafe(24)
    admin_token = None

    log.info("Provisioning club: %s (vehicle_type=%s)", short_name, vehicle_type)

    # 1. Create PostgreSQL user (if not exists)
    if not _db_user_exists(db_user):
        _run_psql(["-c",
                   f"CREATE USER {db_user} WITH PASSWORD '{db_password}'",
                   "postgres"])
        log.info("Created DB user: %s", db_user)
    else:
        log.info("DB user already exists: %s", db_user)

    # 2. Create database (if not exists)
    if not _db_exists(db_name):
        # CREATE DATABASE cannot run inside a transaction, needs postgres db
        _run_psql(["-c", f'CREATE DATABASE "{db_name}" OWNER {db_user}', "postgres"])
        log.info("Created database: %s", db_name)
    else:
        log.info("Database already exists: %s", db_name)

    # 3. Apply schema
    _run_psql(["-d", db_name, "-f", SCHEMA_FILE])
    log.info("Applied schema to %s", db_name)

    # 4. Grant permissions
    _run_psql(["-d", db_name, "-c",
               f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {db_user}"])
    _run_psql(["-d", db_name, "-c",
               f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {db_user}"])
    log.info("Granted permissions to %s", db_user)

    # 5. Build a temporary DSN for seeding
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_admin_user = os.environ.get("PG_ADMIN_USER", "postgres")
    pg_admin_pass = os.environ.get("PG_ADMIN_PASSWORD", "")
    seed_dsn = (f"postgresql://{pg_admin_user}:{pg_admin_pass}"
                f"@{pg_host}:{pg_port}/{db_name}")

    import psycopg2
    import psycopg2.extras
    seed_conn = psycopg2.connect(seed_dsn, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        with seed_conn.cursor() as cur:
            # 5a. Default vehicle
            vehicle_name = "Club Aircraft" if vehicle_type == "plane" else "Club Boat"
            cur.execute(
                "INSERT INTO vehicles (name, vehicle_type) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING RETURNING id",
                (vehicle_name, vehicle_type),
            )
            row = cur.fetchone()
            vehicle_id = row["id"] if row else 1
            log.info("Seeded default vehicle id=%d", vehicle_id)

            # 5b. club_settings
            defaults = _default_settings_for_type(vehicle_type)
            for key, value in defaults.items():
                cur.execute(
                    "INSERT INTO club_settings (key, value) VALUES (%s, %s) "
                    "ON CONFLICT (key) DO NOTHING",
                    (key, value),
                )
            log.info("Seeded club_settings for %s", short_name)

        # 5c. Set contact email on admin user + create a password-set token
        if contact_email:
            import secrets as _sec
            from datetime import datetime, timedelta
            admin_token = _sec.token_urlsafe(32)
            expires = datetime.now() + timedelta(hours=72)
            cur.execute(
                "UPDATE users SET email=%s, password_reset_token=%s, "
                "password_reset_expires=%s WHERE username='admin'",
                (contact_email, admin_token, expires),
            )
            log.info("Seeded admin email + password-set token for %s", short_name)

        seed_conn.commit()
    finally:
        seed_conn.close()

    # 6. Insert master clubs record
    club_row = master_db.create_club(
        name=name,
        short_name=short_name,
        vehicle_type=vehicle_type,
        db_name=db_name,
        db_user=db_user,
        subdomain=short_name,
        contact_email=contact_email,
        timezone=timezone,
        db_password=db_password,
    )
    log.info("Master clubs record created: id=%s", club_row["id"] if club_row else "?")

    # 7. Invalidate club cache so next request re-reads from master (now includes db_password)
    club_resolver.invalidate_cache(short_name)

    # 8. Send welcome email with password-set link
    if contact_email and admin_token:
        import email_notify
        email_notify.notify_club_provisioned(contact_email, name, short_name, admin_token)

    result = dict(master_db.get_club_by_short_name(short_name) or {})
    result["_db_password"] = db_password  # kept for logging; no longer shown in UI
    return result
