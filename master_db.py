"""
Master database connection and query functions for FleetNests.
Connects to the fleetnests_master database for club registry, super-admins,
billing, and shared templates.

Independent of club-specific databases — all club data lives in club_resolver.py / db.py.
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager


def _get_master_connection():
    """Return a psycopg2 connection to the master database."""
    conn = psycopg2.connect(
        os.environ["MASTER_DATABASE_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    return conn


@contextmanager
def get_master_db():
    """Context manager: yields master DB connection, commits on success."""
    conn = _get_master_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _execute(query, params=None, fetch=True):
    with get_master_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            return None


def _fetchone(query, params=None):
    with get_master_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()


def _insert(query, params=None):
    with get_master_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            try:
                return cur.fetchone()
            except psycopg2.ProgrammingError:
                return None


# ---------------------------------------------------------------------------
# Club registry
# ---------------------------------------------------------------------------

def get_club_by_short_name(short_name: str) -> dict | None:
    """Return the club row for a given short_name, or None if not found / inactive."""
    return _fetchone(
        "SELECT * FROM clubs WHERE short_name = %s AND is_active = TRUE",
        (short_name,),
    )


def get_club_by_id(club_id: int) -> dict | None:
    return _fetchone("SELECT * FROM clubs WHERE id=%s", (club_id,))


def get_all_clubs() -> list:
    return _execute("SELECT * FROM clubs ORDER BY name")


def create_club(name: str, short_name: str, vehicle_type: str,
                db_name: str, db_user: str, subdomain: str,
                contact_email: str, timezone: str) -> dict | None:
    return _insert(
        "INSERT INTO clubs (name, short_name, vehicle_type, db_name, db_user, "
        "subdomain, contact_email, timezone) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
        (name, short_name, vehicle_type, db_name, db_user,
         subdomain, contact_email, timezone),
    )


def update_club(club_id: int, **fields):
    """Update arbitrary fields on a club row."""
    if not fields:
        return
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    _execute(
        f"UPDATE clubs SET {set_clause} WHERE id = %s",
        (*fields.values(), club_id),
        fetch=False,
    )


def deactivate_club(club_id: int):
    _execute(
        "UPDATE clubs SET is_active = FALSE WHERE id = %s",
        (club_id,), fetch=False,
    )


# ---------------------------------------------------------------------------
# Super-admin accounts
# ---------------------------------------------------------------------------

def get_super_admin_by_username(username: str) -> dict | None:
    return _fetchone(
        "SELECT * FROM super_admins WHERE username = %s AND is_active = TRUE",
        (username,),
    )


def create_super_admin(username: str, full_name: str, email: str,
                       password_hash: str) -> dict | None:
    return _insert(
        "INSERT INTO super_admins (username, full_name, email, password_hash) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (username, full_name, email, password_hash),
    )


# ---------------------------------------------------------------------------
# Vehicle templates (shared checklists)
# ---------------------------------------------------------------------------

def get_default_template(vehicle_type: str) -> dict | None:
    """Return the default checklist template for a vehicle type."""
    return _fetchone(
        "SELECT * FROM vehicle_templates "
        "WHERE vehicle_type = %s AND is_default = TRUE "
        "ORDER BY id LIMIT 1",
        (vehicle_type,),
    )


def get_all_templates() -> list:
    return _execute("SELECT * FROM vehicle_templates ORDER BY vehicle_type, name")


# ---------------------------------------------------------------------------
# Master audit log
# ---------------------------------------------------------------------------

def log_master_action(admin_id: int | None, action: str,
                      target_type: str = None, target_id: int = None,
                      detail: dict = None):
    """Append an immutable master audit entry. Never raises."""
    import json
    try:
        _insert(
            "INSERT INTO master_audit_log (admin_id, action, target_type, target_id, detail) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (admin_id, action, target_type, target_id,
             json.dumps(detail) if detail else None),
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Demo leads (sample site email capture)
# ---------------------------------------------------------------------------

def save_demo_lead(email: str, club_short_name: str, club_name: str,
                   ip_address: str = None, user_agent: str = None) -> bool:
    """Save a prospect email from a sample site. Returns True on success."""
    try:
        _insert(
            "INSERT INTO demo_leads (email, club_short_name, club_name, ip_address, user_agent) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (email.lower().strip(), club_short_name, club_name, ip_address, user_agent),
        )
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Marketing orders / trial signups
# ---------------------------------------------------------------------------

def create_order(club_name: str, contact_name: str, contact_email: str,
                 tier: str, craft_count: int, amount_cents: int,
                 early_bird: bool, is_trial: bool,
                 billing: str = "annual",
                 custom_domain: str = None, notes: str = None) -> int:
    """Insert a new order/trial record. Returns the new order id."""
    row = _insert(
        "INSERT INTO orders "
        "(club_name, contact_name, contact_email, tier, craft_count, amount_cents, "
        " early_bird, is_trial, billing, custom_domain, notes) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (club_name, contact_name, contact_email, tier, craft_count, amount_cents,
         early_bird, is_trial, billing, custom_domain, notes),
    )
    return row["id"] if row else None


def update_order_payment(order_id: int, payment_method: str, payment_id: str, status: str = "paid"):
    """Record payment details after Stripe/PayPal confirms."""
    _execute(
        "UPDATE orders SET payment_method=%s, payment_id=%s, status=%s WHERE id=%s",
        (payment_method, payment_id, status, order_id),
        fetch=False,
    )


def get_order(order_id: int) -> dict | None:
    return _fetchone("SELECT * FROM orders WHERE id=%s", (order_id,))


def get_order_by_payment_id(payment_id: str) -> dict | None:
    return _fetchone("SELECT * FROM orders WHERE payment_id=%s", (payment_id,))


def get_all_orders() -> list:
    return _execute("SELECT * FROM orders ORDER BY created_at DESC")


def get_pending_orders_for_club(club_name: str) -> list:
    """Return paid, unprovisioned orders whose club_name matches (case-insensitive)."""
    return _execute(
        "SELECT * FROM orders WHERE status='paid' AND is_trial=FALSE "
        "AND LOWER(club_name)=LOWER(%s) ORDER BY created_at DESC",
        (club_name,),
    )


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------

def upsert_subscription(club_id: int, billing: str, amount_cents: int,
                        price_locked_until, renewal_date,
                        plan_tier: str = "standard",
                        order_id: int = None) -> int:
    """Create or update the subscription record for a club. Returns subscription id."""
    existing = get_subscription_by_club_id(club_id)
    if existing:
        _execute(
            "UPDATE subscriptions SET billing=%s, amount_cents=%s, price_locked_until=%s, "
            "renewal_date=%s, plan_tier=%s, order_id=COALESCE(%s, order_id), is_active=TRUE "
            "WHERE club_id=%s",
            (billing, amount_cents, price_locked_until, renewal_date,
             plan_tier, order_id, club_id),
            fetch=False,
        )
        return existing["id"]
    row = _insert(
        "INSERT INTO subscriptions "
        "(club_id, billing, amount_cents, price_locked_until, renewal_date, plan_tier, order_id) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (club_id, billing, amount_cents, price_locked_until, renewal_date, plan_tier, order_id),
    )
    return row["id"] if row else None


def get_subscription_by_club_id(club_id: int) -> dict | None:
    return _fetchone("SELECT * FROM subscriptions WHERE club_id=%s", (club_id,))


def get_all_subscriptions_with_clubs() -> list:
    """Join subscriptions with clubs for the superadmin overview."""
    return _execute(
        "SELECT s.*, c.name AS club_name, c.short_name, c.contact_email "
        "FROM subscriptions s JOIN clubs c ON c.id = s.club_id "
        "ORDER BY s.price_locked_until ASC NULLS LAST, c.name"
    )


def get_demo_leads(club_short_name: str = None) -> list:
    """Return all demo leads, optionally filtered by club."""
    import psycopg2.extras
    with get_master_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if club_short_name:
            cur.execute(
                "SELECT * FROM demo_leads WHERE club_short_name=%s ORDER BY created_at DESC",
                (club_short_name,),
            )
        else:
            cur.execute("SELECT * FROM demo_leads ORDER BY created_at DESC")
        return [dict(r) for r in cur.fetchall()]
