-- FleetNests Master Database Schema
-- Run once after creating the database:
--   psql -U fleetnests_admin -d fleetnests_master -f init_master_db.sql

CREATE TABLE IF NOT EXISTS clubs (
    id            SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    short_name    VARCHAR(30) UNIQUE NOT NULL,   -- used in DB name and subdomain
    vehicle_type  VARCHAR(10) NOT NULL DEFAULT 'boat' CHECK (vehicle_type IN ('boat', 'plane')),
    db_name       VARCHAR(63) NOT NULL,          -- PostgreSQL database name: club-{short_name}
    db_user       VARCHAR(63) NOT NULL,          -- PostgreSQL user: club_{short_name}_user
    db_password   VARCHAR(200),                  -- stored at provision time; used by _build_dsn()
    subdomain     VARCHAR(63),                   -- e.g. bentley -> bentley.fleetnests.com
    contact_email VARCHAR(100),
    timezone      VARCHAR(50) DEFAULT 'America/Chicago',
    is_active     BOOLEAN DEFAULT TRUE,
    provisioned_at TIMESTAMP DEFAULT NOW(),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS super_admins (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(50) UNIQUE NOT NULL,
    full_name     VARCHAR(100) NOT NULL,
    email         VARCHAR(100),
    password_hash VARCHAR(255) NOT NULL,
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id           SERIAL PRIMARY KEY,
    club_id      INTEGER REFERENCES clubs(id),
    plan_tier    VARCHAR(30) DEFAULT 'standard',
    renewal_date DATE,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- Shared checklist templates (JSONB for flexibility)
CREATE TABLE IF NOT EXISTS vehicle_templates (
    id              SERIAL PRIMARY KEY,
    vehicle_type    VARCHAR(10) NOT NULL CHECK (vehicle_type IN ('boat', 'plane')),
    name            VARCHAR(100) NOT NULL,
    checklist_items JSONB NOT NULL DEFAULT '[]',
    categories      JSONB NOT NULL DEFAULT '[]',
    disclaimer      TEXT,
    is_default      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS master_audit_log (
    id          SERIAL PRIMARY KEY,
    admin_id    INTEGER REFERENCES super_admins(id),
    action      VARCHAR(100) NOT NULL,
    target_type VARCHAR(50),
    target_id   INTEGER,
    detail      JSONB,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Marketing orders / trial signups
CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    club_name       VARCHAR(100) NOT NULL,
    contact_name    VARCHAR(100) NOT NULL,
    contact_email   VARCHAR(200) NOT NULL,
    tier            VARCHAR(20)  NOT NULL,   -- 'path' | 'subdomain' | 'custom'
    craft_count     INTEGER      NOT NULL DEFAULT 1,
    amount_cents    INTEGER      NOT NULL DEFAULT 0,
    early_bird      BOOLEAN      NOT NULL DEFAULT FALSE,
    is_trial        BOOLEAN      NOT NULL DEFAULT FALSE,
    custom_domain   VARCHAR(200),
    payment_method  VARCHAR(20),             -- 'stripe' | 'paypal' | null (trial)
    payment_id      VARCHAR(200),            -- Stripe session ID or PayPal order ID
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending',  -- pending/paid/provisioned
    notes           TEXT,
    provisioned_at  TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_clubs_short_name  ON clubs(short_name);
CREATE INDEX IF NOT EXISTS idx_clubs_is_active   ON clubs(is_active);
CREATE INDEX IF NOT EXISTS idx_subs_club         ON subscriptions(club_id);
CREATE INDEX IF NOT EXISTS idx_master_audit_time ON master_audit_log(created_at);

-- -------------------------------------------------------------------------
-- Seed: Default boat checklist (FleetNests 8 items)
-- -------------------------------------------------------------------------
INSERT INTO vehicle_templates (vehicle_type, name, checklist_items, categories, disclaimer, is_default)
VALUES (
    'boat',
    'Default Boat Pre-Launch Checklist',
    '[
        "Unplug shore power (back left of boat), secure both cords. Turn electrical power ON — RED Perko switch to DOWN position.",
        "Verify life vests in left side seat compartments. Fire extinguisher, first aid kit and emergency equipment under Captain''s seat.",
        "Connect & turn on Garmin depth finder, verify approx. 16 ft depth at slip. Monitor depth finder at all times to avoid prop strike.",
        "Visually verify prop is in good condition. Trim motor all the way down before starting. Plug in dash safety kill switch clip.",
        "STARTING: Throttle in neutral. After starting, visually verify cooling water stream exiting on the left side of outboard motor.",
        "Verify fuel gauge near full, passengers secure, then disconnect tiedowns at cleats on the boat (ropes stay tied to dock).",
        "Operate at best idle speed (~900 RPM) in all no-wake zones.",
        "RETURN: Gas up upon return. Get dockhand assistance. Return to slip. Secure boat — follow ALL launch procedures in reverse order. Turn Perko switch OFF (up position), connect shore power. Clean and check belongings."
    ]',
    '[
        {"label": "Pre-Launch",           "indices": [0, 1, 2, 3]},
        {"label": "Starting & Departure", "indices": [4, 5, 6]},
        {"label": "Return & Shutdown",    "indices": [7]}
    ]',
    'Boating involves risks. The Captain and passengers accept all responsibility for any damage, injury, death, or claims from using this boat, and agree to indemnify and hold the Owner and other Boat Club Members harmless. Owner and Members shall not be liable for any physical, financial or any other damages.',
    TRUE
)
ON CONFLICT DO NOTHING;

-- -------------------------------------------------------------------------
-- Seed: Default plane pre-flight checklist (AROW → run-up → tie-down)
-- -------------------------------------------------------------------------
INSERT INTO vehicle_templates (vehicle_type, name, checklist_items, categories, disclaimer, is_default)
VALUES (
    'plane',
    'Default Aircraft Pre-Flight Checklist',
    '[
        "AROW documents in aircraft: Airworthiness certificate, Registration, Operating handbook (POH), Weight & balance.",
        "Fuel: Visually verify fuel quantity and type (100LL). Sump fuel from both wing sumps and gascolator — check for water.",
        "Oil: Check oil level — minimum 6 qts for flight. Inspect for leaks around engine cowling.",
        "Control surfaces: Check ailerons, elevator, rudder for full and free movement. Remove all control locks.",
        "Walk-around: Inspect propeller for nicks/damage. Check tires for proper inflation. Verify pitot tube cover removed.",
        "Engine run-up: Magneto check at 1800 RPM (max 125 RPM drop per mag). Carb heat check. Primer locked.",
        "Before takeoff: Transponder ALT, lights on, fuel on fullest tank, trim set for takeoff, doors latched.",
        "RETURN/TIE-DOWN: Complete landing checklist. Secure to tie-down rings (3 points). Install pitot tube cover. Chocks in. Log Hobbs & tach time."
    ]',
    '[
        {"label": "AROW & Documents",   "indices": [0]},
        {"label": "Pre-Flight Inspect", "indices": [1, 2, 3, 4]},
        {"label": "Run-Up & Departure", "indices": [5, 6]},
        {"label": "Return & Tie-Down",  "indices": [7]}
    ]',
    'Aviation involves inherent risks. The Pilot-in-Command accepts full responsibility for the safe operation of the aircraft and compliance with all applicable FARs. The club, its officers, and members shall not be liable for any injury, death, or property damage arising from the use of club aircraft.',
    TRUE
)
ON CONFLICT DO NOTHING;

-- -------------------------------------------------------------------------
-- Seed: Bentley club row
-- -------------------------------------------------------------------------
INSERT INTO clubs (name, short_name, vehicle_type, db_name, db_user, subdomain, contact_email, timezone)
VALUES (
    'FleetNests',
    'bentley',
    'boat',
    'club-bentley',
    'club_bentley_user',
    'bentley',
    'admin@fleetnests.com',
    'America/Chicago'
)
ON CONFLICT (short_name) DO NOTHING;
