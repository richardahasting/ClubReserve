# FleetNests

A multi-tenant reservation platform for boat and flying clubs. One Flask application
serves any number of clubs, each with full isolation — separate PostgreSQL database,
subdomain routing, and configurable vehicle type (boat or plane).

---

## Features

- **Reservation calendar** — FullCalendar UI with overlap enforcement, blackout dates, and pending-approval workflow
- **Vehicle types** — Boat mode (fuel level buttons, NWS marine weather, Motor Hours) and Plane mode (FAA METAR weather, Hobbs Hours, fuel quantity required on return)
- **Trip check-out / check-in** — Pre-departure captain's checklist, departure/return time, engine hours, fuel logging
- **Time-slot filtering** — Start/end pickers grey out windows already reserved on each craft
- **Waitlist** — Auto-notification when a cancelled slot opens
- **Weather alerts** — NWS API integration (boat: zone/county alerts; plane: METAR), daily 6 AM cron
- **Trip reminders** — Daily 6 PM email cron for upcoming reservations
- **Fuel log & statistics** — Per-trip fuel records, aggregated reporting
- **Maintenance tracking** — Service records with cost and hours; recurring scheduled tasks with overdue detection (calendar and hours-based)
- **Fleet status page** — Member-facing view of maintenance schedule and service history
- **Incident / damage reports** — Structured reports with admin review
- **Message board** — Posts with photo attachments
- **Per-club branding** — Custom primary/accent colors, logo, hero banner image
- **Club photo gallery** — Admin-uploaded gallery visible to all members
- **Vehicle photos** — Primary vehicle photo shown on the reservation form
- **Family accounts** — Two logins sharing one reservation slot
- **iCal feed** — Personal token-based calendar subscription
- **AI-routed member feedback** — Routes feedback to GitHub issues (bugs) or email (other)
- **Monthly PDF statements** — Upload and per-member access control
- **Audit log** — Tracks all admin actions
- **Admin dashboard** — User management, blackouts, approvals, CSV export, trip logs, maintenance
- **Super-admin panel** — Club provisioning, activation/deactivation across all tenants

---

## Architecture

### Two-Database Tier

| Service | Port | Database | Purpose |
|---------|------|----------|---------|
| `db-master` | 5433 | `fleetnests_master` | Club registry, super-admins, vehicle templates |
| `db-clubs` | 5434 | `club-<shortname>` (one per club) | All per-club data |

### Per-Request Club Resolution

`club_resolver.py` runs as a Flask `before_request` hook:

1. Extracts the subdomain from the incoming request (`bentley.fleetnests.com` → `bentley`)
2. Looks up the club in `fleetnests_master`
3. Stores the club record and its DSN on Flask `g`

`CLUB_SHORT_NAME` env var bypasses subdomain lookup for local dev.

`/superadmin` paths skip club resolution entirely and authenticate against the master database.

### Per-Request DSN Switching

`db.py` reads `g.club_dsn` on every query, so each request talks to the correct club's
PostgreSQL database. Falls back to `DATABASE_URL` for single-club / legacy mode.

### Vehicle Types

All vehicle-type behaviour lives in `vehicle_types.py`. The `build_checkout_context()`
function returns the correct constants (fuel level buttons, weather source, hours label,
contact phone label, checklist items) for the current club's vehicle type.

---

## Quick Start (Docker)

```bash
git clone https://github.com/richardahasting/bentley-boat.git FleetNests
cd FleetNests
cp .env.example .env        # fill in all values before proceeding
docker compose up -d
```

On first start:
- `db-master` auto-runs `init_master_db.sql` — creates the master schema and seeds a default super-admin
- `db-clubs` starts empty; clubs are provisioned at runtime via the super-admin panel

> **⚠️ Security**: Change all `changeme-*` passwords in `.env` and the default super-admin
> credentials before any real deployment.

### Provision Your First Club

1. Log in at `/superadmin` (default credentials set in `init_master_db.sql`)
2. Create a new club — choose short name (used for subdomain and DB name) and vehicle type
3. Navigate to `<shortname>.yourdomain.com` — the club is live immediately, no restart needed
4. The club contact receives a welcome email with a one-time set-password link
5. Log in as the club admin and set a secure password when prompted on first login

### Local Dev (Single Club)

```bash
# Skip subdomain routing — always use the "bentley" club
CLUB_SHORT_NAME=bentley python app.py
```

---

## Environment Variables

### Database

| Variable | Required | Description |
|----------|----------|-------------|
| `MASTER_DATABASE_URL` | **Yes** | DSN for `fleetnests_master` (club registry) |
| `MASTER_DB_PASSWORD` | Docker | Password for master DB (used in compose) |
| `PG_HOST` | **Yes** | Host of the clubs PostgreSQL instance |
| `PG_PORT` | No (5434) | Port of the clubs PostgreSQL instance |
| `PG_SUPERUSER` | **Yes** | Superuser on clubs DB (used to create new club DBs) |
| `PG_SUPERUSER_PASSWORD` | **Yes** | Password for `PG_SUPERUSER` |
| `DB_PASS_CLUB_<UPPER>_USER` | Per club | Password for each club's DB user |
| `CLUB_SHORT_NAME` | Dev only | Bypass subdomain routing; always use this club |
| `DATABASE_URL` | Legacy | Single-club fallback if master DB not configured |

### Application

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SECRET_KEY` | **Yes** | — | Flask session secret |
| `APP_PREFIX` | No | `/` | URL prefix when mounted under a path |
| `SESSION_COOKIE_SECURE` | No | `true` | Set `false` for local HTTP dev |

### Email

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `EMAIL_ENABLED` | No | `false` | Enable outbound email |
| `EMAIL_FROM` | No | `noreply@myclub.com` | Sender address |
| `SMTP_HOST` | No | `localhost` | Mail server |
| `SMTP_PORT` | No | `25` | Mail server port |
| `SMTP_USER` | No | — | SMTP username (if auth required) |
| `SMTP_PASS` | No | — | SMTP password |
| `APP_URL` | No | — | Base URL embedded in email links |

### Integrations

| Variable | Required | Description |
|----------|----------|-------------|
| `ANTHROPIC_API_KEY` | No | Enables AI feedback routing |
| `GITHUB_TOKEN` | No | PAT for filing bug-report issues (Issues: Read+Write) |
| `GITHUB_REPO` | No | Target repo for bug reports (`owner/repo`) |
| `FEEDBACK_EMAIL` | No | Destination for non-bug feedback |

---

## Project Structure

```
FleetNests/
├── app.py              # Flask routes and view logic
├── models.py           # Per-club DB query layer / business logic
├── auth.py             # Session auth, login_required, superadmin_required
├── db.py               # Per-request DSN-switching PostgreSQL wrapper
├── club_resolver.py    # before_request hook: subdomain → club record + DSN
├── master_db.py        # Master DB query layer (clubs registry, super-admins)
├── master_models.py    # provision_club() workflow
├── vehicle_types.py    # Boat / plane constants and checkout context builder
├── email_notify.py     # Email notification helpers
├── weather.py          # NWS / METAR alert fetching
├── weather_check.py    # Daily weather alert cron script
├── trip_reminder.py    # Evening reservation reminder cron script
├── feedback.py         # AI-routed member feedback
├── init_master_db.sql  # Master DB schema (clubs, super_admins, vehicle_templates)
├── init_club_db.sql    # Per-club DB schema (all club tables)
├── gunicorn.conf.py    # Server config (bind :5210, 2 workers)
├── docker-compose.yml  # db-master + db-clubs + web services
├── Dockerfile          # Python 3.12-slim image
├── requirements.txt    # Python dependencies
├── templates/          # Jinja2 templates
│   ├── superadmin/     # Super-admin panel templates
│   └── admin/          # Club-admin templates
└── static/             # CSS, JS, icons
```

---

## Database Schema

### Master DB (`fleetnests_master`)

| Table | Purpose |
|-------|---------|
| `clubs` | Club registry: short name, display name, vehicle type, DSN template, active flag |
| `super_admins` | Platform-level admin accounts (bcrypt) |
| `vehicle_templates` | Default checklist items per vehicle type |

### Per-Club DB (`club-<shortname>`)

| Table | Purpose |
|-------|---------|
| `users` | Member accounts, roles, family links, per-member limits |
| `vehicles` | Club vehicles (boat or plane) |
| `reservations` | All booking records — `active`, `cancelled`, `pending_approval` |
| `blackout_dates` | Admin-defined ranges that block reservations (per-vehicle or global) |
| `trip_logs` | Check-out / check-in records with checklist data, engine hours, fuel |
| `fuel_log` | Per-trip fuel records |
| `waitlist` | Queue entries with auto-notify on cancellation |
| `incident_reports` | Damage / incident reports |
| `maintenance_records` | Completed service work per craft (date, category, hours, cost) |
| `maintenance_schedules` | Recurring tasks with calendar-month and/or hours-based intervals |
| `messages` | Message board posts |
| `message_photos` | Photo attachments |
| `feedback_submissions` | Member feedback before AI routing |
| `statements` | Monthly PDF statement uploads with per-member visibility |
| `audit_log` | Admin action history |
| `club_settings` | Key/value store for per-club configuration |
| `club_branding` | Colors, logo, and hero banner image |
| `club_photos` | Admin-uploaded gallery images |
| `vehicle_photos` | Per-vehicle photos (one marked as primary) |

---

## Key Business Rules

- Reservations: 2–6 hours, 30-minute intervals, up to 60 days ahead (all configurable)
- No overlap with existing bookings (`active` or `pending_approval`) or blackout dates
- Per-member limits: consecutive days and total pending reservations (configurable)
- Max concurrent vehicles per member at the same time (configurable)
- Family accounts: two login credentials sharing one reservation slot
- Overlap check enforced at the database level (table lock + atomic insert) to prevent race conditions
- A day is "fully booked" only when no 2-hour gap remains on any vehicle
- Maintenance schedules go overdue when `next_due_date <= today` OR `vehicle.current_hours >= next_due_hours`

---

## Cron Jobs

```cron
# Weather alerts — 6 AM daily
0  6 * * *  /usr/bin/python3 /path/to/weather_check.py  >> /var/log/fleetnests-weather.log 2>&1

# Trip reminders — 6 PM daily
0 18 * * *  /usr/bin/python3 /path/to/trip_reminder.py  >> /var/log/fleetnests-reminder.log 2>&1
```

Both scripts load `.env` via `python-dotenv` and connect directly to PostgreSQL.

---

## Testing

### Setup

Create two PostgreSQL databases and a shared test user:

```sql
CREATE USER fn_test_user WITH PASSWORD 'FnTest2026!';
CREATE DATABASE fleetnests_test_master OWNER fn_test_user;
CREATE DATABASE fleetnests_test_club   OWNER fn_test_user;
GRANT ALL ON DATABASE fleetnests_test_master TO fn_test_user;
GRANT ALL ON DATABASE fleetnests_test_club   TO fn_test_user;
```

Apply the schemas:

```bash
psql -U fn_test_user -d fleetnests_test_master -f init_master_db.sql
psql -U fn_test_user -d fleetnests_test_club   -f init_club_db.sql
```

### Running Tests

```bash
./run_tests.sh              # all 178 tests
./run_tests.sh tests/test_models.py       # single file
./run_tests.sh -k "waitlist"              # matching test names
./run_tests.sh --cov --cov-report=term    # with coverage report
```

### Test Coverage

| File | What's tested |
|------|--------------|
| `test_schema.py` | All tables and critical columns exist in both DBs |
| `test_auth.py` | Password hashing, bcrypt roundtrip, session helpers, decorators |
| `test_club_resolver.py` | Subdomain parsing, DSN priority (env → master DB → DATABASE_URL), cache |
| `test_email.py` | All notification functions with SMTP mocked; disabled-email guards |
| `test_master_db.py` | Club/order/super-admin CRUD, vehicle templates, provisioning helpers |
| `test_models.py` | Reservations, users, password tokens, messages, blackouts, waitlist, settings |
| `test_routes_public.py` | Login (username + email), logout, changeme redirect, password reset flow |
| `test_routes_member.py` | Calendar API, reservation creation/cancellation, profile, messages, iCal |
| `test_routes_admin.py` | User management, blackouts, approvals, maintenance, settings, audit log |
| `test_superadmin.py` | Super-admin login, dashboard, club provisioning, order linking |

### Test Isolation

Every test starts with a clean slate — both databases are fully truncated before each test,
and the `testclub` master record is re-seeded automatically so the club resolver can always
find it. Fixtures use `autocommit=True` connections so inserts are immediately visible to
the application's own connections.

---

## Bare-Metal / Systemd Deployment

```bash
pip install -r requirements.txt
cp .env.example .env && vim .env

# Initialize master DB (clubs registry, super-admin)
psql "$MASTER_DATABASE_URL" -f init_master_db.sql

# Run directly
gunicorn --config gunicorn.conf.py app:app
```

Nginx should proxy to `http://127.0.0.1:5210`. For multi-tenant subdomain routing,
configure a wildcard DNS record (`*.fleetnests.com → server IP`) and a wildcard
`server_name` in Nginx.
