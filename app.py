"""
FleetNests — Multi-club, multi-vehicle reservation platform.
Forked from fleetnests. Multi-tenant via subdomain routing.
"""

import os
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, jsonify, abort, g, Response
)
from werkzeug.middleware.proxy_fix import ProxyFix

from zoneinfo import ZoneInfo

import auth
import db
import models
import email_notify
import club_resolver
import master_db
import vehicle_types
import marketing as marketing_module

CENTRAL = ZoneInfo("America/Chicago")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ["SECRET_KEY"]

    # Sub-path portability
    prefix = os.environ.get("APP_PREFIX", "/")
    app.config["APPLICATION_ROOT"] = prefix
    app.config["PREFERRED_URL_SCHEME"] = "https"
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "true").lower() == "true"

    # Trust the reverse proxy (nginx) for host/scheme/prefix headers
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # Register club resolver (sets g.club, g.vehicle_type, g.club_dsn per request)
    club_resolver.init_app(app)

    # Make current_user and club context available in all templates
    @app.context_processor
    def inject_context():
        club = getattr(g, "club", None)
        vtype = getattr(g, "vehicle_type", "boat")
        settings = {}
        branding = {"primary_color": "#0A2342", "accent_color": "#C9A84C",
                    "logo_data": None, "hero_data": None}
        if club:
            try:
                settings = models.get_all_club_settings()
            except Exception:
                pass
            try:
                branding = models.get_branding()
            except Exception:
                pass
        return {
            "current_user":   auth.current_user(),
            "current_sadmin": auth.current_super_admin(),
            "club":           club,
            "vehicle_type":   vtype,
            "is_boat":        vtype == "boat",
            "is_plane":       vtype == "plane",
            "club_settings":  settings,
            "branding":       branding,
        }

    # Custom error pages
    @app.errorhandler(403)
    def forbidden(e):
        return render_template("403.html"), 403

    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404

    # Marketing site (served on bare root domain — fleetnests.com)
    app.register_blueprint(marketing_module.bp, url_prefix="")

    register_routes(app)
    register_superadmin_routes(app)
    return app


# ---------------------------------------------------------------------------
# Club routes
# ---------------------------------------------------------------------------

def register_routes(app: Flask):

    # -- Root redirect ---------------------------------------------------

    @app.route("/")
    def index():
        return redirect(url_for("calendar"))

    # -- Auth ------------------------------------------------------------

    # Demo sample sites: email-only access (no password)
    DEMO_CLUBS = {
        "sample1": "jwilson",
        "sample2": "rbennett",
    }

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if auth.current_user():
            return redirect(url_for("calendar"))

        club = getattr(g, "club", None)
        is_demo = club and club.get("short_name") in DEMO_CLUBS

        if request.method == "POST" and is_demo:
            # Demo clubs: capture email, log in as the demo admin automatically
            email = request.form.get("email", "").strip().lower()
            if not email or "@" not in email:
                flash("Please enter a valid email address.", "danger")
                return render_template("login.html", is_demo=True)

            ip = request.headers.get("X-Real-IP") or request.remote_addr
            ua = request.headers.get("User-Agent", "")[:500]
            master_db.save_demo_lead(email, club["short_name"], club["name"], ip, ua)
            email_notify.notify_demo_lead(email, club["name"], club["short_name"], ip)

            demo_username = DEMO_CLUBS[club["short_name"]]
            demo_user = db.fetchone("SELECT * FROM users WHERE username = %s", (demo_username,))
            if demo_user:
                auth.login_user(dict(demo_user), club_short_name=club["short_name"])
                return redirect(url_for("calendar"))

        if request.method == "POST" and not is_demo:
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            user = auth.authenticate(username, password)
            if user:
                auth.login_user(user, club_short_name=club["short_name"] if club else None)
                return redirect(url_for("calendar"))
            flash("Invalid username or password.", "danger")

        return render_template("login.html", is_demo=is_demo)

    @app.route("/logout")
    def logout():
        auth.logout_user()
        flash("You have been logged out.", "info")
        return redirect(url_for("login"))

    # -- Help ------------------------------------------------------------

    @app.route("/help")
    @auth.login_required
    def help_page():
        return render_template("help.html")

    # -- Calendar --------------------------------------------------------

    @app.route("/calendar")
    @auth.login_required
    def calendar():
        return render_template("calendar.html")

    @app.route("/api/reservations")
    @auth.login_required
    def api_reservations():
        """JSON feed for FullCalendar. Returns timed events for the requested range."""
        try:
            start = date.fromisoformat(request.args["start"][:10])
            end   = date.fromisoformat(request.args["end"][:10])
        except (KeyError, ValueError):
            return jsonify([])

        rows = models.get_reservations_range(start, end)
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        events = []
        for row in rows:
            mine = (row["user_id"] == eff_id)
            is_pending = (row["status"] == "pending_approval")
            color = "#ffc107" if is_pending else ("#0d6efd" if mine else "#6c757d")
            text_color = "#000000" if is_pending else "#ffffff"
            start_ct = row["start_time"].replace(tzinfo=CENTRAL) if row["start_time"] else None
            end_ct   = row["end_time"].replace(tzinfo=CENTRAL)   if row["end_time"]   else None
            events.append({
                "title": (row["vehicle_name"] + " — " if row.get("vehicle_name") else "") + row["full_name"],
                "start": start_ct.isoformat() if start_ct else row["date"].isoformat(),
                "end":   end_ct.isoformat()   if end_ct   else None,
                "color": color,
                "textColor": text_color,
                "url": url_for("reserve_detail", res_date=row["date"].isoformat()),
            })

        # Add blackout dates as non-clickable red blocks
        blackouts = models.get_blackouts_range(start, end)
        for b in blackouts:
            events.append({
                "title": f"\U0001f6ab {b['reason']}",
                "start": b["start_time"].replace(tzinfo=CENTRAL).isoformat(),
                "end":   b["end_time"].replace(tzinfo=CENTRAL).isoformat(),
                "color": "#dc3545",
                "textColor": "#ffffff",
                "display": "block",
            })

        return jsonify(events)

    # -- Reserve / Cancel ------------------------------------------------

    @app.route("/reserve/<res_date>", methods=["GET", "POST"])
    @auth.login_required
    def reserve_detail(res_date: str):
        try:
            day = date.fromisoformat(res_date)
        except ValueError:
            abort(404)

        user    = auth.current_user()
        eff_id  = models.get_effective_user_id(user)
        vtype   = getattr(g, "vehicle_type", "boat")
        settings = models.get_all_club_settings()
        vehicles = models.get_all_vehicles()   # all active vehicles for selection
        existing = models.get_reservations_for_date(day)

        def _render(extra=None):
            import json as _json
            from collections import defaultdict
            user_res_ids = {r["user_id"] for r in existing}
            # A day is only "fully booked" when every vehicle has no 2-hr gap remaining
            fully_booked = all(
                models.is_day_fully_booked([r for r in existing if r.get("vehicle_id") == v["id"]])
                for v in vehicles
            ) if vehicles else False
            # Build booked-time windows per vehicle for JS time-slot filtering (#7)
            _bw: dict = defaultdict(list)
            for r in existing:
                vid = r.get("vehicle_id")
                st  = r.get("start_time")
                et  = r.get("end_time")
                if vid and st and et:
                    _bw[vid].append({
                        "s": st.hour * 60 + st.minute,
                        "e": et.hour * 60 + et.minute,
                    })
            kw = dict(
                day=day, existing=existing, today=date.today(),
                vehicles=vehicles,
                user_has_res=eff_id in user_res_ids,
                on_waitlist=models.is_on_waitlist(eff_id, day),
                day_is_fully_booked=fully_booked,
                vehicle_photo=models.get_primary_vehicle_photo(),
                res_settings=settings,
                booked_windows_json=_json.dumps(dict(_bw)),
            )
            if extra:
                kw.update(extra)
            return render_template("reserve.html", **kw)

        if request.method == "POST":
            start_str    = request.form.get("start_time", "").strip()
            end_str      = request.form.get("end_time",   "").strip()
            end_date_str = request.form.get("end_date", "").strip() or res_date
            notes        = request.form.get("notes", "").strip()[:300]
            selected_ids = request.form.getlist("vehicle_id")

            # Validate inputs
            if not selected_ids:
                flash("Please select at least one vehicle.", "danger")
                return _render()
            try:
                vehicle_ids = [int(v) for v in selected_ids]
            except ValueError:
                flash("Invalid vehicle selection.", "danger")
                return _render()
            # Ensure all selected vehicle IDs belong to this club's active fleet
            valid_ids = {v["id"] for v in vehicles}
            if not all(vid in valid_ids for vid in vehicle_ids):
                flash("Invalid vehicle selection.", "danger")
                return _render()

            try:
                start_dt = datetime.fromisoformat(f"{res_date}T{start_str}")
                end_dt   = datetime.fromisoformat(f"{end_date_str}T{end_str}")
            except ValueError:
                flash("Invalid time format.", "danger")
                return _render()

            vnoun = vehicle_types.get_vehicle_noun(vtype)
            # Validate each selected vehicle independently
            errors = []
            for vid in vehicle_ids:
                err = models.validate_reservation(eff_id, start_dt, end_dt,
                                                  vehicle_id=vid, vehicle_noun=vnoun,
                                                  settings=settings)
                if err:
                    vname = next((v["name"] for v in vehicles if v["id"] == vid), f"Vehicle {vid}")
                    errors.append(f"{vname}: {err}")
            if errors:
                for e in errors:
                    flash(e, "danger")
                return _render()

            approval_setting = settings.get("approval_required", "false").lower() == "true"
            status_after     = "pending_approval" if approval_setting else "active"

            results = models.make_reservation_multi(eff_id, vehicle_ids, start_dt, end_dt,
                                                    notes=notes, status=status_after)
            if results is None:
                flash("A time slot conflict was detected. Please choose a different time or vehicle.", "danger")
                existing = models.get_reservations_for_date(day)
                return _render()

            for result in results:
                models.log_action(user["id"], "reservation_created", "reservation",
                                  result["id"],
                                  {"date": res_date, "start": start_str, "end": end_str,
                                   "vehicle_count": len(vehicle_ids)})

            vehicle_names = [v["name"] for v in vehicles if v["id"] in vehicle_ids]
            vlist = " & ".join(vehicle_names)
            if approval_setting:
                flash(f"Reservation request submitted for {day.strftime('%B %d')} ({vlist}) — awaiting admin approval.", "info")
                admins = [u for u in models.get_all_active_users() if u["is_admin"]]
                email_notify.notify_approval_needed(admins, user,
                    {"date": day, "start_time": start_dt, "end_time": end_dt})
            else:
                flash(f"Reserved {day.strftime('%B %d')} "
                      f"{start_dt.strftime('%-I:%M %p')}–{end_dt.strftime('%-I:%M %p')} CT"
                      f" — {vlist}!", "success")
                email_notify.notify_reservation_confirmed(user,
                    {"date": day, "start_time": start_dt, "end_time": end_dt})
            return redirect(url_for("calendar"))

        return _render()

    @app.route("/cancel/<int:res_id>", methods=["POST"])
    @auth.login_required
    def cancel_reservation(res_id: int):
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        res = models.get_reservation_by_id(res_id)
        ok = models.cancel_reservation(res_id, eff_id, is_admin=user["is_admin"])
        if ok and res:
            day = res["date"].strftime("%B %d, %Y")
            flash(f"Reservation for {day} cancelled.", "success")
            models.log_action(user["id"], "reservation_cancelled", "reservation", res_id)
            if user["is_admin"] and res["user_id"] != user["id"]:
                owner = models.get_user_by_id(res["user_id"])
                if owner:
                    email_notify.notify_reservation_cancelled(owner, res)
            elif not user["is_admin"]:
                email_notify.notify_reservation_cancelled(user, res)
            models.notify_and_clear_waitlist(res["date"])
        else:
            flash("Could not cancel that reservation.", "danger")
        referrer = request.referrer or ""
        if "my-reservations" in referrer:
            return redirect(url_for("my_reservations"))
        return redirect(url_for("calendar"))

    # -- My Reservations -------------------------------------------------

    @app.route("/my-reservations")
    @auth.login_required
    def my_reservations():
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        data = models.get_user_reservations(eff_id)
        waitlist = models.get_user_waitlist(eff_id)
        ical_token = models.get_or_create_ical_token(eff_id)
        tlogs = models.get_trip_logs_for_user(eff_id)
        trip_logs_map = {tlog["res_id"]: tlog for tlog in tlogs}
        today = date.today()
        return render_template("my_reservations.html",
                               upcoming=data["upcoming"],
                               past=data["past"],
                               waitlist=waitlist,
                               ical_token=ical_token,
                               trip_logs_map=trip_logs_map,
                               today=today,
                               tomorrow=today + timedelta(days=1))

    # -- Stats -----------------------------------------------------------

    @app.route("/stats")
    @auth.login_required
    def stats():
        rows = models.get_usage_stats()
        return render_template("stats.html", rows=rows)

    # -- Messages --------------------------------------------------------

    @app.route("/messages")
    @auth.login_required
    def messages():
        msgs = models.get_messages()
        message_photos_map = {msg["id"]: models.get_message_photos(msg["id"]) for msg in msgs}
        return render_template("messages.html", messages=msgs,
                               message_photos_map=message_photos_map)

    @app.route("/messages/new", methods=["GET", "POST"])
    @auth.login_required
    def new_message():
        user = auth.current_user()
        if request.method == "POST":
            title = request.form.get("title", "").strip()
            body  = request.form.get("body", "").strip()
            is_ann = bool(request.form.get("is_announcement")) and user["is_admin"]
            if not title or not body:
                flash("Title and body are required.", "danger")
            else:
                result = models.create_message(user["id"], title, body, is_ann)
                if result:
                    msg_id = result["id"]
                    photos = request.files.getlist("photos")
                    for f in photos[:5]:
                        if not f or not f.filename:
                            continue
                        if not f.content_type.startswith("image/"):
                            continue
                        data = f.read()
                        if len(data) > 5 * 1024 * 1024:
                            continue
                        models.add_message_photo(msg_id, data, f.content_type, f.filename)
                flash("Message posted.", "success")
                return redirect(url_for("messages"))
        return render_template("message_form.html")

    @app.route("/messages/<int:msg_id>/delete", methods=["POST"])
    @auth.login_required
    def delete_message(msg_id: int):
        user = auth.current_user()
        ok = models.delete_message(msg_id, user["id"], is_admin=user["is_admin"])
        if ok:
            flash("Message deleted.", "success")
        else:
            flash("You cannot delete that message.", "danger")
        return redirect(url_for("messages"))

    # -- Admin -----------------------------------------------------------

    @app.route("/admin/users")
    @auth.admin_required
    def admin_users():
        users = models.get_all_active_users()
        return render_template("admin/users.html", users=users)

    @app.route("/admin/users/new", methods=["GET", "POST"])
    @auth.admin_required
    def admin_new_user():
        if request.method == "POST":
            import secrets
            username  = request.form.get("username", "").strip().lower()
            full_name = request.form.get("full_name", "").strip()
            email     = request.form.get("email", "").strip()
            password  = request.form.get("password", "")
            is_admin  = bool(request.form.get("is_admin"))
            max_consec = int(request.form.get("max_consecutive_days", 3))
            max_pend   = int(request.form.get("max_pending", 7))

            if not username or not full_name:
                flash("Username and full name are required.", "danger")
            elif not password and not email:
                flash("Provide either a password or an email address (for welcome email).", "danger")
            else:
                pw_hash = auth.hash_password(password) if password else auth.hash_password(secrets.token_urlsafe(32))
                try:
                    row = models.create_user(username, full_name, email, pw_hash, is_admin,
                                             max_consecutive_days=max_consec, max_pending=max_pend)
                    new_id = row["id"]
                    models.log_action(auth.current_user()["id"], "user_created", "user", new_id, {"username": username})
                    if not password and email:
                        token = models.create_password_token(new_id)
                        user_dict = {"full_name": full_name, "email": email}
                        email_notify.notify_welcome(user_dict, token)
                        flash(f"Member {username} created — welcome email sent to {email}.", "success")
                    else:
                        flash(f"Member {username} created.", "success")
                    return redirect(url_for("admin_users"))
                except Exception as e:
                    if "unique" in str(e).lower():
                        flash("That username is already taken.", "danger")
                    else:
                        flash("Error creating user.", "danger")

        return render_template("admin/users.html",
                               users=models.get_all_active_users(),
                               show_form=True)

    @app.route("/admin/users/<int:user_id>/deactivate", methods=["POST"])
    @auth.admin_required
    def admin_deactivate_user(user_id: int):
        me = auth.current_user()
        if user_id == me["id"]:
            flash("You cannot deactivate your own account.", "danger")
        else:
            models.deactivate_user(user_id)
            models.log_action(auth.current_user()["id"], "user_deactivated", "user", user_id)
            flash("User deactivated.", "success")
        return redirect(url_for("admin_users"))

    @app.route("/admin/users/<int:user_id>/reset-password", methods=["GET", "POST"])
    @auth.admin_required
    def admin_reset_password(user_id: int):
        user = models.get_user_by_id(user_id)
        if not user:
            abort(404)

        if request.method == "POST":
            new_pw = request.form.get("password", "")
            confirm = request.form.get("confirm", "")
            if not new_pw or len(new_pw) < 8:
                flash("Password must be at least 8 characters.", "danger")
            elif new_pw != confirm:
                flash("Passwords do not match.", "danger")
            else:
                models.update_password(user_id, auth.hash_password(new_pw))
                models.log_action(auth.current_user()["id"], "password_reset", "user", user_id)
                flash(f"Password reset for {user['username']}.", "success")
                return redirect(url_for("admin_users"))

        return render_template("admin/reset_pw.html", user=user)

    @app.route("/admin/blackouts")
    @auth.admin_required
    def admin_blackouts():
        blackouts = models.get_all_blackouts()
        vehicles  = models.get_all_vehicles()
        return render_template("admin/blackouts.html", blackouts=blackouts, vehicles=vehicles)

    @app.route("/admin/blackouts/new", methods=["GET", "POST"])
    @auth.admin_required
    def admin_new_blackout():
        vehicles = models.get_all_vehicles()
        if request.method == "POST":
            start_date_str = request.form.get("start_date", "").strip()
            end_date_str   = request.form.get("end_date", "").strip() or start_date_str
            start_str      = request.form.get("start_time", "").strip()
            end_str        = request.form.get("end_time", "").strip()
            reason         = request.form.get("reason", "").strip()
            all_day        = bool(request.form.get("all_day"))
            vehicle_ids    = request.form.getlist("vehicle_id")  # [] means all vehicles
            if not start_date_str or not reason:
                flash("Start date and reason are required.", "danger")
            else:
                try:
                    if all_day:
                        start_dt = datetime.fromisoformat(f"{start_date_str}T00:00:00")
                        end_dt   = datetime.fromisoformat(f"{end_date_str}T23:59:59")
                    else:
                        start_dt = datetime.fromisoformat(f"{start_date_str}T{start_str}")
                        end_dt   = datetime.fromisoformat(f"{end_date_str}T{end_str}")
                    if end_dt <= start_dt:
                        flash("End must be after start.", "danger")
                    else:
                        user = auth.current_user()
                        if vehicle_ids:
                            for vid in vehicle_ids:
                                models.create_blackout(start_dt, end_dt, reason,
                                                       user["id"], int(vid))
                        else:
                            models.create_blackout(start_dt, end_dt, reason, user["id"])
                        flash("Blackout added.", "success")
                        return redirect(url_for("admin_blackouts"))
                except ValueError:
                    flash("Invalid date or time.", "danger")
        return render_template("admin/blackouts.html", blackouts=models.get_all_blackouts(),
                               vehicles=vehicles, show_form=True)

    @app.route("/admin/blackouts/<int:blackout_id>/delete", methods=["POST"])
    @auth.admin_required
    def admin_delete_blackout(blackout_id: int):
        models.delete_blackout(blackout_id)
        flash("Blackout removed.", "success")
        return redirect(url_for("admin_blackouts"))

    @app.route("/admin/export-csv")
    @auth.admin_required
    def admin_export_csv():
        import csv, io
        rows = models.get_usage_stats()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Member", "Past", "Upcoming", "Total", "Cancelled"])
        for r in rows:
            writer.writerow([r["full_name"], r["past"], r["upcoming"], r["total"], r["cancelled"]])
        from flask import Response
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=reservations.csv"},
        )

    @app.route("/admin/audit-log")
    @auth.admin_required
    def admin_audit_log():
        days = int(request.args.get("days", 30))
        after = models.now_ct() - timedelta(days=days)
        entries = models.get_audit_log(limit=500, after_date=after)
        return render_template("admin/audit_log.html", entries=entries, days=days)

    @app.route("/admin/approvals")
    @auth.admin_required
    def admin_approvals():
        pending = models.get_pending_approval()
        return render_template("admin/approvals.html", pending=pending)

    @app.route("/admin/approvals/<int:res_id>/approve", methods=["POST"])
    @auth.admin_required
    def admin_approve(res_id: int):
        models.approve_reservation(res_id)
        flash("Reservation approved.", "success")
        return redirect(url_for("admin_approvals"))

    @app.route("/admin/approvals/<int:res_id>/deny", methods=["POST"])
    @auth.admin_required
    def admin_deny(res_id: int):
        res = models.get_reservation_by_id(res_id)
        models.deny_reservation(res_id)
        flash("Reservation denied.", "success")
        if res:
            owner = models.get_user_by_id(res["user_id"])
            if owner:
                email_notify.notify_reservation_cancelled(owner, res)
        return redirect(url_for("admin_approvals"))

    # -- Incident / Damage Reports ----------------------------------------

    @app.route("/incidents/new", methods=["GET", "POST"])
    @auth.login_required
    def new_incident():
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        res_id = request.args.get("res_id", type=int)
        res = models.get_reservation_by_id(res_id) if res_id else None
        if res and not user["is_admin"] and res["user_id"] != eff_id:
            abort(403)

        if request.method == "POST":
            res_id_form = request.form.get("res_id", type=int)
            severity    = request.form.get("severity", "minor")
            description = request.form.get("description", "").strip()
            report_date = request.form.get("report_date", "").strip()

            if not description or not report_date:
                flash("Description and date are required.", "danger")
            else:
                try:
                    rdate = date.fromisoformat(report_date)
                    models.create_incident(user["id"], res_id_form, rdate, severity, description)
                    models.log_action(user["id"], "incident_reported", "reservation", res_id_form,
                                      {"severity": severity})
                    flash("Incident report submitted. Thank you.", "success")
                    return redirect(url_for("my_reservations"))
                except ValueError:
                    flash("Invalid date.", "danger")

        return render_template("incident_form.html", res=res,
                               today=date.today().isoformat())

    @app.route("/admin/incidents")
    @auth.admin_required
    def admin_incidents():
        incidents = models.get_all_incidents()
        return render_template("admin/incidents.html", incidents=incidents)

    @app.route("/admin/incidents/<int:inc_id>/resolve", methods=["POST"])
    @auth.admin_required
    def admin_resolve_incident(inc_id: int):
        user = auth.current_user()
        models.resolve_incident(inc_id, user["id"])
        flash("Incident marked as resolved.", "success")
        return redirect(url_for("admin_incidents"))

    # -- Maintenance Records & Schedules ----------------------------------

    MAINT_CATEGORIES = [
        "general", "engine", "hull", "electrical", "safety",
        "annual_inspection", "avionics", "fuel_system", "cooling",
        "rigging", "propeller", "airframe", "other",
    ]

    @app.route("/fleet-status")
    @auth.login_required
    def fleet_status():
        """Member-facing fleet maintenance status overview."""
        vehicles = models.get_all_vehicles()
        records = models.get_maintenance_records()
        schedules = models.get_maintenance_schedules()
        overdue = models.get_overdue_schedules()
        overdue_ids = {r["id"] for r in overdue}
        today = date.today()
        warn_date = today + timedelta(days=30)
        return render_template(
            "fleet_status.html",
            vehicles=vehicles,
            records=records,
            schedules=schedules,
            overdue_ids=overdue_ids,
            today=today,
            warn_date=warn_date,
        )

    @app.route("/admin/maintenance")
    @auth.admin_required
    def admin_maintenance():
        vehicles = models.get_all_vehicles()
        records = models.get_maintenance_records()
        schedules = models.get_maintenance_schedules(active_only=False)
        overdue = models.get_overdue_schedules()
        overdue_ids = {r["id"] for r in overdue}
        today = date.today()
        warn_date = today + timedelta(days=30)
        return render_template(
            "admin/maintenance.html",
            vehicles=vehicles,
            records=records,
            schedules=schedules,
            overdue_ids=overdue_ids,
            today=today,
            warn_date=warn_date,
            categories=MAINT_CATEGORIES,
        )

    @app.route("/admin/maintenance/records/new", methods=["POST"])
    @auth.admin_required
    def admin_maintenance_record_new():
        vehicle_id   = request.form.get("vehicle_id", type=int)
        performed_by = request.form.get("performed_by", "").strip() or None
        performed_at = request.form.get("performed_at", "").strip()
        category     = request.form.get("category", "general").strip()
        description  = request.form.get("description", "").strip()
        hours_str    = request.form.get("hours_at_service", "").strip()
        cost_str     = request.form.get("cost", "").strip()
        notes        = request.form.get("notes", "").strip() or None

        if not vehicle_id or not performed_at or not description:
            flash("Vehicle, date, and description are required.", "danger")
            return redirect(url_for("admin_maintenance"))
        try:
            done_date = date.fromisoformat(performed_at)
        except ValueError:
            flash("Invalid date format.", "danger")
            return redirect(url_for("admin_maintenance"))

        hours    = float(hours_str) if hours_str else None
        cost     = float(cost_str)  if cost_str  else None
        user     = auth.current_user()
        models.create_maintenance_record(
            vehicle_id, performed_by, done_date, category, description,
            hours, cost, notes, user["id"],
        )
        models.log_action(user["id"], "maintenance_record_added", "vehicle", vehicle_id,
                          {"category": category, "performed_at": str(done_date)})
        flash("Maintenance record saved.", "success")
        return redirect(url_for("admin_maintenance"))

    @app.route("/admin/maintenance/records/<int:record_id>/delete", methods=["POST"])
    @auth.admin_required
    def admin_maintenance_record_delete(record_id: int):
        models.delete_maintenance_record(record_id)
        models.log_action(auth.current_user()["id"], "maintenance_record_deleted",
                          "maintenance_record", record_id)
        flash("Maintenance record deleted.", "success")
        return redirect(url_for("admin_maintenance"))

    @app.route("/admin/maintenance/schedules/new", methods=["POST"])
    @auth.admin_required
    def admin_maintenance_schedule_new():
        vehicle_id    = request.form.get("vehicle_id", type=int)
        task_name     = request.form.get("task_name", "").strip()
        category      = request.form.get("category", "general").strip()
        description   = request.form.get("description", "").strip() or None
        int_months    = request.form.get("interval_months", "").strip()
        int_hours     = request.form.get("interval_hours", "").strip()
        next_due_date = request.form.get("next_due_date", "").strip()
        next_due_hrs  = request.form.get("next_due_hours", "").strip()
        priority      = request.form.get("priority", "normal").strip()

        if not vehicle_id or not task_name:
            flash("Vehicle and task name are required.", "danger")
            return redirect(url_for("admin_maintenance"))

        try:
            months  = int(int_months)   if int_months  else None
            hours   = float(int_hours)  if int_hours   else None
            due_d   = date.fromisoformat(next_due_date) if next_due_date else None
            due_h   = float(next_due_hrs) if next_due_hrs else None
        except ValueError:
            flash("Invalid numeric value.", "danger")
            return redirect(url_for("admin_maintenance"))

        models.create_maintenance_schedule(
            vehicle_id, task_name, category, description,
            months, hours, None, None, due_d, due_h, priority,
        )
        user = auth.current_user()
        models.log_action(user["id"], "maintenance_schedule_added", "vehicle", vehicle_id,
                          {"task": task_name})
        flash(f"Scheduled task '{task_name}' added.", "success")
        return redirect(url_for("admin_maintenance"))

    @app.route("/admin/maintenance/schedules/<int:schedule_id>/done", methods=["POST"])
    @auth.admin_required
    def admin_maintenance_schedule_done(schedule_id: int):
        done_date_str = request.form.get("done_date", date.today().isoformat()).strip()
        done_hours_str = request.form.get("done_hours", "").strip()
        try:
            done_date_val = date.fromisoformat(done_date_str)
        except ValueError:
            flash("Invalid date.", "danger")
            return redirect(url_for("admin_maintenance"))
        done_hours_val = float(done_hours_str) if done_hours_str else None
        models.mark_schedule_done(schedule_id, done_date_val, done_hours_val)
        user = auth.current_user()
        models.log_action(user["id"], "maintenance_schedule_done",
                          "maintenance_schedule", schedule_id,
                          {"done_date": str(done_date_val)})
        flash("Task marked as completed and next due date updated.", "success")
        return redirect(url_for("admin_maintenance"))

    @app.route("/admin/maintenance/schedules/<int:schedule_id>/delete", methods=["POST"])
    @auth.admin_required
    def admin_maintenance_schedule_delete(schedule_id: int):
        models.delete_maintenance_schedule(schedule_id)
        models.log_action(auth.current_user()["id"], "maintenance_schedule_deleted",
                          "maintenance_schedule", schedule_id)
        flash("Schedule deleted.", "success")
        return redirect(url_for("admin_maintenance"))

    # -- Fuel Log ---------------------------------------------------------

    @app.route("/fuel/new", methods=["GET", "POST"])
    @auth.login_required
    def new_fuel_entry():
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        res_id = request.args.get("res_id", type=int)
        res = models.get_reservation_by_id(res_id) if res_id else None
        if res and not user["is_admin"] and res["user_id"] != eff_id:
            abort(403)

        if request.method == "POST":
            res_id_form     = request.form.get("res_id", type=int)
            vehicle_id_form = request.form.get("vehicle_id", type=int)
            log_date        = request.form.get("log_date", "").strip()
            gallons_str     = request.form.get("gallons", "").strip()
            price_str       = request.form.get("price_per_gallon", "").strip()
            total_str       = request.form.get("total_cost", "").strip()
            notes           = request.form.get("notes", "").strip()[:300]

            if not log_date:
                flash("Date is required.", "danger")
            elif not gallons_str:
                flash("Gallons is required.", "danger")
            else:
                try:
                    ldate   = date.fromisoformat(log_date)
                    gallons = float(gallons_str)
                    price   = float(price_str) if price_str else None
                    total   = float(total_str) if total_str else (
                        round(gallons * price, 2) if price else None
                    )
                    if gallons <= 0:
                        flash("Gallons must be a positive number.", "danger")
                    else:
                        models.create_fuel_entry(user["id"], res_id_form, ldate, gallons,
                                                 price, total, notes, vehicle_id_form)
                        flash(f"Fuel entry logged: {gallons} gal on {ldate.strftime('%B %d, %Y')}.", "success")
                        return redirect(url_for("my_reservations"))
                except ValueError:
                    flash("Invalid date or number format. Please check your entries.", "danger")

        vehicles = models.get_all_vehicles()
        # Pre-select vehicle from linked reservation
        preselect_vid = res["vehicle_id"] if res and res.get("vehicle_id") else None
        return render_template("fuel_form.html", res=res, vehicles=vehicles,
                               preselect_vehicle_id=preselect_vid,
                               today=date.today().isoformat())

    @app.route("/admin/fuel")
    @auth.admin_required
    def admin_fuel():
        entries = models.get_all_fuel_entries()
        stats   = models.get_fuel_stats()
        return render_template("admin/fuel.html", entries=entries, stats=stats)

    # -- Waitlist ---------------------------------------------------------

    @app.route("/waitlist/<res_date>/join", methods=["POST"])
    @auth.login_required
    def waitlist_join(res_date: str):
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        try:
            day = date.fromisoformat(res_date)
        except ValueError:
            abort(404)
        notes = request.form.get("notes", "").strip()[:300]
        models.add_to_waitlist(eff_id, day, notes)
        flash(f"You're on the waitlist for {day.strftime('%B %d')}. "
              "We'll email you if a spot opens.", "info")
        return redirect(url_for("reserve_detail", res_date=res_date))

    @app.route("/waitlist/<res_date>/leave", methods=["POST"])
    @auth.login_required
    def waitlist_leave(res_date: str):
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        try:
            day = date.fromisoformat(res_date)
        except ValueError:
            abort(404)
        models.remove_from_waitlist(eff_id, day)
        flash("Removed from waitlist.", "info")
        return redirect(url_for("reserve_detail", res_date=res_date))

    # -- iCal feed (no login — token is the auth) -------------------------

    @app.route("/ical/<token>.ics")
    def ical_feed(token: str):
        from flask import Response
        user = models.get_user_by_ical_token(token)
        if not user:
            abort(404)
        reservations = models.get_user_ical_reservations(user["id"])
        club = getattr(g, "club", {}) or {}
        club_name = club.get("name", "Club")
        vtype = club.get("vehicle_type", "boat")
        vnoun = vehicle_types.get_vehicle_noun(vtype).title()

        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            f"PRODID:-//{club_name}//Reservation System//EN",
            f"X-WR-CALNAME:My {vnoun} Reservations",
            "X-WR-TIMEZONE:America/Chicago",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
        ]
        for r in reservations:
            def fmt(dt):
                return dt.strftime("%Y%m%dT%H%M%S")
            uid = f"res-{r['id']}@fleetnests"
            lines += [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTART;TZID=America/Chicago:{fmt(r['start_time'])}",
                f"DTEND;TZID=America/Chicago:{fmt(r['end_time'])}",
                f"SUMMARY:{vnoun} Reservation",
                f"DESCRIPTION:{(r.get('notes') or '').replace(chr(10), ' ')}",
                f"CREATED:{fmt(r['created_at'])}",
                "END:VEVENT",
            ]
        lines.append("END:VCALENDAR")

        ics_content = "\r\n".join(lines) + "\r\n"
        return Response(
            ics_content,
            mimetype="text/calendar",
            headers={"Content-Disposition": f"attachment; filename=reservations.ics"},
        )

    @app.route("/ical-token")
    @auth.login_required
    def ical_token_page():
        user = auth.current_user()
        token = models.get_or_create_ical_token(user["id"])
        return render_template("ical_token.html", token=token)

    # -- Profile ----------------------------------------------------------

    @app.route("/profile", methods=["GET", "POST"])
    @auth.login_required
    def profile():
        from flask import Response
        user_session = auth.current_user()
        user = models.get_user_by_id(user_session["id"])

        if request.method == "POST":
            action = request.form.get("action", "")

            if action == "profile":
                phone = request.form.get("phone", "").strip()[:20]
                models.update_profile(user["id"], phone)
                flash("Profile updated.", "success")

            elif action == "avatar":
                f = request.files.get("avatar")
                if not f or not f.filename:
                    flash("Please choose an image file.", "danger")
                elif not f.content_type.startswith("image/"):
                    flash("Only image files are allowed.", "danger")
                else:
                    data = f.read()
                    if len(data) > 5 * 1024 * 1024:
                        flash("Image must be 5 MB or smaller.", "danger")
                    else:
                        models.update_avatar(user["id"], data, f.content_type)
                        flash("Profile photo updated.", "success")

            elif action == "email":
                import secrets
                new_email = request.form.get("new_email", "").strip().lower()
                if not new_email or "@" not in new_email:
                    flash("Enter a valid email address.", "danger")
                elif new_email == (user.get("email") or "").lower():
                    flash("That's already your current email.", "warning")
                elif user.get("pending_email"):
                    flash("A verification email is already pending. Check your inbox or wait for it to expire.", "warning")
                else:
                    taken = db.fetchone(
                        "SELECT id FROM users WHERE (email = %s OR username = %s) AND id != %s",
                        (new_email, new_email, user["id"]),
                    )
                    if taken:
                        flash("That email is already in use.", "danger")
                    else:
                        token = secrets.token_urlsafe(32)
                        expires = models.now_ct() + timedelta(hours=24)
                        models.initiate_email_change(user["id"], new_email, token, expires)
                        email_notify.notify_email_verify(user, new_email, token)
                        flash(f"Verification email sent to {new_email}. "
                              "Click the link within 24 hours to confirm.", "info")

            elif action == "password":
                current_pw = request.form.get("current_password", "")
                new_pw     = request.form.get("new_password", "")
                confirm_pw = request.form.get("confirm_password", "")
                full_user  = db.fetchone("SELECT password_hash FROM users WHERE id = %s",
                                         (user["id"],))
                if not auth.check_password(current_pw, full_user["password_hash"]):
                    flash("Current password is incorrect.", "danger")
                elif len(new_pw) < 8:
                    flash("New password must be at least 8 characters.", "danger")
                elif new_pw != confirm_pw:
                    flash("Passwords do not match.", "danger")
                else:
                    models.update_password(user["id"], auth.hash_password(new_pw))
                    models.log_action(user["id"], "password_changed", "user", user["id"])
                    flash("Password changed successfully.", "success")

            elif action == "member_name":
                name = request.form.get("member_name", "").strip()
                if not name:
                    flash("Member name cannot be blank.", "danger")
                else:
                    models.update_member_name(user["id"], name)
                    flash("Member name updated.", "success")

            elif action == "family_login":
                email2   = request.form.get("email2", "").strip().lower()
                new_pw2  = request.form.get("new_password2", "")
                confirm2 = request.form.get("confirm_password2", "")
                clear    = request.form.get("clear_family", "")
                if clear:
                    models.update_family_credentials(user["id"], None, None)
                    flash("Family login removed.", "success")
                elif not email2 or "@" not in email2:
                    flash("Enter a valid email address for the family login.", "danger")
                elif email2 == (user.get("email") or "").lower():
                    flash("Family email cannot be the same as your primary email.", "danger")
                elif new_pw2 and len(new_pw2) < 8:
                    flash("Family password must be at least 8 characters.", "danger")
                elif new_pw2 and new_pw2 != confirm2:
                    flash("Family passwords do not match.", "danger")
                else:
                    taken = db.fetchone(
                        "SELECT id FROM users WHERE (LOWER(email)=%s OR LOWER(email2)=%s) AND id!=%s",
                        (email2, email2, user["id"]),
                    )
                    if taken:
                        flash("That email is already in use by another account.", "danger")
                    else:
                        pw2_hash = auth.hash_password(new_pw2) if new_pw2 else user.get("password_hash2")
                        models.update_family_credentials(user["id"], email2, pw2_hash)
                        flash("Family login updated.", "success")

            return redirect(url_for("profile"))

        user = models.get_user_by_id(user_session["id"])
        return render_template("profile.html", user=user)

    @app.route("/profile/photo/<int:user_id>")
    @auth.login_required
    def profile_photo(user_id: int):
        from flask import Response
        row = models.get_avatar(user_id)
        if not row or not row["avatar"]:
            abort(404)
        return Response(bytes(row["avatar"]),
                        mimetype=row["avatar_content_type"] or "image/jpeg",
                        headers={"Cache-Control": "max-age=3600"})

    @app.route("/verify-email/<token>")
    def verify_email(token: str):
        user = models.confirm_email_change(token)
        if not user:
            flash("That verification link is invalid or has expired.", "danger")
            return redirect(url_for("login"))
        flash("Email address verified. Please log in with your new email.", "success")
        auth.logout_user()
        return redirect(url_for("login"))

    @app.route("/messages/photo/<int:photo_id>")
    @auth.login_required
    def message_photo(photo_id: int):
        from flask import Response
        row = models.get_message_photo_data(photo_id)
        if not row or not row["photo_data"]:
            abort(404)
        return Response(bytes(row["photo_data"]),
                        mimetype=row["content_type"] or "image/jpeg")

    # -- Trip Log (Checkout / Check-in) -----------------------------------

    @app.route("/trips/<int:res_id>/checkout", methods=["GET", "POST"])
    @auth.login_required
    def trip_checkout(res_id: int):
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        res = models.get_reservation_by_id(res_id)
        if not res:
            abort(404)
        if not user["is_admin"] and res["user_id"] != eff_id:
            abort(403)
        if res["date"] != date.today():
            flash("Check-out is only available on the day of your reservation.", "warning")
            return redirect(url_for("my_reservations"))
        trip_log = models.get_trip_log(res_id)
        if trip_log:
            flash("Check-out already recorded for this reservation.", "info")
            return redirect(url_for("my_reservations"))

        vtype = getattr(g, "vehicle_type", "boat")
        settings = models.get_all_club_settings()
        ctx = vehicle_types.build_checkout_context(vtype, settings)

        if request.method == "POST":
            time_str    = request.form.get("checkout_time", "").strip()
            hours_str   = request.form.get("primary_hours_out", "").strip()
            fuel_level  = request.form.get("fuel_level_out", "").strip()
            condition   = request.form.get("condition_out", "").strip()[:500]
            checklist   = [int(x) for x in request.form.getlist("checklist") if x.isdigit()]
            try:
                checkout_dt = datetime.fromisoformat(f"{res['date'].isoformat()}T{time_str}")
                hours_out   = float(hours_str) if hours_str else None
                vehicle_id  = res.get("vehicle_id") or models.get_default_vehicle_id()
                models.create_checkout(res_id, user["id"], checkout_dt,
                                       hours_out, fuel_level or None, condition, checklist,
                                       vehicle_id=vehicle_id)
                models.log_action(user["id"], "trip_checkout", "reservation", res_id,
                                  {"fuel_level": fuel_level, "checklist_count": len(checklist)})
                flash("Check-out recorded. Have a great trip!", "success")
                return redirect(url_for("my_reservations"))
            except (ValueError, KeyError) as ex:
                flash(f"Invalid input: {ex}", "danger")

        now_time = datetime.now(CENTRAL).strftime("%H:%M")
        return render_template("checkout.html", res=res, trip_log=None,
                               now_time=now_time, **ctx)

    @app.route("/trips/<int:res_id>/checkin", methods=["GET", "POST"])
    @auth.login_required
    def trip_checkin(res_id: int):
        user = auth.current_user()
        eff_id = models.get_effective_user_id(user)
        res = models.get_reservation_by_id(res_id)
        if not res:
            abort(404)
        if not user["is_admin"] and res["user_id"] != eff_id:
            abort(403)
        trip_log = models.get_trip_log(res_id)
        if not trip_log:
            flash("No check-out found for this reservation.", "warning")
            return redirect(url_for("my_reservations"))

        vtype = getattr(g, "vehicle_type", "boat")
        settings = models.get_all_club_settings()
        ctx = vehicle_types.build_checkout_context(vtype, settings)

        if request.method == "POST":
            time_str      = request.form.get("checkin_time", "").strip()
            hours_str     = request.form.get("primary_hours_in", "").strip()
            gallons_str   = request.form.get("fuel_added_gallons", "").strip()
            cost_str      = request.form.get("fuel_added_cost", "").strip()
            condition     = request.form.get("condition_in", "").strip()[:500]
            try:
                checkin_dt  = datetime.fromisoformat(f"{res['date'].isoformat()}T{time_str}")
                hours_in    = float(hours_str) if hours_str else None
                gallons     = float(gallons_str) if gallons_str else None
                cost        = float(cost_str) if cost_str else None
                models.update_checkin(res_id, checkin_dt, hours_in, gallons, cost, condition)
                if gallons and gallons > 0:
                    models.create_fuel_entry(user["id"], res_id, res["date"],
                                             gallons, None, cost,
                                             "Auto-logged from trip check-in")
                models.log_action(user["id"], "trip_checkin", "reservation", res_id,
                                  {"hours_in": hours_in, "fuel_gallons": gallons})
                flash("Check-in complete. Welcome back!", "success")
                return redirect(url_for("my_reservations"))
            except (ValueError, KeyError) as ex:
                flash(f"Invalid input: {ex}", "danger")

        now_time = datetime.now(CENTRAL).strftime("%H:%M")
        return render_template("checkin.html", res=res, trip_log=trip_log,
                               now_time=now_time, **ctx)

    @app.route("/admin/trip-logs")
    @auth.admin_required
    def admin_trip_logs():
        logs = models.get_all_trip_logs()
        vtype = getattr(g, "vehicle_type", "boat")
        settings = models.get_all_club_settings()
        hours_lbl = settings.get("hours_label") or vehicle_types.get_hours_label(vtype)
        return render_template("admin/trip_logs.html", logs=logs, HOURS_LABEL=hours_lbl)

    @app.route("/admin/feedback")
    @auth.admin_required
    def admin_feedback():
        submissions = models.get_all_feedback_submissions()
        return render_template("admin/feedback.html", submissions=submissions)

    @app.route("/admin/users/<int:user_id>/edit", methods=["GET", "POST"])
    @auth.admin_required
    def admin_edit_user(user_id: int):
        target = models.get_user_by_id(user_id)
        if not target:
            abort(404)
        all_users = models.get_all_active_users()
        if request.method == "POST":
            display_name     = request.form.get("display_name", "").strip()
            family_str       = request.form.get("family_account_id", "").strip()
            family_account_id = int(family_str) if family_str.isdigit() else None
            if family_account_id == user_id:
                family_account_id = None
            models.update_user_profile(user_id, display_name or None, family_account_id)
            models.log_action(auth.current_user()["id"], "user_profile_updated", "user", user_id,
                              {"display_name": display_name, "family_account_id": family_account_id})
            flash(f"Profile updated for {target['full_name']}.", "success")
            return redirect(url_for("admin_users"))
        return render_template("admin/edit_user.html", target=target, all_users=all_users)

    # -- Club Rules & Checklist ---------------------------------------------

    @app.route("/rules")
    @auth.login_required
    def rules_page():
        import json as _json
        vtype = getattr(g, "vehicle_type", "boat")
        settings = models.get_all_club_settings()
        ctx = vehicle_types.build_checkout_context(vtype, settings)
        raw = settings.get("member_rules_json")
        member_rules = []
        if raw:
            try:
                member_rules = _json.loads(raw)
            except Exception:
                pass
        ctx["member_rules"] = member_rules
        return render_template("rules.html", **ctx)

    @app.route("/checklist")
    @auth.login_required
    def checklist_page():
        vtype = getattr(g, "vehicle_type", "boat")
        settings = models.get_all_club_settings()
        ctx = vehicle_types.build_checkout_context(vtype, settings)
        return render_template("checklist.html", **ctx)

    # -- Monthly Statements ---------------------------------------------------

    @app.route("/statements")
    @auth.login_required
    def statements():
        stmts = models.get_all_statements()
        return render_template("statements.html", statements=stmts)

    @app.route("/statements/<int:stmt_id>/download")
    @auth.login_required
    def download_statement(stmt_id: int):
        stmt = models.get_statement_by_id(stmt_id)
        if not stmt:
            abort(404)
        from flask import Response
        return Response(
            bytes(stmt["file_data"]),
            mimetype="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{stmt["filename"]}"'},
        )

    @app.route("/admin/statements", methods=["GET", "POST"])
    @auth.statements_manager_required
    def admin_statements():
        if request.method == "POST":
            action = request.form.get("action")
            if action == "delete":
                stmt_id = int(request.form.get("stmt_id", 0))
                models.delete_statement(stmt_id)
                models.log_action(auth.current_user()["id"], "statement_deleted", "statement", stmt_id)
                flash("Statement deleted.", "success")
                return redirect(url_for("admin_statements"))

            display_name = request.form.get("display_name", "").strip()
            f = request.files.get("pdf_file")
            if not display_name:
                flash("Please enter a name for the statement.", "danger")
            elif not f or not f.filename:
                flash("Please select a PDF file.", "danger")
            elif f.content_type not in ("application/pdf", "application/octet-stream") and \
                 not f.filename.lower().endswith(".pdf"):
                flash("Only PDF files are allowed.", "danger")
            else:
                data = f.read()
                if len(data) > 20 * 1024 * 1024:
                    flash("File too large — maximum is 20 MB.", "danger")
                else:
                    filename = os.path.basename(f.filename)
                    stmt_id = models.create_statement(display_name, filename, data,
                                                      auth.current_user()["id"])
                    models.log_action(auth.current_user()["id"], "statement_uploaded",
                                      "statement", stmt_id, {"name": display_name})
                    flash(f'"{display_name}" uploaded successfully.', "success")
                    return redirect(url_for("admin_statements"))

        stmts = models.get_all_statements()
        return render_template("admin/statements.html", statements=stmts)

    @app.route("/admin/settings", methods=["GET", "POST"])
    @auth.admin_required
    def admin_settings():
        vtype = getattr(g, "vehicle_type", "boat")
        if request.method == "POST":
            # Boolean toggles — absent from form data means False
            bool_keys = ["has_hours_meter", "has_fuel_level_enum", "fuel_required_on_return",
                         "approval_required"]
            for key in bool_keys:
                models.update_club_setting(key, "true" if request.form.get(key) else "false")
            # Text fields — save as-is (empty string clears the override)
            for key in ["hours_label", "marina_phone", "fbo_phone",
                        "weather_zone", "nws_county", "aviation_station",
                        "min_res_hours", "max_res_hours",
                        "max_advance_days", "max_future_reservations",
                        "max_concurrent_vehicles"]:
                val = request.form.get(key, "").strip()
                models.update_club_setting(key, val)
            user = auth.current_user()
            models.log_action(user["id"], "settings_updated", "club_settings", None)
            flash("Settings saved.", "success")
            return redirect(url_for("admin_settings"))
        settings = models.get_all_club_settings()
        return render_template("admin/settings.html", settings=settings,
                               vehicle_type=vtype,
                               default_hours_label=vehicle_types.get_hours_label(vtype),
                               vehicle_photos=models.get_vehicle_photos(),
                               gallery_photos=models.get_club_photos(),
                               branding=models.get_branding())

    # -- Password reset / welcome set-password --------------------------------

    @app.route("/forgot-password", methods=["GET", "POST"])
    def forgot_password():
        if auth.current_user():
            return redirect(url_for("calendar"))
        if request.method == "POST":
            import secrets
            login = request.form.get("login", "").strip()
            user = db.fetchone(
                "SELECT * FROM users WHERE (username = %s OR LOWER(email) = LOWER(%s)) AND is_active = TRUE",
                (login, login),
            )
            if user and user.get("email"):
                token = models.create_password_token(user["id"])
                email_notify.notify_password_reset(user, token)
            flash("If that account exists and has an email address, a reset link has been sent.", "info")
            return redirect(url_for("forgot_password"))
        return render_template("forgot_password.html")

    @app.route("/set-password/<token>", methods=["GET", "POST"])
    def set_password(token: str):
        if auth.current_user():
            return redirect(url_for("calendar"))
        if request.method == "POST":
            user = models.consume_password_token(token)
            if not user:
                flash("That link is invalid or has expired. Request a new one.", "danger")
                return redirect(url_for("login"))
            new_pw  = request.form.get("new_password", "")
            confirm = request.form.get("confirm_password", "")
            if len(new_pw) < 8:
                flash("Password must be at least 8 characters.", "danger")
                return redirect(url_for("set_password", token=token))
            if new_pw != confirm:
                flash("Passwords do not match.", "danger")
                return redirect(url_for("set_password", token=token))
            models.update_password(user["id"], auth.hash_password(new_pw))
            models.log_action(user["id"], "password_set_via_token", "user", user["id"])
            flash("Password set — please log in.", "success")
            return redirect(url_for("login"))

        user = models.get_user_by_password_token(token)
        if not user:
            flash("That link is invalid or has expired. Request a new one.", "danger")
            return redirect(url_for("login"))
        return render_template("set_password.html", token=token, user=user)

    # -- Member Feedback (AI-routed) ----------------------------------------

    @app.route("/feedback", methods=["POST"])
    @auth.login_required
    def submit_feedback():
        import feedback as fb

        user = auth.current_user()
        text = request.form.get("feedback_text", "").strip()
        if not text:
            return jsonify({"ok": False, "error": "Feedback text is required."}), 400
        if len(text) > 4000:
            return jsonify({"ok": False, "error": "Feedback must be 4000 characters or fewer."}), 400

        file_bytes = None
        file_type  = None
        file_name  = None
        attachment = request.files.get("screenshot") or request.files.get("attachment")
        if attachment and attachment.filename:
            file_type = attachment.content_type or "application/octet-stream"
            file_name = attachment.filename
            data = attachment.read(10 * 1024 * 1024 + 1)
            if len(data) > 10 * 1024 * 1024:
                return jsonify({"ok": False,
                                "error": "Attachment must be 10 MB or smaller."}), 400
            file_bytes = data

        ok, action, saved_path, github_url = fb.process_feedback(
            user, text, file_bytes, file_type, file_name)
        if not ok:
            return jsonify({"ok": False,
                            "error": "Could not deliver your feedback. Please try again."}), 500

        models.save_feedback_submission(
            user["id"], text, saved_path, file_name, file_type, action, github_url)
        models.log_action(user["id"], "feedback_submitted", None, None,
                          {"action": action, "length": len(text)})
        return jsonify({"ok": True, "action": action})


# ---------------------------------------------------------------------------
# Club branding & photo routes
# ---------------------------------------------------------------------------

    @app.route("/club-logo")
    def club_logo():
        """Serve the club logo image."""
        row = models.get_branding_logo()
        if not row:
            return app.send_static_file("images/badge.svg")
        return Response(
            bytes(row["logo_data"]),
            content_type=row["logo_content_type"] or "image/png",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    @app.route("/club-hero")
    def club_hero():
        """Serve the club hero/banner image."""
        row = models.get_branding_hero()
        if not row:
            abort(404)
        return Response(
            bytes(row["hero_data"]),
            content_type=row["hero_content_type"] or "image/jpeg",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    @app.route("/gallery")
    @auth.login_required
    def gallery():
        """Club photo gallery page."""
        photos = models.get_club_photos()
        return render_template("gallery.html", photos=photos)

    @app.route("/club-photo/<int:photo_id>")
    @auth.login_required
    def club_photo(photo_id: int):
        """Serve a gallery photo."""
        photo = models.get_club_photo(photo_id)
        if not photo:
            abort(404)
        return Response(
            bytes(photo["photo_data"]),
            content_type=photo["content_type"] or "image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @app.route("/vehicle-photo/<int:photo_id>")
    @auth.login_required
    def vehicle_photo(photo_id: int):
        """Serve a vehicle photo."""
        photo = models.get_vehicle_photo(photo_id)
        if not photo:
            abort(404)
        return Response(
            bytes(photo["photo_data"]),
            content_type=photo["content_type"] or "image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    # -- Admin branding & photos --------------------------------------------

    @app.route("/admin/branding", methods=["POST"])
    @auth.admin_required
    def admin_branding():
        """Update club colors and/or logo/hero images."""
        action = request.form.get("action", "")
        ALLOWED_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml"}
        MAX_SIZE = 5 * 1024 * 1024

        if action == "colors":
            primary = request.form.get("primary_color", "#0A2342").strip()
            accent  = request.form.get("accent_color",  "#C9A84C").strip()
            if not (primary.startswith("#") and len(primary) == 7):
                flash("Invalid primary color.", "danger")
                return redirect(url_for("admin_settings"))
            if not (accent.startswith("#") and len(accent) == 7):
                flash("Invalid accent color.", "danger")
                return redirect(url_for("admin_settings"))
            models.update_branding_colors(primary, accent)
            flash("Colors updated.", "success")

        elif action == "logo":
            f = request.files.get("logo")
            if f and f.filename:
                if f.content_type not in ALLOWED_TYPES:
                    flash("Logo must be an image file.", "danger")
                    return redirect(url_for("admin_settings"))
                data = f.read(MAX_SIZE + 1)
                if len(data) > MAX_SIZE:
                    flash("Logo must be 5 MB or smaller.", "danger")
                    return redirect(url_for("admin_settings"))
                models.update_branding_logo(data, f.content_type)
                flash("Logo updated.", "success")

        elif action == "delete_logo":
            models.delete_branding_logo()
            flash("Logo removed.", "success")

        elif action == "hero":
            f = request.files.get("hero")
            if f and f.filename:
                if f.content_type not in ALLOWED_TYPES:
                    flash("Hero image must be an image file.", "danger")
                    return redirect(url_for("admin_settings"))
                data = f.read(MAX_SIZE + 1)
                if len(data) > MAX_SIZE:
                    flash("Hero image must be 5 MB or smaller.", "danger")
                    return redirect(url_for("admin_settings"))
                models.update_branding_hero(data, f.content_type)
                flash("Hero image updated.", "success")

        elif action == "delete_hero":
            models.delete_branding_hero()
            flash("Hero image removed.", "success")

        return redirect(url_for("admin_settings"))

    @app.route("/admin/photos/upload", methods=["POST"])
    @auth.admin_required
    def admin_photo_upload():
        """Upload a gallery photo."""
        user = auth.current_user()
        ALLOWED_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
        MAX_SIZE = 10 * 1024 * 1024
        f = request.files.get("photo")
        if not f or not f.filename:
            flash("No file selected.", "danger")
            return redirect(url_for("admin_settings"))
        if f.content_type not in ALLOWED_TYPES:
            flash("Gallery photos must be JPEG, PNG, GIF, or WebP.", "danger")
            return redirect(url_for("admin_settings"))
        data = f.read(MAX_SIZE + 1)
        if len(data) > MAX_SIZE:
            flash("Photo must be 10 MB or smaller.", "danger")
            return redirect(url_for("admin_settings"))
        title = request.form.get("title", "").strip() or None
        models.add_club_photo(title, data, f.content_type, user["id"])
        flash("Photo added to gallery.", "success")
        return redirect(url_for("admin_settings"))

    @app.route("/admin/photos/<int:photo_id>/delete", methods=["POST"])
    @auth.admin_required
    def admin_photo_delete(photo_id: int):
        """Delete a gallery photo."""
        models.delete_club_photo(photo_id)
        flash("Photo deleted.", "success")
        return redirect(url_for("admin_settings"))

    @app.route("/admin/vehicle-photos/upload", methods=["POST"])
    @auth.admin_required
    def admin_vehicle_photo_upload():
        """Upload a vehicle photo."""
        ALLOWED_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
        MAX_SIZE = 10 * 1024 * 1024
        f = request.files.get("photo")
        if not f or not f.filename:
            flash("No file selected.", "danger")
            return redirect(url_for("admin_settings"))
        if f.content_type not in ALLOWED_TYPES:
            flash("Vehicle photos must be JPEG, PNG, GIF, or WebP.", "danger")
            return redirect(url_for("admin_settings"))
        data = f.read(MAX_SIZE + 1)
        if len(data) > MAX_SIZE:
            flash("Photo must be 10 MB or smaller.", "danger")
            return redirect(url_for("admin_settings"))
        caption   = request.form.get("caption", "").strip() or None
        is_primary = request.form.get("is_primary") == "1"
        models.add_vehicle_photo(caption, data, f.content_type, is_primary)
        flash("Vehicle photo added.", "success")
        return redirect(url_for("admin_settings"))

    @app.route("/admin/vehicle-photos/<int:photo_id>/set-primary", methods=["POST"])
    @auth.admin_required
    def admin_vehicle_photo_set_primary(photo_id: int):
        models.set_primary_vehicle_photo(photo_id)
        flash("Primary vehicle photo updated.", "success")
        return redirect(url_for("admin_settings"))

    @app.route("/admin/vehicle-photos/<int:photo_id>/delete", methods=["POST"])
    @auth.admin_required
    def admin_vehicle_photo_delete(photo_id: int):
        models.delete_vehicle_photo(photo_id)
        flash("Vehicle photo deleted.", "success")
        return redirect(url_for("admin_settings"))


# ---------------------------------------------------------------------------
# Super-admin routes
# ---------------------------------------------------------------------------

def register_superadmin_routes(app: Flask):

    @app.route("/superadmin/login", methods=["GET", "POST"])
    def superadmin_login():
        if auth.current_super_admin():
            return redirect(url_for("superadmin_dashboard"))

        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            admin = auth.authenticate_super_admin(username, password)
            if admin:
                auth.login_super_admin(admin)
                return redirect(url_for("superadmin_dashboard"))
            flash("Invalid super-admin credentials.", "danger")

        return render_template("superadmin/login.html")

    @app.route("/superadmin/logout")
    def superadmin_logout():
        auth.logout_super_admin()
        return redirect(url_for("superadmin_login"))

    @app.route("/superadmin/")
    @auth.superadmin_required
    def superadmin_dashboard():
        import master_db as mdb
        clubs = mdb.get_all_clubs()
        return render_template("superadmin/dashboard.html", clubs=clubs)

    @app.route("/superadmin/clubs/new", methods=["GET", "POST"])
    @auth.superadmin_required
    def superadmin_new_club():
        if request.method == "POST":
            name         = request.form.get("name", "").strip()
            short_name   = request.form.get("short_name", "").strip().lower()
            vtype        = request.form.get("vehicle_type", "boat")
            contact_email = request.form.get("contact_email", "").strip()
            timezone     = request.form.get("timezone", "America/Chicago").strip()

            if not name or not short_name:
                flash("Club name and short name are required.", "danger")
            elif vtype not in ("boat", "plane"):
                flash("Vehicle type must be 'boat' or 'plane'.", "danger")
            else:
                try:
                    import master_models
                    result = master_models.provision_club(
                        name, short_name, vtype, contact_email, timezone)
                    import master_db as mdb
                    mdb.log_master_action(
                        auth.current_super_admin()["id"],
                        "club_provisioned", "club",
                        result.get("id"),
                        {"short_name": short_name, "vehicle_type": vtype},
                    )
                    flash(
                        f"Club '{name}' provisioned. DB user password: {result.get('_db_password', '(see logs)')}",
                        "success",
                    )
                    return redirect(url_for("superadmin_dashboard"))
                except Exception as exc:
                    flash(f"Provisioning failed: {exc}", "danger")

        return render_template("superadmin/new_club.html")

    @app.route("/superadmin/clubs/<int:club_id>/deactivate", methods=["POST"])
    @auth.superadmin_required
    def superadmin_deactivate_club(club_id: int):
        import master_db as mdb
        mdb.deactivate_club(club_id)
        mdb.log_master_action(
            auth.current_super_admin()["id"],
            "club_deactivated", "club", club_id,
        )
        club_resolver.invalidate_cache()
        flash("Club deactivated.", "success")
        return redirect(url_for("superadmin_dashboard"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, port=5210)
