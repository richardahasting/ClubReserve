"""
Email notifications for FleetNests.
Uses local Postfix (localhost:25) — no SMTP auth needed on same server.
Activate by setting EMAIL_ENABLED=true in .env.

All notify_* functions are vehicle-type aware via _get_club_context().
"""
import os
import smtplib
from email.mime.text import MIMEText

EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
SMTP_HOST     = os.environ.get("SMTP_HOST", "localhost")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "25"))


def _get_club_context() -> dict:
    """
    Build email context from current Flask request's club (g.club).
    Falls back to env vars for dev / single-club use.
    """
    try:
        from flask import g
        club = getattr(g, "club", None)
        if club:
            vehicle_type = club.get("vehicle_type", "boat")
            club_name    = club.get("name", "Club")
            app_url      = os.environ.get("APP_URL", "https://fleetnests.com")
            email_from   = os.environ.get("EMAIL_FROM",
                                          f"noreply@{club.get('subdomain', 'club')}.fleetnests.com")
            return {
                "club_name":     club_name,
                "vehicle_type":  vehicle_type,
                "vehicle_noun":  "boat" if vehicle_type == "boat" else "aircraft",
                "checklist_name": "Captain's Checklist" if vehicle_type == "boat" else "Pre-Flight Checklist",
                "vehicle_label":  "boat" if vehicle_type == "boat" else "aircraft",
                "app_url":       app_url,
                "email_from":    email_from,
                "signature":     f"— {club_name}",
            }
    except RuntimeError:
        pass

    # Fallback (no request context)
    return {
        "club_name":      "Club",
        "vehicle_type":   "boat",
        "vehicle_noun":   "boat",
        "checklist_name": "Captain's Checklist",
        "vehicle_label":  "boat",
        "app_url":        os.environ.get("APP_URL", "https://fleetnests.com"),
        "email_from":     os.environ.get("EMAIL_FROM", "noreply@fleetnests.com"),
        "signature":      "— FleetNests",
    }


def send_email(to_addr: str, subject: str, body_text: str) -> bool:
    """Send a plain-text email via local Postfix. Returns True on success."""
    if not EMAIL_ENABLED or not to_addr:
        return False
    ctx = _get_club_context()
    try:
        msg = MIMEText(body_text, "plain")
        msg["Subject"] = subject
        msg["From"]    = ctx["email_from"]
        msg["To"]      = to_addr
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=5) as smtp:
            smtp.sendmail(ctx["email_from"], [to_addr], msg.as_string())
        return True
    except Exception:
        return False


def notify_reservation_confirmed(user: dict, res: dict) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    d = res["date"].strftime("%A, %B %d, %Y")
    s = res["start_time"].strftime("%-I:%M %p") if res.get("start_time") else ""
    e = res["end_time"].strftime("%-I:%M %p")   if res.get("end_time")   else ""
    return send_email(
        user["email"],
        f"{ctx['vehicle_noun'].title()} Reservation Confirmed — {d}",
        f"Hi {user['full_name']},\n\n"
        f"Your {ctx['vehicle_noun']} reservation is confirmed:\n\n"
        f"  Date: {d}\n"
        f"  Time: {s} – {e} CT\n\n"
        f"Before you head out, review the {ctx['checklist_name']}:\n"
        f"  {ctx['app_url']}/checklist\n\n"
        f"Manage your trips at: {ctx['app_url']}\n\n"
        f"{ctx['signature']}",
    )


def notify_reservation_cancelled(user: dict, res: dict) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    d = res["date"].strftime("%A, %B %d, %Y")
    return send_email(
        user["email"],
        f"{ctx['vehicle_noun'].title()} Reservation Cancelled — {d}",
        f"Hi {user['full_name']},\n\n"
        f"Your reservation for {d} has been cancelled.\n\n"
        f"Book again at: {ctx['app_url']}\n\n"
        f"{ctx['signature']}",
    )


def notify_reservation_approved(user: dict, res: dict) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    d = res["date"].strftime("%A, %B %d, %Y")
    s = res["start_time"].strftime("%-I:%M %p") if res.get("start_time") else ""
    e = res["end_time"].strftime("%-I:%M %p")   if res.get("end_time")   else ""
    return send_email(
        user["email"],
        f"{ctx['vehicle_noun'].title()} Reservation Approved — {d}",
        f"Hi {user['full_name']},\n\n"
        f"Your reservation request has been approved!\n\n"
        f"  Date: {d}\n"
        f"  Time: {s} – {e} CT\n\n"
        f"Before you head out, review the {ctx['checklist_name']}:\n"
        f"  {ctx['app_url']}/checklist\n\n"
        f"View it at: {ctx['app_url']}\n\n"
        f"{ctx['signature']}",
    )


def notify_approval_needed(admins: list, user: dict, res: dict):
    """Email all admins that a reservation needs approval."""
    ctx = _get_club_context()
    d = res["date"].strftime("%A, %B %d, %Y")
    for admin in admins:
        if admin.get("email"):
            send_email(
                admin["email"],
                f"Approval Needed — {user['full_name']}",
                f"A reservation request is waiting for approval:\n\n"
                f"  Member: {user['full_name']} ({user['username']})\n"
                f"  Date:   {d}\n\n"
                f"Review at: {ctx['app_url']}/admin/approvals\n\n"
                f"{ctx['signature']}",
            )


def notify_email_verify(user: dict, new_email: str, token: str) -> bool:
    ctx = _get_club_context()
    return send_email(
        new_email,
        f"Verify your new email — {ctx['club_name']}",
        f"Hi {user['full_name']},\n\n"
        f"You requested to change your {ctx['club_name']} email address to this one.\n\n"
        f"Click the link below to confirm (expires in 24 hours):\n\n"
        f"  {ctx['app_url']}/verify-email/{token}\n\n"
        f"If you did not request this, ignore this email. Your current address is unchanged.\n\n"
        f"{ctx['signature']}",
    )


def notify_welcome(user: dict, token: str) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    return send_email(
        user["email"],
        f"Welcome to {ctx['club_name']} — set your password",
        f"Hi {user['full_name']},\n\n"
        f"Your {ctx['club_name']} account has been created.\n\n"
        f"Click the link below to set your password and get started (expires in 72 hours):\n\n"
        f"  {ctx['app_url']}/set-password/{token}\n\n"
        f"If you have any questions, contact your club administrator.\n\n"
        f"{ctx['signature']}",
    )


def notify_password_reset(user: dict, token: str) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    return send_email(
        user["email"],
        f"Reset your {ctx['club_name']} password",
        f"Hi {user['full_name']},\n\n"
        f"We received a request to reset your password.\n\n"
        f"Click the link below to choose a new password (expires in 72 hours):\n\n"
        f"  {ctx['app_url']}/set-password/{token}\n\n"
        f"If you did not request this, you can safely ignore this email — your password has not changed.\n\n"
        f"{ctx['signature']}",
    )


def notify_weather_alert(user: dict, res_date, alerts: list) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    d = res_date.strftime("%A, %B %d, %Y")
    alert_lines = []
    for a in alerts:
        alert_lines.append(f"  ⚠ {a['event']} ({a['severity']})")
        alert_lines.append(f"     {a['headline']}")
        if a.get("instruction"):
            alert_lines.append(f"     {a['instruction'].splitlines()[0]}")
    alert_text = "\n".join(alert_lines)
    return send_email(
        user["email"],
        f"⚠ Weather Alert — Your {ctx['vehicle_noun'].title()} Reservation on {d}",
        f"Hi {user['full_name']},\n\n"
        f"There are active alerts for your area on the date of your reservation ({d}):\n\n"
        f"{alert_text}\n\n"
        f"Please review the conditions and decide whether to keep or cancel your reservation.\n\n"
        f"  Manage your trips: {ctx['app_url']}/reservations\n\n"
        f"Stay safe.\n\n"
        f"{ctx['signature']}",
    )


def notify_trip_reminder(user: dict, res: dict) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    d = res["date"].strftime("%A, %B %d, %Y")
    s = res["start_time"].strftime("%-I:%M %p") if res.get("start_time") else ""
    e = res["end_time"].strftime("%-I:%M %p")   if res.get("end_time")   else ""
    return send_email(
        user["email"],
        f"Reminder — {ctx['vehicle_noun'].title()} Reservation Tomorrow ({d})",
        f"Hi {user['full_name']},\n\n"
        f"Just a reminder that you have a {ctx['vehicle_noun']} reservation tomorrow:\n\n"
        f"  Date: {d}\n"
        f"  Time: {s} – {e} CT\n\n"
        f"Before you head out, review the {ctx['checklist_name']}:\n"
        f"  {ctx['app_url']}/checklist\n\n"
        f"On the day of your trip, use the Check Out button on your reservations\n"
        f"page to complete the pre-departure checklist:\n"
        f"  {ctx['app_url']}/my-reservations\n\n"
        f"Stay safe and have a great trip!\n\n"
        f"{ctx['signature']}",
    )


def notify_waitlist_available(user: dict, desired_date) -> bool:
    if not user.get("email"):
        return False
    ctx = _get_club_context()
    d = desired_date.strftime("%A, %B %d, %Y")
    return send_email(
        user["email"],
        f"{ctx['vehicle_noun'].title()} Available — {d}",
        f"Hi {user['full_name']},\n\n"
        f"Good news! The {ctx['vehicle_noun']} is now available on {d}.\n\n"
        f"Book now (before someone else does):\n"
        f"{ctx['app_url']}/reserve/{desired_date}\n\n"
        f"{ctx['signature']}",
    )


def notify_club_provisioned(contact_email: str, club_name: str,
                            short_name: str, token: str) -> bool:
    """
    Welcome email sent from provision_club() — no Flask g context available.
    Uses env vars directly for app_url and from address.
    """
    if not EMAIL_ENABLED or not contact_email:
        return False
    app_url   = os.environ.get("APP_URL", "https://fleetnests.com")
    from_addr = os.environ.get("EMAIL_FROM", "noreply@fleetnests.com")
    club_url  = f"{app_url}/{short_name}"
    body = (
        f"Hello,\n\n"
        f"Your FleetNests club '{club_name}' has been provisioned and is ready to use.\n\n"
        f"  Club URL:  {club_url}/\n"
        f"  Login:     {club_url}/login\n\n"
        f"Set your administrator password using the link below (expires in 72 hours):\n\n"
        f"  {club_url}/set-password/{token}\n\n"
        f"After setting your password, log in and customize your club — add vehicles,\n"
        f"invite members, and configure your settings.\n\n"
        f"— The FleetNests Team"
    )
    try:
        msg = MIMEText(body, "plain")
        msg["Subject"] = f"Welcome to FleetNests — set up {club_name}"
        msg["From"]    = from_addr
        msg["To"]      = contact_email
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=5) as smtp:
            smtp.sendmail(from_addr, [contact_email], msg.as_string())
        return True
    except Exception as exc:
        import logging as _log
        _log.getLogger(__name__).warning(
            "Failed to send provisioning welcome email for %s: %s", short_name, exc)
        return False


def notify_feedback_resolved(to_email: str, submitter_name: str,
                             issue_title: str, issue_url: str) -> bool:
    """Email a member when the GitHub issue they filed has been closed."""
    if not EMAIL_ENABLED or not to_email:
        return False
    ctx = _get_club_context()
    return send_email(
        to_email,
        "Your feedback has been resolved",
        f"Hi {submitter_name},\n\n"
        f"The issue you reported has been closed:\n\n"
        f"  {issue_title}\n"
        f"  {issue_url}\n\n"
        f"Thank you for helping us improve {ctx['club_name']}.\n\n"
        f"{ctx['signature']}",
    )


def notify_demo_lead(prospect_email: str, club_name: str, club_short_name: str,
                     ip_address: str = None) -> bool:
    """Notify richard@hastingtx.org that a prospect accessed a sample site."""
    if not EMAIL_ENABLED:
        return False
    notify_addr = os.environ.get("DEMO_NOTIFY_EMAIL", "richard@hastingtx.org")
    subject = f"[FleetNests] New prospect: {prospect_email} viewed {club_name}"
    body = (
        f"A prospect just accessed the {club_name} demo site on FleetNests.\n\n"
        f"  Email:   {prospect_email}\n"
        f"  Club:    {club_name} ({club_short_name})\n"
        f"  Site:    https://fleetnests.com/{club_short_name}/\n"
    )
    if ip_address:
        body += f"  IP:      {ip_address}\n"
    body += "\nAll demo leads are stored in the fleetnests_master.demo_leads table.\n"
    try:
        msg = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"]    = os.environ.get("EMAIL_FROM", "admin@fleetnests.com")
        msg["To"]      = notify_addr
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=5) as smtp:
            smtp.sendmail(msg["From"], [notify_addr], msg.as_string())
        return True
    except Exception:
        return False
