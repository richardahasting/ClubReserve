"""
FleetNests Marketing Site Blueprint.

Served only when g.is_marketing is True (bare root domain — fleetnests.com).
Handles landing page, pricing, trial signup, and Stripe/PayPal order flow.
"""

import json
import logging
import os
from datetime import date

import requests
from flask import (
    Blueprint, abort, g, jsonify, redirect, render_template,
    request, session, url_for,
)

import email_notify
import master_db

log = logging.getLogger(__name__)

bp = Blueprint("marketing", __name__, template_folder="templates/marketing")

# ---------------------------------------------------------------------------
# Pricing constants (cents)
# ---------------------------------------------------------------------------
TIER_PRICES = {
    "path":      3500,   # $35.00/yr — fleetnests.com/clubname
    "subdomain": 4500,   # $45.00/yr — clubname.fleetnests.com
    "custom":    5500,   # $55.00/yr — clubname.com (+ domain separately)
}
EXTRA_CRAFT_CENTS = 1250   # $12.50/yr each beyond first
EARLY_BIRD_PCT    = 40     # 40% off first year
EARLY_BIRD_DEADLINE = date.fromisoformat(
    os.environ.get("EARLY_BIRD_DEADLINE", "2026-07-04")
)


def _is_early_bird() -> bool:
    return date.today() <= EARLY_BIRD_DEADLINE


def _calc_price(tier: str, craft_count: int, early_bird: bool) -> int:
    """Return total in cents for first-year subscription."""
    base  = TIER_PRICES.get(tier, TIER_PRICES["path"])
    extra = max(0, craft_count - 1) * EXTRA_CRAFT_CENTS
    total = base + extra
    if early_bird:
        total = round(total * (1 - EARLY_BIRD_PCT / 100))
    return total


# ---------------------------------------------------------------------------
# Guard — only serve if this is a marketing request
# ---------------------------------------------------------------------------

@bp.before_request
def require_marketing():
    if not getattr(g, "is_marketing", False):
        abort(404)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@bp.route("/")
def index():
    early_bird = _is_early_bird()
    return render_template(
        "index.html",
        early_bird=early_bird,
        early_bird_deadline=EARLY_BIRD_DEADLINE.strftime("%B %-d, %Y"),
        tier_prices=TIER_PRICES,
        extra_craft_cents=EXTRA_CRAFT_CENTS,
        early_bird_pct=EARLY_BIRD_PCT,
    )


@bp.route("/pricing")
def pricing():
    early_bird = _is_early_bird()
    return render_template(
        "pricing.html",
        early_bird=early_bird,
        early_bird_deadline=EARLY_BIRD_DEADLINE.strftime("%B %-d, %Y"),
        tier_prices=TIER_PRICES,
        extra_craft_cents=EXTRA_CRAFT_CENTS,
        early_bird_pct=EARLY_BIRD_PCT,
    )


@bp.route("/order")
def order_form():
    early_bird = _is_early_bird()
    tier       = request.args.get("tier", "subdomain")
    craft      = max(1, int(request.args.get("craft", 1)))
    amount     = _calc_price(tier, craft, early_bird)
    return render_template(
        "order.html",
        early_bird=early_bird,
        early_bird_deadline=EARLY_BIRD_DEADLINE.strftime("%B %-d, %Y"),
        tier_prices=TIER_PRICES,
        extra_craft_cents=EXTRA_CRAFT_CENTS,
        early_bird_pct=EARLY_BIRD_PCT,
        selected_tier=tier,
        selected_craft=craft,
        initial_amount=amount,
        stripe_public_key=os.environ.get("STRIPE_PUBLIC_KEY", ""),
        paypal_client_id=os.environ.get("PAYPAL_CLIENT_ID", ""),
    )


@bp.route("/order/stripe", methods=["POST"])
def stripe_checkout():
    """Create a Stripe Checkout Session and redirect the user to it."""
    try:
        import stripe
        stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    except ImportError:
        log.error("stripe package not installed")
        abort(503)

    tier        = request.form.get("tier", "subdomain")
    craft_count = max(1, int(request.form.get("craft_count", 1)))
    club_name   = request.form.get("club_name", "").strip()
    contact     = request.form.get("contact_name", "").strip()
    email       = request.form.get("contact_email", "").strip()
    domain      = request.form.get("custom_domain", "").strip() or None
    notes       = request.form.get("notes", "").strip() or None
    early_bird  = _is_early_bird()
    amount      = _calc_price(tier, craft_count, early_bird)

    if not all([club_name, contact, email]):
        return redirect(url_for("marketing.order_form", tier=tier, craft=craft_count))

    # Save pending order so webhook can update it
    order_id = master_db.create_order(
        club_name=club_name, contact_name=contact, contact_email=email,
        tier=tier, craft_count=craft_count, amount_cents=amount,
        early_bird=early_bird, is_trial=False,
        custom_domain=domain, notes=notes,
    )

    tier_labels = {"path": "Shared Path", "subdomain": "Subdomain", "custom": "Custom Domain"}
    description = (
        f"FleetNests {tier_labels.get(tier, tier)} plan — {craft_count} craft"
        + (" (Early Bird 40% off)" if early_bird else "")
    )

    app_url = os.environ.get("APP_URL", "https://fleetnests.com")
    session_obj = stripe.checkout.Session.create(
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "product_data": {"name": "FleetNests Annual Subscription", "description": description},
                "unit_amount": amount,
            },
            "quantity": 1,
        }],
        mode="payment",
        customer_email=email,
        success_url=f"{app_url}/thanks?session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{app_url}/order?tier={tier}&craft={craft_count}",
        metadata={
            "order_id":   str(order_id),
            "club_name":  club_name,
            "tier":       tier,
            "craft_count": str(craft_count),
        },
    )

    # Store order_id in Flask session so /thanks can look it up if webhook is slow
    session["pending_order_id"] = order_id

    return redirect(session_obj.url, code=303)


@bp.route("/order/paypal/create", methods=["POST"])
def paypal_create():
    """Create a PayPal order and return its ID to the JS SDK."""
    data = request.get_json() or {}
    tier        = data.get("tier", "subdomain")
    craft_count = max(1, int(data.get("craft_count", 1)))
    early_bird  = _is_early_bird()
    amount      = _calc_price(tier, craft_count, early_bird)
    amount_str  = f"{amount / 100:.2f}"

    token = _paypal_access_token()
    if not token:
        return jsonify({"error": "PayPal unavailable"}), 503

    mode    = os.environ.get("PAYPAL_MODE", "sandbox")
    base    = "https://api-m.sandbox.paypal.com" if mode == "sandbox" else "https://api-m.paypal.com"
    resp    = requests.post(
        f"{base}/v2/checkout/orders",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "intent": "CAPTURE",
            "purchase_units": [{
                "amount": {"currency_code": "USD", "value": amount_str},
                "description": f"FleetNests {tier} plan — {craft_count} craft",
            }],
        },
        timeout=10,
    )
    if not resp.ok:
        log.error("PayPal create order failed: %s", resp.text)
        return jsonify({"error": "PayPal order creation failed"}), 502

    paypal_order_id = resp.json().get("id")
    # Stash enough info in session so /capture can persist the order
    session["paypal_pending"] = {
        "tier": tier, "craft_count": craft_count,
        "amount_cents": amount, "early_bird": early_bird,
        "paypal_order_id": paypal_order_id,
    }
    return jsonify({"id": paypal_order_id})


@bp.route("/order/paypal/capture", methods=["POST"])
def paypal_capture():
    """Capture the PayPal order, save to DB, and email admin."""
    data = request.get_json() or {}
    paypal_order_id = data.get("orderID")
    club_name       = data.get("club_name", "").strip()
    contact         = data.get("contact_name", "").strip()
    email           = data.get("contact_email", "").strip()
    domain          = data.get("custom_domain", "").strip() or None
    notes           = data.get("notes", "").strip() or None

    pending = session.get("paypal_pending", {})
    tier        = pending.get("tier", "subdomain")
    craft_count = pending.get("craft_count", 1)
    amount      = pending.get("amount_cents", 0)
    early_bird  = pending.get("early_bird", False)

    token = _paypal_access_token()
    if not token:
        return jsonify({"error": "PayPal unavailable"}), 503

    mode  = os.environ.get("PAYPAL_MODE", "sandbox")
    base  = "https://api-m.sandbox.paypal.com" if mode == "sandbox" else "https://api-m.paypal.com"
    resp  = requests.post(
        f"{base}/v2/checkout/orders/{paypal_order_id}/capture",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=10,
    )
    if not resp.ok:
        log.error("PayPal capture failed: %s", resp.text)
        return jsonify({"error": "PayPal capture failed"}), 502

    order_id = master_db.create_order(
        club_name=club_name, contact_name=contact, contact_email=email,
        tier=tier, craft_count=craft_count, amount_cents=amount,
        early_bird=early_bird, is_trial=False,
        custom_domain=domain, notes=notes,
    )
    master_db.update_order_payment(order_id, "paypal", paypal_order_id, "paid")
    session["confirmed_order_id"] = order_id
    session.pop("paypal_pending", None)

    _send_order_emails(club_name, contact, email, tier, craft_count, amount, early_bird)

    return jsonify({"redirect": url_for("marketing.thanks", _external=False) + "?paid=1"})


@bp.route("/webhooks/stripe", methods=["POST"])
def stripe_webhook():
    """Handle Stripe webhook events (mark order paid, email admin)."""
    try:
        import stripe
        stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    except ImportError:
        abort(503)

    payload = request.get_data()
    sig     = request.headers.get("Stripe-Signature", "")
    secret  = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig, secret)
    except Exception as exc:
        log.warning("Stripe webhook signature error: %s", exc)
        return jsonify({"error": "invalid signature"}), 400

    if event["type"] == "checkout.session.completed":
        sess     = event["data"]["object"]
        meta     = sess.get("metadata", {})
        order_id = int(meta.get("order_id", 0))
        if order_id:
            master_db.update_order_payment(order_id, "stripe", sess["id"], "paid")
            order = master_db.get_order(order_id)
            if order:
                _send_order_emails(
                    order["club_name"], order["contact_name"], order["contact_email"],
                    order["tier"], order["craft_count"], order["amount_cents"],
                    order["early_bird"],
                )

    return jsonify({"status": "ok"})


@bp.route("/thanks")
def thanks():
    paid  = request.args.get("paid") or request.args.get("session_id")
    trial = request.args.get("trial")
    order_id = session.get("confirmed_order_id") or session.get("pending_order_id")
    order = master_db.get_order(order_id) if order_id else None
    return render_template("thanks.html", paid=bool(paid), trial=bool(trial), order=order)


@bp.route("/trial", methods=["GET", "POST"])
def trial():
    early_bird = _is_early_bird()
    if request.method == "POST":
        club_name   = request.form.get("club_name", "").strip()
        contact     = request.form.get("contact_name", "").strip()
        email       = request.form.get("contact_email", "").strip()
        craft_count = max(1, int(request.form.get("craft_count", 1)))
        craft_type  = request.form.get("craft_type", "boat")
        notes       = request.form.get("notes", "").strip() or None

        if not all([club_name, contact, email]):
            return render_template("trial.html", early_bird=early_bird,
                                   early_bird_deadline=EARLY_BIRD_DEADLINE.strftime("%B %-d, %Y"),
                                   error="Please fill in all required fields.")

        order_id = master_db.create_order(
            club_name=club_name, contact_name=contact, contact_email=email,
            tier="path", craft_count=craft_count, amount_cents=0,
            early_bird=False, is_trial=True, notes=notes,
        )
        _send_trial_emails(club_name, contact, email, craft_count, craft_type)
        session["confirmed_order_id"] = order_id
        return redirect(url_for("marketing.thanks") + "?trial=1")

    return render_template(
        "trial.html",
        early_bird=early_bird,
        early_bird_deadline=EARLY_BIRD_DEADLINE.strftime("%B %-d, %Y"),
    )


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def _admin_email() -> str:
    return os.environ.get("MARKETING_ADMIN_EMAIL", "admin@fleetnests.com")


TIER_LABELS = {"path": "Shared Path (fleetnests.com/clubname)",
               "subdomain": "Subdomain (clubname.fleetnests.com)",
               "custom": "Custom Domain (clubname.com)"}


def _send_order_emails(club_name, contact, email, tier, craft_count, amount_cents, early_bird):
    amt = f"${amount_cents / 100:.2f}"
    eb  = " (Early Bird 40% off)" if early_bird else ""
    tier_label = TIER_LABELS.get(tier, tier)

    email_notify.send_email(
        _admin_email(),
        f"[FleetNests] New order: {club_name} — {amt}",
        f"A new FleetNests subscription order has been paid.\n\n"
        f"  Club name:    {club_name}\n"
        f"  Contact:      {contact} <{email}>\n"
        f"  Plan:         {tier_label}{eb}\n"
        f"  Craft count:  {craft_count}\n"
        f"  Amount paid:  {amt}\n\n"
        f"Next step: provision their club via the super-admin panel, then email them their login URL.\n"
        f"  https://fleetnests.com/superadmin\n",
    )

    app_url = os.environ.get("APP_URL", "https://fleetnests.com")
    email_notify.send_email(
        email,
        "Welcome to FleetNests — your subscription is confirmed!",
        f"Hi {contact},\n\n"
        f"Thank you for subscribing to FleetNests! Your payment has been received.\n\n"
        f"  Club:    {club_name}\n"
        f"  Plan:    {tier_label}\n"
        f"  Amount:  {amt}\n\n"
        f"We're setting up your club now and will email you your login URL within one business day.\n\n"
        f"In the meantime, you can explore the demo sites:\n"
        f"  Flying club: {app_url}/sample1/\n"
        f"  Boat club:   {app_url}/sample2/\n\n"
        f"Questions? Just reply to this email.\n\n"
        f"— The FleetNests Team",
    )


def _send_trial_emails(club_name, contact, email, craft_count, craft_type):
    app_url = os.environ.get("APP_URL", "https://fleetnests.com")

    email_notify.send_email(
        _admin_email(),
        f"[FleetNests] New trial request: {club_name}",
        f"A new trial signup has been submitted.\n\n"
        f"  Club name:    {club_name}\n"
        f"  Contact:      {contact} <{email}>\n"
        f"  Craft count:  {craft_count}\n"
        f"  Craft type:   {craft_type}\n\n"
        f"Next step: provision their trial club via the super-admin panel:\n"
        f"  {app_url}/superadmin\n",
    )

    email_notify.send_email(
        email,
        "Your FleetNests free trial is on its way!",
        f"Hi {contact},\n\n"
        f"Thank you for signing up for a free FleetNests trial!\n\n"
        f"We'll have your club set up at fleetnests.com/{club_name.lower().replace(' ', '-')} "
        f"within one business day and will email you your login details.\n\n"
        f"Your free trial runs for 30 days with no credit card required. "
        f"You can upgrade to a paid plan at any time.\n\n"
        f"While you wait, check out the live demo sites:\n"
        f"  Flying club: {app_url}/sample1/\n"
        f"  Boat club:   {app_url}/sample2/\n\n"
        f"Questions? Just reply to this email.\n\n"
        f"— The FleetNests Team",
    )


# ---------------------------------------------------------------------------
# PayPal helper
# ---------------------------------------------------------------------------

def _paypal_access_token() -> str | None:
    """Fetch a short-lived PayPal access token via client credentials."""
    client_id     = os.environ.get("PAYPAL_CLIENT_ID", "")
    client_secret = os.environ.get("PAYPAL_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        log.warning("PayPal credentials not configured")
        return None

    mode = os.environ.get("PAYPAL_MODE", "sandbox")
    base = "https://api-m.sandbox.paypal.com" if mode == "sandbox" else "https://api-m.paypal.com"
    try:
        resp = requests.post(
            f"{base}/v1/oauth2/token",
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            timeout=10,
        )
        if resp.ok:
            return resp.json().get("access_token")
    except Exception as exc:
        log.error("PayPal token error: %s", exc)
    return None
