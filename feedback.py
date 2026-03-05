"""
AI-routed member feedback for FleetNests.

process_feedback(user, text, image_bytes, image_content_type) -> (bool, str)

Routes via the `claude` CLI (no ANTHROPIC_API_KEY needed in the app):
- Bugs / feature requests  → GitHub issue
- Everything else          → email to FEEDBACK_EMAIL
- claude CLI not found     → fallback plain email, no crash
- Any error                → fallback plain email, no crash
"""

import json
import logging
import mimetypes
import os
import subprocess
import uuid
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "static", "feedback_uploads")

_PROMPT = """\
You are the feedback assistant for FleetNests, a reservation platform.
A logged-in member has submitted feedback through the app. Triage it and decide
where to route it.

Rules:
- Route to github_issue for: bugs, broken features, UI glitches, error messages,
  performance problems, or feature requests / suggestions to improve the app.
- Route to email for: general comments, questions, praise, thanks, concerns about
  club policy, or anything that is not a software bug or feature request.
- Be concise but include all relevant context in the title/body.
- Always attribute the feedback to the member by name in the body.
- If a screenshot is referenced, note what it shows.

Output ONLY a single JSON object — no other text, no markdown fences.

For bugs / feature requests:
{"action":"github_issue","title":"<short title>","body":"<full body>","labels":["bug"|"enhancement"]}

For everything else:
{"action":"email","subject":"<subject>","body":"<full body>"}
"""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def process_feedback(
    user: dict,
    text: str,
    file_bytes: bytes | None,
    file_content_type: str | None,
    original_filename: str | None = None,
) -> tuple[bool, str, str | None, str | None]:
    """Triage and route member feedback via the claude CLI.

    Returns (success, action, saved_path, github_issue_url).
    saved_path is the relative path under static/ if a file was saved, else None.
    """
    saved_path = None
    saved_name = None

    # Persist attachment before calling claude so path is stable
    if file_bytes and file_content_type:
        ext = mimetypes.guess_extension(file_content_type.split(";")[0].strip()) or ""
        # guess_extension returns odd things for common types; fix them
        ext = {".jpe": ".jpg", ".jpeg": ".jpg"}.get(ext, ext)
        unique_name = f"{uuid.uuid4().hex}{ext}"
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        full_path = os.path.join(UPLOAD_DIR, unique_name)
        with open(full_path, "wb") as f:
            f.write(file_bytes)
        saved_path = f"feedback_uploads/{unique_name}"
        saved_name = original_filename or unique_name
        log.info("Attachment saved: %s (%s)", saved_path, file_content_type)

    try:
        result = _call_claude_cli(user, text, saved_path, file_content_type)
    except Exception as exc:
        log.error("claude CLI error in process_feedback: %s", exc)
        ok, action = _fallback_email(user, text)
        return ok, action, saved_path, None

    action = result.get("action")
    github_url = None

    if action == "github_issue":
        github_url, ok = _create_github_issue(result["title"], result["body"],
                                               result.get("labels", []), saved_path, user)
        if ok:
            return True, "github_issue", saved_path, github_url
        log.warning("GitHub issue creation failed; falling back to email for: %s", result["title"])
        ok = _send_email(f"[Feature/Bug] {result['title']}", result["body"], saved_path)
        return ok, "fallback_email", saved_path, None

    if action == "email":
        ok = _send_email(result["subject"], result["body"], saved_path)
        return ok, "email", saved_path, None

    log.warning("Unexpected action from claude CLI: %s; falling back to email", action)
    ok, action = _fallback_email(user, text)
    return ok, action, saved_path, None


# ---------------------------------------------------------------------------
# claude CLI call
# ---------------------------------------------------------------------------

def _call_claude_cli(
    user: dict,
    text: str,
    saved_path: str | None,
    file_content_type: str | None,
) -> dict:
    """Invoke the claude CLI and return parsed JSON routing decision."""
    message = (
        f"Member: {user['full_name']} ({user.get('email', 'unknown')})\n\n"
        f"Feedback:\n{text}"
    )
    # Pass full filesystem path of the saved attachment if it's an image claude can read
    is_image = file_content_type and file_content_type.split("/")[0] == "image"
    if saved_path and is_image:
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "static", saved_path)
        message += f"\n\nAttachment (image): {full_path}"
    elif saved_path:
        message += f"\n\nAttachment (file): {saved_path}"

    claude_bin = os.environ.get("CLAUDE_BIN", "/home/richard/.local/bin/claude")
    # Remove CLAUDECODE so nested-session detection doesn't block us
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    result = subprocess.run(
        [claude_bin, "-p", _PROMPT],
        input=message,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    if result.returncode != 0:
        raise RuntimeError(f"claude exited {result.returncode}: {result.stderr.strip()}")

    output = result.stdout.strip()
    # Strip any accidental markdown fences
    if output.startswith("```"):
        output = output.split("```")[1]
        if output.startswith("json"):
            output = output[4:]
    return json.loads(output)


# ---------------------------------------------------------------------------
# GitHub issue creation (stdlib urllib only)
# ---------------------------------------------------------------------------

def _create_github_issue(title: str, body: str, labels: list,
                          saved_path: str | None,
                          user: dict | None = None) -> tuple[str | None, bool]:
    """Returns (issue_url, success)."""
    token = os.environ.get("GITHUB_TOKEN")
    repo  = os.environ.get("GITHUB_REPO", "richardahasting/fleetnests")
    app_url = os.environ.get("APP_URL", "https://fleetnests.com")
    if not token:
        log.error("GITHUB_TOKEN not set; cannot create GitHub issue: %s", title)
        return None, False

    full_body = body
    if saved_path:
        full_body += f"\n\n**Attachment:** {app_url}/static/{saved_path}"
    if user:
        name  = user.get("full_name") or "Unknown"
        email = user.get("email") or "no email on file"
        full_body += f"\n\n---\n**Submitted by:** {name} <{email}>"

    payload = json.dumps({"title": title, "body": full_body, "labels": labels}).encode()
    url = f"https://api.github.com/repos/{repo}/issues"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "Content-Type":         "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status in (200, 201):
                data = json.loads(resp.read())
                return data.get("html_url"), True
            return None, False
    except urllib.error.HTTPError as exc:
        log.error("GitHub API HTTP %s: %s", exc.code, exc.read())
        return None, False
    except Exception as exc:
        log.error("GitHub API error: %s", exc)
        return None, False


# ---------------------------------------------------------------------------
# Email sending (delegates to email_notify)
# ---------------------------------------------------------------------------

def _send_email(subject: str, body: str, saved_path: str | None = None) -> bool:
    import email_notify
    to_addr = os.environ.get("FEEDBACK_EMAIL", "")
    app_url = os.environ.get("APP_URL", "https://fleetnests.com")
    full_body = body
    if saved_path:
        full_body += f"\n\nAttachment: {app_url}/static/{saved_path}"
    return email_notify.send_email(to_addr, subject, full_body)


def _fallback_email(user: dict, text: str) -> tuple[bool, str]:
    """Send a plain email when the claude CLI is unavailable."""
    subject = f"Member Feedback from {user['full_name']}"
    body = (
        f"Feedback submitted by {user['full_name']} ({user.get('email', 'unknown')})\n\n"
        f"{text}"
    )
    ok = _send_email(subject, body)
    return ok, "fallback_email"
