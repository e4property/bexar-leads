"""
Speed-to-lead email alert — foreclosure (NOF/TAX) leads only.

Runs after fetch.py. Reads dashboard/records.json, finds records that are
both is_new=True and type in (NOF, TAX), and emails a summary if any exist.
APPT/VBP/CE/LP leads are intentionally excluded — foreclosure-only per
2026-08-05 request, so notification volume doesn't dilute the highest-
urgency lead type.

Requires GMAIL_USER + GMAIL_APP_PASSWORD env vars (Gmail SMTP with an
App Password, not the account password — set one at
myaccount.google.com/apppasswords). NOTIFY_TO is optional, defaults to
GMAIL_USER (self-notification).
"""
import json
import os
import smtplib
from email.mime.text import MIMEText

RECORDS_PATH = os.environ.get("RECORDS_PATH", "dashboard/records.json")
DASHBOARD_URL = "https://e4property.github.io/bexar-leads/"


def main():
    with open(RECORDS_PATH, encoding="utf-8") as f:
        records = json.load(f)

    new_foreclosure = [
        r for r in records
        if r.get("is_new") and r.get("type") in ("NOF", "TAX")
    ]

    if not new_foreclosure:
        print("No new foreclosure leads this run — no email sent.")
        return

    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD")
    notify_to = os.environ.get("NOTIFY_TO", gmail_user)

    if not gmail_user or not gmail_pass:
        print("GMAIL_USER / GMAIL_APP_PASSWORD not set — skipping email, "
              f"but found {len(new_foreclosure)} new foreclosure lead(s).")
        return

    lines = [f"{len(new_foreclosure)} new foreclosure lead(s) just scraped:\n"]
    for r in new_foreclosure:
        addr = r.get("address") or "—"
        owner = r.get("owner") or "—"
        ltype = "Tax Foreclosure" if r.get("type") == "TAX" else "Mortgage Foreclosure"
        score = r.get("score", "—")
        lines.append(f"- {addr} | {owner} | {ltype} | score {score}")

    lines.append(f"\nDashboard: {DASHBOARD_URL}")
    body = "\n".join(lines)

    msg = MIMEText(body)
    msg["Subject"] = f"[Bexar] {len(new_foreclosure)} new foreclosure lead(s)"
    msg["From"] = gmail_user
    msg["To"] = notify_to

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(gmail_user, gmail_pass)
        server.sendmail(gmail_user, [notify_to], msg.as_string())

    print(f"Sent email alert for {len(new_foreclosure)} new foreclosure lead(s).")


if __name__ == "__main__":
    main()
