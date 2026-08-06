"""
Speed-to-lead SMS alert — foreclosure (NOF/TAX) leads only.

Runs after fetch.py. Reads dashboard/records.json, finds records that are
both is_new=True and type in (NOF, TAX), and texts a summary via GHL/Jarvis
if any exist. APPT/VBP/CE/LP leads are intentionally excluded --
foreclosure-only per 2026-08-05 request, so notification volume doesn't
dilute the highest-urgency lead type.

Requires GHL_API_KEY env var — a GHL Private Integration token with
contacts + conversations write scope (Jarvis: Settings > Private
Integrations > Create new integration). GHL_LOCATION_ID and TARGET_PHONE
have working defaults below but can be overridden via env vars.
"""
import json
import os
import requests

RECORDS_PATH = os.environ.get("RECORDS_PATH", "dashboard/records.json")
DASHBOARD_URL = "https://e4property.github.io/bexar-leads/"
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
TARGET_PHONE = os.environ.get("TARGET_PHONE", "+12109643444")
GHL_API_BASE = "https://services.leadconnectorhq.com"


def main():
    with open(RECORDS_PATH, encoding="utf-8") as f:
        records = json.load(f)

    new_foreclosure = [
        r for r in records
        if r.get("is_new") and r.get("type") in ("NOF", "TAX")
    ]

    if not new_foreclosure:
        print("No new foreclosure leads this run — no text sent.")
        return

    api_key = os.environ.get("GHL_API_KEY")
    if not api_key:
        print("GHL_API_KEY not set — skipping SMS, but found "
              f"{len(new_foreclosure)} new foreclosure lead(s).")
        return

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Version": "2021-07-28",
    }

    lines = [f"{len(new_foreclosure)} new foreclosure lead(s) just scraped:"]
    for r in new_foreclosure[:5]:
        addr = r.get("address") or "—"
        ltype = "TAX" if r.get("type") == "TAX" else "NOF"
        score = r.get("score", "—")
        lines.append(f"- {addr} ({ltype}, score {score})")
    if len(new_foreclosure) > 5:
        lines.append(f"...and {len(new_foreclosure) - 5} more.")
    lines.append(DASHBOARD_URL)
    message = "\n".join(lines)

    # Upsert a contact for the target phone so there's a contactId to message.
    contact_res = requests.post(
        f"{GHL_API_BASE}/contacts/",
        headers=headers,
        json={
            "phone": TARGET_PHONE,
            "firstName": "Xavier",
            "locationId": GHL_LOCATION_ID,
            "source": "bexar-leads-notify",
        },
        timeout=30,
    )
    contact_data = contact_res.json()
    contact_id = (contact_data.get("contact") or {}).get("id") or contact_data.get("id")
    if not contact_id:
        print(f"Could not resolve a contactId — contact API responded: {contact_data}")
        contact_res.raise_for_status()
        return

    sms_res = requests.post(
        f"{GHL_API_BASE}/conversations/messages",
        headers=headers,
        json={
            "type": "SMS",
            "contactId": contact_id,
            "message": message,
        },
        timeout=30,
    )
    if not sms_res.ok:
        print(f"SMS send failed: {sms_res.status_code} {sms_res.text}")
        sms_res.raise_for_status()

    print(f"Sent SMS alert for {len(new_foreclosure)} new foreclosure lead(s).")


if __name__ == "__main__":
    main()
