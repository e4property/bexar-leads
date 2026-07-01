"""
prefore_backfill.py v2
Tags existing APPT (Appointment of Substitute Trustee) leads from last 30 days
with 'bexar_prefore' in GHL so they enter the Pre-Fore outreach sequence.

Run manually: python scraper/prefore_backfill.py
Needs: GHL_API_KEY, GHL_LOCATION_ID env vars (same as scraper)
"""
import json, os, time, logging
from datetime import datetime, timedelta
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
RECORDS_PATH    = "dashboard/records.json"
CUTOFF_DAYS     = 30
TAG             = "bexar_prefore"

HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Content-Type":  "application/json",
    "Version":       "2021-07-28",
}

def load_records():
    with open(RECORDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def within_cutoff(date_str, days=CUTOFF_DAYS):
    if not date_str:
        return False
    cutoff = datetime.now() - timedelta(days=days)
    try:
        p = date_str.strip().split("/")
        if len(p) == 3:
            dt = datetime(int(p[2]), int(p[0]), int(p[1]))
        elif len(p) == 2:
            dt = datetime(int(p[1]), int(p[0]), 1)
        else:
            return False
        return dt >= cutoff
    except Exception:
        return False

def find_contact(phone=None, name=None):
    try:
        params = {"locationId": GHL_LOCATION_ID}
        if phone:
            params["phone"] = phone
        elif name:
            params["name"] = name
        else:
            return None
        r = requests.get(
            "https://services.leadconnectorhq.com/contacts/",
            headers=HEADERS, params=params, timeout=15
        )
        contacts = r.json().get("contacts", [])
        return contacts[0].get("id") if contacts else None
    except Exception as e:
        log.warning(f"  GHL search error: {e}")
        return None

def add_tag(contact_id):
    try:
        r = requests.post(
            f"https://services.leadconnectorhq.com/contacts/{contact_id}/tags/",
            headers=HEADERS,
            json={"tags": [TAG]},
            timeout=15
        )
        return r.status_code in (200, 201)
    except Exception as e:
        log.warning(f"  Tag error: {e}")
        return False

def main():
    if not GHL_API_KEY:
        log.error("GHL_API_KEY not set")
        return

    records = load_records()
    log.info(f"Loaded {len(records)} records")

    candidates = [
        r for r in records
        if r.get("type") == "APPT"
        and within_cutoff(r.get("date_filed") or r.get("opened_date"))
        and (r.get("dash_phone") or r.get("ghl_id"))
    ]

    log.info(f"Found {len(candidates)} APPT leads (last {CUTOFF_DAYS}d) with GHL data")

    tagged = skipped = errors = 0

    for rec in candidates:
        owner  = rec.get("owner", "")
        addr   = rec.get("address", "")
        phone  = rec.get("dash_phone", "")
        ghl_id = rec.get("ghl_id", "")

        log.info(f"  {owner} | {addr} | phone={phone}")

        cid = ghl_id or find_contact(phone=phone) or find_contact(name=owner)

        if not cid:
            log.info(f"    No GHL contact — skipping")
            skipped += 1
            continue

        if add_tag(cid):
            log.info(f"    ✓ Tagged {cid} → {TAG}")
            tagged += 1
        else:
            log.warning(f"    ✗ Failed to tag {cid}")
            errors += 1

        time.sleep(0.4)

    log.info(f"\nDone: {tagged} tagged | {skipped} no GHL contact | {errors} errors")

if __name__ == "__main__":
    main()
