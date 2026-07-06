"""
purge_past_auctions.py
Removes leads where sale_date (auction date) has already passed.
Run manually via GitHub Actions workflow_dispatch — NOT part of regular scrape.

Also removes:
- NOF/TAX leads with no sale_date older than 180 days (auction passed unknown)
- Keeps: LP, VBP, CE, APPT, any lead with future sale_date, any with GHL activity
"""
import json, logging, re
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RECORDS_PATH = Path("dashboard/records.json")
TODAY = datetime.now()

def parse_date(s):
    if not s:
        return None
    s = s.strip()
    try:
        p = s.split("/")
        if len(p) == 3:
            return datetime(int(p[2]), int(p[0]), int(p[1]))
        if len(p) == 2:
            return datetime(int(p[1]), int(p[0]), 1)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(s[:10])
    except Exception:
        pass
    return None

def days_old(rec):
    dt = parse_date(rec.get("date_filed") or rec.get("opened_date"))
    if not dt:
        return 0
    return (TODAY - dt).days

def has_ghl_activity(rec):
    return bool(
        rec.get("dash_phone") or
        rec.get("ghl_id") or
        rec.get("ghl_pushed") or
        rec.get("dash_dispo") not in (None, "", "new") or
        rec.get("dash_notes")
    )

def should_purge(rec):
    lead_type = rec.get("type", "")
    source    = rec.get("source", "")

    # Never purge leads with GHL activity — someone worked these
    if has_ghl_activity(rec):
        return False, "has_ghl_activity"

    # Never purge VBP, CE, LP, APPT — no auction date relevance
    if lead_type in ("LP", "APPT", "VBP", "CE") or source in ("vbp_ce", "code_enforcement"):
        return False, "non_foreclosure_type"

    # NOF / TAX leads
    if lead_type in ("NOF", "TAX"):
        sale_date = rec.get("sale_date", "")
        if sale_date:
            dt = parse_date(sale_date)
            if dt and dt < TODAY:
                return True, f"auction_passed ({sale_date})"
        # No sale date + older than 180 days = stale, auction likely passed
        age = days_old(rec)
        if age > 180:
            return True, f"no_sale_date_stale ({age}d old)"

    return False, "keep"

def main():
    if not RECORDS_PATH.exists():
        log.error(f"records.json not found at {RECORDS_PATH}")
        return

    records = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))
    log.info(f"Loaded {len(records)} records")

    keep    = []
    purged  = []
    reasons = {}

    for rec in records:
        should, reason = should_purge(rec)
        if should:
            purged.append(rec)
            reasons[reason] = reasons.get(reason, 0) + 1
        else:
            keep.append(rec)

    log.info(f"\nPurge results:")
    log.info(f"  Keeping:  {len(keep)}")
    log.info(f"  Purging:  {len(purged)}")
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        log.info(f"    {reason}: {count}")

    if not purged:
        log.info("Nothing to purge — records.json unchanged")
        return

    # Log sample of purged leads
    log.info(f"\nSample purged leads:")
    for rec in purged[:10]:
        log.info(f"  {rec.get('owner','—')} | {rec.get('address','—')} | sale={rec.get('sale_date','—')} | filed={rec.get('date_filed','—')}")

    RECORDS_PATH.write_text(
        json.dumps(keep, ensure_ascii=False),
        encoding="utf-8"
    )
    log.info(f"\nrecords.json saved: {len(keep)} records remaining")

if __name__ == "__main__":
    main()
