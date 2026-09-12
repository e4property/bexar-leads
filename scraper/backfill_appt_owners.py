"""
One-off backfill: re-derive owner/address for APPT leads already sitting
in records.json with no address, or with an entity name (loan servicer,
trustee company) instead of a real homeowner as the owner.

Why this needs to be separate from the normal scrape: appointment_scraper.py
skips any doc_number already in known_docs (`if doc_num in known_docs:
continue`) so the 2026-09-11 owner-extraction fix (always re-derive the
owner from the doc page's structured Parties table, not just when
owner_unverified was already True) only ever applies to brand-new filings
going forward -- it can't reach records already scraped in earlier runs,
even though their doc detail page has the same correct data sitting on it
right now.

v1 of this script (searching PublicSearch's quickSearch endpoint for
each doc number individually) and v2 (a hand-rolled listing crawl) both
came back empty on live runs, and hand-testing in a real browser turned
up something stranger: ANY recordedDateRange with a real, non-"1800"
start date returns "No Results Found" for this site -- confirmed even
for a safely historical window (April 2023) known to contain real
APPOINTMENT filings. Only the literal sentinel start "18000101" (the
same value appointment_scraper.py's own PublicSearch quickSearch
functions already always use for date-agnostic doc-number searches)
actually returns results.

v3 (this version) stops trying to reconstruct search URLs at all and
just calls appointment_scraper.scrape_appointments() directly -- the
exact function that runs daily in production and demonstrably works
(every APPT record currently in records.json came from it) -- with a
known_docs set that has the backfill targets removed and days_back
widened past its normal 30-day window, so it "rediscovers" exactly
those docs as new and reprocesses them through the real, tested
pipeline (including the 2026-09-11 owner fix), leaving every other
already-known record it passes in the same widened window untouched.
Mirrors run_appointment.py's own call shape exactly, including running
with no PublicSearch login (confirmed that's how the daily job already
runs this scraper).

workflow_dispatch-only, not scheduled -- this is a single retroactive
pass over the current backlog, not a recurring job.
"""
import json
import logging
import sys
from datetime import datetime, timezone

sys.path.insert(0, "scraper")
import appointment_scraper as aps
from appointment_scraper import scrape_appointments

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Matches fetch.py's own convention — run from repo root (python
# scraper/backfill_appt_owners.py), same as every other workflow step.
RECORDS_PATH = "dashboard/records.json"

# Target doc numbers span 7/2026-9/2026 in the current backlog; wide
# margin since this only costs a slower crawl, not correctness.
DAYS_BACK = 100


def get_driver():
    """Same standalone Selenium setup run_appointment.py uses — no login,
    confirmed that's how the daily appointment scrape already runs."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
    )
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-web-security")
    opts.add_argument("--allow-running-insecure-content")

    try:
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
    except Exception:
        driver = webdriver.Chrome(options=opts)

    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
        """
    })
    return driver


def main():
    with open(RECORDS_PATH, encoding="utf-8") as f:
        data = json.load(f)

    by_doc = {r["doc_number"]: r for r in data if r.get("doc_number")}
    targets = {
        doc_number: r for doc_number, r in by_doc.items()
        if r.get("type") == "APPT"
        and (
            not (r.get("address") or "").strip()
            or (r.get("owner") and aps.is_entity_name(r.get("owner")))
        )
    }
    log.info(f"backfill candidates: {len(targets)}")

    # Every doc number EXCEPT the targets counts as "known" — scrape_appointments()
    # will skip past everything else it re-walks in the widened window and
    # only return fresh records for the ones we actually want reprocessed.
    known_docs = set(by_doc.keys()) - set(targets.keys())

    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_appt = scrape_appointments(known_docs, get_driver, run_timestamp, days_back=DAYS_BACK)
    log.info(f"scrape_appointments returned {len(new_appt)} records")

    fixed = 0
    for new_rec in new_appt:
        doc_number = new_rec.get("doc_number")
        rec = targets.get(doc_number)
        if not rec:
            log.info(f"  [{doc_number}] returned but wasn't a backfill target — skipping")
            continue
        changed = False
        if new_rec.get("owner") and new_rec["owner"] != rec.get("owner"):
            log.info(f"  [{doc_number}] owner: {rec.get('owner')!r} -> {new_rec['owner']!r}")
            rec["owner"] = new_rec["owner"]
            changed = True
        if new_rec.get("address") and not (rec.get("address") or "").strip():
            log.info(f"  [{doc_number}] address: -> {new_rec['address']!r}")
            rec["address"] = new_rec["address"]
            if new_rec.get("city"):
                rec["city"] = new_rec["city"]
            if new_rec.get("zip"):
                rec["zip"] = new_rec["zip"]
            changed = True
        if changed:
            rec["score"] = aps.score_appt_record(rec)
            fixed += 1

    log.info(f"backfill: {fixed}/{len(targets)} records updated")

    with open(RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()
