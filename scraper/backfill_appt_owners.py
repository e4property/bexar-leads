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

v3 stopped trying to reconstruct search URLs at all and just called
appointment_scraper.scrape_appointments() directly -- the exact
function that runs daily in production and demonstrably works (every
APPT record currently in records.json came from it) -- with a
known_docs set that has the backfill targets removed and days_back
widened past its normal 30-day window, so it "rediscovers" exactly
those docs as new and reprocesses them through the real, tested
pipeline (including the 2026-09-11 owner fix). Mirrors
run_appointment.py's own call shape, including running with no
PublicSearch login (confirmed that's how the daily job already runs
this scraper).

v3 made real progress (page one always resolved correctly) but hit a
wall three separate live runs in a row: offset=50 on the one-shot
100-day window times out every single time, no matter how generous the
timeout/retry/cooldown settings get. Reads as a genuine backend limit
on deep pagination over a broad window, not rate-limiting -- the daily
job's normal 30-day window almost never accumulates enough new records
to page past offset=0, so this was presumably never hit before.

v4 (this version) drops the single wide window and instead calls
scrape_appointments() once per narrow ~15-day date chunk (via its new
date_start/date_end override), walking backward from today across the
full backlog span. Each chunk's total result count is small enough
that it should never need to page past offset=0 at all, sidestepping
the broken deep-pagination path entirely instead of trying to push
through it.

workflow_dispatch-only, not scheduled -- this is a single retroactive
pass over the current backlog, not a recurring job.
"""
import json
import logging
import sys
from datetime import datetime, timezone, timedelta

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
    # will skip past everything else it re-walks in each window and only
    # return fresh records for the ones we actually want reprocessed. Updated
    # after every chunk with whatever that chunk found, target or not, so
    # adjacent/overlapping windows never redo work.
    known_docs = set(by_doc.keys()) - set(targets.keys())

    # 2026-09-12/13: three separate live runs (including one with a 45s
    # timeout, 25s retry backoff, and 6 retries) all hit the exact same
    # wall — offset=50 on the one-shot 100-day window times out every
    # single time, no matter how much patience is given. Reads as a
    # genuine backend limit on deep pagination over a broad window, not
    # rate-limiting. Instead of one wide window, run several narrow ones
    # in sequence — each with few enough total results that it never
    # needs to page past offset=0, the same page size the daily job
    # almost always stays within (which is presumably why this was never
    # caught before).
    today = datetime.now(timezone.utc)
    chunk_days = 15
    fixed = 0
    remaining_targets = set(targets.keys())

    chunk_start = today
    while remaining_targets and (today - chunk_start).days < DAYS_BACK:
        chunk_end = chunk_start
        chunk_start = chunk_end - timedelta(days=chunk_days)
        date_end = chunk_end.strftime("%Y%m%d")
        date_start = chunk_start.strftime("%Y%m%d")
        log.info(f"=== chunk {date_start}-{date_end} | {len(remaining_targets)} targets still remaining ===")

        run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        new_appt = scrape_appointments(
            known_docs, get_driver, run_timestamp,
            stop_on_partial_page=False,
            page_timeout=45, retry_sleep=20, max_page_retries=3,
            post_burst_cooldown=20,
            date_start=date_start, date_end=date_end,
        )
        log.info(f"  chunk returned {len(new_appt)} records")

        for new_rec in new_appt:
            doc_number = new_rec.get("doc_number")
            known_docs.add(doc_number)
            rec = targets.get(doc_number)
            if not rec:
                continue
            remaining_targets.discard(doc_number)
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

        # A target's doc page having nothing new to extract (a genuine
        # commercial filing with no personal co-grantor) still means the
        # chunk that contains it visited it — no point revisiting the
        # same doc again in a later chunk if it fell outside every
        # chunk's date window regardless; only drop targets actually
        # returned this chunk (handled above via remaining_targets.discard).

    log.info(f"backfill: {fixed}/{len(targets)} records updated | {len(remaining_targets)} targets never reached")

    with open(RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()
