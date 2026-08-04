"""
loan_backfill.py
Backfills loan_amount/loan_date/lender/trustee for existing NOF/TAX records
in records.json that were never enriched because fetch_doc_details() relied
on a broken href-based doc-ID lookup (see fetch.py goto_doc_by_click / v28.21
fix). This is a one-time catch-up for the historical backlog; the daily
scraper now handles newly-filed leads on its own via the same click-through.

Resumable: only ever touches records still missing loan_amount, capped at
MAX_PER_RUN so a single GitHub Actions run stays well under its timeout.
Run repeatedly (workflow_dispatch) until "remaining" hits 0.

Usage:
  python scraper/loan_backfill.py
"""

import json
import logging
import sys
import time
import urllib.parse
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from fetch import get_driver, goto_doc_by_click, extract_loan_details, PUBLICSEARCH_BASE

RECORDS_PATH = Path("dashboard/records.json")
DATA_PATH    = Path("data/records.json")
MAX_PER_RUN  = 150


def search_url_for(doc_number):
    q = urllib.parse.quote(doc_number, safe="")
    return (f"{PUBLICSEARCH_BASE}/results?department=FC"
            f"&limit=10&offset=0&sort=desc&sortBy=recordedDate&docNumber={q}")


def main():
    if not RECORDS_PATH.exists():
        log.error(f"ABORT: {RECORDS_PATH} not found — run main scrape first")
        sys.exit(1)

    records = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))
    log.info(f"Loaded {len(records)} records")

    candidates = [
        r for r in records
        if r.get("type") in ("NOF", "TAX")
        and not r.get("loan_amount")
        and r.get("source", "publicsearch") == "publicsearch"
        and r.get("doc_number")
    ]
    log.info(f"Backlog: {len(candidates)} NOF/TAX leads missing loan data")

    batch = candidates[:MAX_PER_RUN]
    remaining_after = max(0, len(candidates) - len(batch))
    log.info(f"Processing {len(batch)} this run ({remaining_after} will remain)")

    if not batch:
        log.info("Nothing to do.")
        return

    driver = get_driver()
    fetched = 0
    try:
        for rec in batch:
            doc_num = rec["doc_number"]
            url = search_url_for(doc_num)
            log.info(f"  Doc [{doc_num}] locating via row click")

            if not goto_doc_by_click(driver, url, doc_num):
                log.info(f"  Doc [{doc_num}] click-through failed — skipping")
                continue

            details = extract_loan_details(driver)
            rec["loan_amount"] = details["loan_amount"]
            rec["loan_date"]   = details["loan_date"]
            rec["lender"]      = details["lender"]
            rec["trustee"]     = details["trustee"]
            fetched += 1

            log.info(f"  → amt={details['loan_amount'] or '—'} | "
                     f"lender={details['lender'][:35] if details['lender'] else '—'} | "
                     f"date={details['loan_date'] or '—'}")
            time.sleep(1)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    log.info(f"Backfill: {fetched}/{len(batch)} enriched | {remaining_after} still remaining")

    json_str = json.dumps(records, separators=(",", ":"), ensure_ascii=True)
    RECORDS_PATH.write_text(json_str, encoding="utf-8")
    if DATA_PATH.exists():
        DATA_PATH.write_text(json.dumps(records, indent=2), encoding="utf-8")
    log.info(f"Saved {len(records)} records to {RECORDS_PATH}" +
             (f" and {DATA_PATH}" if DATA_PATH.exists() else ""))


if __name__ == "__main__":
    main()
