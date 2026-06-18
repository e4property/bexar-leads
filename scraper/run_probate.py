"""
run_probate.py — standalone runner for the probate scraper.
Called by .github/workflows/probate.yml
Loads existing records, runs scrape_probate(), merges and saves.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Add scraper dir to path so imports work
sys.path.insert(0, str(Path(__file__).parent))

from probate_scraper import scrape_probate
from fetch import get_driver

RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
RECORDS_PATH = Path("dashboard/records.json")


def main():
    log.info("=" * 60)
    log.info("Bexar County Probate Scraper")
    log.info(f"Run timestamp: {RUN_TIMESTAMP}")
    log.info("=" * 60)

    # Load existing records
    if not RECORDS_PATH.exists():
        log.error(f"ABORT: {RECORDS_PATH} not found — run main scrape first")
        sys.exit(1)

    try:
        existing = json.loads(RECORDS_PATH.read_text())
        log.info(f"Loaded {len(existing)} existing records from {RECORDS_PATH}")
    except Exception as e:
        log.error(f"ABORT: Could not load records.json: {e}")
        sys.exit(1)

    # Build known_docs set
    known_docs = {
        str(r.get("doc_number", ""))
        for r in existing
        if r.get("doc_number")
    }
    log.info(f"Known doc numbers: {len(known_docs)}")

    # Run probate scrape
    new_probate = scrape_probate(known_docs, get_driver, RUN_TIMESTAMP)

    if not new_probate:
        log.info("No new probate records found — records.json unchanged")
        return

    # Mark all existing records as not new
    for r in existing:
        r["is_new"] = False

    # Merge: new probate records first so they appear at top
    merged = new_probate + existing
    log.info(f"Total after merge: {len(merged)} records")

    # Write atomically
    os.makedirs("dashboard", exist_ok=True)
    tmp = RECORDS_PATH.with_suffix(".tmp")
    json_str = json.dumps(merged, separators=(",", ":"), ensure_ascii=True)
    tmp.write_text(json_str)
    tmp.replace(RECORDS_PATH)

    log.info(
        f"Saved {len(merged)} records ({len(json_str):,} bytes) to {RECORDS_PATH}"
    )
    log.info(
        f"Probate summary: {len(new_probate)} new records added"
    )

    # Breakdown by type
    from probate_scraper import DOC_TYPE_LABELS
    for dt, label in DOC_TYPE_LABELS.items():
        count = sum(1 for r in new_probate if r.get("probate_type") == dt)
        if count:
            log.info(f"  {label}: {count}")

    log.info("Done.")


if __name__ == "__main__":
    main()
