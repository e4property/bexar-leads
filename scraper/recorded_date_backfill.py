"""
recorded_date_backfill.py

One-time backfill: fills in date_recorded (exact MM/DD/YYYY) for existing
NOF/TAX records that only have month/year (date_filed), because the exact
date fetch.py now captures going forward wasn't being kept before
2026-08-31. Re-walks the FC department in 7-day chunks (same pattern
fetch.py uses), but unlike a normal scrape it does NOT skip already-known
doc numbers -- it records every (doc_number, recorded_date) pair it sees
and backfills date_recorded on any local record missing it.

Run manually via GitHub Actions workflow_dispatch (needs CLERK_EMAIL /
CLERK_PASSWORD secrets, same login as fetch.py).
"""
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from fetch import get_driver, login_publicsearch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PUBLICSEARCH_BASE = "https://bexar.tx.publicsearch.us"
RECORDS_PATH = Path("dashboard/records.json")
DATA_PATH = Path("data/records.json")

TODAY = datetime.now(timezone.utc)
WINDOW_DAYS = 130  # comfortably covers the oldest date_filed we need (June 2026)
CHUNK_DAYS = 7


def scrape_dates_for_window(driver, start, end):
    """Walk one date window, return {doc_number: recorded_date}."""
    found = {}
    offset = 0
    consecutive_empty = 0
    cutoff_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    while True:
        url = (
            f"{PUBLICSEARCH_BASE}/results"
            f"?department=FC"
            f"&recordedDateRange={cutoff_str}%2C{end_str}"
            f"&keywordSearch=false&limit=50&offset={offset}"
            f"&sort=desc&sortBy=recordedDate&searchType=advancedSearch"
        )
        try:
            driver.get(url)
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//table//tr/td | //h1[contains(text(),'No Results')]")
                )
            )
            time.sleep(1.5)
        except Exception as e:
            log.warning(f"  timeout offset={offset}: {e}")
            consecutive_empty += 1
            if consecutive_empty >= 2:
                break
            time.sleep(3)
            continue

        src = driver.page_source
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", src, re.DOTALL | re.IGNORECASE)
        data_rows_present = any(
            not re.search(r"<th|thead|DOC.TYPE|RECORDED|SALE.DATE", row, re.IGNORECASE)
            for row in rows
        )
        if not data_rows_present:
            if driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]"):
                break
            time.sleep(3)
            src = driver.page_source
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", src, re.DOTALL | re.IGNORECASE)

        page_count = 0
        for row in rows:
            if re.search(r"<th|thead|DOC.TYPE|RECORDED|SALE.DATE", row, re.IGNORECASE):
                continue
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells if c.strip()]
            if len(cells) < 4:
                continue
            doc_number = next((c for c in cells if re.match(r"^\d{7,12}$", c)), "")
            dates = [c for c in cells if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c)]
            if not doc_number or not dates:
                continue
            page_count += 1
            found[doc_number] = dates[0]

        log.info(f"  offset={offset} | {page_count} rows")
        consecutive_empty = 0 if page_count else consecutive_empty + 1
        if consecutive_empty >= 2 or 0 < page_count < 50:
            break
        offset += 50
        time.sleep(1)

    return found


def main():
    existing = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))
    targets = {
        r["doc_number"] for r in existing
        if r.get("type") in ("NOF", "TAX") and not r.get("date_recorded")
    }
    log.info(f"Targeting {len(targets)} records missing date_recorded")

    driver = get_driver()
    all_found = {}
    try:
        login_publicsearch(driver)
        chunks = WINDOW_DAYS // CHUNK_DAYS + 1
        window_end = TODAY
        for i in range(chunks):
            window_start = window_end - timedelta(days=CHUNK_DAYS)
            log.info(f"Chunk {i+1}/{chunks}: {window_start.date()} -> {window_end.date()}")
            found = scrape_dates_for_window(driver, window_start, window_end)
            all_found.update(found)
            window_end = window_start
            remaining = targets - set(all_found.keys())
            log.info(f"  running total found={len(all_found)}, still missing={len(remaining)}")
            if not remaining:
                log.info("All targets found -- stopping early")
                break
            time.sleep(1)
    finally:
        driver.quit()

    filled = 0
    for r in existing:
        if r.get("type") in ("NOF", "TAX") and not r.get("date_recorded"):
            dr = all_found.get(r["doc_number"])
            if dr:
                r["date_recorded"] = dr
                filled += 1

    log.info(f"Backfill: {filled} records filled, {len(targets) - filled} still missing")

    RECORDS_PATH.write_text(json.dumps(existing, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
    if DATA_PATH.exists():
        DATA_PATH.write_text(json.dumps(existing, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
    log.info("Saved.")


if __name__ == "__main__":
    main()
