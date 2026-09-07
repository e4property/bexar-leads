"""
One-off diagnostic: sample two candidate lien doc types (HOA Lien, Medical
Lien) to see if they're worth building into a real lead source, now that
Mechanics Liens have been paused for being low-signal (nice houses, no real
distress). Read-only -- does NOT write to records.json or push anything.

Run via GitHub Actions (needs CLERK_EMAIL/CLERK_PASSWORD secrets, same
login as the main scraper) since anonymous/unauthenticated requests to
this site don't reliably return advancedSearch results.
"""
import logging
import time
import urllib.parse as _uparse
from datetime import datetime, timedelta, timezone

from fetch import get_driver, login_publicsearch, PUBLICSEARCH_BASE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

CANDIDATE_TYPES = ["LNHOA", "MEDLN"]
LOOKBACK_DAYS = 90


def sample_doc_type(driver, doc_type, start_str, end_str):
    doc_types_q = _uparse.quote(doc_type)
    url = (
        f"{PUBLICSEARCH_BASE}/results"
        f"?department=RP"
        f"&docTypes={doc_types_q}"
        f"&recordedDateRange={start_str}%2C{end_str}"
        f"&keywordSearch=false"
        f"&limit=25"
        f"&offset=0"
        f"&sort=desc"
        f"&sortBy=recordedDate"
        f"&searchType=advancedSearch"
    )
    log.info(f"=== {doc_type} === {url}")
    driver.set_page_load_timeout(30)
    driver.get(url)
    time.sleep(4)

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    try:
        WebDriverWait(driver, 25).until(
            lambda d: (
                d.find_elements(By.CSS_SELECTOR, "table tbody tr")
                or d.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]")
                or "no results" in d.page_source.lower()
            )
        )
    except Exception as e:
        log.warning(f"{doc_type}: wait failed — {e}")

    if "no results" in driver.page_source.lower():
        log.info(f"{doc_type}: 0 results in last {LOOKBACK_DAYS} days")
        return

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    log.info(f"{doc_type}: {len(rows)} rows on first page")
    for i, row in enumerate(rows[:15]):
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            texts = [c.text.strip() for c in cells]
            # real columns start at index 3 per this repo's own confirmed
            # offset (first 3 <td> are blank checkbox/icon columns)
            data = texts[3:] if len(texts) > 3 else texts
            log.info(f"  [{i}] {data}")
        except Exception as e:
            log.warning(f"  [{i}] row read error: {e}")


def main():
    today = datetime.now(timezone.utc)
    start_str = (today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    end_str = today.strftime("%Y%m%d")

    driver = get_driver()
    try:
        logged_in = login_publicsearch(driver)
        log.info(f"login_publicsearch: {logged_in}")
        for doc_type in CANDIDATE_TYPES:
            sample_doc_type(driver, doc_type, start_str, end_str)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
