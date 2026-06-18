"""
run_appointment.py — standalone runner for the Appointment of Substitute Trustee scraper.
Called by .github/workflows/appointment.yml
Loads existing records, runs scrape_appointments(), merges and saves.
Completely independent of fetch.py / scrape.yml.
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

sys.path.insert(0, str(Path(__file__).parent))

from appointment_scraper import scrape_appointments

RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
RECORDS_PATH = Path("dashboard/records.json")


def get_driver():
    """Standalone Selenium driver setup — does not depend on fetch.py."""
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
    log.info("=" * 60)
    log.info("Bexar County Appointment of Substitute Trustee Scraper")
    log.info(f"Run timestamp: {RUN_TIMESTAMP}")
    log.info("=" * 60)

    if not RECORDS_PATH.exists():
        log.error(f"ABORT: {RECORDS_PATH} not found — run main scrape first")
        sys.exit(1)

    try:
        existing = json.loads(RECORDS_PATH.read_text())
        log.info(f"Loaded {len(existing)} existing records from {RECORDS_PATH}")
    except Exception as e:
        log.error(f"ABORT: Could not load records.json: {e}")
        sys.exit(1)

    known_docs = {
        str(r.get("doc_number", ""))
        for r in existing
        if r.get("doc_number")
    }
    log.info(f"Known doc numbers: {len(known_docs)}")

    new_appt = scrape_appointments(known_docs, get_driver, RUN_TIMESTAMP)

    if not new_appt:
        log.info("No new appointment records found — records.json unchanged")
        print("new_count=0")
        return

    for r in existing:
        r["is_new"] = False

    merged = new_appt + existing
    log.info(f"Total after merge: {len(merged)} records")

    os.makedirs("dashboard", exist_ok=True)
    tmp = RECORDS_PATH.with_suffix(".tmp")
    json_str = json.dumps(merged, separators=(",", ":"), ensure_ascii=True)
    tmp.write_text(json_str)
    tmp.replace(RECORDS_PATH)

    log.info(f"Saved {len(merged)} records ({len(json_str):,} bytes) to {RECORDS_PATH}")
    log.info(f"Appointment summary: {len(new_appt)} new pre-foreclosure records added")
    print(f"new_count={len(new_appt)}")
    log.info("Done.")


if __name__ == "__main__":
    main()
