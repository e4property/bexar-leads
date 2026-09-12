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
right now. This walks that existing backlog directly via PublicSearch's
quickSearch-by-doc-number URL (the same one already proven live for
fetch.py's Deed-of-Trust hop, search_and_ocr_referenced_doc) instead of
requiring re-discovery through a date-range page-by-page crawl.

workflow_dispatch-only, not scheduled -- this is a single retroactive
pass over the current backlog, not a recurring job.
"""
import json
import logging
import re
import sys
import time
from datetime import timedelta

sys.path.insert(0, "scraper")
import fetch
import appointment_scraper as aps

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Matches fetch.py's own convention — run from repo root (python
# scraper/backfill_appt_owners.py), same as every other workflow step.
RECORDS_PATH = "dashboard/records.json"


def find_doc_and_extract(driver, doc_number):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    today_str = (fetch.TODAY_NAIVE - timedelta(days=3)).strftime("%Y%m%d")
    url = fetch.QUICK_SEARCH_URL_TMPL.format(today=today_str, doc_number=doc_number)

    try:
        driver.set_page_load_timeout(20)
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH, "//table//tr/td | //h1[contains(text(),'No Results')]")
            )
        )
        if driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]"):
            log.info(f"  [{doc_number}] no search results")
            return None
        time.sleep(1)
        row = driver.find_element(By.CSS_SELECTOR, "table tbody tr")
        row.click()
        WebDriverWait(driver, 20).until(EC.url_contains("/doc/"))
        time.sleep(1.5)
    except Exception as e:
        log.info(f"  [{doc_number}] search/click-through failed: {e}")
        return None

    page_src = driver.page_source
    result = {}

    # Address — same pattern already proven in appointment_scraper.py's
    # Summary-detail-page fetch.
    addr_match = re.search(
        r"Property Address.*?(\d+\s+[A-Z0-9][^\n<]{5,60}(?:SAN ANTONIO|TEXAS|TX)[^\n<]{0,20})",
        page_src, re.IGNORECASE | re.DOTALL)
    if addr_match:
        raw = re.sub(r"<[^>]+>", "", addr_match.group(1)).strip()
        if aps._looks_like_address(raw):
            if "," in raw:
                parts = [p.strip() for p in raw.split(",")]
                result["address"] = parts[0].upper()
                if len(parts) >= 2:
                    zip_m = re.search(r"\b(\d{5})\b", parts[1])
                    if zip_m:
                        result["zip"] = zip_m.group(1)
                    city_c = re.sub(r"\b(TX|TEXAS)\b", "", parts[1]).strip()
                    city_c = re.sub(r"\d{5}", "", city_c).strip()
                    if city_c:
                        result["city"] = city_c.upper()
            else:
                result["address"] = raw.upper()

    # Owner — the same structured Parties-table parse now in
    # appointment_scraper.py (data-testid="docPreviewParty": a run of
    # <a>NAME</a><span class="...summary-group-label">ROLE</span> pairs).
    party_pairs = re.findall(
        r'<a[^>]*>([^<]+)</a>\s*<span class="doc-preview-group__summary-group-label">([^<]+)</span>',
        page_src)
    grantors = [n.strip() for n, role in party_pairs if role.strip().upper() == "GRANTOR"]
    found_personal = next((g for g in grantors if g and not aps.is_entity_name(g)), "")
    if found_personal:
        result["owner"] = found_personal.title()

    return result


def main():
    with open(RECORDS_PATH, encoding="utf-8") as f:
        data = json.load(f)

    targets = [
        r for r in data
        if r.get("type") == "APPT"
        and (
            not (r.get("address") or "").strip()
            or (r.get("owner") and aps.is_entity_name(r.get("owner")))
        )
    ]
    log.info(f"backfill candidates: {len(targets)}")

    driver = fetch.get_driver()
    fixed = 0
    try:
        logged_in = fetch.login_publicsearch(driver)
        log.info(f"login: {logged_in}")

        for rec in targets:
            doc_number = rec.get("doc_number")
            result = find_doc_and_extract(driver, doc_number)
            if not result:
                time.sleep(1)
                continue
            changed = False
            if result.get("owner") and result["owner"] != rec.get("owner"):
                log.info(f"  [{doc_number}] owner: {rec.get('owner')!r} -> {result['owner']!r}")
                rec["owner"] = result["owner"]
                changed = True
            if result.get("address") and not (rec.get("address") or "").strip():
                log.info(f"  [{doc_number}] address: -> {result['address']!r}")
                rec["address"] = result["address"]
                if result.get("city"):
                    rec["city"] = result["city"]
                if result.get("zip"):
                    rec["zip"] = result["zip"]
                changed = True
            if changed:
                fixed += 1
            time.sleep(1)

        log.info(f"backfill: {fixed}/{len(targets)} records updated")
    finally:
        driver.quit()

    with open(RECORDS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()
