"""
Bexar County Probate Scraper
Scrapes probate-related doc types from bexar.tx.publicsearch.us PR department.
Target doc types:
  LT   - Letters Testamentary (executor appointed, estate has real property)
  ADMIN - Letters of Administration (no will, administrator appointed)
  SEA  - Small Estate Affidavit (simplified probate, property transfer)
  INV  - Inventory (estate inventory filed, property listed)
  MUNW - Muniment of Title (will admitted, title transferred without full probate)

These filings indicate a deceased owner's estate is being settled —
heirs often need to sell quickly, especially when property has a mortgage.
Early contact (before listing) is the highest-conversion window.
"""

import logging
import re
import time
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

PUBLICSEARCH_BASE = "https://bexar.tx.publicsearch.us"

# Doc types to scrape from PR department
PROBATE_DOC_TYPES = ["LT", "ADMIN", "SEA", "INV", "MUNW"]

# Encoded for URL: %5B%22LT%22%2C%22ADMIN%22%2C%22SEA%22%2C%22INV%22%2C%22MUNW%22%5D
PROBATE_TYPES_ENCODED = "%5B%22LT%22%2C%22ADMIN%22%2C%22SEA%22%2C%22INV%22%2C%22MUNW%22%5D"

# Human-readable labels for each doc type
DOC_TYPE_LABELS = {
    "LT":    "Letters Testamentary",
    "ADMIN": "Letters of Administration",
    "SEA":   "Small Estate Affidavit",
    "INV":   "Estate Inventory",
    "MUNW":  "Muniment of Title",
}

# ── Boilerplate rejection ────────────────────────────────────────────────────
# Same fix class as appointment_scraper.py's 2026-08-07 address bug: the
# grantor/grantee extraction below is a "longest plausible cell" heuristic
# with no guard against page footer/copyright text, so port the same
# boilerplate keyword rejection here before it's trusted with real names.
GARBAGE_KEYWORDS = [
    "RIGHTS RESERVE", "COPYRIGHT", "ALL RIGHTS", "BEXAR COUNTY,",
    "CLERK OF", "GOVOS", "ACCESSIBILITY",
]

def _looks_like_boilerplate(s):
    if not s:
        return False
    upper = s.upper()
    return any(kw in upper for kw in GARBAGE_KEYWORDS)


def scrape_probate(known_docs, get_driver_fn, run_timestamp):
    """
    Scrape probate filings from PublicSearch PR department.

    Args:
        known_docs: set of doc_number strings already in records
        get_driver_fn: callable that returns a configured Selenium WebDriver
        run_timestamp: ISO timestamp string for this run

    Returns:
        list of new probate lead dicts
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    new_records = []
    driver = None
    today = datetime.now(timezone.utc)
    # 90-day window — probate takes time, leads stay relevant for months
    cutoff = (today - timedelta(days=90)).strftime("%Y%m%d")
    today_str = today.strftime("%Y%m%d")

    try:
        driver = get_driver_fn()
        offset = 0
        consecutive_empty = 0

        while True:
            url = (
                f"{PUBLICSEARCH_BASE}/results"
                f"?department=PR"
                f"&docTypes={PROBATE_TYPES_ENCODED}"
                f"&recordedDateRange={cutoff}%2C{today_str}"
                f"&keywordSearch=false&offset={offset}"
                f"&limit=50&sort=desc&sortBy=recordedDate"
            )
            log.info(f"Probate offset={offset}")

            try:
                driver.get(url)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table tr td, .no-results, [class*='no-result']")
                    )
                )
                time.sleep(2)
            except Exception as e:
                log.warning(f"Probate timeout offset={offset}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    log.info("3 consecutive timeouts — stopping probate scrape")
                    break
                time.sleep(5)
                continue

            src = driver.page_source

            # Check total results count
            m = re.search(r"(\d[\d,]*)\s*of\s*(\d[\d,]*)\s*results?", src, re.IGNORECASE)
            if m:
                log.info(f"Probate results: {m.group(0)}")

            # 2026-08-28: `"no results" in src.lower()` is a substring match
            # against the ENTIRE raw page source, not a scoped element check
            # -- confirmed the identical bug live in fetch.py's own FC-dept
            # scraper, silently dropping real leads while reporting success.
            # Only trust "no results" if there's truly no data row AND a
            # genuine No-Results heading is present.
            rows_check = re.findall(r"<tr[^>]*>(.*?)</tr>", src, re.DOTALL | re.IGNORECASE)
            data_rows_present = any(
                not re.search(r"<th|thead|DOC.TYPE|RECORDED|GRANTOR|GRANTEE", row, re.IGNORECASE)
                for row in rows_check
            )
            if not data_rows_present:
                if driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]"):
                    log.info(f"Probate offset={offset} | no results — stopping")
                    break
                time.sleep(3)
                src = driver.page_source

            page_records = []

            # Parse rows from page source
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", src, re.DOTALL | re.IGNORECASE)
            for row in rows:
                # Skip header rows
                if re.search(r"<th|thead|DOC.TYPE|RECORDED|GRANTOR|GRANTEE", row, re.IGNORECASE):
                    continue

                cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
                cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells if c.strip()]

                if len(cells) < 3:
                    continue

                # Extract doc number (9-12 digit number)
                doc_num = next(
                    (c for c in cells if re.match(r"^\d{9,12}$", c.strip())), "")
                if not doc_num or doc_num in known_docs:
                    continue

                # Extract dates (MM/DD/YYYY format)
                dates = [c for c in cells if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c.strip())]
                recorded_date = dates[0] if dates else ""

                # Extract doc type from cells
                doc_type_raw = next(
                    (c for c in cells
                     if c.upper() in PROBATE_DOC_TYPES
                     or any(dt in c.upper() for dt in PROBATE_DOC_TYPES)), "")

                # Normalize doc type
                doc_type_clean = "PROBATE"
                for dt in PROBATE_DOC_TYPES:
                    if dt in doc_type_raw.upper():
                        doc_type_clean = dt
                        break

                # Extract grantor/grantee (estate name / heir name)
                # Typically the longest non-date, non-docnum cell
                name_candidates = [
                    c for c in cells
                    if len(c) > 5
                    and c not in dates
                    and not re.match(r"^\d{9,12}$", c)
                    and not re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c)
                    and c.upper() not in PROBATE_DOC_TYPES
                    and len(re.findall(r"[A-Za-z]", c)) > 3
                    and not _looks_like_boilerplate(c)
                ]

                # Grantor = deceased owner (estate of X)
                grantor = ""
                grantee = ""
                if len(name_candidates) >= 2:
                    grantor = name_candidates[0]
                    grantee = name_candidates[1]
                elif len(name_candidates) == 1:
                    grantor = name_candidates[0]

                # Clean up "ESTATE OF" prefix for owner field
                owner = grantor
                if owner.upper().startswith("ESTATE OF "):
                    owner = owner[10:].strip()
                elif "ESTATE OF" in owner.upper():
                    owner = re.sub(r"ESTATE OF\s+", "", owner, flags=re.IGNORECASE).strip()

                # Parse month/year from recorded date
                month, year = "", ""
                if recorded_date:
                    parts = recorded_date.split("/")
                    if len(parts) == 3:
                        month, year = parts[0], parts[2]

                doc_label = DOC_TYPE_LABELS.get(doc_type_clean, "Probate Filing")

                rec = {
                    "doc_number":      doc_num,
                    "type":            "PROBATE",
                    "source":          "probate",
                    "county":          "bexar",
                    "owner":           owner.title() if owner else "",
                    "grantor":         grantor.title() if grantor else "",
                    "grantee":         grantee.title() if grantee else "",
                    "address":         "N/A",   # enriched by ArcGIS owner lookup
                    "city":            "SAN ANTONIO",
                    "zip":             "",
                    "date_filed":      recorded_date,
                    "month":           month,
                    "year":            year,
                    "sale_date":       "",
                    "is_new":          True,
                    "run_ts":          run_timestamp,
                    "score":           6,        # high base score — heirs motivated to sell
                    "flags":           ["PROBATE", doc_label.upper()],
                    "probate_type":    doc_type_clean,
                    "probate_label":   doc_label,
                    "absentee":        False,
                    "duplicate":       False,
                    "days_until_sale": None,
                    "loan_amount":     "",
                    "loan_date":       "",
                    "lender":          "",
                    "trustee":         "",
                    "appraised_value": "",
                    "annual_taxes":    "",
                    "mail_addr":       "",
                    "ps_doc_id":       "",
                    "stacked":         False,
                    "ce_violations":   False,
                }
                page_records.append(rec)

            log.info(f"Probate offset={offset} | {len(page_records)} on page")

            for rec in page_records:
                if rec["doc_number"] not in known_docs:
                    known_docs.add(rec["doc_number"])
                    new_records.append(rec)

            # Exit conditions
            if len(page_records) == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            if consecutive_empty >= 2:
                log.info(f"Probate done — {len(new_records)} new records (2 empty pages)")
                break

            if 0 < len(page_records) < 50:
                log.info(f"Probate done — {len(new_records)} new records (partial page)")
                break

            offset += 50
            time.sleep(1.5)

    except Exception as e:
        log.error(f"Probate scraper error: {e}", exc_info=True)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    log.info(f"Probate scrape: {len(new_records)} new probate records")
    return new_records
