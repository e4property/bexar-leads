"""
Bexar County Lis Pendens Scraper
Scrapes LP/LP2 doc types from bexar.tx.publicsearch.us RP department.
LP docs filed weeks/months before NOF — early motivated seller signal.
"""

import logging
import re
import time
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

PUBLICSEARCH_BASE = "https://bexar.tx.publicsearch.us"

# ── Address/owner sanity check ──────────────────────────────────────────────
# Ported from appointment_scraper.py's 2026-08-07 fix: a loose leading-digit
# regex lets page footer/copyright text ("2026 Bexar County... All Rights
# Reserved") slip through as a real address since the year reads as a street
# number. Require an actual street-suffix word and reject known boilerplate.
STREET_SUFFIXES = {
    "ST","AVE","DR","RD","LN","CT","CIR","BLVD","WAY","PL","TRL","PKWY",
    "HWY","LOOP","PASS","CV","PT","HLS","TRAIL","GROVE","RIDGE","CREEK",
    "LAKE","PARK","GLEN","RUN","XING","STREET","AVENUE","DRIVE","ROAD",
    "LANE","COURT","CIRCLE","BOULEVARD","PLACE","TERRACE","TER","WALK",
    "ROW","BND","BEND","VW","VIEW","CV","COVE","MNR","MANOR","SQ","SQUARE",
}
GARBAGE_ADDRESS_KEYWORDS = [
    "RIGHTS RESERVE", "COPYRIGHT", "ALL RIGHTS", "BEXAR COUNTY,",
    "CLERK OF", "GOVOS", "ACCESSIBILITY",
]

def _looks_like_address(s):
    if not s:
        return False
    upper = s.upper()
    if any(kw in upper for kw in GARBAGE_ADDRESS_KEYWORDS):
        return False
    words = re.split(r"[\s,]+", upper)
    return any(w.rstrip(".") in STREET_SUFFIXES for w in words)

def _looks_like_boilerplate(s):
    if not s:
        return False
    upper = s.upper()
    return any(kw in upper for kw in GARBAGE_ADDRESS_KEYWORDS)


def scrape_lis_pendens(known_docs, get_driver_fn, run_timestamp):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    new_records = []
    driver = None
    today = datetime.now(timezone.utc)
    cutoff = (today - timedelta(days=21)).strftime("%Y%m%d")
    today_str = today.strftime("%Y%m%d")

    try:
        driver = get_driver_fn()
        offset = 0
        consecutive_empty = 0

        while True:
            url = (
                f"{PUBLICSEARCH_BASE}/results"
                f"?department=RP"
                f"&docTypes=%5B%22LP%22%2C%22LP2%22%5D"
                f"&recordedDateRange={cutoff}%2C{today_str}"
                f"&keywordSearch=false&offset={offset}"
            )
            log.info(f"LP offset={offset}")
            try:
                driver.get(url)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table tr td, .no-results")))
                time.sleep(2)
            except Exception as e:
                log.warning(f"LP timeout offset={offset}: {e}")
                time.sleep(3)

            src = driver.page_source
            m = re.search(r"(\d[\d,]*)\s*of\s*(\d[\d,]*)\s*results?", src, re.IGNORECASE)
            if m:
                log.info(f"LP results: {m.group(0)}")

            page_records = []
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", src, re.DOTALL | re.IGNORECASE)
            for row in rows:
                if re.search(r"<th|thead|DOC.TYPE|RECORDED", row, re.IGNORECASE):
                    continue
                cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
                cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells if c.strip()]
                if len(cells) < 3:
                    continue
                doc_num = next(
                    (c for c in cells if re.match(r"^\d{9,12}$", c.strip())), "")
                if not doc_num or doc_num in known_docs:
                    continue
                dates = [c for c in cells if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c.strip())]
                owner = next(
                    (c for c in cells
                     if len(c) > 5
                     and c not in dates
                     and not re.match(r"^\d{9,12}$", c)
                     and re.search(r"[A-Z]{2,}", c)
                     and c.upper() not in ("N/A", "LES PENDENS", "LIS PENDENS", "LP")
                     and not _looks_like_boilerplate(c)), "")
                address = next(
                    (c for c in cells
                     if re.search(r"\d+\s+[A-Z]", c) and len(c) > 8
                     and _looks_like_address(c)), "")
                page_records.append({
                    "doc_number":      doc_num,
                    "type":            "LP",
                    "source":          "publicsearch",
                    "county":          "bexar",
                    "owner":           owner.title() if owner else "",
                    "address":         address or "N/A",
                    "city":            "SAN ANTONIO",
                    "zip":             "",
                    "date_filed":      dates[0] if dates else "",
                    "sale_date":       "",
                    "is_new":          True,
                    "run_ts":          run_timestamp,
                    "score":           4,
                    "flags":           ["LP"],
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
                })

            log.info(f"LP offset={offset} | {len(page_records)} on page")
            for rec in page_records:
                if rec["doc_number"] not in known_docs:
                    known_docs.add(rec["doc_number"])
                    new_records.append(rec)

            if len(page_records) == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            if consecutive_empty >= 2 or (0 < len(page_records) < 50):
                log.info(f"LP done — {len(new_records)} new LP records")
                break

            offset += 50
            time.sleep(1.5)

    except Exception as e:
        log.error(f"LP scraper error: {e}", exc_info=True)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    return new_records
