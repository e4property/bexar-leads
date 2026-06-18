"""
Bexar County Appointment of Substitute Trustee Scraper
Scrapes 'APPOINTMENT' doc type from bexar.tx.publicsearch.us RP department.

Appointment of Substitute Trustee filings occur 2-4 weeks BEFORE the
Notice of Foreclosure (NOF) hits the FC department — giving early access
to distressed homeowners before competitors see them.

Records are treated identically to NOF leads:
- Same GHL tags (bexar_lead, bexar-contacted)
- Same Initial Outreach SMS workflow
- Same owner enrichment via ArcGIS
- Type: APPT

v1.1 fix:
  - Filter out lender/servicer/trustee entities from name extraction.
    These docs list multiple parties (servicer, trustee firm, AND the
    actual homeowner) -- the homeowner name was being overwritten by
    the loan servicer name (e.g. "Lakeview Loan Servicing LLC").
    Now skips known corporate/entity keywords and keeps personal names.
"""

import logging
import re
import time
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

PUBLICSEARCH_BASE = "https://bexar.tx.publicsearch.us"

# Keywords that indicate a corporate/entity party (servicer, trustee firm,
# bank, etc.) rather than the actual homeowner. Any name cell containing
# these (case-insensitive) is skipped when looking for the homeowner.
ENTITY_KEYWORDS = [
    "LLC", "LLP", "L.L.C", "L.L.P", "INC", "CORP", "CORPORATION",
    "LOAN SERVICING", "LOAN SERV", "MORTGAGE", "BANK", "N.A.", " NA ",
    "TRUST", "TRUSTEE", "SERVICES", "SERVICING", "FINANCIAL",
    "AUCTION.COM", "AUCTION COM", "BARRETT DAFFIN", "FRAPPIER",
    "TURNER", "ENGEL", "ASSOCIATION", "FEDERAL", "SAVINGS",
    "HOLDINGS", "CAPITAL", "FUNDING", "PARTNERS", "GROUP",
    "COMPANY", " CO ", "ATTORNEY", "LAW FIRM", "LAW OFFICE",
    "SUBSTITUTE TRUSTEE", "DEPARTMENT", "AGENCY", "SYSTEMS",
    "FREDDIE MAC", "FANNIE MAE", "HUD", "VA LOAN", "USDA",
]


def is_entity_name(name):
    """Return True if name appears to be a corporate/servicer entity
    rather than a personal homeowner name."""
    if not name:
        return True
    upper = name.upper()
    return any(kw in upper for kw in ENTITY_KEYWORDS)


def scrape_appointments(known_docs, get_driver_fn, run_timestamp):
    """
    Scrape Appointment of Substitute Trustee filings from PublicSearch RP dept.

    Args:
        known_docs: set of doc_number strings already in records
        get_driver_fn: callable that returns a configured Selenium WebDriver
        run_timestamp: ISO timestamp string for this run

    Returns:
        list of new appointment lead dicts
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    new_records = []
    driver = None
    today = datetime.now(timezone.utc)
    # 30-day window — these are very recent pre-foreclosure signals
    cutoff = (today - timedelta(days=30)).strftime("%Y%m%d")
    today_str = today.strftime("%Y%m%d")

    try:
        driver = get_driver_fn()
        offset = 0
        consecutive_empty = 0

        while True:
            url = (
                f"{PUBLICSEARCH_BASE}/results"
                f"?department=RP"
                f"&keywordSearch=false"
                f"&limit=50"
                f"&offset={offset}"
                f"&recordedDateRange={cutoff}%2C{today_str}"
                f"&searchOcrText=false"
                f"&searchType=quickSearch"
                f"&searchValue=appointment"
                f"&sort=desc"
                f"&sortBy=recordedDate"
            )
            log.info(f"Appointment offset={offset}")

            try:
                driver.get(url)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "table tr td, .no-results, [class*='no-result']")
                    )
                )
                time.sleep(2)
            except Exception as e:
                log.warning(f"Appointment timeout offset={offset}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    log.info("3 consecutive timeouts — stopping appointment scrape")
                    break
                time.sleep(5)
                continue

            src = driver.page_source

            m = re.search(r"(\d[\d,]*)\s*of\s*(\d[\d,]*)\s*results?", src, re.IGNORECASE)
            if m:
                log.info(f"Appointment results: {m.group(0)}")

            if "no results" in src.lower() or "0 of 0" in src:
                log.info(f"Appointment offset={offset} | no results — stopping")
                break

            page_records = []

            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", src, re.DOTALL | re.IGNORECASE)
            for row in rows:
                if re.search(r"<th|thead|DOC.TYPE|RECORDED|GRANTOR|GRANTEE|LEGAL.DESC", row, re.IGNORECASE):
                    continue

                cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
                cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells if c.strip()]

                if len(cells) < 3:
                    continue

                doc_num = next(
                    (c for c in cells if re.match(r"^\d{9,12}$", c.strip())), "")
                if not doc_num or doc_num in known_docs:
                    continue

                ps_doc_id = ""
                href_matches = re.findall(r'/doc/(\d+)', row)
                if href_matches:
                    ps_doc_id = href_matches[0]

                dates = [c for c in cells if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c.strip())]
                recorded_date = dates[0] if dates else ""

                # Build candidate name list (exclude dates, doc numbers, doc type)
                name_candidates = [
                    c for c in cells
                    if len(c) > 4
                    and c not in dates
                    and not re.match(r"^\d{9,12}$", c)
                    and not re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", c)
                    and re.search(r"[A-Za-z]{2,}", c)
                    and "N/A" not in c.upper()
                    and "APPOINTMENT" not in c.upper()
                ]

                # Find first candidate that is NOT a corporate/servicer entity —
                # this is the actual homeowner (grantor)
                grantor = ""
                for cand in name_candidates:
                    if not is_entity_name(cand):
                        grantor = cand
                        break
                # Fallback: if every candidate looked like an entity, use the
                # first one anyway (better than nothing) but flag it
                used_fallback_entity = False
                if not grantor and name_candidates:
                    grantor = name_candidates[0]
                    used_fallback_entity = True

                # Try to get address from grid (Property Address column)
                address_raw = next(
                    (c for c in cells
                     if re.match(r"^\d+\s+[A-Z]", c.upper())
                     and len(c) > 8
                     and "N/A" not in c.upper()), "")

                month, year = "", ""
                if recorded_date:
                    parts = recorded_date.split("/")
                    if len(parts) == 3:
                        month, year = parts[0], parts[2]

                address = ""
                city = ""
                zip_code = ""
                if address_raw:
                    if "," in address_raw:
                        addr_parts = [p.strip() for p in address_raw.split(",")]
                        address = addr_parts[0].upper()
                        if len(addr_parts) >= 2:
                            city_state = addr_parts[1].strip().upper()
                            zip_m = re.search(r"\b(\d{5})\b", city_state)
                            if zip_m:
                                zip_code = zip_m.group(1)
                            city_clean = re.sub(r"\b(TX|TEXAS)\b", "", city_state).strip()
                            city_clean = re.sub(r"\d{5}", "", city_clean).strip().rstrip(",").strip()
                            city = city_clean
                    else:
                        address = address_raw.upper()

                rec = {
                    "doc_number":      doc_num,
                    "ps_doc_id":       ps_doc_id,
                    "type":            "APPT",
                    "source":          "publicsearch",
                    "county":          "bexar",
                    "owner":           grantor.title() if grantor else "",
                    "owner_unverified": used_fallback_entity,
                    "address":         address,
                    "city":            city or "SAN ANTONIO",
                    "zip":             zip_code,
                    "date_filed":      f"{month}/{year}".strip("/"),
                    "month":           month,
                    "year":            year,
                    "sale_date":       "",
                    "is_new":          True,
                    "run_ts":          run_timestamp,
                    "score":           7,
                    "flags":           ["APPT", "PRE-FORE"],
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
                    "stacked":         False,
                    "ce_violations":   False,
                }
                page_records.append(rec)

            log.info(f"Appointment offset={offset} | {len(page_records)} on page")

            # Second pass: click into Summary for records missing address
            # OR missing a confirmed (non-entity) owner name
            need_summary = [
                r for r in page_records
                if (not r["address"] or r.get("owner_unverified"))
                and r["ps_doc_id"]
            ]
            if need_summary:
                log.info(f"Fetching Summary detail for {len(need_summary)} records...")
                for rec in need_summary:
                    try:
                        doc_url = f"{PUBLICSEARCH_BASE}/doc/{rec['ps_doc_id']}"
                        driver.get(doc_url)
                        time.sleep(2)

                        try:
                            tabs = driver.find_elements(By.CSS_SELECTOR, ".tab-item, [role='tab'], button, a")
                            for tab in tabs:
                                txt = (tab.text or "").strip().lower()
                                if txt == "summary" or txt.startswith("summar"):
                                    tab.click()
                                    time.sleep(1.5)
                                    break
                        except Exception:
                            pass

                        page_src = driver.page_source

                        # Address
                        if not rec["address"]:
                            addr_match = re.search(
                                r"Property Address.*?(\d+\s+[A-Z0-9][^\n<]{5,60}(?:SAN ANTONIO|TEXAS|TX)[^\n<]{0,20})",
                                page_src, re.IGNORECASE | re.DOTALL)
                            if addr_match:
                                raw = re.sub(r"<[^>]+>", "", addr_match.group(1)).strip()
                                if "," in raw:
                                    parts = [p.strip() for p in raw.split(",")]
                                    rec["address"] = parts[0].upper()
                                    if len(parts) >= 2:
                                        zip_m = re.search(r"\b(\d{5})\b", parts[1])
                                        if zip_m:
                                            rec["zip"] = zip_m.group(1)
                                        city_c = re.sub(r"\b(TX|TEXAS)\b", "", parts[1]).strip()
                                        city_c = re.sub(r"\d{5}", "", city_c).strip()
                                        if city_c:
                                            rec["city"] = city_c.upper()
                                else:
                                    rec["address"] = raw.upper()
                                log.info(f"  Got address for {rec['doc_number']}: {rec['address']}")

                        # Owner — find all GRANTOR-tagged names in Summary, pick
                        # first one that is NOT an entity
                        if rec.get("owner_unverified") or not rec["owner"]:
                            grantor_block = re.findall(
                                r"([A-Z][A-Z\s\.\-']{3,50})\s*</[^>]+>\s*<[^>]*>\s*GRANTOR",
                                page_src, re.IGNORECASE)
                            # Fallback simpler pattern: lines followed by GRANTOR label
                            if not grantor_block:
                                grantor_block = re.findall(
                                    r">([A-Z][A-Z\s\.\-']{3,50})<.*?GRANTOR",
                                    page_src)

                            found_personal = ""
                            for cand in grantor_block:
                                cand_clean = cand.strip()
                                if not is_entity_name(cand_clean):
                                    found_personal = cand_clean
                                    break

                            if found_personal:
                                rec["owner"] = found_personal.title()
                                rec["owner_unverified"] = False
                                log.info(f"  Confirmed owner for {rec['doc_number']}: {rec['owner']}")

                        time.sleep(1)
                    except Exception as e:
                        log.debug(f"Summary fetch error for {rec.get('doc_number')}: {e}")

            # Clean up internal-only field before adding to output
            for rec in page_records:
                rec.pop("owner_unverified", None)
                if rec["doc_number"] not in known_docs:
                    known_docs.add(rec["doc_number"])
                    new_records.append(rec)

            if len(page_records) == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            if consecutive_empty >= 2:
                log.info(f"Appointment done — {len(new_records)} new records")
                break

            if 0 < len(page_records) < 50:
                log.info(f"Appointment done — {len(new_records)} new records (partial page)")
                break

            offset += 50
            time.sleep(1.5)

    except Exception as e:
        log.error(f"Appointment scraper error: {e}", exc_info=True)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    log.info(f"Appointment scrape: {len(new_records)} new pre-foreclosure records")
    return new_records
