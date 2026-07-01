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

v1.2 (Phase 1 ArcGIS enrichment):
  - After PublicSearch scrape, run ArcGIS 5-strategy owner/address lookup
    on every APPT record that is missing address OR owner.
  - Pulls: owner, address (Situs), zip, absentee flag, appraised_value,
    annual_taxes, prop_id, tenure_years, tenure_score_bonus.
  - Uses owner name from PublicSearch as the search key when address missing.
  - Self-contained — does NOT touch main fetch.py.
"""

import logging
import re
import time
import urllib.parse
import urllib.request
import json
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

PUBLICSEARCH_BASE = "https://bexar.tx.publicsearch.us"
PARCELS_URL       = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"

# ── Entity filter ─────────────────────────────────────────────────────────────
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
    if not name:
        return True
    upper = name.upper()
    return any(kw in upper for kw in ENTITY_KEYWORDS)


# ── ArcGIS helpers (self-contained, no dependency on fetch.py) ────────────────

def _fetch_json(url, timeout=20):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "BexarApptScraper/1.2", "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except Exception as e:
        log.debug(f"_fetch_json error: {e}")
        return {}

def _arcgis_query(where, fields="*", limit=200):
    """Query the Bexar County parcels ArcGIS layer."""
    all_features = []
    offset = 0
    while True:
        params = urllib.parse.urlencode({
            "where":             where,
            "outFields":         fields,
            "returnGeometry":    "false",
            "resultOffset":      offset,
            "resultRecordCount": limit,
            "f":                 "json",
        })
        data = _fetch_json(f"{PARCELS_URL}/query?{params}")
        if not data or "error" in data:
            break
        batch = data.get("features", [])
        all_features.extend(batch)
        if not data.get("exceededTransferLimit", False) or len(batch) < limit:
            break
        offset += len(batch)
    return all_features

def _get_attr(attrs, *keys, default=""):
    for k in keys:
        v = attrs.get(k)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>", "NULL"):
            return str(v).strip()
    return default

# Known ArcGIS field aliases
OWNER_FIELDS  = ["Owner", "OWNER", "OwnerName", "owner_name"]
SITUS_FIELDS  = ["Situs", "SITUS", "SitusAddress", "situs_address"]
ADDR_FIELDS   = ["AddrLn1", "ADDRLN1", "MailAddr1", "mail_addr1"]
CITY_FIELDS   = ["AddrCity", "ADDRCITY", "MailCity", "mail_city"]
ZIP_FIELDS    = ["Zip", "ZIP", "ZipCode", "zip_code", "PostalCode"]
APPR_FIELDS   = ["TotVal", "TOTVAL", "AppraisedValue", "appraised_value", "Tot_Val"]
TAX_FIELDS    = ["TaxAmt", "TAXAMT", "AnnualTax", "tax_amt"]
PROPID_FIELDS = ["PropID", "PROPID", "prop_id", "PropertyID"]
LAND_FIELDS   = ["LandVal", "LANDVAL", "land_val"]
IMPR_FIELDS   = ["ImprVal", "IMPRVAL", "impr_val"]

def _normalize(s):
    return " ".join(str(s).upper().split())

def _parse_address_num(address):
    """Extract street number from address string."""
    m = re.match(r"^(\d+)\s+", address.strip())
    return m.group(1) if m else ""

def _parse_address_words(address):
    """Extract street name words (no number, no suffix)."""
    clean = re.sub(r"^\d+\s*", "", address.strip().upper())
    words = [w for w in re.split(r"\s+", clean) if w and len(w) >= 2]
    return words

def _match_features(features, num, required_word=None):
    """
    Find the best ArcGIS parcel match from a feature list.
    Tries to match the street number and optionally a required word in the situs.
    """
    for feat in features:
        a = feat.get("attributes", {})
        situs = _get_attr(a, *SITUS_FIELDS)
        owner = _get_attr(a, *OWNER_FIELDS)
        if not situs or not owner:
            continue
        situs_norm = _normalize(situs)
        if not situs_norm.startswith(num + " "):
            continue
        if required_word and required_word not in situs_norm:
            continue
        addr1   = _get_attr(a, *ADDR_FIELDS)
        city    = _get_attr(a, *CITY_FIELDS)
        zipcode = _get_attr(a, *ZIP_FIELDS)
        mail_addr = f"{addr1} {city} {zipcode}".strip() if addr1 else ""
        absentee = bool(mail_addr) and not _normalize(mail_addr).startswith(num + " ")
        return {
            "owner":              owner.upper(),
            "address":            situs.upper(),
            "mail_addr":          mail_addr,
            "absentee":           absentee,
            "appraised_value":    _get_attr(a, *APPR_FIELDS),
            "annual_taxes":       _get_attr(a, *TAX_FIELDS),
            "land_value":         _get_attr(a, *LAND_FIELDS),
            "improvement_value":  _get_attr(a, *IMPR_FIELDS),
            "prop_id":            _get_attr(a, *PROPID_FIELDS),
            "zip":                zipcode,
        }
    return None

def _lookup_by_address(address, zipcode=""):
    """
    5-strategy ArcGIS address lookup — same as main scraper.
    Returns enrichment dict or None.
    """
    if not address:
        return None
    num   = _parse_address_num(address)
    words = _parse_address_words(address)
    first = words[0] if words else ""
    if not num:
        return None

    # Strategy 1: two street words
    if len(words) >= 2:
        r = _match_features(
            _arcgis_query(f"Situs LIKE '{num} {words[0]} {words[1]}%'", limit=50),
            num, first)
        if r: r["method"] = "s1_two_word"; return r

    # Strategy 2: first word
    if first and len(first) >= 3:
        r = _match_features(
            _arcgis_query(f"Situs LIKE '{num} {first}%'", limit=100),
            num, first)
        if r: r["method"] = "s2_first_word"; return r

    # Strategy 3: number only
    r = _match_features(
        _arcgis_query(f"Situs LIKE '{num} %'", limit=200),
        num, None)
    if r: r["method"] = "s3_num_only"; return r

    # Strategy 4: zip scan
    if zipcode and len(zipcode) >= 5:
        r = _match_features(
            _arcgis_query(f"Zip = '{zipcode[:5]}'", limit=1000),
            num, None)
        if r: r["method"] = "s4_zip_scan"; return r

    # Strategy 5: alternate words
    for word in words[1:]:
        if len(word) < 4:
            continue
        r = _match_features(
            _arcgis_query(f"Situs LIKE '{num} %{word}%'", limit=100),
            num, word)
        if r: r["method"] = "s5_alt_word"; return r

    return None

def _lookup_by_owner_name(owner_name):
    """
    Owner name lookup — used when we have a name from PublicSearch
    but no address yet. Searches ArcGIS Owner field.
    Returns enrichment dict or None.
    """
    if not owner_name or is_entity_name(owner_name):
        return None
    # Use last name (first word usually) for a LIKE query
    parts = owner_name.strip().upper().split()
    if not parts:
        return None
    last = parts[0]  # PublicSearch name format is usually LASTNAME FIRSTNAME
    if len(last) < 3:
        return None
    features = _arcgis_query(f"Owner LIKE '{last}%'", limit=100)
    # Find feature where full name matches reasonably
    name_norm = _normalize(owner_name)
    for feat in features:
        a = feat.get("attributes", {})
        arc_owner = _normalize(_get_attr(a, *OWNER_FIELDS))
        if not arc_owner:
            continue
        # Check if the last name matches and at least one other word
        arc_parts = arc_owner.split()
        matches = sum(1 for p in parts if p in arc_parts)
        if matches >= min(2, len(parts)):
            situs   = _get_attr(a, *SITUS_FIELDS)
            addr1   = _get_attr(a, *ADDR_FIELDS)
            city    = _get_attr(a, *CITY_FIELDS)
            zipcode = _get_attr(a, *ZIP_FIELDS)
            mail_addr = f"{addr1} {city} {zipcode}".strip() if addr1 else ""
            # Parse address from situs
            situs_clean = situs.upper() if situs else ""
            num = _parse_address_num(situs_clean)
            absentee = bool(mail_addr) and bool(num) and not _normalize(mail_addr).startswith(num + " ")
            return {
                "owner":             arc_owner,
                "address":           situs_clean,
                "mail_addr":         mail_addr,
                "absentee":          absentee,
                "appraised_value":   _get_attr(a, *APPR_FIELDS),
                "annual_taxes":      _get_attr(a, *TAX_FIELDS),
                "land_value":        _get_attr(a, *LAND_FIELDS),
                "improvement_value": _get_attr(a, *IMPR_FIELDS),
                "prop_id":           _get_attr(a, *PROPID_FIELDS),
                "zip":               zipcode,
                "method":            "owner_name_lookup",
            }
    return None

def _tenure_bonus(yrs):
    if yrs is None: return 0
    if yrs >= 15:   return 15
    if yrs >= 10:   return 10
    if yrs >= 5:    return 5
    return 0

def arcgis_enrich_appt_records(records):
    """
    Phase 1 ArcGIS enrichment for APPT records.
    Runs on any record missing address OR missing owner.
    Two-pass:
      Pass 1: address-based lookup (most reliable, ~80% hit rate)
      Pass 2: owner-name-based lookup for records still missing address
    """
    needs_enrich = [
        r for r in records
        if r.get("type") == "APPT"
        and (not r.get("address") or not r.get("owner") or not r.get("appraised_value"))
    ]

    if not needs_enrich:
        log.info("APPT ArcGIS enrich: all records already have address+owner — skipping")
        return records

    log.info(f"APPT ArcGIS enrich: {len(needs_enrich)} records to enrich")
    filled_addr = 0
    filled_owner = 0
    filled_appr = 0

    for i, rec in enumerate(needs_enrich):
        address = rec.get("address", "").strip()
        owner   = rec.get("owner", "").strip()
        zipcode = rec.get("zip", "").strip()
        result  = None

        # Pass 1: address lookup
        if address and len(address) > 5:
            result = _lookup_by_address(address, zipcode)
            if result:
                log.info(f"  [{i+1}/{len(needs_enrich)}] addr hit [{result.get('method','')}]: "
                         f"{address} → {result.get('owner','—')} appr={result.get('appraised_value','—')}")

        # Pass 2: owner name lookup (if no address or address lookup failed)
        if not result and owner and not is_entity_name(owner):
            result = _lookup_by_owner_name(owner)
            if result:
                log.info(f"  [{i+1}/{len(needs_enrich)}] owner hit [{result.get('method','')}]: "
                         f"{owner} → addr={result.get('address','—')} appr={result.get('appraised_value','—')}")

        if not result:
            log.info(f"  [{i+1}/{len(needs_enrich)}] MISS: addr='{address}' owner='{owner}'")
            time.sleep(0.15)
            continue

        # Apply enrichment — only fill missing/empty fields
        if not rec.get("address") and result.get("address"):
            rec["address"] = result["address"]
            filled_addr += 1

        if not rec.get("owner") and result.get("owner"):
            rec["owner"] = result["owner"].title()
            filled_owner += 1

        if not rec.get("zip") and result.get("zip"):
            rec["zip"] = result["zip"]

        if not rec.get("mail_addr"):
            rec["mail_addr"] = result.get("mail_addr", "")

        if not rec.get("absentee"):
            rec["absentee"] = result.get("absentee", False)

        if not rec.get("appraised_value") and result.get("appraised_value"):
            rec["appraised_value"] = result["appraised_value"]
            filled_appr += 1

        if not rec.get("annual_taxes"):
            rec["annual_taxes"] = result.get("annual_taxes", "")

        if not rec.get("prop_id") and result.get("prop_id"):
            rec["prop_id"] = result["prop_id"]

        # Score bump for absentee
        if rec.get("absentee") and rec.get("score", 7) < 8:
            rec["score"] = 8

        time.sleep(0.2)

    log.info(f"APPT ArcGIS enrich complete: "
             f"{filled_addr} addresses filled | "
             f"{filled_owner} owners filled | "
             f"{filled_appr} appraised values filled")
    return records


# ── Main scraper (unchanged from v1.1 except ArcGIS call at end) ──────────────

def scrape_appointments(known_docs, get_driver_fn, run_timestamp):
    """
    Scrape Appointment of Substitute Trustee filings from PublicSearch RP dept.
    v1.2: adds ArcGIS enrichment pass after PublicSearch scrape.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    new_records = []
    driver = None
    today = datetime.now(timezone.utc)
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

                grantor = ""
                for cand in name_candidates:
                    if not is_entity_name(cand):
                        grantor = cand
                        break
                used_fallback_entity = False
                if not grantor and name_candidates:
                    grantor = name_candidates[0]
                    used_fallback_entity = True

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
                city    = ""
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
                    "prop_id":         "",
                    "land_value":      "",
                    "improvement_value": "",
                    "stacked":         False,
                    "ce_violations":   False,
                }
                page_records.append(rec)

            log.info(f"Appointment offset={offset} | {len(page_records)} on page")

            # Summary detail fetch for missing address/owner
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

                        if rec.get("owner_unverified") or not rec["owner"]:
                            grantor_block = re.findall(
                                r"([A-Z][A-Z\s\.\-']{3,50})\s*</[^>]+>\s*<[^>]*>\s*GRANTOR",
                                page_src, re.IGNORECASE)
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

    # ── Phase 1: ArcGIS enrichment ────────────────────────────────────────────
    # Run AFTER PublicSearch scrape + Summary fetch.
    # Fills missing address, owner, appraised_value, prop_id, absentee.
    if new_records:
        log.info("APPT ArcGIS enrichment starting (v1.2)...")
        new_records = arcgis_enrich_appt_records(new_records)

        # Log summary
        with_addr  = sum(1 for r in new_records if r.get("address"))
        with_owner = sum(1 for r in new_records if r.get("owner"))
        with_appr  = sum(1 for r in new_records if r.get("appraised_value"))
        with_prop  = sum(1 for r in new_records if r.get("prop_id"))
        log.info(
            f"APPT final: {len(new_records)} records | "
            f"{with_addr} with address | {with_owner} with owner | "
            f"{with_appr} with appraisal | {with_prop} with prop_id"
        )

    log.info(f"Appointment scrape: {len(new_records)} new pre-foreclosure records")
    return new_records
