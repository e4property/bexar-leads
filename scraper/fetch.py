"""
Bexar County Motivated Seller Lead Scraper v28.9
HYBRID SCRAPER:
  Primary:   bexar.tx.publicsearch.us  (Selenium, runs 3x daily)
             - 7-day chunks covering 90-day window
             - Inline row-level date skip
             - 180s timeout per page
             - Stops pagination after page 2+ with no new records
             - known_docs loaded from GitHub Pages only
  Secondary: ArcGIS GIS layer (urllib, runs weekly on Sunday)
  Tertiary:  SA 311 Code Enforcement (ArcGIS FeatureServer, runs 3x daily)
             - Filters to motivated-seller violation categories only
             - Deduped by CaseID against known_docs
             - Owner enrichment via Bexar parcel lookup

  Owner enrichment: 5-strategy ArcGIS parcel lookup

  v28.9 change:
    - DOC_FETCH_DAYS extended to 34 to backfill mortgage intel
      (loan_amount, lender, loan_date, trustee) for leads from 4/30/2026 onward.
    - Reverts to 6 after confirmed working.

  v28.8 additions:
    - fetch_doc_details(): reads PublicSearch NTS docs for loan_amount/lender/loan_date/trustee
    - owner enrichment pulls appraised_value/annual_taxes from BCAD

  v28.2 additions:
    - fetch_code_enforcement(): queries SA 311 FeatureServer for distressed-
      property violation categories (absentee, minimum housing, dangerous
      premises, vacant/unsecured structures)
    - Code enforcement records flow through same owner enrichment, duplicate
      detection, scoring, and dashboard pipeline as foreclosure records
    - CE records keyed by "CE-{CaseID}" to avoid collision with doc numbers
    - CE-specific flags: "CODE ENFORCE", "OPEN VIOLATION", "DANGEROUS PREMISES",
      "ABSENTEE PROP", "VACANT STRUCT"
    - score_record() updated to award points for CE source + open status

  v28.1 fix:
    - clean_address() now correctly handles publicsearch no-comma format
      e.g. "7733 CHAMPION CREEK  SAN ANTONIO  TEXAS  78253"
      was returning full string; now returns just "7733 CHAMPION CREEK"
    - parse_city_zip() also handles no-comma format — extracts city and zip
      correctly from double-space-separated fields
    - This fixes: missing leads in dashboard, failed ArcGIS owner lookups,
      and incorrect address display
"""

import json
import logging
import os
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── URLS ──────────────────────────────────────────────────────────────────────
PUBLICSEARCH_BASE  = "https://bexar.tx.publicsearch.us"
FORECLOSURE_BASE   = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
PARCELS_URL        = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"
PAGES_RECORDS      = "https://e4property.github.io/bexar-leads/records.json"
CODE_ENFORCE_URL   = (
    "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest"
    "/services/311_All_Service_Calls/FeatureServer/0"
)

LAYERS = [
    {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
    {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
]

# ── CODE ENFORCEMENT TARGET CATEGORIES ───────────────────────────────────────
CE_CATEGORIES = {
    "AP1": "Absentee Property Assessment",
    "M03": "Minimum Housing: Premises",
    "M04": "Minimum Housing: Interior",
    "M05": "Minimum Housing: Exterior",
    "Z89": "Dangerous Premises: Cut & Clean",
    "Z90": "Dangerous Premises: Secure Only",
    "Z91": "Emergency: Main Structure",
    "Z92": "Emergency: Accessory Structure",
    "Z97": "Emergency: Main & Accessory",
    "Z98": "Dangerous Premises: Clean & Secure",
    "Z99": "BSB Ordered: All",
    "Z82": "Vacant Structure Unsecured",
    "VCB": "Vacant Structure Inventory",
    "H90": "Historic Bldg: No Permits/COA",
}

CE_DANGEROUS = {"Z89", "Z90", "Z91", "Z92", "Z97", "Z98", "Z99"}
CE_VACANT    = {"Z82", "VCB", "H90"}
CE_ABSENTEE  = {"AP1"}
CE_MIN_HOUS  = {"M03", "M04", "M05"}

RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TODAY         = datetime.now(timezone.utc)
TODAY_NAIVE   = datetime.now()
IS_SUNDAY     = TODAY.weekday() == 6
KEEP_DAYS     = 90
CHUNK_DAYS    = 7
PAGE_TIMEOUT  = 180
CUTOFF_DATE   = TODAY_NAIVE - timedelta(days=KEEP_DAYS)

# ── v28.9: Extended to backfill mortgage intel from 4/30/2026 ─────────────────
DOC_FETCH_DAYS = 34


# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/28.9", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                log.debug(f"fetch failed: {e}")
                return {}


def arcgis_query(layer_url, where, fields="*", limit=200):
    all_features = []
    offset = 0
    while True:
        try:
            params = urllib.parse.urlencode({
                "where":             where,
                "outFields":         fields,
                "returnGeometry":    "false",
                "resultOffset":      offset,
                "resultRecordCount": limit,
                "f":                 "json",
            })
            data = fetch_json(f"{layer_url}/query?{params}")
            if not data or "error" in data:
                break
            batch = data.get("features", [])
            all_features.extend(batch)
            if not data.get("exceededTransferLimit", False) or len(batch) < limit:
                break
            offset += len(batch)
        except Exception as e:
            log.debug(f"arcgis_query error: {e}")
            break
    return all_features


def pick(attrs, *candidates, default=""):
    for c in candidates:
        v = attrs.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>", "NULL"):
            return str(v).strip()
    return default


def normalize(s):
    return " ".join(str(s).upper().split())


def load_known_docs():
    url = PAGES_RECORDS + "?nocache=" + str(int(time.time()))
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "BexarScraper/28.9",
                     "Accept": "application/json",
                     "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            prev = json.loads(r.read().decode("utf-8", errors="replace"))
        docs = {str(rec.get("doc_number", "")) for rec in prev if rec.get("doc_number")}
        log.info(f"Loaded {len(docs)} known doc numbers from GitHub Pages")
        return docs, prev
    except Exception as e:
        log.info(f"No previous records found (first run?): {e}")
        return set(), []


def parse_recorded_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except Exception:
        return None


def ms_to_date_str(ms):
    if not ms:
        return ""
    try:
        return datetime.utcfromtimestamp(int(ms) / 1000).strftime("%m/%d/%Y")
    except Exception:
        return ""


# ── RECORD FILTER ─────────────────────────────────────────────────────────────
def should_keep(rec):
    addr = rec.get("address", "").strip().upper()
    if not addr and not rec.get("owner") and not rec.get("sale_date"):
        return False
    if addr in ("N/A", "NA") and not rec.get("owner") and not rec.get("sale_date"):
        return False
    sale_date_str = rec.get("sale_date", "")
    if sale_date_str:
        try:
            if datetime.strptime(sale_date_str.strip(), "%m/%d/%Y") >= TODAY_NAIVE:
                return True
        except Exception:
            pass
    date_filed = rec.get("date_filed", "")
    if date_filed:
        try:
            parts = date_filed.strip().split("/")
            if len(parts) == 2:
                filed_dt = datetime(int(parts[1]), int(parts[0]), 1)
                return filed_dt >= CUTOFF_DATE
        except Exception:
            pass
    if rec.get("source") == "code_enforcement":
        opened = rec.get("opened_date", "")
        if opened:
            try:
                opened_dt = datetime.strptime(opened, "%m/%d/%Y")
                return opened_dt >= CUTOFF_DATE
            except Exception:
                pass
        return True
    return True


# ── SELENIUM SETUP ────────────────────────────────────────────────────────────
def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    try:
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager
        service = ChromeService(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        return webdriver.Chrome(options=opts)


# ── SINGLE CHUNK SCRAPER ──────────────────────────────────────────────────────
def scrape_chunk(driver, known_docs, start_dt, end_dt):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    start_str = start_dt.strftime("%Y%m%d")
    end_str   = end_dt.strftime("%Y%m%d")

    search_url = (
        f"{PUBLICSEARCH_BASE}/results"
        f"?department=FC"
        f"&instrumentDateRange={start_str}%2C{end_str}"
        f"&keywordSearch=false"
        f"&limit=50"
        f"&offset=0"
        f"&sort=desc"
        f"&sortBy=recordedDate"
        f"&sortDir=desc"
    )

    wait    = WebDriverWait(driver, PAGE_TIMEOUT)
    records = []
    page    = 0
    offset  = 0

    while True:
        url = search_url.replace("offset=0", f"offset={offset}")
        log.info(f"    [{start_str}-{end_str}] Page {page+1} (offset={offset})")
        driver.get(url)

        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "td.col-3")))
            time.sleep(2)
        except Exception:
            log.info(f"    Timeout page {page+1} — stopping chunk")
            break

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if not rows:
            col3s = driver.find_elements(By.CSS_SELECTOR, "td.col-3")
            rows  = []
            for cell in col3s:
                try:
                    rows.append(cell.find_element(By.XPATH, ".."))
                except Exception:
                    pass
        if not rows:
            log.info("    No rows — stopping chunk")
            break

        page_new   = 0
        page_old   = 0
        page_known = 0

        for row in rows:
            try:
                def get_col(row, cls):
                    try:
                        return row.find_element(
                            By.CSS_SELECTOR, f"td.{cls}").text.strip()
                    except Exception:
                        return ""

                doc_type_text = get_col(row, "col-3")
                recorded_date = get_col(row, "col-4")
                sale_date     = get_col(row, "col-5")
                doc_number    = get_col(row, "col-6")
                address_raw   = get_col(row, "col-8")

                doc_number  = doc_number.strip()
                sale_date   = sale_date.strip() if sale_date.strip() not in ("N/A", "") else ""

                if not doc_number:
                    continue

                rec_date = parse_recorded_date(recorded_date)
                if rec_date and rec_date < CUTOFF_DATE:
                    page_old += 1
                    continue

                if doc_number in known_docs:
                    page_known += 1
                    continue

                rec_type       = "TAX" if "TAX" in doc_type_text.upper() else "NOF"
                address        = clean_address(address_raw)
                city, zip_code = parse_city_zip(address_raw)
                month, year    = parse_month_year(recorded_date)

                ps_doc_id = ""
                try:
                    link = row.find_element(By.CSS_SELECTOR, "a[href*='/doc/']")
                    href = link.get_attribute("href") or ""
                    import re as _re
                    m = _re.search(r"/doc/(\d+)", href)
                    if m:
                        ps_doc_id = m.group(1)
                except Exception:
                    pass

                rec = {
                    "type":        rec_type,
                    "address":     address,
                    "owner":       "",
                    "mail_addr":   "",
                    "absentee":    False,
                    "duplicate":   False,
                    "is_new":      True,
                    "doc_number":  doc_number,
                    "ps_doc_id":   ps_doc_id,
                    "year":        year,
                    "month":       month,
                    "city":        city,
                    "zip":         zip_code,
                    "school_dist": "",
                    "date_filed":  f"{month}/{year}".strip("/"),
                    "sale_date":   sale_date,
                    "run_ts":      RUN_TIMESTAMP,
                    "flags":       [],
                    "source":      "publicsearch",
                    "lender":      "",
                    "loan_amount": "",
                    "loan_date":   "",
                    "trustee":     "",
                }
                records.append(rec)
                page_new += 1

            except Exception as e:
                log.debug(f"    Row parse error: {e}")

        log.info(f"    Page {page+1}: {page_new} new | {page_known} known | {page_old} old")

        if page_new == 0 and page > 0:
            log.info("    No new records — stopping chunk")
            break

        if page_old > 0 and page_old == len(rows):
            log.info("    Full page of old rows — stopping chunk")
            break

        if len(rows) < 50:
            break

        offset += 50
        page   += 1
        time.sleep(1.5)

    return records


# ── PUBLICSEARCH SCRAPER (chunked) ────────────────────────────────────────────
def scrape_publicsearch(known_docs):
    chunks    = []
    chunk_end = TODAY_NAIVE + timedelta(days=1)
    cutoff    = TODAY_NAIVE - timedelta(days=KEEP_DAYS)

    while chunk_end > cutoff:
        chunk_start = max(chunk_end - timedelta(days=CHUNK_DAYS), cutoff)
        chunks.append((chunk_start, chunk_end))
        chunk_end = chunk_start

    log.info(f"PublicSearch: {len(chunks)} x {CHUNK_DAYS}d chunks = {KEEP_DAYS}d | "
             f"timeout={PAGE_TIMEOUT}s | cutoff={CUTOFF_DATE.strftime('%Y-%m-%d')}")

    all_records = []
    driver      = None

    try:
        driver = get_driver()

        for i, (cs, ce) in enumerate(chunks):
            log.info(f"Chunk {i+1}/{len(chunks)}: "
                     f"{cs.strftime('%Y-%m-%d')} → {ce.strftime('%Y-%m-%d')}")
            chunk_recs = scrape_chunk(driver, known_docs, cs, ce)
            all_records.extend(chunk_recs)
            log.info(f"  Chunk {i+1} done: {len(chunk_recs)} new "
                     f"(total so far: {len(all_records)})")
            if i < len(chunks) - 1:
                time.sleep(2)

    except Exception as e:
        log.error(f"PublicSearch scrape error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    log.info(f"PublicSearch: {len(all_records)} total new records")
    return all_records


# ── ADDRESS PARSING ───────────────────────────────────────────────────────────
def clean_address(raw):
    if not raw:
        return ""
    raw = raw.strip()
    if "," in raw:
        parts = [p.strip() for p in raw.split(",")]
        return parts[0].strip().upper()
    upper = raw.upper()
    upper = re.sub(r'\s+\d{5}(-\d{4})?\s*$', '', upper).strip()
    upper = re.sub(r'\s+[A-Z]{2,}\s*$', '', upper).strip()
    parts = re.split(r'\s{2,}', upper)
    return parts[0].strip() if parts else upper


def parse_city_zip(raw):
    if not raw:
        return "", ""
    raw = raw.strip()
    if "," in raw:
        parts = [p.strip() for p in raw.split(",")]
        city     = ""
        zip_code = ""
        if len(parts) >= 4:
            city = parts[1].strip().upper()
            m    = re.search(r'\b(\d{5})\b', parts[3])
            zip_code = m.group(1) if m else parts[3].strip()
        elif len(parts) == 3:
            city = parts[1].strip().upper()
            m    = re.search(r'\b(\d{5})\b', parts[2])
            zip_code = m.group(1) if m else ""
        return city, zip_code
    upper    = raw.upper()
    zip_m    = re.search(r'\b(\d{5})\b', upper)
    zip_code = zip_m.group(1) if zip_m else ""
    parts    = re.split(r'\s{2,}', upper)
    city     = parts[1].strip() if len(parts) >= 2 else ""
    return city, zip_code


def parse_month_year(date_str):
    try:
        parts = date_str.strip().split("/")
        if len(parts) >= 3:
            return parts[0], parts[2]
        if len(parts) == 2:
            return parts[0], parts[1]
    except Exception:
        pass
    return "", ""


# ── CODE ENFORCEMENT SCRAPER ──────────────────────────────────────────────────
def fetch_code_enforcement(known_docs):
    import json as _json
    import urllib.parse as _up
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    CE_QUERY_BASE = (
        "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest"
        "/services/311_All_Service_Calls/FeatureServer/0/query"
    )
    OUT_FIELDS = "CaseID,Category,ReasonName,TypeName,ObjectDescription,CaseStatus,OpenedDateTime,CouncilDistrict"
    PAGE_SIZE  = 2000
    cutoff_ms  = int(CUTOFF_DATE.timestamp() * 1000)

    log.info(f"Code Enforcement: Selenium ArcGIS fetch | {len(CE_CATEGORIES)} categories | cutoff={CUTOFF_DATE.strftime('%Y-%m-%d')}")

    new_leads = []
    driver    = None

    try:
        driver = get_driver()

        for cat_code, cat_label in CE_CATEGORIES.items():
            cutoff_str = CUTOFF_DATE.strftime("%Y-%m-%d %H:%M:%S")
            where = "Category = '{}' AND OpenedDateTime >= timestamp '{}'".format(cat_code, cutoff_str)
            offset = 0
            page   = 0
            cat_new = 0

            while True:
                params = _up.urlencode({
                    "where":             where,
                    "outFields":         OUT_FIELDS,
                    "orderByFields":     "OpenedDateTime DESC",
                    "returnGeometry":    "false",
                    "resultOffset":      offset,
                    "resultRecordCount": PAGE_SIZE,
                    "f":                 "json",
                })
                url = f"{CE_QUERY_BASE}?{params}"

                try:
                    driver.get(url)
                    time.sleep(1.5)
                    body = driver.find_element(By.TAG_NAME, "body").text
                    data = _json.loads(body)
                except Exception as e:
                    log.warning(f"  CE [{cat_code}] page {page+1} error: {e}")
                    break

                if "error" in data:
                    log.warning(f"  CE [{cat_code}] query error: {data['error']}")
                    break

                features = data.get("features", [])

                for feat in features:
                    a = feat.get("attributes", {})
                    case_id = a.get("CaseID")
                    if not case_id:
                        continue
                    doc_key = f"CE-{case_id}"
                    if doc_key in known_docs:
                        continue

                    category   = (a.get("Category")          or "").strip().upper()
                    reason     = (a.get("ReasonName")        or "").strip()
                    type_name  = (a.get("TypeName")          or "").strip()
                    addr_raw   = (a.get("ObjectDescription") or "").strip()
                    status     = (a.get("CaseStatus")        or "").strip()
                    opened_ms  = a.get("OpenedDateTime")
                    district   = a.get("CouncilDistrict", "")

                    address        = clean_address(addr_raw)
                    city, zip_code = parse_city_zip(addr_raw)
                    opened_str     = ms_to_date_str(opened_ms)
                    month, year    = parse_month_year(opened_str) if opened_str else ("", "")

                    if not address or address.upper() in ("", "N/A", "NA", "UNKNOWN"):
                        continue

                    rec = {
                        "type":        "CE",
                        "address":     address,
                        "owner":       "",
                        "mail_addr":   "",
                        "absentee":    False,
                        "duplicate":   False,
                        "is_new":      True,
                        "doc_number":  doc_key,
                        "year":        year,
                        "month":       month,
                        "city":        city,
                        "zip":         zip_code,
                        "school_dist": "",
                        "date_filed":  f"{month}/{year}".strip("/"),
                        "sale_date":   "",
                        "run_ts":      RUN_TIMESTAMP,
                        "flags":       [],
                        "source":      "code_enforcement",
                        "ce_case_id":  str(case_id),
                        "ce_category": category,
                        "ce_reason":   reason,
                        "ce_type":     type_name,
                        "ce_status":   status,
                        "opened_date": opened_str,
                        "ce_district": str(district) if district else "",
                        "ce_cat_label": cat_label,
                    }
                    new_leads.append(rec)
                    known_docs.add(doc_key)
                    cat_new += 1

                if len(features) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE
                page   += 1
                time.sleep(0.3)

            if cat_new > 0:
                log.info(f"  CE [{cat_code}] {cat_label}: {cat_new} new")

    except Exception as e:
        log.error(f"Code Enforcement scrape error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    total = len(new_leads)
    log.info(f"Code Enforcement: {total} new leads fetched")
    return new_leads


# ── ARCGIS BACKFILL (weekly) ──────────────────────────────────────────────────
def fetch_arcgis_backfill(known_docs):
    log.info("ArcGIS weekly backfill starting...")
    raw = []

    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{FORECLOSURE_BASE}/{idx}"
        log.info(f"  Layer {idx} ({layer['label']})...")
        features, offset = [], 0

        while True:
            try:
                params = urllib.parse.urlencode({
                    "where": "1=1", "outFields": "*",
                    "returnGeometry": "false",
                    "resultOffset": offset,
                    "resultRecordCount": 1000,
                    "f": "json",
                })
                data  = fetch_json(f"{layer_url}/query?{params}")
                batch = data.get("features", [])
                features.extend(batch)
                log.info(f"    offset={offset}: {len(batch)} (total={len(features)})")
                if len(batch) < 1000:
                    break
                offset += len(batch)
            except Exception as e:
                log.error(f"Layer {idx} error: {e}")
                break

        for feat in features:
            a     = feat["attributes"]
            month = pick(a, "MONTH", "MO", default="")
            year  = pick(a, "YEAR",  "YR", default="")
            doc   = pick(a, "DOC_NUMBER", "DOCNUM", "DOC_NUM")
            if doc in known_docs:
                continue
            raw.append({
                "type":        layer["type"],
                "address":     pick(a, "ADDRESS", "SITUS_ADD", "ADDR"),
                "owner":       "",
                "mail_addr":   "",
                "absentee":    False,
                "duplicate":   False,
                "is_new":      True,
                "doc_number":  doc,
                "year":        year,
                "month":       month,
                "city":        pick(a, "CITY", "MAIL_CITY", default=""),
                "zip":         pick(a, "ZIP", "ZIPCODE", "ZIP_CODE", default=""),
                "school_dist": pick(a, "SCHOOL_DIST", default=""),
                "date_filed":  f"{month}/{year}".strip("/"),
                "sale_date":   "",
                "run_ts":      RUN_TIMESTAMP,
                "flags":       [],
                "source":      "arcgis",
            })
            known_docs.add(doc)

    log.info(f"ArcGIS backfill: {len(raw)} new records")
    return raw


# ── DOCUMENT DETAIL FETCHER ───────────────────────────────────────────────────
def fetch_doc_details(records, driver):
    """
    For new leads filed within DOC_FETCH_DAYS, load the PublicSearch document
    page and extract mortgage details from the SUMMARY tab.
    Fields: lender, loan_amount, loan_date, trustee
    v28.9: DOC_FETCH_DAYS = 34 to backfill from 4/30/2026.

    KEY CHANGE: also re-enriches EXISTING leads (is_new=False) that are missing
    loan_amount, so backfill works on the current 691 records too.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import re as _re

    cutoff = TODAY_NAIVE - timedelta(days=DOC_FETCH_DAYS)

    # Include both new AND existing leads missing loan data, within window
    candidates = [
        r for r in records
        if r.get("ps_doc_id")
        and r.get("source") == "publicsearch"
        and r.get("type") in ("NOF", "TAX")
        and not r.get("loan_amount")  # only those missing data
    ]

    recent = []
    for r in candidates:
        date_filed = r.get("date_filed", "")
        try:
            parts = date_filed.split("/")
            if len(parts) == 2:
                filed_dt = datetime(int(parts[1]), int(parts[0]), 1)
                if filed_dt >= cutoff:
                    recent.append(r)
            else:
                recent.append(r)
        except Exception:
            recent.append(r)

    if not recent:
        log.info(f"Doc fetch: no leads within {DOC_FETCH_DAYS}d window missing loan data — skipping")
        return records

    log.info(f"Doc fetch: enriching {len(recent)} leads with mortgage intel (window={DOC_FETCH_DAYS}d)...")
    fetched = 0

    for rec in recent:
        ps_id   = rec["ps_doc_id"]
        doc_num = rec["doc_number"]
        url     = f"{PUBLICSEARCH_BASE}/doc/{ps_id}"
        log.info(f"  Doc [{doc_num}] id={ps_id}")

        try:
            driver.get(url)
            time.sleep(3)

            try:
                tabs = driver.find_elements(By.CSS_SELECTOR, ".tab, [role='tab'], .nav-link, button")
                for tab in tabs:
                    if "summary" in (tab.text or "").lower():
                        tab.click()
                        time.sleep(1.5)
                        break
            except Exception:
                pass

            page_text = driver.find_element(By.TAG_NAME, "body").text

            loan_amount = ""
            m = _re.search(
                r"(?:original|principal)\s+amount\s+of\s+\$([\d,]+(?:\.\d{2})?)",
                page_text, _re.IGNORECASE)
            if m:
                loan_amount = "$" + m.group(1)

            loan_date = ""
            m = _re.search(
                r"(?:deed of trust|note|lien)\s+dated\s+([A-Za-z]+ \d{1,2},\s*\d{4}|\d{1,2}/\d{1,2}/\d{4})",
                page_text, _re.IGNORECASE)
            if m:
                loan_date = m.group(1).strip()

            lender = ""
            lender_labels = ["Original Mortgage", "Original Mortgagee", "Lender", "Beneficiary", "Mortgagee"]
            lines = page_text.split("\n")
            for i, line in enumerate(lines):
                for lbl in lender_labels:
                    if lbl.lower() in line.lower():
                        val = line.split(":")[-1].strip() if ":" in line else ""
                        if not val and i+1 < len(lines):
                            val = lines[i+1].strip()
                        if val:
                            lender = val
                            break
                if lender:
                    break
            if not lender:
                m = _re.search(r"nominee for ([^,]+)", page_text, _re.IGNORECASE)
                if m:
                    lender = m.group(1).strip()

            trustee = ""
            trustee_labels = ["Original Trustee", "Substitute Trustee", "Trustee"]
            for i, line in enumerate(lines):
                for lbl in trustee_labels:
                    if lbl.lower() in line.lower():
                        val = line.split(":")[-1].strip() if ":" in line else ""
                        if not val and i+1 < len(lines):
                            val = lines[i+1].strip()
                        if val:
                            trustee = val
                            break
                if trustee:
                    break

            rec["loan_amount"] = loan_amount
            rec["loan_date"]   = loan_date
            rec["lender"]      = lender
            rec["trustee"]     = trustee
            fetched += 1

            log.info(f"  → amount={loan_amount or '—'} | lender={lender[:40] if lender else '—'} | date={loan_date or '—'}")
            time.sleep(1)

        except Exception as e:
            log.warning(f"  Doc [{doc_num}] fetch error: {e}")

    log.info(f"Doc fetch: {fetched}/{len(recent)} enriched")
    return records


# ── OWNER ENRICHMENT ──────────────────────────────────────────────────────────
def parse_address_parts(address):
    if not address:
        return None
    parts = address.strip().upper().split()
    if not parts or not parts[0].isdigit():
        return None
    num  = parts[0]
    rest = parts[1:]
    SUFFIXES = {
        "ST","AVE","DR","RD","LN","CT","CIR","BLVD","WAY","PL","TRL","PKWY",
        "HWY","LOOP","PASS","CV","PT","HLS","TRAIL","GROVE","RIDGE","CREEK",
        "LAKE","PARK","GLEN","RUN","XING","STREET","AVENUE","DRIVE","ROAD",
        "LANE","COURT","CIRCLE","BOULEVARD","PARKWAY","HIGHWAY",
    }
    words  = rest[:]
    suffix = ""
    if words and words[-1] in SUFFIXES:
        suffix = words.pop()
    return {"num": num, "street": " ".join(rest), "words": words,
            "suffix": suffix, "full": address.strip().upper()}


def match_features(feats, num, required_word=None):
    for feat in feats:
        a       = feat.get("attributes", {})
        owner   = str(a.get("Owner",    "") or "").strip()
        situs   = str(a.get("Situs",    "") or "").strip()
        addr1   = str(a.get("AddrLn1",  "") or "").strip()
        city    = str(a.get("AddrCity", "") or "").strip()
        zipcode = str(a.get("Zip",      "") or "").strip()

        if not owner or owner.upper() in ("NULL", "NONE", ""):
            continue
        situs_norm = normalize(situs)
        if not situs_norm.startswith(num + " "):
            continue
        if required_word and required_word not in situs_norm:
            continue

        mail_addr = ""
        if addr1 and addr1.upper() not in ("NULL", "NONE", ""):
            mail_addr = f"{addr1} {city} {zipcode}".strip()
        absentee = bool(mail_addr) and not normalize(mail_addr).startswith(num + " ")

        appraised = str(a.get("AppraisedVal", "") or a.get("Appraised", "") or "").strip()
        land_val  = str(a.get("LandVal", "") or "").strip()
        impr_val  = str(a.get("ImprovVal", "") or "").strip()
        tax_amt   = str(a.get("TaxAmt", "") or a.get("TaxAmount", "") or "").strip()

        return {
            "owner":             owner.upper(),
            "mail_addr":         mail_addr,
            "absentee":          absentee,
            "appraised_value":   appraised,
            "land_value":        land_val,
            "improvement_value": impr_val,
            "annual_taxes":      tax_amt,
        }
    return None


def lookup_owner(address, zipcode=""):
    parsed = parse_address_parts(address)
    if not parsed:
        return {}
    num        = parsed["num"]
    words      = parsed["words"]
    first_word = words[0] if words else ""
    FIELDS     = "Situs,Owner,AddrLn1,AddrCity,Zip,AppraisedVal,LandVal,ImprovVal,TaxAmt"

    if len(words) >= 2:
        r = match_features(
            arcgis_query(PARCELS_URL, f"Situs LIKE '{num} {words[0]} {words[1]}%'",
                         fields=FIELDS, limit=50), num, first_word)
        if r: r["method"] = "s1_two_word"; return r

    if first_word and len(first_word) >= 3:
        r = match_features(
            arcgis_query(PARCELS_URL, f"Situs LIKE '{num} {first_word}%'",
                         fields=FIELDS, limit=100), num, first_word)
        if r: r["method"] = "s2_first_word"; return r

    r = match_features(
        arcgis_query(PARCELS_URL, f"Situs LIKE '{num} %'",
                     fields=FIELDS, limit=200), num, first_word or None)
    if r: r["method"] = "s3_num_only"; return r

    if zipcode and len(zipcode) >= 5:
        r = match_features(
            arcgis_query(PARCELS_URL, f"Zip = '{zipcode[:5]}'",
                         fields=FIELDS, limit=1000), num, None)
        if r: r["method"] = "s4_zip_scan"; return r

    for word in words[1:]:
        if len(word) < 4:
            continue
        r = match_features(
            arcgis_query(PARCELS_URL, f"Situs LIKE '{num} %{word}%'",
                         fields=FIELDS, limit=100), num, word)
        if r: r["method"] = "s5_alt_word"; return r

    return {}


def enrich_owners(records):
    missing = [r for r in records
               if not r.get("owner")
               and r.get("address", "").strip().upper() not in ("", "N/A", "NA")]
    log.info(f"Owner enrichment: {len(missing)} records need lookup")
    found = 0
    for i, rec in enumerate(missing):
        addr = rec.get("address", "")
        zip_ = rec.get("zip", "")
        result = lookup_owner(addr, zip_)
        if result and result.get("owner"):
            rec["owner"]             = result["owner"]
            rec["mail_addr"]         = result.get("mail_addr", "")
            rec["absentee"]          = result.get("absentee", False)
            rec["appraised_value"]   = result.get("appraised_value", "")
            rec["annual_taxes"]      = result.get("annual_taxes", "")
            rec["land_value"]        = result.get("land_value", "")
            found += 1
            if found <= 10 or found % 25 == 0:
                log.info(f"  [{i+1}/{len(missing)}] {addr} -> {result['owner']} "
                         f"[{result.get('method','')}]")
        else:
            log.debug(f"  [{i+1}/{len(missing)}] No match: {addr}")
        time.sleep(0.2)
    log.info(f"Owner enrichment: {found}/{len(missing)} filled")
    return records


# ── DUPLICATE DETECTION ───────────────────────────────────────────────────────
def detect_duplicates(records):
    from collections import Counter
    counts = Counter(
        r["owner"].upper().strip()
        for r in records
        if r.get("owner") and r["owner"].upper().strip() not in ("", "NULL")
    )
    dupes = 0
    for r in records:
        key = (r.get("owner") or "").upper().strip()
        if key and counts[key] > 1:
            r["duplicate"] = True
            dupes += 1
    log.info(f"Duplicate owners flagged: {dupes}")
    return records


# ── SCORING ───────────────────────────────────────────────────────────────────
def score_record(rec):
    s = 0
    if rec.get("address"):                   s += 3
    if rec.get("owner"):                     s += 3
    if rec.get("type") == "TAX":             s += 2
    if rec.get("absentee"):                  s += 2
    if rec.get("sale_date"):                 s = min(s + 1, 10)
    if rec.get("source") == "code_enforcement":
        s += 1
        cat = rec.get("ce_category", "")
        if cat in CE_DANGEROUS:              s += 2
        if cat in CE_ABSENTEE:              s += 2
        if rec.get("ce_status", "").upper() == "OPEN":
            s += 1
    return min(s, 10)


def days_until_sale(sale_date_str):
    try:
        delta = (datetime.strptime(sale_date_str.strip(), "%m/%d/%Y") - datetime.now()).days
        return max(delta, 0)
    except Exception:
        return None


# ── DASHBOARD ─────────────────────────────────────────────────────────────────
def build_dashboard(records):
    os.makedirs("dashboard", exist_ok=True)
    clean    = [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]
    json_str = json.dumps(clean, separators=(",", ":"), ensure_ascii=True)
    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        f.write(json_str)
    with open("dashboard/index.html", "w", encoding="utf-8") as f:
        f.write('<!DOCTYPE html><html><head><meta charset="UTF-8"/>'
                '<meta http-equiv="refresh" content="0;url=leads.html"/>'
                '<title>Redirecting...</title></head>'
                '<body><script>window.location.href="leads.html";</script></body></html>')
    log.info(f"Dashboard: {len(clean)} records, "
             f"{os.path.getsize('dashboard/records.json'):,} bytes")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("data",      exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    log.info("=" * 60)
    log.info("Bexar County Lead Scraper v28.9 (Hybrid)")
    log.info(f"Primary:   PublicSearch.us ({KEEP_DAYS}d window, {CHUNK_DAYS}d chunks, {PAGE_TIMEOUT}s timeout)")
    log.info(f"Secondary: ArcGIS weekly backfill = {IS_SUNDAY}")
    log.info(f"Tertiary:  Code Enforcement 311 ({len(CE_CATEGORIES)} categories, {KEEP_DAYS}d window)")
    log.info(f"Doc fetch: backfill window = {DOC_FETCH_DAYS}d (covers 4/30/2026 onward)")
    log.info(f"Filter:    {KEEP_DAYS}-day cutoff ({CUTOFF_DATE.strftime('%Y-%m-%d')}) | live auctions always kept")
    log.info("=" * 60)

    known_docs, prev_records = load_known_docs()

    # ── Step 1: PublicSearch chunked scrape ───────────────────────────────────
    new_records = scrape_publicsearch(known_docs)

    # ── Step 1b: Doc detail fetch — new AND existing leads missing loan data ──
    doc_driver = None
    try:
        doc_driver = get_driver()
        # Pass full prev_records + new_records so backfill hits existing leads too
        all_for_doc_fetch = new_records + prev_records
        all_for_doc_fetch = fetch_doc_details(all_for_doc_fetch, doc_driver)
        # Separate back out — prev_records were mutated in place
        new_records = [r for r in all_for_doc_fetch if r.get("is_new")]
    except Exception as e:
        log.warning(f"Doc fetch driver error: {e}")
    finally:
        if doc_driver:
            try:
                doc_driver.quit()
            except Exception:
                pass

    # ── Step 2: ArcGIS weekly backfill (Sundays only) ────────────────────────
    arcgis_records = []
    if IS_SUNDAY:
        arcgis_records = fetch_arcgis_backfill(known_docs)
        log.info(f"ArcGIS backfill added {len(arcgis_records)} records")

    # ── Step 3: Code Enforcement (every run) ─────────────────────────────────
    ce_records = fetch_code_enforcement(known_docs)
    log.info(f"Code Enforcement added {len(ce_records)} records")

    # ── Step 4: Merge ─────────────────────────────────────────────────────────
    for r in prev_records:
        r["is_new"] = False
    seen = {}
    for r in new_records + arcgis_records + ce_records + prev_records:
        doc = r.get("doc_number", "")
        if doc and doc not in seen:
            seen[doc] = r
    records = list(seen.values())
    log.info(f"After dedup: {len(records)} total records")

    # ── Step 5: 90-day filter ─────────────────────────────────────────────────
    before  = len(records)
    records = [r for r in records if should_keep(r)]
    log.info(f"After filter: {len(records)} kept, {before - len(records)} dropped")

    # ── Step 6: Owner enrichment ──────────────────────────────────────────────
    records = enrich_owners(records)

    # ── Step 7: Duplicate detection ───────────────────────────────────────────
    records = detect_duplicates(records)

    # ── Step 8: Flag + score ──────────────────────────────────────────────────
    for r in records:
        r["flags"] = []
        if r["type"] == "TAX":                    r["flags"].append("TAX FORE")
        if r["type"] == "CE":                     r["flags"].append("CODE ENFORCE")
        if r.get("absentee"):                     r["flags"].append("ABSENTEE")
        if r.get("duplicate"):                    r["flags"].append("DUPLICATE")
        if r.get("is_new"):                       r["flags"].append("NEW")
        if not r.get("owner"):                    r["flags"].append("NO OWNER")
        if r.get("sale_date"):                    r["flags"].append("HAS SALE DATE")
        d = days_until_sale(r.get("sale_date", ""))
        if d is not None and d <= 30:             r["flags"].append("AUCTION SOON")
        if d is not None and d <= 14:             r["flags"].append("URGENT")
        if r.get("source") == "code_enforcement":
            cat = r.get("ce_category", "")
            if r.get("ce_status", "").upper() == "OPEN":
                r["flags"].append("OPEN VIOLATION")
            if cat in CE_DANGEROUS:               r["flags"].append("DANGEROUS PREMISES")
            if cat in CE_ABSENTEE:               r["flags"].append("ABSENTEE PROP")
            if cat in CE_VACANT:                  r["flags"].append("VACANT STRUCT")
            if cat in CE_MIN_HOUS:                r["flags"].append("MIN HOUSING")
        r["score"]           = score_record(r)
        r["days_until_sale"] = d

    def sort_key(r):
        d = r.get("days_until_sale")
        u = 0 if (d is not None and d <= 14) else (1 if (d is not None and d <= 30) else 2)
        return (u, -r["score"], d if d is not None else 9999)

    records.sort(key=sort_key)

    # ── Step 9: Summary ───────────────────────────────────────────────────────
    named    = sum(1 for r in records if r.get("owner"))
    absentee = sum(1 for r in records if r.get("absentee"))
    new_ct   = sum(1 for r in records if r.get("is_new"))
    urgent   = sum(1 for r in records if "URGENT"       in r.get("flags", []))
    soon     = sum(1 for r in records if "AUCTION SOON" in r.get("flags", []))
    has_date = sum(1 for r in records if r.get("sale_date"))
    ce_ct    = sum(1 for r in records if r.get("source") == "code_enforcement")
    ce_open  = sum(1 for r in records if "OPEN VIOLATION" in r.get("flags", []))
    ce_dang  = sum(1 for r in records if "DANGEROUS PREMISES" in r.get("flags", []))
    enriched = sum(1 for r in records if r.get("loan_amount"))

    log.info(f"Final: {len(records)} total | {named} named | {absentee} absentee")
    log.info(f"       {new_ct} new | {has_date} with sale date | "
             f"{soon} auction <=30d | {urgent} URGENT <=14d")
    log.info(f"       CE: {ce_ct} total | {ce_open} open violations | {ce_dang} dangerous premises")
    log.info(f"       Mortgage intel: {enriched} leads with loan_amount populated")

    # ── Step 10: Save ─────────────────────────────────────────────────────────
    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    build_dashboard(records)
    log.info("Done.")
