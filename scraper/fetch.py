"""
Bexar County Motivated Seller Lead Scraper v28.20
HYBRID SCRAPER:
  Primary:   bexar.tx.publicsearch.us  (Selenium, runs 3x daily)
  Secondary: ArcGIS GIS layer (urllib, runs weekly on Sunday)
  Tertiary:  SA 311 Code Enforcement (ArcGIS FeatureServer, runs 3x daily)
  Owner enrichment: 5-strategy ArcGIS parcel lookup

  v28.11 fixes:
    - TotVal used for appraised value (confirmed from BCAD field probe)
    - lookup_ps_doc_id(): searches PublicSearch by doc number to resolve
      internal ID for existing leads that were scraped before ps_doc_id existed
    - fetch_doc_details: regex patterns updated to match actual rendered HTML
      doc format (Deed of Trust dated, nominee for, Trustor, Current Beneficiary)
    - Loan amount still page 2 scanned image — skipped; all other fields work

  v28.10: ps_doc_id 3-strategy capture, flexible BCAD field matching
  v28.9: DOC_FETCH_DAYS=34 backfill, re-enriches existing leads
  v28.8: fetch_doc_details(), BCAD appraised_value/annual_taxes
  v28.2: Code enforcement 311 scraper
  v28.1: Address parsing fixes
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

# ── Reverted to 6 days — PublicSearch blocks headless login, loan amounts
# ── sourced from BCAD TotVal (appraised) instead. Re-enable when login solved.
DOC_FETCH_DAYS = 6


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
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")
    # Anti-detection
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

    # Patch navigator.webdriver to undefined
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
        """
    })
    return driver


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

                import re as _re
                ps_doc_id = ""
                # S1: direct href link
                try:
                    link = row.find_element(By.CSS_SELECTOR, "a[href*='/doc/']")
                    href = link.get_attribute("href") or ""
                    m = _re.search(r"/doc/(\d+)", href)
                    if m:
                        ps_doc_id = m.group(1)
                except Exception:
                    pass
                # S2: scan all element attributes for doc id
                if not ps_doc_id:
                    try:
                        for el in row.find_elements(By.CSS_SELECTOR, "a,button,[onclick]"):
                            for attr in ["href","onclick","data-id","data-href"]:
                                try:
                                    val = el.get_attribute(attr) or ""
                                    m = _re.search(r"/doc/(\d+)", val)
                                    if m:
                                        ps_doc_id = m.group(1)
                                        break
                                except Exception:
                                    pass
                            if ps_doc_id:
                                break
                    except Exception:
                        pass
                # S3: click row, read URL, come back
                if not ps_doc_id:
                    try:
                        cur = driver.current_url
                        row.click()
                        time.sleep(2)
                        m = _re.search(r"/doc/(\d+)", driver.current_url)
                        if m:
                            ps_doc_id = m.group(1)
                        driver.get(cur)
                        time.sleep(2)
                    except Exception:
                        pass
                log.debug(f"    ps_doc_id [{doc_number}]: {ps_doc_id or 'NOT FOUND'}")

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
    """
    Fetch CE leads from ArcGIS FeatureServer via direct urllib JSON queries.
    No Selenium needed — pure HTTP JSON API, paginated by motivated TYPENAME.
    """
    import urllib.request, urllib.parse, json as _json

    CE_API = (
        "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services"
        "/311_All_Service_Calls/FeatureServer/0/query"
    )
    FIELDS = "CASEID,Category,ReasonName,TypeName,CaseStatus,OpenedDateTime,ObjectDescription,CouncilDistrict"
    PAGE   = 2000
    cutoff_ms = int(CUTOFF_DATE.timestamp() * 1000)

    # Filter locally after fetching — TypeName keywords from confirmed CSV data
    MOTIVATED_KEYWORDS = [
        "dangerous premise", "property structure", "vacant", "structure maintenance",
        "minimum housing", "substandard", "unsecured", "condemned",
    ]

    log.info(f"Code Enforcement: ArcGIS urllib JSON | Category=Property Maintenance | cutoff={CUTOFF_DATE.strftime('%Y-%m-%d')}")
    new_leads = []
    skipped   = 0

    # Single broad query — filter by Category and date only, no TypeName in SQL
    where  = f"Category = 'Property Maintenance' AND OpenedDateTime >= {cutoff_ms}"
    offset = 0

    while True:
        params = urllib.parse.urlencode({
            "where":             where,
            "outFields":         FIELDS,
            "orderByFields":     "OpenedDateTime DESC",
            "returnGeometry":    "false",
            "resultOffset":      offset,
            "resultRecordCount": PAGE,
            "f":                 "json",
        })
        url = f"{CE_API}?{params}"

        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; BexarLeads/1.0)",
                "Accept":     "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                data = _json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            log.warning(f"CE API fetch error: {e}")
            break

        if "error" in data:
            log.warning(f"CE API error: {data['error']}")
            break

        features = data.get("features", [])
        log.info(f"  CE page offset={offset}: {len(features)} records")

        for feat in features:
            a = feat.get("attributes", {})
            case_id  = a.get("CASEID") or a.get("CaseID")
            if not case_id:
                skipped += 1
                continue

            ce_key = f"CE-{case_id}"
            if ce_key in known_docs:
                skipped += 1
                continue

            typename = (a.get("TypeName") or "").strip()
            status   = (a.get("CaseStatus") or "").strip()
            reason   = (a.get("ReasonName") or "").strip()
            addr_raw = (a.get("ObjectDescription") or "").strip()
            district = str(a.get("CouncilDistrict") or "")
            opened_ms_val = a.get("OpenedDateTime")

            # Local filter: motivated seller signal
            tl = typename.lower()
            if not any(kw in tl for kw in MOTIVATED_KEYWORDS):
                skipped += 1
                continue

            # Parse address
            addr_clean = addr_raw.strip().lstrip()
            parts  = [p.strip() for p in addr_clean.split(",")]
            street  = parts[0].upper() if parts else ""
            city    = parts[1].strip().upper() if len(parts) >= 2 else "SAN ANTONIO"
            zipcode = ""
            if len(parts) >= 3:
                zm = re.search(r"\b(\d{5})\b", parts[2])
                zipcode = zm.group(1) if zm else parts[2].strip()

            if not street or not re.match(r"^\d+\s", street):
                skipped += 1
                continue

            opened_str = ms_to_date_str(opened_ms_val) if opened_ms_val else ""
            month, year = parse_month_year(opened_str) if opened_str else ("", "")

            flags = ["CODE ENFORCE"]
            if "dangerous" in tl:            flags.append("DANGEROUS PREMISES")
            if status.lower() == "open":     flags.append("OPEN VIOLATION")
            if "vacant" in tl:               flags.append("VACANT STRUCT")

            score = 3
            if "DANGEROUS PREMISES" in flags: score += 3
            if "OPEN VIOLATION"     in flags: score += 2
            if "VACANT STRUCT"      in flags: score += 2

            lead = {
                "doc_number":      ce_key,
                "type":            "CE",
                "source":          "code_enforcement",
                "address":         street,
                "city":            city,
                "zip":             zipcode,
                "date_filed":      f"{month}/{year}".strip("/"),
                "sale_date":       "",
                "owner":           "",
                "mail_addr":       "",
                "absentee":        False,
                "duplicate":       False,
                "is_new":          True,
                "run_ts":          RUN_TIMESTAMP,
                "flags":           flags,
                "score":           score,
                "ce_case_id":      str(case_id),
                "ce_category":     "Property Maintenance",
                "ce_cat_label":    typename,
                "ce_status":       status,
                "ce_reason":       reason,
                "ce_district":     district,
                "opened_date":     opened_str,
                "loan_amount":     "",
                "loan_date":       "",
                "lender":          "",
                "trustee":         "",
                "appraised_value": "",
                "annual_taxes":    "",
                "ps_doc_id":       "",
            }
            new_leads.append(lead)
            known_docs.add(ce_key)

        if len(features) < PAGE:
            break
        offset += PAGE

    log.info(f"Code Enforcement: {len(new_leads)} new leads fetched ({skipped} skipped)")
    dangerous = sum(1 for l in new_leads if "DANGEROUS PREMISES" in l.get("flags", []))
    open_viol = sum(1 for l in new_leads if "OPEN VIOLATION" in l.get("flags", []))
    log.info(f"CE breakdown: {open_viol} open violations | {dangerous} dangerous premises")
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


def login_publicsearch(driver):
    """Log in to PublicSearch using clerk credentials from environment."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    email    = os.environ.get("CLERK_EMAIL", "")
    password = os.environ.get("CLERK_PASSWORD", "")
    if not email or not password:
        log.warning("No CLERK_EMAIL/CLERK_PASSWORD — skipping login")
        return False
    try:
        driver.set_page_load_timeout(20)
        driver.get(f"{PUBLICSEARCH_BASE}/login")
        time.sleep(4)

        # Log page title so we know what loaded
        log.info(f"Login page title: {driver.title} | url: {driver.current_url}")

        # Try all common input selectors
        email_el = None
        for sel in ["input[type='email']", "input[name='email']",
                    "input[name='username']", "input[placeholder*='mail']",
                    "input[placeholder*='ser']", "input:not([type='password']):not([type='hidden'])"]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    email_el = els[0]
                    log.info(f"  Found email field: {sel}")
                    break
            except Exception:
                pass

        if not email_el:
            log.warning("  No email field found — logging page inputs")
            for inp in driver.find_elements(By.CSS_SELECTOR, "input"):
                log.info(f"    input type={inp.get_attribute('type')} name={inp.get_attribute('name')} id={inp.get_attribute('id')}")
            return False

        email_el.clear()
        email_el.send_keys(email)

        pass_el = None
        for sel in ["input[type='password']", "input[name='password']", "input[placeholder*='ass']"]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    pass_el = els[0]
                    break
            except Exception:
                pass

        if not pass_el:
            log.warning("  No password field found")
            return False

        pass_el.clear()
        pass_el.send_keys(password)

        # Click submit
        submitted = False
        for sel in ["button[type='submit']", "input[type='submit']", "button.login", "button.submit", "button"]:
            try:
                btns = driver.find_elements(By.CSS_SELECTOR, sel)
                for btn in btns:
                    txt = (btn.text or "").lower()
                    if any(x in txt for x in ["sign in","login","log in","submit",""]):
                        btn.click()
                        submitted = True
                        break
            except Exception:
                pass
            if submitted:
                break

        if not submitted:
            pass_el.submit()

        time.sleep(4)
        log.info(f"Post-login url: {driver.current_url}")
        if "login" not in driver.current_url.lower():
            log.info("PublicSearch login OK")
            return True
        log.warning("PublicSearch login failed — still on login page")
        return False
    except Exception as e:
        log.warning(f"PublicSearch login error: {e}")
        return False


def lookup_ps_doc_id(doc_number, driver):
    """
    Look up PublicSearch internal doc ID by doc number.
    Uses keyword search to find exact doc, reads internal ID from URL.
    """
    from selenium.webdriver.common.by import By
    import re as _re
    try:
        driver.set_page_load_timeout(15)
        url = (f"{PUBLICSEARCH_BASE}/results?department=FC"
               f"&limit=10&offset=0&sort=desc&sortBy=recordedDate"
               f"&docNumber={doc_number}")
        driver.get(url)
        time.sleep(3)

        # Strategy 1: find any link with /doc/ and verify doc number nearby
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/doc/']")
        for link in links:
            href = link.get_attribute("href") or ""
            m = _re.search(r"/doc/(\d+)", href)
            if m:
                # Verify the row contains our doc number
                try:
                    row = link.find_element(By.XPATH, "ancestor::tr")
                    if doc_number in (row.text or ""):
                        return m.group(1)
                except Exception:
                    return m.group(1)  # take it if we can't verify

        # Strategy 2: click first row, read URL, verify, come back
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        for row in rows:
            if doc_number in (row.text or ""):
                row.click()
                time.sleep(2)
                m = _re.search(r"/doc/(\d+)", driver.current_url)
                if m:
                    return m.group(1)
                break

    except Exception as e:
        log.debug(f"  ps_doc_id lookup error [{doc_number}]: {e}")
    return ""
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

    # Candidates: any NOF/TAX lead missing loan data within window
    # Relax source check — older records may not have source field
    candidates = [
        r for r in records
        if r.get("type") in ("NOF", "TAX")
        and not r.get("loan_amount")
        and r.get("source", "publicsearch") == "publicsearch"
    ]
    log.info(f"Doc fetch: {len(candidates)} candidates missing loan data")

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

    # Cap at 20 per run to avoid long runtimes
    recent = recent[:20]
    log.info(f"Doc fetch: {len(recent)} within {DOC_FETCH_DAYS}d window (capped at 20)")

    if not recent:
        log.info(f"Doc fetch: no leads within {DOC_FETCH_DAYS}d window missing loan data — skipping")
        return records

    # No login needed — doc pages are publicly accessible without auth.
    # Login page errors on server IPs but doc pages load fine.

    # ── Resolve missing ps_doc_ids via search ─────────────────────────────────
    missing_id = [r for r in recent if not r.get("ps_doc_id")]
    if missing_id:
        log.info(f"Doc fetch: resolving ps_doc_id for {len(missing_id)} leads...")
        seen_ids = set()
        for r in missing_id:
            pid = lookup_ps_doc_id(r["doc_number"], driver)
            if pid:
                if pid in seen_ids:
                    log.warning(f"  DUPLICATE ID [{r['doc_number']}] → {pid} — skipping")
                    continue
                r["ps_doc_id"] = pid
                seen_ids.add(pid)
                log.info(f"  Resolved [{r['doc_number']}] → {pid}")
            else:
                log.info(f"  No ID found for [{r['doc_number']}]")
            time.sleep(1)

    # Re-filter to only those with a ps_doc_id now — dedupe by ps_doc_id
    seen = set()
    deduped = []
    for r in recent:
        pid = r.get("ps_doc_id")
        if pid and pid not in seen:
            seen.add(pid)
            deduped.append(r)
    recent = deduped
    if not recent:
        log.info("Doc fetch: no ps_doc_ids resolved — skipping")
        return records

    log.info(f"Doc fetch: enriching {len(recent)} leads with mortgage intel (window={DOC_FETCH_DAYS}d)...")
    fetched = 0

    for rec in recent:
        ps_id   = rec["ps_doc_id"]
        doc_num = rec["doc_number"]
        url     = f"{PUBLICSEARCH_BASE}/doc/{ps_id}"
        log.info(f"  Doc [{doc_num}] id={ps_id}")

        try:
            driver.set_page_load_timeout(20)
            driver.get(url)
            time.sleep(3)
        except Exception:
            log.warning(f"  Doc [{doc_num}] page load timeout — skipping")
            continue

        # Log first 200 chars of page so we can see what loaded
        try:
            preview = driver.find_element(By.TAG_NAME, "body").text[:300].replace("\n"," ")
            log.info(f"  Page preview: {preview}")
        except Exception:
            pass

        # Click SUMMARY tab
        try:
            tabs = driver.find_elements(By.CSS_SELECTOR, ".tab-item, .tab, [role='tab'], .nav-link, button, a")
            for tab in tabs:
                txt = (tab.text or tab.get_attribute("textContent") or "").strip().lower()
                if txt == "summary" or txt.startswith("summar"):
                    tab.click()
                    time.sleep(2)
                    break
        except Exception:
            pass

        loan_amount = ""
        loan_date   = ""
        lender      = ""
        trustee     = ""

        # ── Strategy A: read structured SUMMARY table fields ──────────────
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr, .summary-row, .detail-row, dl dt, .field-label, .label")
            for el in rows:
                label_text = (el.text or "").strip().lower()
                try:
                    sib = el.find_element(By.XPATH, "following-sibling::*[1]")
                    val = (sib.text or "").strip()
                except Exception:
                    val = ""
                if not val:
                    try:
                        parent = el.find_element(By.XPATH, "..")
                        val = (parent.text or "").replace(el.text or "", "").strip()
                    except Exception:
                        pass
                if any(x in label_text for x in ["original amount","loan amount","principal amount"]):
                    m = _re.search(r"\$?([\d,]+(?:\.\d{2})?)", val)
                    if m:
                        loan_amount = "$" + m.group(1)
                elif any(x in label_text for x in ["original beneficiary","mortgagee","beneficiary","lender"]):
                    if val and len(val) > 2:
                        lender = val
                elif any(x in label_text for x in ["trustor","trustee","substitute trustee"]):
                    if val and len(val) > 2 and not trustee:
                        trustee = val
                elif any(x in label_text for x in ["deed of trust","loan date","dated","instrument date"]):
                    if val:
                        loan_date = val
        except Exception as e:
            log.debug(f"  Summary table parse: {e}")

        # ── Strategy B: full body text ────────────────────────────────────
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text
            lines = page_text.split("\n")

            if not loan_date:
                m = _re.search(
                    r"Deed of Trust is dated\s+(\d{1,2}/\d{1,2}/\d{4}|\w+ \d{1,2},\s*\d{4})",
                    page_text, _re.IGNORECASE)
                if not m:
                    m = _re.search(
                        r"dated\s+(\d{1,2}/\d{1,2}/\d{4}|\w+ \d{1,2},\s*\d{4})",
                        page_text, _re.IGNORECASE)
                if m:
                    loan_date = m.group(1).strip()

            if not lender:
                m = _re.search(
                    r"nominee for\s+([A-Z][^\n,]{4,60}?)(?:\s*,|\s+AN\s|\s+ITS\s|\s+A\s)",
                    page_text, _re.IGNORECASE)
                if m:
                    lender = m.group(1).strip()
            if not lender:
                for i, line in enumerate(lines):
                    ll = line.lower()
                    if any(x in ll for x in ["original beneficiary","beneficiary:","mortgagee:"]):
                        val = line.split(":")[-1].strip() if ":" in line else ""
                        if not val and i+1 < len(lines):
                            val = lines[i+1].strip()
                        if val and len(val) > 3:
                            lender = val
                            break
            if not lender:
                m = _re.search(r"Current\s+Beneficiary[:\s]+([A-Z][^\n]{4,60})", page_text)
                if m:
                    lender = m.group(1).strip()

            if not trustee:
                m = _re.search(r"Trustor[s]?\(?s?\)?[:\s]+([A-Z][^\n]{4,80})", page_text)
                if m:
                    trustee = m.group(1).strip()

            if not loan_amount:
                m = _re.search(
                    r"(?:original|principal)\s+(?:loan\s+)?amount[:\s]+\$?([\d,]+(?:\.\d{2})?)",
                    page_text, _re.IGNORECASE)
                if m:
                    loan_amount = "$" + m.group(1)

        except Exception as e:
            log.debug(f"  Body text parse: {e}")

        rec["loan_amount"] = loan_amount
        rec["loan_date"]   = loan_date
        rec["lender"]      = lender
        rec["trustee"]     = trustee
        fetched += 1

        log.info(f"  → amt={loan_amount or '—'} | lender={lender[:35] if lender else '—'} | date={loan_date or '—'}")
        time.sleep(1)

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
    # BCAD field name candidates — try multiple since field names vary by layer version
    OWNER_FIELDS    = ["Owner","OWNER","owner","OwnerName","OWNER_NAME"]
    SITUS_FIELDS    = ["Situs","SITUS","situs","SitusAddress","SITUS_ADDRESS","Address","ADDRESS"]
    ADDR1_FIELDS    = ["AddrLn1","ADDR_LN1","MailAddr1","MAIL_ADDR1","MailAddress","MAIL_ADDRESS"]
    CITY_FIELDS     = ["AddrCity","ADDR_CITY","MailCity","MAIL_CITY","City","CITY"]
    ZIP_FIELDS      = ["Zip","ZIP","ZipCode","ZIPCODE","ZIP_CODE","MailZip","MAIL_ZIP"]
    APPR_FIELDS     = ["TotVal","TOT_VAL","TotalVal","TOTAL_VAL","AppraisedVal","APPRAISED_VAL","AppraisedValue","APPRAISED_VALUE","MarketValue","MARKET_VALUE"]
    TAX_FIELDS      = ["TaxAmt","TAX_AMT","TaxAmount","TAX_AMOUNT","TotalTax","TOTAL_TAX","AnnualTax","ANNUAL_TAX","LandVal","LAND_VAL"]
    LAND_FIELDS     = ["LandVal","LAND_VAL","LandValue","LAND_VALUE"]
    IMPR_FIELDS     = ["ImprovVal","IMPROV_VAL","ImprovValue","IMPROV_VALUE","ImpVal","IMP_VAL"]

    def get_field(a, candidates):
        for c in candidates:
            v = a.get(c)
            if v is not None and str(v).strip() not in ("","None","null","<Null>","NULL","0"):
                return str(v).strip()
        return ""

    for feat in feats:
        a       = feat.get("attributes", {})
        owner   = get_field(a, OWNER_FIELDS)
        situs   = get_field(a, SITUS_FIELDS)
        addr1   = get_field(a, ADDR1_FIELDS)
        city    = get_field(a, CITY_FIELDS)
        zipcode = get_field(a, ZIP_FIELDS)

        if not owner:
            continue
        situs_norm = normalize(situs)
        if not situs_norm.startswith(num + " "):
            continue
        if required_word and required_word not in situs_norm:
            continue

        mail_addr = ""
        if addr1:
            mail_addr = f"{addr1} {city} {zipcode}".strip()
        absentee = bool(mail_addr) and not normalize(mail_addr).startswith(num + " ")

        return {
            "owner":             owner.upper(),
            "mail_addr":         mail_addr,
            "absentee":          absentee,
            "appraised_value":   get_field(a, APPR_FIELDS),
            "land_value":        get_field(a, LAND_FIELDS),
            "improvement_value": get_field(a, IMPR_FIELDS),
            "annual_taxes":      get_field(a, TAX_FIELDS),
        }
    return None


def lookup_owner(address, zipcode=""):
    parsed = parse_address_parts(address)
    if not parsed:
        return {}
    num        = parsed["num"]
    words      = parsed["words"]
    first_word = words[0] if words else ""
    # Request all fields — let match_features handle name variations
    FIELDS = "*"

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

    # ── Probe actual BCAD field names on first query ──────────────────────────
    if missing:
        try:
            probe = fetch_json(f"{PARCELS_URL}/query?where=1%3D1&outFields=*&resultRecordCount=1&f=json")
            feat0 = probe.get("features", [{}])[0]
            actual_fields = list((feat0.get("attributes") or {}).keys())
            log.info(f"BCAD actual fields: {actual_fields[:20]}")
        except Exception as e:
            log.debug(f"BCAD probe failed: {e}")
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
            log.info(f"  [{i+1}/{len(missing)}] OK: {addr} -> {result['owner']} "
                     f"[{result.get('method','')}] appr={result.get('appraised_value','—')}")
        else:
            log.info(f"  [{i+1}/{len(missing)}] MISS: '{addr}' zip={zip_}")
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
    log.info("Bexar County Lead Scraper v28.23 (Hybrid)")
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
