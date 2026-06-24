"""
Bexar County Motivated Seller Lead Scraper v28.31
HYBRID SCRAPER:
  Primary:   bexar.tx.publicsearch.us  (Selenium, runs 2x daily)
  Secondary: ArcGIS GIS layer (urllib, runs weekly on Sunday)
  Owner enrichment: 5-strategy ArcGIS parcel lookup

  v28.31 changes:
    - Store PropID from ArcGIS parcel layer on every enriched record
    - fetch_deed_and_arv(): Selenium scrape of bexar.trueautomation.com for new leads only
      * Deed History row 1 -> deed_date, tenure_years, tenure_score_bonus
      * Values section -> last_sale_amt (ARV comp / equity signal)
    - Capped at 30 leads/run, skips leads that already have deed_date
    - Preserve prop_id, deed_date, last_sale_amt, tenure_years in merge step

  v28.30 changes:
    - Tenure scoring: pull SaleDate from ArcGIS parcel layer (no field found — superseded by v28.31)
    - Store sale_date_arcgis (last owner purchase date), tenure_years, tenure_score_bonus
    - score_record() adds tenure bonus: 15+yr=+25, 10-14yr=+15, 5-9yr=+5

  v28.29 fixes:
    - Removed fetch_code_enforcement step — bulk CE API returns 403 Forbidden
      from GitHub Actions IPs; per-address CE lookup in vbp_scraper works fine
    - CE Only leads will age out naturally via 90-day filter
    - Merge step preserves VBP CE violation fields from prev_records
    - Early exit: 2 consecutive pages with 0 new leads stops chunk immediately
"""

import json
import logging
import os
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from lp_scraper import scrape_lis_pendens

from pathlib import Path

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

BCAD_DETAIL_URL    = "https://bexar.trueautomation.com/clientdb/Property.aspx?cid=110&prop_id={prop_id}"
DEED_FETCH_LIMIT   = 30   # max new leads to hit BCAD detail page per run

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
DOC_FETCH_DAYS = 6


# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/28.30", "Accept": "application/json"})
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
    local_path = Path("dashboard/records.json")
    if local_path.exists():
        try:
            prev = json.loads(local_path.read_text())
            docs = {str(rec.get("doc_number", "")) for rec in prev if rec.get("doc_number")}
            log.info(f"Loaded {len(docs)} known doc numbers from local records.json")
            return docs, prev
        except Exception as e:
            log.warning(f"Local records.json load failed: {e}")

    url = PAGES_RECORDS + "?nocache=" + str(int(time.time()))
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "BexarScraper/28.30",
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


# ── TENURE HELPERS ────────────────────────────────────────────────────────────
def parse_arcgis_sale_date(raw_val):
    """
    ArcGIS SaleDate can come back as epoch ms (int) or a date string.
    Returns (date_str "MM/DD/YYYY", tenure_years int) or ("", None).
    """
    if not raw_val:
        return "", None
    # Epoch milliseconds
    if isinstance(raw_val, (int, float)) and raw_val > 1_000_000:
        try:
            dt = datetime.utcfromtimestamp(int(raw_val) / 1000)
            years = (TODAY_NAIVE - dt).days // 365
            return dt.strftime("%m/%d/%Y"), max(years, 0)
        except Exception:
            return "", None
    # String date
    raw_str = str(raw_val).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            dt = datetime.strptime(raw_str, fmt)
            years = (TODAY_NAIVE - dt).days // 365
            return dt.strftime("%m/%d/%Y"), max(years, 0)
        except Exception:
            continue
    return "", None


def tenure_bonus(tenure_years):
    """Return score bonus based on years of ownership."""
    if tenure_years is None:
        return 0
    if tenure_years >= 15:
        return 25
    if tenure_years >= 10:
        return 15
    if tenure_years >= 5:
        return 5
    return 0


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
    if rec.get("source") == "vbp_ce" or rec.get("type") == "VBP":
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


# ── SINGLE CHUNK SCRAPER ──────────────────────────────────────────────────────
def scrape_chunk(driver, known_docs, start_dt, end_dt):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

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

    records    = []
    page       = 0
    offset     = 0
    zero_new_streak = 0

    while True:
        url = search_url.replace("offset=0", f"offset={offset}")
        log.info(f"    [{start_str}-{end_str}] Page {page+1} (offset={offset})")

        loaded = False
        for attempt in range(2):
            try:
                driver.set_page_load_timeout(PAGE_TIMEOUT)
                driver.get(url)
                try:
                    WebDriverWait(driver, PAGE_TIMEOUT).until(
                        lambda d: (
                            d.find_elements(By.CSS_SELECTOR, "table tbody tr") or
                            d.find_elements(By.CSS_SELECTOR, "td.col-3") or
                            "no results" in d.page_source.lower()
                        )
                    )
                    time.sleep(1.5)
                    loaded = True
                    break
                except Exception:
                    log.info(f"    Timeout attempt {attempt+1} — {'retrying' if attempt==0 else 'stopping chunk'}")
                    if attempt == 0:
                        time.sleep(5)
            except Exception as e:
                log.info(f"    Page load error attempt {attempt+1}: {e}")
                if attempt == 0:
                    time.sleep(5)

        if not loaded:
            log.info(f"    Timeout page {page+1} — stopping chunk")
            break

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if not rows:
            rows = driver.find_elements(By.CSS_SELECTOR, "tr.a11y-table__row")
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
                        el = row.find_element(By.CSS_SELECTOR, f"td.{cls}")
                        return driver.execute_script(
                            "return arguments[0].innerText;", el
                        ).strip()
                    except Exception:
                        return ""

                doc_type_text = get_col(row, "col-3")
                recorded_date = get_col(row, "col-4")
                sale_date     = get_col(row, "col-5")
                doc_number    = get_col(row, "col-6")
                address_raw   = get_col(row, "col-8")

                if not doc_number:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) >= 6:
                        doc_type_text = doc_type_text or tds[2].text.strip()
                        recorded_date = recorded_date or tds[3].text.strip()
                        sale_date     = sale_date or tds[4].text.strip()
                        doc_number    = doc_number or tds[5].text.strip()
                        if len(tds) >= 9:
                            address_raw = address_raw or tds[8].text.strip()

                doc_number = doc_number.strip()
                sale_date  = sale_date.strip() if sale_date.strip() not in ("N/A", "") else ""

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
                try:
                    link = row.find_element(By.CSS_SELECTOR, "a[href*='/doc/']")
                    href = link.get_attribute("href") or ""
                    m = _re.search(r"/doc/(\d+)", href)
                    if m:
                        ps_doc_id = m.group(1)
                except Exception:
                    pass
                if not ps_doc_id:
                    try:
                        for el in row.find_elements(By.CSS_SELECTOR, "a,button,[onclick]"):
                            for attr in ["href", "onclick", "data-id", "data-href"]:
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

                rec = {
                    "type":                rec_type,
                    "address":             address,
                    "owner":               "",
                    "mail_addr":           "",
                    "absentee":            False,
                    "duplicate":           False,
                    "is_new":              True,
                    "doc_number":          doc_number,
                    "ps_doc_id":           ps_doc_id,
                    "year":                year,
                    "month":               month,
                    "city":                city,
                    "zip":                 zip_code,
                    "school_dist":         "",
                    "date_filed":          f"{month}/{year}".strip("/"),
                    "sale_date":           sale_date,
                    "run_ts":              RUN_TIMESTAMP,
                    "flags":               [],
                    "source":              "publicsearch",
                    "lender":              "",
                    "loan_amount":         "",
                    "loan_date":           "",
                    "trustee":             "",
                    "sale_date_arcgis":    "",
                    "tenure_years":        None,
                    "tenure_score_bonus":  0,
                    "prop_id":             "",
                    "deed_date":           "",
                    "last_sale_amt":       "",
                }
                records.append(rec)
                known_docs.add(doc_number)
                page_new += 1

            except Exception as e:
                log.debug(f"    Row parse error: {e}")

        log.info(f"    Page {page+1}: {page_new} new | {page_known} known | {page_old} old")

        if page_new == 0 and page_known == 0 and page > 0:
            log.info("    No new or known records — stopping chunk")
            break

        if page_new == 0:
            zero_new_streak += 1
        else:
            zero_new_streak = 0

        if zero_new_streak >= 2 and page > 1:
            log.info(f"    2 consecutive pages with 0 new leads — stopping chunk early")
            zero_new_streak = 0
            break

        if page_old > 0 and page_old >= len(rows) * 0.8:
            log.info("    Mostly old rows — stopping chunk")
            break

        if len(rows) < 48:
            break

        offset += 50
        page   += 1
        time.sleep(2)

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
    CE_API = (
        "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services"
        "/311_All_Service_Calls/FeatureServer/0/query"
    )
    FIELDS = "CASEID,Category,ReasonName,TypeName,CaseStatus,OpenedDateTime,ObjectDescription,CouncilDistrict"
    PAGE   = 2000
    cutoff_ms = int(CUTOFF_DATE.timestamp() * 1000)

    MOTIVATED_KEYWORDS = [
        "dangerous premise", "property structure", "vacant", "structure maintenance",
        "minimum housing", "substandard", "unsecured", "condemned",
    ]

    log.info(f"Code Enforcement: ArcGIS urllib JSON | Category=Property Maintenance | cutoff={CUTOFF_DATE.strftime('%Y-%m-%d')}")
    new_leads = []
    skipped   = 0

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
                data = json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            log.warning(f"CE API fetch error: {e}")
            break

        if "error" in data:
            log.warning(f"CE API error: {data['error']}")
            break

        features = data.get("features", [])

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

            tl = typename.lower()
            if not any(kw in tl for kw in MOTIVATED_KEYWORDS):
                skipped += 1
                continue

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
                "doc_number":         ce_key,
                "type":               "CE",
                "source":             "code_enforcement",
                "address":            street,
                "city":               city,
                "zip":                zipcode,
                "date_filed":         f"{month}/{year}".strip("/"),
                "sale_date":          "",
                "owner":              "",
                "mail_addr":          "",
                "absentee":           False,
                "duplicate":          False,
                "is_new":             True,
                "run_ts":             RUN_TIMESTAMP,
                "flags":              flags,
                "score":              score,
                "ce_case_id":         str(case_id),
                "ce_category":        "Property Maintenance",
                "ce_cat_label":       typename,
                "ce_status":          status,
                "ce_reason":          reason,
                "ce_district":        district,
                "opened_date":        opened_str,
                "loan_amount":        "",
                "loan_date":          "",
                "lender":             "",
                "trustee":            "",
                "appraised_value":    "",
                "annual_taxes":       "",
                "ps_doc_id":          "",
                "sale_date_arcgis":   "",
                "tenure_years":       None,
                "tenure_score_bonus": 0,
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
                "type":               layer["type"],
                "address":            pick(a, "ADDRESS", "SITUS_ADD", "ADDR"),
                "owner":              "",
                "mail_addr":          "",
                "absentee":           False,
                "duplicate":          False,
                "is_new":             True,
                "doc_number":         doc,
                "year":               year,
                "month":              month,
                "city":               pick(a, "CITY", "MAIL_CITY", default=""),
                "zip":                pick(a, "ZIP", "ZIPCODE", "ZIP_CODE", default=""),
                "school_dist":        pick(a, "SCHOOL_DIST", default=""),
                "date_filed":         f"{month}/{year}".strip("/"),
                "sale_date":          "",
                "run_ts":             RUN_TIMESTAMP,
                "flags":              [],
                "source":             "arcgis",
                "sale_date_arcgis":   "",
                "tenure_years":       None,
                "tenure_score_bonus": 0,
                "prop_id":            "",
                "deed_date":          "",
                "last_sale_amt":      "",
            })
            known_docs.add(doc)

    log.info(f"ArcGIS backfill: {len(raw)} new records")
    return raw


def login_publicsearch(driver):
    from selenium.webdriver.common.by import By

    email    = os.environ.get("CLERK_EMAIL", "")
    password = os.environ.get("CLERK_PASSWORD", "")
    if not email or not password:
        log.warning("No CLERK_EMAIL/CLERK_PASSWORD — skipping login")
        return False
    try:
        driver.set_page_load_timeout(20)
        driver.get(f"{PUBLICSEARCH_BASE}/login")
        time.sleep(4)
        log.info(f"Login page title: {driver.title} | url: {driver.current_url}")

        email_el = None
        for sel in ["input[type='email']", "input[name='email']",
                    "input[name='username']", "input[placeholder*='mail']",
                    "input[placeholder*='ser']"]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    email_el = els[0]
                    break
            except Exception:
                pass

        if not email_el:
            return False

        email_el.clear()
        email_el.send_keys(email)

        pass_el = None
        for sel in ["input[type='password']", "input[name='password']"]:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    pass_el = els[0]
                    break
            except Exception:
                pass

        if not pass_el:
            return False

        pass_el.clear()
        pass_el.send_keys(password)

        submitted = False
        for sel in ["button[type='submit']", "input[type='submit']", "button"]:
            try:
                btns = driver.find_elements(By.CSS_SELECTOR, sel)
                for btn in btns:
                    txt = (btn.text or "").lower()
                    if any(x in txt for x in ["sign in", "login", "log in", "submit", ""]):
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
        if "login" not in driver.current_url.lower():
            log.info("PublicSearch login OK")
            return True
        return False
    except Exception as e:
        log.warning(f"PublicSearch login error: {e}")
        return False


def lookup_ps_doc_id(doc_number, driver):
    from selenium.webdriver.common.by import By
    import re as _re
    try:
        driver.set_page_load_timeout(15)
        url = (f"{PUBLICSEARCH_BASE}/results?department=FC"
               f"&limit=10&offset=0&sort=desc&sortBy=recordedDate"
               f"&docNumber={doc_number}")
        driver.get(url)
        time.sleep(3)

        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/doc/']")
        for link in links:
            href = link.get_attribute("href") or ""
            m = _re.search(r"/doc/(\d+)", href)
            if m:
                try:
                    row = link.find_element(By.XPATH, "ancestor::tr")
                    if doc_number in (row.text or ""):
                        return m.group(1)
                except Exception:
                    return m.group(1)

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
    from selenium.webdriver.common.by import By
    import re as _re

    cutoff = TODAY_NAIVE - timedelta(days=DOC_FETCH_DAYS)

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

    recent = recent[:20]
    log.info(f"Doc fetch: {len(recent)} within {DOC_FETCH_DAYS}d window (capped at 20)")

    if not recent:
        log.info(f"Doc fetch: no leads within {DOC_FETCH_DAYS}d window missing loan data — skipping")
        return records

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

    log.info(f"Doc fetch: enriching {len(recent)} leads with mortgage intel...")
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
                    if any(x in ll for x in ["original beneficiary", "beneficiary:", "mortgagee:"]):
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
    OWNER_FIELDS    = ["Owner","OWNER","owner","OwnerName","OWNER_NAME"]
    SITUS_FIELDS    = ["Situs","SITUS","situs","SitusAddress","SITUS_ADDRESS","Address","ADDRESS"]
    ADDR1_FIELDS    = ["AddrLn1","ADDR_LN1","MailAddr1","MAIL_ADDR1","MailAddress","MAIL_ADDRESS"]
    CITY_FIELDS     = ["AddrCity","ADDR_CITY","MailCity","MAIL_CITY","City","CITY"]
    ZIP_FIELDS      = ["Zip","ZIP","ZipCode","ZIPCODE","ZIP_CODE","MailZip","MAIL_ZIP"]
    APPR_FIELDS     = ["TotVal","TOT_VAL","TotalVal","TOTAL_VAL","AppraisedVal","APPRAISED_VAL","AppraisedValue","APPRAISED_VALUE","MarketValue","MARKET_VALUE"]
    TAX_FIELDS      = ["TaxAmt","TAX_AMT","TaxAmount","TAX_AMOUNT","TotalTax","TOTAL_TAX","AnnualTax","ANNUAL_TAX"]
    LAND_FIELDS     = ["LandVal","LAND_VAL","LandValue","LAND_VALUE"]
    IMPR_FIELDS     = ["ImprovVal","IMPROV_VAL","ImprovValue","IMPROV_VALUE","ImpVal","IMP_VAL"]
    # v28.30: SaleDate field candidates for tenure scoring
    SALE_DATE_FIELDS = ["SaleDate","SALE_DATE","saleDate","LastSaleDate","LAST_SALE_DATE","SaleDt","SALE_DT"]
    # v28.31: PropID for BCAD detail page lookup
    PROPID_FIELDS   = ["PropID","PROP_ID","PropId","prop_id","PropertyID","PROPERTY_ID","ParcelID","PARCEL_ID"]

    def get_field(a, candidates):
        for c in candidates:
            v = a.get(c)
            if v is not None and str(v).strip() not in ("","None","null","<Null>","NULL","0"):
                return str(v).strip()
        return ""

    def get_raw(a, candidates):
        """Return raw value (including 0 / epoch int) for date fields."""
        for c in candidates:
            v = a.get(c)
            if v is not None and str(v).strip() not in ("","None","null","<Null>","NULL"):
                return v
        return None

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

        # v28.30: parse SaleDate for tenure scoring
        raw_sale_date = get_raw(a, SALE_DATE_FIELDS)
        sale_date_arcgis, tenure_years = parse_arcgis_sale_date(raw_sale_date)

        return {
            "owner":              owner.upper(),
            "mail_addr":          mail_addr,
            "absentee":           absentee,
            "appraised_value":    get_field(a, APPR_FIELDS),
            "land_value":         get_field(a, LAND_FIELDS),
            "improvement_value":  get_field(a, IMPR_FIELDS),
            "annual_taxes":       get_field(a, TAX_FIELDS),
            "sale_date_arcgis":   sale_date_arcgis,
            "tenure_years":       tenure_years,
            "tenure_score_bonus": tenure_bonus(tenure_years),
            "prop_id":            get_field(a, PROPID_FIELDS),
        }
    return None


def lookup_owner(address, zipcode=""):
    parsed = parse_address_parts(address)
    if not parsed:
        return {}
    num        = parsed["num"]
    words      = parsed["words"]
    first_word = words[0] if words else ""
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
            rec["owner"]              = result["owner"]
            rec["mail_addr"]          = result.get("mail_addr", "")
            rec["absentee"]           = result.get("absentee", False)
            rec["appraised_value"]    = result.get("appraised_value", "")
            rec["annual_taxes"]       = result.get("annual_taxes", "")
            rec["land_value"]         = result.get("land_value", "")
            # v28.30: tenure fields from ArcGIS parcel
            rec["sale_date_arcgis"]   = result.get("sale_date_arcgis", "")
            rec["tenure_years"]       = result.get("tenure_years", None)
            rec["tenure_score_bonus"] = result.get("tenure_score_bonus", 0)
            # v28.31: PropID for BCAD detail page
            if result.get("prop_id"):
                rec["prop_id"] = result["prop_id"]
            found += 1
            tenure_info = ""
            if rec["tenure_years"] is not None:
                tenure_info = f" tenure={rec['tenure_years']}yr(+{rec['tenure_score_bonus']})"
            log.info(f"  [{i+1}/{len(missing)}] OK: {addr} -> {result['owner']} "
                     f"[{result.get('method','')}] appr={result.get('appraised_value','—')}{tenure_info}")
        else:
            log.info(f"  [{i+1}/{len(missing)}] MISS: '{addr}' zip={zip_}")
        time.sleep(0.2)
    log.info(f"Owner enrichment: {found}/{len(missing)} filled")
    return records


# ── BCAD DEED HISTORY + ARV (new leads only) ─────────────────────────────────
def fetch_deed_and_arv(records, driver):
    """
    For new leads that have a prop_id and no deed_date yet:
    Hit bexar.trueautomation.com property detail page, parse:
      - Deed History row 1 -> deed_date, tenure_years, tenure_score_bonus
      - Values section     -> last_sale_amt (ARV comp)
    Capped at DEED_FETCH_LIMIT leads per run to keep runtime under 5 min.
    """
    from selenium.webdriver.common.by import By
    import re as _re

    candidates = [
        r for r in records
        if r.get("is_new")
        and r.get("prop_id")
        and not r.get("deed_date")
        and r.get("type") in ("NOF", "TAX", "LP", "PRE_FORE")
    ]
    candidates = candidates[:DEED_FETCH_LIMIT]

    if not candidates:
        log.info("BCAD deed+ARV: no eligible new leads with prop_id — skipping")
        return records

    log.info(f"BCAD deed+ARV: {len(candidates)} new leads to enrich")
    fetched = 0
    errors  = 0

    for rec in candidates:
        prop_id = rec["prop_id"]
        addr    = rec.get("address", "")
        url     = BCAD_DETAIL_URL.format(prop_id=prop_id)
        log.info(f"  BCAD [{prop_id}] {addr}")

        try:
            driver.set_page_load_timeout(25)
            driver.get(url)
            time.sleep(3)
        except Exception as e:
            log.warning(f"  BCAD [{prop_id}] load timeout: {e}")
            errors += 1
            continue

        deed_date     = ""
        tenure_yrs    = None
        last_sale_amt = ""

        try:
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            # ── Click open "Deed History" accordion if collapsed ──────────────
            # trueautomation uses clickable header rows to expand sections
            try:
                headers = driver.find_elements(By.CSS_SELECTOR,
                    "tr.sectionHeader, tr[onclick], td.sectionHeader, div.sectionHeader, "
                    "h3, h4, .accordion-header, [data-toggle], td[style*='cursor']")
                for hdr in headers:
                    txt = (hdr.text or "").lower()
                    if "deed" in txt:
                        driver.execute_script("arguments[0].click();", hdr)
                        time.sleep(2)
                        log.info(f"  BCAD [{prop_id}] clicked deed history header")
                        break
            except Exception:
                pass

            # ── Also try clicking "Values" section for last_sale_amt ──────────
            try:
                headers2 = driver.find_elements(By.CSS_SELECTOR,
                    "tr.sectionHeader, tr[onclick], td.sectionHeader, div.sectionHeader, "
                    "h3, h4, .accordion-header, [data-toggle], td[style*='cursor']")
                for hdr in headers2:
                    txt = (hdr.text or "").lower()
                    if "value" in txt or "sale" in txt:
                        driver.execute_script("arguments[0].click();", hdr)
                        time.sleep(1)
                        break
            except Exception:
                pass

            # ── Wait for a date pattern to appear in any table cell ───────────
            try:
                WebDriverWait(driver, 8).until(
                    lambda d: any(
                        _re.search(r"\d{1,2}/\d{1,2}/\d{4}", cell.text or "")
                        for cell in d.find_elements(By.CSS_SELECTOR, "td")
                    )
                )
            except Exception:
                pass  # proceed with whatever rendered

            body = driver.find_element(By.TAG_NAME, "body").text
            lines = [l.strip() for l in body.split("\n") if l.strip()]

            # ── Deed History: find header then grab first data row ────────────
            # Page text has "Deed History" section with rows like:
            # "9/13/2017  RES  Rescind of previous deed  MULTIMADERAS USA LLC  GUERRERO..."
            # We scan for date pattern MM/DD/YYYY after the Deed History heading
            in_deed_section = False
            for line in lines:
                if "deed history" in line.lower():
                    in_deed_section = True
                    continue
                if in_deed_section:
                    # Look for a line starting with a date MM/DD/YYYY
                    m = _re.match(r"^(\d{1,2}/\d{1,2}/\d{4})\b", line)
                    if m:
                        deed_date = m.group(1)
                        break
                    # Stop if we hit another section header
                    if line.isupper() and len(line) > 10:
                        break

            # ── Values section: last sale amount ─────────────────────────────
            # Look for patterns like "Sale Price: $142,500" or "Prior Sales Price $142,500"
            # or a line containing dollar amount near "sale" keyword
            for i, line in enumerate(lines):
                ll = line.lower()
                if any(x in ll for x in ["sale price", "prior sale", "sales price", "deed amount", "consideration"]):
                    # Try to find dollar amount on same line or next line
                    amt_m = _re.search(r"\$?([\d,]+(?:\.\d{2})?)", line)
                    if not amt_m and i + 1 < len(lines):
                        amt_m = _re.search(r"\$?([\d,]+(?:\.\d{2})?)", lines[i + 1])
                    if amt_m:
                        raw_amt = amt_m.group(1).replace(",", "")
                        try:
                            if float(raw_amt) > 1000:  # filter out noise like page numbers
                                last_sale_amt = "$" + amt_m.group(1)
                                break
                        except Exception:
                            pass

            # ── Also try table cells via Selenium if text parse missed ────────
            if not deed_date:
                try:
                    # Scan ALL table cells for date pattern — deed history rows
                    # have dates in first cell. Find the section by scanning for
                    # a date after any row that mentions "deed"
                    all_cells = driver.find_elements(By.CSS_SELECTOR, "td")
                    found_deed_section = False
                    for cell in all_cells:
                        txt = (cell.text or "").strip()
                        if "deed" in txt.lower() and len(txt) < 60:
                            found_deed_section = True
                            continue
                        if found_deed_section:
                            m = _re.match(r"^(\d{1,2}/\d{1,2}/\d{4})\b", txt)
                            if m:
                                deed_date = m.group(1)
                                log.info(f"  BCAD [{prop_id}] found deed date via cell scan")
                                break
                            # Reset if we've moved too far without finding a date
                            if len(txt) > 5 and not _re.search(r"\d", txt):
                                found_deed_section = False
                except Exception as ce:
                    log.debug(f"  BCAD [{prop_id}] cell scan error: {ce}")

            # ── Last resort: scan ALL cells for any date ──────────────────────
            if not deed_date:
                try:
                    all_rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
                    for row in all_rows:
                        row_txt = (row.text or "").lower()
                        if "deed" not in row_txt and "sale" not in row_txt:
                            continue
                        cells = row.find_elements(By.TAG_NAME, "td")
                        for cell in cells:
                            m = _re.match(r"^(\d{1,2}/\d{1,2}/\d{4})\b", (cell.text or "").strip())
                            if m:
                                deed_date = m.group(1)
                                break
                        if deed_date:
                            break
                except Exception:
                    pass

        except Exception as e:
            log.debug(f"  BCAD [{prop_id}] parse error: {e}")
            errors += 1
            continue

        # ── Calculate tenure from deed_date ───────────────────────────────────
        if deed_date:
            try:
                deed_dt  = datetime.strptime(deed_date.strip(), "%m/%d/%Y")
                tenure_yrs = max((TODAY_NAIVE - deed_dt).days // 365, 0)
            except Exception:
                tenure_yrs = None

        rec["deed_date"]          = deed_date
        rec["last_sale_amt"]      = last_sale_amt
        rec["tenure_years"]       = tenure_yrs
        rec["tenure_score_bonus"] = tenure_bonus(tenure_yrs)
        fetched += 1

        log.info(f"  → deed={deed_date or '—'} tenure={tenure_yrs}yr "
                 f"last_sale={last_sale_amt or '—'} bonus=+{tenure_bonus(tenure_yrs)}")
        time.sleep(1.5)

    log.info(f"BCAD deed+ARV: {fetched} enriched, {errors} errors out of {len(candidates)} candidates")
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
    # v28.30: tenure bonus — long-held = high motivation signal
    s += rec.get("tenure_score_bonus", 0)
    return s


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
    log.info("Bexar County Lead Scraper v28.31 (Hybrid)")
    log.info(f"Primary:   PublicSearch.us ({KEEP_DAYS}d window, {CHUNK_DAYS}d chunks, {PAGE_TIMEOUT}s timeout)")
    log.info(f"Secondary: ArcGIS weekly backfill = {IS_SUNDAY}")
    log.info(f"Tertiary:  Code Enforcement 311 ({len(CE_CATEGORIES)} categories, {KEEP_DAYS}d window)")
    log.info(f"Doc fetch: backfill window = {DOC_FETCH_DAYS}d")
    log.info(f"Tenure:    scoring active — 15+yr=+25pts, 10-14yr=+15pts, 5-9yr=+5pts")
    log.info(f"BCAD:      deed history + ARV via trueautomation.com (new leads only, cap={DEED_FETCH_LIMIT})")
    log.info(f"Filter:    {KEEP_DAYS}-day cutoff ({CUTOFF_DATE.strftime('%Y-%m-%d')}) | live auctions always kept")
    log.info("=" * 60)

    known_docs, prev_records = load_known_docs()

    # ── Step 1: PublicSearch chunked scrape ───────────────────────────────────
    new_records = scrape_publicsearch(known_docs)

    # ── Step 1b: Doc detail fetch ─────────────────────────────────────────────
    doc_driver = None
    try:
        doc_driver = get_driver()
        all_for_doc_fetch = new_records + prev_records
        all_for_doc_fetch = fetch_doc_details(all_for_doc_fetch, doc_driver)
        new_records = [r for r in all_for_doc_fetch if r.get("is_new")]
    except Exception as e:
        log.warning(f"Doc fetch driver error: {e}")
    finally:
        if doc_driver:
            try:
                doc_driver.quit()
            except Exception:
                pass

    # ── Step 1c: Lis Pendens early detection ─────────────────────────────────
    lp_records = scrape_lis_pendens(known_docs, get_driver, RUN_TIMESTAMP)
    log.info(f"LP scrape: {len(lp_records)} new Lis Pendens records")

    # ── Step 2: ArcGIS weekly backfill (Sundays only) ────────────────────────
    arcgis_records = []
    if IS_SUNDAY:
        arcgis_records = fetch_arcgis_backfill(known_docs)
        log.info(f"ArcGIS backfill added {len(arcgis_records)} records")

    # ── Step 3: Code Enforcement — disabled (bulk CE API blocked from GH Actions)
    ce_records = []

    # ── Step 4: Merge ─────────────────────────────────────────────────────────
    for r in prev_records:
        r["is_new"] = False

    # Build prev_records index for VBP CE field preservation
    prev_by_doc = {r["doc_number"]: r for r in prev_records if r.get("doc_number")}

    # VBP CE fields written by vbp_scraper — must never be lost on fetch.py runs
    VBP_CE_FIELDS = [
        "stacked", "ce_violations", "ce_viol_types", "ce_count",
        "ce_cat_label", "ce_status", "opened_date", "ce_case_id",
    ]

    # v28.30: tenure fields to preserve from prev_records if already enriched
    # v28.31: also preserve prop_id, deed_date, last_sale_amt
    TENURE_FIELDS = ["sale_date_arcgis", "tenure_years", "tenure_score_bonus",
                     "prop_id", "deed_date", "last_sale_amt"]

    seen = {}
    for r in new_records + arcgis_records + lp_records + ce_records + prev_records:
        doc = r.get("doc_number", "")
        if not doc:
            continue
        if doc not in seen:
            seen[doc] = r

    # Preserve VBP CE fields and tenure fields from prev_records
    vbp_ce_preserved = 0
    tenure_preserved = 0
    for doc, r in seen.items():
        if doc in prev_by_doc:
            prev = prev_by_doc[doc]
            if r.get("type") == "VBP":
                for field in VBP_CE_FIELDS:
                    if prev.get(field) and not r.get(field):
                        r[field] = prev[field]
                        vbp_ce_preserved += 1
            # Preserve tenure data on any record type if already resolved
            for field in TENURE_FIELDS:
                if prev.get(field) is not None and prev.get(field) != "" and not r.get(field):
                    r[field] = prev[field]
                    tenure_preserved += 1

    if vbp_ce_preserved:
        log.info(f"Preserved {vbp_ce_preserved} VBP CE field values from prev_records")
    if tenure_preserved:
        log.info(f"Preserved {tenure_preserved} tenure field values from prev_records")

    records = list(seen.values())
    log.info(f"After dedup: {len(records)} total records")

    # ── Step 5: 90-day filter ─────────────────────────────────────────────────
    before  = len(records)
    records = [r for r in records if should_keep(r)]
    log.info(f"After filter: {len(records)} kept, {before - len(records)} dropped")

    # ── Step 6: Owner enrichment ──────────────────────────────────────────────
    records = enrich_owners(records)

    # ── Step 6b: BCAD deed history + ARV (new leads with prop_id only) ───────
    bcad_driver = None
    try:
        new_with_prop = [r for r in records if r.get("is_new") and r.get("prop_id") and not r.get("deed_date")]
        if new_with_prop:
            bcad_driver = get_driver()
            records = fetch_deed_and_arv(records, bcad_driver)
        else:
            log.info("BCAD deed+ARV: no new leads with prop_id to enrich")
    except Exception as e:
        log.warning(f"BCAD deed+ARV driver error: {e}")
    finally:
        if bcad_driver:
            try:
                bcad_driver.quit()
            except Exception:
                pass

    # ── Step 7: Duplicate detection ───────────────────────────────────────────
    records = detect_duplicates(records)

    # ── Step 8: Flag + score ──────────────────────────────────────────────────
    for r in records:
        existing_stacked = r.get("stacked", False)
        r["flags"] = []
        if r["type"] == "TAX":                    r["flags"].append("TAX FORE")
        if r["type"] == "CE":                     r["flags"].append("CODE ENFORCE")
        if r["type"] == "VBP" and existing_stacked: r["flags"].append("STACKED")
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
        # v28.30/31: LONG HELD flag — tenure_years set by deed_date (v28.31) or ArcGIS (v28.30)
        if (r.get("tenure_years") or 0) >= 10:   r["flags"].append("LONG HELD")
        r["stacked"]           = existing_stacked
        r["score"]             = score_record(r)
        r["days_until_sale"]   = d

    def sort_key(r):
        d = r.get("days_until_sale")
        u = 0 if (d is not None and d <= 14) else (1 if (d is not None and d <= 30) else 2)
        return (u, -r["score"], d if d is not None else 9999)

    records.sort(key=sort_key)

    # ── Step 9: Summary ───────────────────────────────────────────────────────
    named       = sum(1 for r in records if r.get("owner"))
    absentee    = sum(1 for r in records if r.get("absentee"))
    new_ct      = sum(1 for r in records if r.get("is_new"))
    urgent      = sum(1 for r in records if "URGENT"       in r.get("flags", []))
    soon        = sum(1 for r in records if "AUCTION SOON" in r.get("flags", []))
    has_date    = sum(1 for r in records if r.get("sale_date"))
    enriched    = sum(1 for r in records if r.get("loan_amount"))
    stacked     = sum(1 for r in records if r.get("stacked"))
    long_held   = sum(1 for r in records if "LONG HELD"    in r.get("flags", []))
    with_tenure = sum(1 for r in records if r.get("tenure_years") is not None)
    with_deed   = sum(1 for r in records if r.get("deed_date"))
    with_arv    = sum(1 for r in records if r.get("last_sale_amt"))
    with_propid = sum(1 for r in records if r.get("prop_id"))

    log.info(f"Final: {len(records)} total | {named} named | {absentee} absentee")
    log.info(f"       {new_ct} new | {has_date} with sale date | "
             f"{soon} auction <=30d | {urgent} URGENT <=14d")
    log.info(f"       VBP stacked: {stacked} confirmed VBP+CE leads")
    log.info(f"       Mortgage intel: {enriched} leads with loan_amount populated")
    log.info(f"       Tenure: {with_tenure} with tenure | {long_held} LONG HELD (10+ yr) | {with_deed} deed dates")
    log.info(f"       BCAD: {with_propid} with prop_id | {with_arv} with last_sale_amt (ARV comp)")

    # ── Step 10: Save ─────────────────────────────────────────────────────────
    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    build_dashboard(records)
    log.info("Done.")
