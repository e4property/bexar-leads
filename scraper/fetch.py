"""
Bexar County Motivated Seller Lead Scraper v28.34
HYBRID SCRAPER:
  Primary:   bexar.tx.publicsearch.us  (Selenium, runs 2x daily)
  Secondary: ArcGIS GIS layer (urllib, runs weekly on Sunday)
  Owner enrichment: 5-strategy ArcGIS parcel lookup

  v28.32 changes:
    - fetch_deed_and_arv(): fix BCAD accordion click using JS targeting
      tr[onclick]/td[onclick] — the actual trueautomation.com mechanism
    - Backfill eligibility: changed from is_new-only to any record with
      prop_id and no deed_date (backfills all existing leads with prop_id)
    - 3-strategy fallback: JS click -> Selenium element click -> table scan
    - Explicit 10s wait for date pattern after accordion click

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
DEED_FETCH_LIMIT   = 30   # max leads to hit BCAD detail page per run
ARV_FETCH_LIMIT    = 30   # max leads to look up via HomeHarvest/Realtor.com per run
ON_MARKET_STATUSES     = {"FOR_SALE", "PENDING", "FOR_RENT"}
ON_MARKET_REFRESH_DAYS = 7    # re-check a lead's market status at most this often
# 2026-08-21: a single run doing ARV(30) then refresh(30) back-to-back hit a
# hard Realtor.com AuthenticationError wall after ~27 consecutive requests
# that never recovered for the rest of the run (all 30 refresh calls failed).
# Keeping this pass small so the combined per-run total stays under that.
ON_MARKET_REFRESH_LIMIT = 15  # max already-checked leads to re-check per run

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
DOC_FETCH_LIMIT = 8    # max leads to OCR for loan/lender detail per run -- was 20, but each lead can
                        # need a page load + screenshot + tesseract OCR + a second page hop, and a run
                        # hit the 90-min workflow timeout and got killed (2026-08-19, 106 min elapsed,
                        # nothing deployed) with this cap at 20. Trading backlog speed for runs that
                        # actually finish.


# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/28.32", "Accept": "application/json"})
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
            headers={"User-Agent": "BexarScraper/28.32",
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
    if not raw_val:
        return "", None
    if isinstance(raw_val, (int, float)) and raw_val > 1_000_000:
        try:
            dt = datetime.utcfromtimestamp(int(raw_val) / 1000)
            years = (TODAY_NAIVE - dt).days // 365
            return dt.strftime("%m/%d/%Y"), max(years, 0)
        except Exception:
            return "", None
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
STRUCK_OFF_DOC_RE = re.compile(r"^\d{4}TA\d+$")

def should_keep(rec):
    # Found 2026-08-26: 42 records with a doc_number like "2019TA100098" turned
    # out to be a mis-scraped Land Records DEED where COUNTY OF BEXAR is both
    # grantor and grantee -- the county formally taking ownership of a
    # property struck off at a PAST tax sale (nobody bid). The doc_number
    # field here was never actually a document number; it's the old tax-suit
    # cause number sitting in the Legal Description column, misread as the
    # doc number by whatever path produced these. There's no homeowner left
    # to work -- the county already owns it. 10 of the 42 had already been
    # pushed to Jarvis with a phone on file before this was caught. Reject
    # unconditionally regardless of what else looks populated on the record.
    if STRUCK_OFF_DOC_RE.match(rec.get("doc_number", "") or ""):
        return False
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

    # 2026-08-28 v3: switched from instrumentDateRange to recordedDateRange.
    # instrumentDateRange started returning inconsistent/incomplete results
    # for this department -- confirmed live across 3 consecutive scrape
    # runs, the exact same 14 doc numbers (a contiguous, oldest-in-window
    # cluster) never surfaced no matter how the "no rows" detection was
    # hardened, and a clean UI-driven reproduction (not a direct-nav cold
    # load) showed 0 results for instrumentDateRange on this exact window
    # while recordedDateRange correctly returned and fully paginated
    # through all 63 real records for the same week. Also added
    # searchType=advancedSearch, present on every URL the site's own UI
    # generates but missing from this scraper's construction.
    search_url = (
        f"{PUBLICSEARCH_BASE}/results"
        f"?department=FC"
        f"&recordedDateRange={start_str}%2C{end_str}"
        f"&keywordSearch=false"
        f"&limit=50"
        f"&offset=0"
        f"&sort=desc"
        f"&sortBy=recordedDate"
        f"&sortDir=desc"
        f"&searchType=advancedSearch"
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
                    # 2026-08-28: the old check used `"no results" in
                    # d.page_source.lower()` -- a substring match against the
                    # ENTIRE raw page source, not a scoped element check. The
                    # page's static/hidden markup (help text, a collapsed
                    # "No Results" container that's always in the DOM, just
                    # CSS-hidden until needed) contains that phrase on every
                    # page load, results or not -- so this condition was true
                    # from the instant the page started loading, before the
                    # real <tr> rows ever rendered. WebDriverWait returned
                    # immediately, the code checked for rows before they
                    # existed, found none, and wrongly concluded "no more
                    # results" -- confirmed live: page 2 of the current-week
                    # chunk falsely stopped this way, silently dropping 34 of
                    # 53 real NOF filings recorded 8/27/2026. Same fix as the
                    # other functions in this file: only trust a genuine,
                    # properly-scoped "No Results" <h1>, not a raw substring
                    # match against the whole page.
                    WebDriverWait(driver, PAGE_TIMEOUT).until(
                        lambda d: (
                            d.find_elements(By.CSS_SELECTOR, "table tbody tr") or
                            d.find_elements(By.CSS_SELECTOR, "td.col-3") or
                            d.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]")
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

        # 2026-08-28 v2: the wait condition above can resolve true on a
        # still-rendering page (an early skeleton/placeholder satisfies one
        # of the OR branches) before the real <tr> rows have finished
        # painting -- confirmed live, this let a genuine-looking "no rows"
        # false-stop still slip through intermittently even after the
        # substring-match fix, converging only after several re-runs rather
        # than failing outright. Only trust "no rows" immediately when a
        # real No-Results heading is actually present; otherwise give the
        # page one more real chance to finish rendering before concluding
        # the chunk is exhausted.
        if not rows:
            confirmed_empty = driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]")
            if not confirmed_empty:
                time.sleep(3)
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
                    "_source_url":         url,
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
                    "appr_history":        [],
                    "appr_trend":          "",
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
        driver.get(f"{PUBLICSEARCH_BASE}/signin")
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


QUICK_SEARCH_URL_TMPL = (
    "https://bexar.tx.publicsearch.us/results?department=RP&keywordSearch=false"
    "&recordedDateRange=18000101%2C{today}&searchOcrText=false"
    "&searchType=quickSearch&searchValue={doc_number}"
)


def ocr_current_doc_page(driver):
    """
    The PublicSearch doc viewer renders the recorded instrument as an
    <svg role="img" aria-label="Document preview image">, not selectable
    DOM text and NOT a <canvas> — confirmed via live DevTools inspection
    2026-08-11 (the previous two fix attempts both assumed canvas, which
    doesn't exist anywhere on this page, hence the wait always timing
    out and silently falling back to a full-page screenshot that OCR'd
    navbar chrome instead of the document). The neighboring "Summary" tab
    is generic accessibility boilerplate and never contains the
    instrument's actual text, which is why loan_amount extraction never
    worked reading it. This screenshots the SVG and OCRs it instead. No
    purchase/"Add to Cart" step needed — the viewer already renders the
    page for free.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import pytesseract
    from PIL import Image
    import io

    try:
        img_el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'svg[aria-label="Document preview image"], svg[role="img"]')
            )
        )
        time.sleep(1)  # let the page-render paint finish inside the svg
        png = img_el.screenshot_as_png
    except Exception as e:
        log.warning(f"  ocr_current_doc_page: document preview svg never appeared ({e}) — falling back to full-page screenshot")
        try:
            png = driver.get_screenshot_as_png()
        except Exception as e2:
            log.warning(f"  ocr_current_doc_page: screenshot failed: {e2}")
            return ""

    try:
        return pytesseract.image_to_string(Image.open(io.BytesIO(png)))
    except Exception as e:
        log.warning(f"  ocr_current_doc_page: tesseract failed: {e}")
        return ""


def search_and_ocr_referenced_doc(driver, doc_number, timeout=20):
    """
    Looks up a doc number (e.g. the original Deed of Trust referenced inside
    a Notice of [Substitute] Trustee's Sale) via the quickSearch URL pattern
    confirmed live 2026-08-10, clicks the single result row, and OCRs that
    page. Returns "" if the search comes up empty or the click-through fails
    — this is a best-effort second hop, not guaranteed to resolve.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # v28.40: end date capped 3 days behind real today -- confirmed live
    # 2026-08-23 (same root cause found on the Nueces scraper) that
    # recordedDateRange's end date can't exceed PublicSearch's own
    # "Certified through" date (runs ~1-2 days behind real today) or the
    # search returns "No Results Found" even for a document well inside
    # the range. This alone was silently zeroing out every loan_backfill
    # run even after login was fixed. Costs nothing here since every doc
    # being looked up was already recorded in the past.
    today_str = (TODAY_NAIVE - timedelta(days=3)).strftime("%Y%m%d")
    url = QUICK_SEARCH_URL_TMPL.format(today=today_str, doc_number=doc_number)

    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        # v28.42: same fix as goto_doc_by_docnumber -- a genuine "No Results"
        # page renders its own <h1> with a hashed CSS-in-JS class, never a
        # stable one, so a plain "table tr td" wait spins the full timeout
        # on every zero-match lookup. XPath text match catches it either way.
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.XPATH, "//table//tr/td | //h1[contains(text(),'No Results')]")
            )
        )
        if driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]"):
            log.info(f"  search_and_ocr_referenced_doc: no results for {doc_number}")
            return ""
        time.sleep(1)

        row = driver.find_element(By.CSS_SELECTOR, "table tbody tr")
        row.click()
        WebDriverWait(driver, timeout).until(EC.url_contains("/doc/"))
        time.sleep(1.5)
        return ocr_current_doc_page(driver)
    except Exception as e:
        log.warning(f"  search_and_ocr_referenced_doc: lookup failed for {doc_number}: {e}")
        return ""


def goto_doc_by_docnumber(driver, doc_number, timeout=20):
    """
    v28.38: replaces goto_doc_by_click + _source_url for fetch_doc_details.
    _source_url only survives in memory for leads discovered brand-new in
    the SAME run (it's stripped before every save — "only needed during
    this run's doc-page click-through"), so the old candidate filter
    (type in NOF/TAX, missing loan_amount, has _source_url) was
    structurally empty on every "0 new" day, which is most days —
    confirmed live 2026-08-19: OCR fixes were all correct but never got a
    single candidate to run on. This uses the same quickSearch URL hop
    already proven for the Deed-of-Trust lookup, which works for ANY doc
    number regardless of when it was originally scraped — unlocks OCR for
    the full backlog, not just today's rare new leads. Returns True/False
    to match goto_doc_by_click's contract.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # v28.43: this doesn't share QUICK_SEARCH_URL_TMPL (department=RP,
    # recordedDateRange) with search_and_ocr_referenced_doc anymore -- that
    # was the actual root cause of the 0/100 backfill, found live 2026-08-23
    # via the site's own Advanced Search UI: NOF/TAX foreclosure notices are
    # filed under the "Foreclosures" department (code FC), not "Land
    # Records" (RP) -- confirmed with a real doc (20260900427), which only
    # ever matched under department=FC. RP+recordedDateRange (the v28.40/
    # 28.41 fixes) was correct for the Deed-of-Trust hop, which does live
    # under RP -- just wrong for locating the original Notice itself.
    #
    # instrumentDateRange's bounds are oddly brittle -- live-verified
    # 2026-08-23 that ONLY the exact pair the site's own Advanced Search UI
    # auto-filled for the Foreclosures department (20000404, 20261201)
    # returns results for a real, confirmed-existing doc; every other
    # logically-wider-or-equivalent range tried (1990/2020 starts, a
    # dynamically-computed today+30d end) returned "No Results Found" for
    # the SAME document, including ranges that fully contain it. Not a real
    # date-boundary rule as far as could be determined -- reads as a site
    # quirk/bug tied to these specific values. Hardcoded rather than
    # computed for now since it reliably works and every current backlog
    # doc falls well inside it; the end bound will need revisiting before
    # 2026-12-01 (re-derive fresh values from the site's own Advanced
    # Search UI with department=Foreclosures selected, doc number blank).
    doc_json = urllib.parse.quote(f'["{doc_number}"]')
    url = (
        f"{PUBLICSEARCH_BASE}/results?department=FC"
        f"&documentNumberRange={doc_json}"
        f"&instrumentDateRange=20000404%2C20261201"
        f"&searchType=advancedSearch"
    )

    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        # v28.42: same fix as search_and_ocr_referenced_doc -- a genuine
        # "No Results" page renders its own <h1> with a hashed CSS-in-JS
        # class, never a stable one, so a plain "table tr td" wait spins
        # the full timeout on every zero-match lookup. XPath text match
        # catches it either way -- live-verified 2026-08-23 (interactive,
        # non-headless) this is why goto_doc_by_docnumber never moved off
        # 0/100 even after the date-range fix.
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.XPATH, "//table//tr/td | //h1[contains(text(),'No Results')]")
            )
        )
        if driver.find_elements(By.XPATH, "//h1[contains(text(),'No Results')]"):
            log.info(f"  goto_doc_by_docnumber: no results for {doc_number}")
            return False
        time.sleep(1)

        row = driver.find_element(By.CSS_SELECTOR, "table tbody tr")
        row.click()
        WebDriverWait(driver, timeout).until(EC.url_contains("/doc/"))
        time.sleep(1.5)
        return True
    except Exception as e:
        # v28.41: dump what's actually on the page -- the date-range fix
        # (v28.40) didn't move this off 0/100, same blank "Message: " as
        # before with no way to tell whether it's a real timeout, a "No
        # Results" state, or a bot-challenge interstitial headless Chrome
        # hits that an interactive session doesn't (already documented
        # behavior on the Nueces scraper's analogous function).
        try:
            src = driver.page_source
            log.warning(f"  goto_doc_by_docnumber: lookup failed for {doc_number} "
                        f"(url={driver.current_url!r}, title={driver.title!r}): {e} | "
                        f"page_source len={len(src)} snippet={src[:600]!r}")
        except Exception as e2:
            log.warning(f"  goto_doc_by_docnumber: lookup failed for {doc_number}: {e} "
                        f"| could not read page_source: {e2}")
        return False


def extract_loan_details(driver):
    """
    Assumes the driver is already sitting on a PublicSearch /doc/<id> page
    for a Notice of [Substitute] Trustee's Sale (e.g. after
    goto_doc_by_click). OCRs that page for the lender name and the doc
    number of the original Deed of Trust it references, then — if found —
    hops to that referenced document and OCRs it too for the actual
    principal loan amount, which foreclosure notices don't state directly.
    Returns a dict with all four keys, blank string where nothing matched.
    """
    import re as _re

    loan_amount = ""
    loan_date   = ""
    lender      = ""
    trustee     = ""

    notice_text = ocr_current_doc_page(driver)

    if notice_text:
        m = _re.search(
            r"(?:duly\s+)?recorded[^.]*?Document\s+Number\s+(\d{6,})\s+on\s+"
            r"([A-Za-z]+ \d{1,2},\s*\d{4})",
            notice_text, _re.IGNORECASE)
        referenced_doc, dot_date = (m.group(1), m.group(2)) if m else (None, None)
        if dot_date:
            loan_date = dot_date.strip()

        m = _re.search(
            r"([A-Z][A-Za-z0-9,.&\s]{2,60}?)\s+is\s+the\s*[\"'“]?Lender",
            notice_text)
        if m:
            lender = m.group(1).strip().rstrip(",")

        m = _re.search(
            r"Substitute\s+Trustee[s]?[:\s,]+([A-Z][^\n,]{4,80})",
            notice_text, _re.IGNORECASE)
        if m:
            trustee = m.group(1).strip()

        if referenced_doc:
            log.info(f"  Notice references Deed of Trust {referenced_doc} ({dot_date}) — hopping to it")
            dot_text = search_and_ocr_referenced_doc(driver, referenced_doc)
            if dot_text:
                for pattern in [
                    r"principal\s+amount\s+of\s*\$?([\d,]+(?:\.\d{2})?)",
                    r"principal\s+sum\s+of\s*\$?([\d,]+(?:\.\d{2})?)",
                    r"in\s+the\s+(?:original\s+)?amount\s+of\s*\$?([\d,]+(?:\.\d{2})?)",
                ]:
                    m = _re.search(pattern, dot_text, _re.IGNORECASE)
                    if m:
                        loan_amount = "$" + m.group(1)
                        break
                if not loan_amount:
                    log.warning(f"  Deed of Trust {referenced_doc} OCR'd but no amount pattern matched — "
                                f"first 300 chars: {dot_text[:300]!r}")
            else:
                log.warning(f"  Deed of Trust {referenced_doc}: OCR/lookup returned nothing")
        else:
            log.warning("  Notice OCR'd but no lender/referenced-doc pattern matched — "
                         f"first 400 chars: {notice_text[:400]!r}")
    else:
        log.warning("  extract_loan_details: OCR of the Notice page returned no text")

    if not loan_amount:
        log.warning("  extract_loan_details: loan_amount still blank after OCR + referenced-doc hop")

    return {"loan_amount": loan_amount, "loan_date": loan_date,
            "lender": lender, "trustee": trustee}


def fetch_doc_details(records, driver):
    # v28.36: dropped the date_filed-based recency window — date_filed only
    # has month/year granularity (e.g. "8/2026"), which the old code turned
    # into "the 1st of the month" and compared against a DOC_FETCH_DAYS-ago
    # cutoff. Past the 6th of any month that comparison always fails, so
    # every candidate was silently excluded for most of every month —
    # confirmed live 2026-08-11: 8 candidates found, 0 survived the window.
    # Backfill-eligible like BCAD/ARV instead: no date filter, just capped.
    #
    # v28.38: also dropped the _source_url requirement. That field only
    # survives in memory for leads discovered brand-new in THIS run (it's
    # stripped before every save), so on any "0 new" day -- most days --
    # this candidate list was structurally empty regardless of the above
    # fix. goto_doc_by_docnumber() looks up any doc number directly via
    # quickSearch instead, so it no longer matters when a lead was found.
    candidates = [
        r for r in records
        if r.get("type") in ("NOF", "TAX")
        and not r.get("loan_amount")
        and r.get("source", "publicsearch") == "publicsearch"
        and r.get("doc_number")
    ]
    log.info(f"Doc fetch: {len(candidates)} candidates missing loan data")

    recent = candidates[:DOC_FETCH_LIMIT]
    log.info(f"Doc fetch: {len(recent)} selected (capped at {DOC_FETCH_LIMIT})")

    if not recent:
        log.info("Doc fetch: no leads missing loan data — skipping")
        return records

    log.info(f"Doc fetch: enriching {len(recent)} leads with mortgage intel...")
    fetched = 0

    for rec in recent:
        doc_num = rec["doc_number"]
        log.info(f"  Doc [{doc_num}] locating via quickSearch")

        if not goto_doc_by_docnumber(driver, doc_num):
            log.info(f"  Doc [{doc_num}] lookup failed — skipping")
            continue

        details = extract_loan_details(driver)
        rec["loan_amount"] = details["loan_amount"]
        rec["loan_date"]   = details["loan_date"]
        rec["lender"]      = details["lender"]
        rec["trustee"]     = details["trustee"]
        fetched += 1

        log.info(f"  → amt={details['loan_amount'] or '—'} | "
                 f"lender={details['lender'][:35] if details['lender'] else '—'} | "
                 f"date={details['loan_date'] or '—'}")
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
    SALE_DATE_FIELDS = ["SaleDate","SALE_DATE","saleDate","LastSaleDate","LAST_SALE_DATE","SaleDt","SALE_DT"]
    PROPID_FIELDS   = ["PropID","PROP_ID","PropId","prop_id","PropertyID","PROPERTY_ID","ParcelID","PARCEL_ID"]

    def get_field(a, candidates):
        for c in candidates:
            v = a.get(c)
            if v is not None and str(v).strip() not in ("","None","null","<Null>","NULL","0"):
                return str(v).strip()
        return ""

    def get_raw(a, candidates):
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
            rec["sale_date_arcgis"]   = result.get("sale_date_arcgis", "")
            rec["tenure_years"]       = result.get("tenure_years", None)
            rec["tenure_score_bonus"] = result.get("tenure_score_bonus", 0)
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


# ── BCAD DEED HISTORY + ARV ───────────────────────────────────────────────────
def fetch_deed_and_arv(records, driver):
    """
    v28.33: Fix for trueautomation.com structure.
    KEY FINDING: accordion sections use div.titleBar / div.details pattern.
    Deed history is ALREADY EXPANDED on page load (opened="true").
    No clicking needed — read #deedHistoryDetails table directly.
    Also reads #valuesDetails for last sale amount.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    import re as _re

    candidates = [
        r for r in records
        if r.get("prop_id")
        and not r.get("deed_date")
        and r.get("type") in ("NOF", "TAX", "LP", "APPT")
    ]
    candidates = candidates[:DEED_FETCH_LIMIT]

    if not candidates:
        log.info("BCAD deed+ARV: no eligible leads with prop_id — skipping")
        return records

    log.info(f"BCAD deed+ARV: {len(candidates)} leads to enrich (v28.33 — direct read)")
    fetched = 0
    errors  = 0

    for rec in candidates:
        prop_id = rec["prop_id"]
        addr    = rec.get("address", "")
        url     = BCAD_DETAIL_URL.format(prop_id=int(float(prop_id)))
        log.info(f"  BCAD [{prop_id}] {addr}")

        try:
            driver.set_page_load_timeout(30)
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
            # ── Ensure deed history section is visible ────────────────────────
            # div#deedHistory is the titleBar; div#deedHistoryDetails is content.
            # Page loads with it expanded (opened="true") but click it just in case.
            try:
                hdr = driver.find_element(By.CSS_SELECTOR, "div#deedHistory")
                opened = (hdr.get_attribute("opened") or "").lower()
                if opened != "true":
                    driver.execute_script("arguments[0].click();", hdr)
                    time.sleep(2)
                    log.info(f"  BCAD [{prop_id}] clicked deedHistory titleBar")
                else:
                    log.info(f"  BCAD [{prop_id}] deedHistory already open")
            except Exception:
                # Try by class if id not found
                try:
                    hdrs = driver.find_elements(By.CSS_SELECTOR, "div.titleBar")
                    for hdr in hdrs:
                        if "deed" in (hdr.text or "").lower():
                            driver.execute_script("arguments[0].click();", hdr)
                            time.sleep(2)
                            log.info(f"  BCAD [{prop_id}] clicked deed titleBar by class")
                            break
                except Exception:
                    pass

            # ── Also open Values section for last_sale_amt ────────────────────
            try:
                val_hdrs = driver.find_elements(By.CSS_SELECTOR, "div.titleBar")
                for hdr in val_hdrs:
                    txt = (hdr.text or "").lower()
                    if "value" in txt and "deed" not in txt:
                        opened = (hdr.get_attribute("opened") or "").lower()
                        if opened != "true":
                            driver.execute_script("arguments[0].click();", hdr)
                            time.sleep(1)
                        break
            except Exception:
                pass

            # Wait for deed history details to be visible
            try:
                WebDriverWait(driver, 8).until(
                    lambda d: d.find_element(
                        By.CSS_SELECTOR, "div#deedHistoryDetails"
                    ).is_displayed()
                )
            except Exception:
                pass

            # ── Read deed history table ───────────────────────────────────────
            # Structure: div#deedHistoryDetails > table > tbody > tr
            # First data row columns: #, Deed Date, Type, Description, Grantor, Grantee...
            try:
                deed_section = driver.find_element(
                    By.CSS_SELECTOR, "div#deedHistoryDetails"
                )
                rows = deed_section.find_elements(By.CSS_SELECTOR, "table tbody tr")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 2:
                        continue
                    for cell in cells:
                        ctxt = (cell.text or "").strip()
                        m = _re.match(r"^(\d{1,2}/\d{1,2}/\d{4})$", ctxt)
                        if m:
                            deed_date = m.group(1)
                            log.info(f"  BCAD [{prop_id}] deed date from #deedHistoryDetails: {deed_date}")
                            break
                    if deed_date:
                        break
            except Exception as de:
                log.debug(f"  BCAD [{prop_id}] deedHistoryDetails read error: {de}")

            # Fallback: scan all tables if #deedHistoryDetails not found
            if not deed_date:
                try:
                    body_text = driver.find_element(By.TAG_NAME, "body").text
                    lines = [l.strip() for l in body_text.split("\n") if l.strip()]
                    in_deed = False
                    for line in lines:
                        if "deed history" in line.lower():
                            in_deed = True
                            continue
                        if in_deed:
                            m = _re.match(r"^(\d{1,2}/\d{1,2}/\d{4})$", line)
                            if m:
                                deed_date = m.group(1)
                                log.info(f"  BCAD [{prop_id}] deed date from body text: {deed_date}")
                                break
                            if line.isupper() and len(line) > 15:
                                break
                except Exception:
                    pass

            # ── Read Roll Value History for appraisal trend ───────────────────
            # div#rollHistoryDetails table: Year | Improvements | Land Market |
            #   Ag Valuation | Appraised | HS Cap | Assessed
            # We want the Appraised column (index 4) for last 3 years.
            appr_history  = []   # list of {"year": "2026", "appraised": 291750}
            appr_trend    = ""   # "up", "down", "flat"
            try:
                # Click Roll Value History open if collapsed
                try:
                    roll_hdr = driver.find_element(By.CSS_SELECTOR, "div#rollHistory")
                    opened = (roll_hdr.get_attribute("opened") or "").lower()
                    if opened != "true":
                        driver.execute_script("arguments[0].click();", roll_hdr)
                        time.sleep(1.5)
                except Exception:
                    # Fallback: find by text
                    try:
                        hdrs = driver.find_elements(By.CSS_SELECTOR, "div.titleBar")
                        for hdr in hdrs:
                            if "roll value" in (hdr.text or "").lower():
                                driver.execute_script("arguments[0].click();", hdr)
                                time.sleep(1.5)
                                break
                    except Exception:
                        pass

                roll_section = driver.find_element(
                    By.CSS_SELECTOR, "div#rollHistoryDetails"
                )
                rows = roll_section.find_elements(By.CSS_SELECTOR, "table tbody tr")
                for row in rows[:4]:  # last 4 years max
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        yr_txt   = (cells[0].text or "").strip()
                        appr_txt = (cells[4].text or "").strip().replace("$", "").replace(",", "")
                        try:
                            yr   = int(yr_txt)
                            appr = float(appr_txt)
                            if yr > 2000 and appr > 0:
                                appr_history.append({"year": str(yr), "appraised": int(appr)})
                        except Exception:
                            continue
            except Exception as re:
                log.debug(f"  BCAD [{prop_id}] roll history error: {re}")

            # Calculate trend from most recent 2 years
            if len(appr_history) >= 2:
                latest  = appr_history[0]["appraised"]
                prev    = appr_history[1]["appraised"]
                pct_chg = (latest - prev) / prev * 100 if prev else 0
                if pct_chg > 2:
                    appr_trend = "up"
                elif pct_chg < -2:
                    appr_trend = "down"
                else:
                    appr_trend = "flat"
                log.info(f"  BCAD [{prop_id}] roll history: {appr_history[:3]} trend={appr_trend} ({pct_chg:+.1f}%)")
            else:
                log.info(f"  BCAD [{prop_id}] roll history: no data found")

        except Exception as e:
            log.warning(f"  BCAD [{prop_id}] parse error: {e}")
            errors += 1
            continue

        # ── Calculate tenure from deed_date ───────────────────────────────────
        if deed_date:
            try:
                deed_dt    = datetime.strptime(deed_date.strip(), "%m/%d/%Y")
                tenure_yrs = max((TODAY_NAIVE - deed_dt).days // 365, 0)
            except Exception:
                tenure_yrs = None

        rec["deed_date"]          = deed_date
        rec["last_sale_amt"]      = last_sale_amt
        rec["tenure_years"]       = tenure_yrs
        rec["tenure_score_bonus"] = tenure_bonus(tenure_yrs)
        rec["appr_history"]       = appr_history   # v28.35: roll value history
        rec["appr_trend"]         = appr_trend     # v28.35: "up"/"down"/"flat"
        fetched += 1

        log.info(f"  → deed={deed_date or '—'} tenure={tenure_yrs}yr "
                 f"trend={appr_trend or '—'} bonus=+{tenure_bonus(tenure_yrs)}")
        time.sleep(2)

    log.info(f"BCAD deed+ARV: {fetched} enriched, {errors} errors out of {len(candidates)} candidates")
    return records


def fetch_arv_homeharvest(records):
    """
    Free ARV estimate via homeharvest (pip, MIT license) scraping Realtor.com's
    public page data — no API key, no cost. Realtor.com blends CoreLogic,
    Collateral Analytics, and Quantarium AVMs into an estimated_value field
    that's present even for off-market/distressed properties (confirmed live
    2026-08-10 against real Bexar addresses, sold and pending alike).

    Chosen over Zillow/Redfin scraping: both run aggressive bot detection
    (PerimeterX/Cloudflare) that blocks datacenter IPs — including GitHub
    Actions runners — within minutes. Realtor.com is comparatively tractable
    right now, but its ToS still prohibits automated access, so this is kept
    as a soft dependency: any failure (network, no match, library error)
    just leaves arv_estimate blank rather than breaking the run, and the
    lead still has BCAD's appraised_value as a fallback value signal.

    No Selenium driver needed — homeharvest does its own HTTP requests.
    """
    import pandas as pd
    from homeharvest import scrape_property

    def clean(val):
        """pandas NA/NaN don't survive `in (None, "", ...)` checks — they
        raise 'boolean value of NA is ambiguous' instead of comparing
        False, which is exactly what crashed record #12 out of 30 here on
        2026-08-10 and silently killed every candidate after it."""
        if val is None or pd.isna(val):
            return None
        s = str(val).strip()
        return None if s in ("", "nan", "<NA>", "None") else val

    all_eligible = [
        r for r in records
        if r.get("address") and not r.get("arv_estimate")
    ]
    # 2026-08-27: VBP leads were starving forever behind the NOF/TAX
    # backlog -- confirmed live, 0 of 394 VBP leads had ever been checked
    # despite this function covering them with no type filter, since
    # candidates were taken in plain list order out of 1300+ records.
    # But NOF/TAX have their own real backlog too (449 unchecked, some
    # auction-driven) -- fully displacing them to clear VBP would just
    # trade one starved queue for another. Split the existing budget
    # instead: up to half reserved for VBP (newest first, so a fresh
    # weekly drop gets checked promptly), the rest fills from everyone
    # else in original order. Realtor.com hard-blocks after ~27
    # consecutive requests per run (see ON_MARKET_REFRESH_LIMIT note
    # below) -- this doesn't raise the total request count, just
    # reallocates the same capped budget.
    vbp_pool = [r for r in all_eligible if r.get("type") == "VBP"]
    vbp_pool.sort(key=lambda r: r.get("date_filed", ""), reverse=True)
    other_pool = [r for r in all_eligible if r.get("type") != "VBP"]
    vbp_share = min(len(vbp_pool), ARV_FETCH_LIMIT // 2)
    candidates = vbp_pool[:vbp_share] + other_pool[:ARV_FETCH_LIMIT - vbp_share]
    candidates = candidates[:ARV_FETCH_LIMIT]

    if not candidates:
        log.info("ARV (HomeHarvest): no eligible leads — skipping")
        return records

    log.info(f"ARV (HomeHarvest): {len(candidates)} leads to look up (cap={ARV_FETCH_LIMIT})")
    fetched = 0
    errors  = 0

    for rec in candidates:
        full_addr = f"{rec['address']}, {rec.get('city', '')}, TX {rec.get('zip', '')}".strip(", ")
        try:
            df = scrape_property(location=full_addr)
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            if df is None or len(df) == 0:
                log.info(f"  ARV [{rec.get('doc_number')}] {full_addr}: no match on Realtor.com")
                rec["on_market_checked_at"] = now_iso
                continue

            row = df.iloc[0]
            status = clean(row.get("status")) or ""
            rec["on_market"]            = status in ON_MARKET_STATUSES
            rec["on_market_status"]     = status
            rec["on_market_checked_at"] = now_iso

            est = clean(row.get("estimated_value"))
            if est is not None:
                rec["arv_estimate"]   = int(est)
                rec["arv_status"]     = status
                sqft_val = clean(row.get("sqft"))
                rec["arv_sqft"]       = int(sqft_val) if sqft_val is not None else None
                rec["arv_fetched_at"] = now_iso
                fetched += 1
                log.info(f"  ARV [{rec.get('doc_number')}] {full_addr}: ${int(est):,} (status={status})")
            else:
                log.info(f"  ARV [{rec.get('doc_number')}] {full_addr}: matched but no estimated_value (status={status})")
        except Exception as e:
            # Per-record guard — one bad row (pandas NA quirks, unexpected
            # schema, network blip) must never take the rest of the batch
            # down with it, which is exactly what happened here before.
            log.warning(f"  ARV [{rec.get('doc_number')}] {full_addr}: error: {e}")
            errors += 1
            # v28.44: fall back to BCAD's own appraised_value rather than
            # leaving arv_estimate permanently blank when Realtor.com blocks
            # the request (see refresh_on_market_status's v28.44 note --
            # this function needs extra_property_data=True for
            # estimated_value, which is exactly what's being blocked, so
            # there's no equivalent unblocking fix available here). County
            # appraised value is a real, if more conservative, value signal
            # -- BCAD deed+ARV already runs before this and populates it.
            # arv_status "APPRAISED_FALLBACK" marks it as lower-confidence
            # than a true Realtor.com estimate so downstream code/dashboard
            # can tell the two apart.
            bcad_value = rec.get("appraised_value")
            if bcad_value and not rec.get("arv_estimate"):
                try:
                    rec["arv_estimate"]   = int(float(bcad_value))
                    rec["arv_status"]     = "APPRAISED_FALLBACK"
                    rec["arv_fetched_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                except (TypeError, ValueError):
                    pass
        finally:
            time.sleep(1)

    log.info(f"ARV (HomeHarvest): {fetched} enriched, {errors} errors out of {len(candidates)} candidates")
    return records


def refresh_on_market_status(records):
    """
    Re-checks on-market status for leads that already went through the ARV
    pass above (so they were excluded from those candidates) but haven't
    had a fresh on-market check in ON_MARKET_REFRESH_DAYS — a lead can get
    listed by someone else weeks after we first looked it up, and the ARV
    step only ever looks at a lead once (gated on `not arv_estimate`).
    Same soft-dependency handling as fetch_arv_homeharvest: any failure
    just skips that lead, never breaks the run.

    v28.44: extra_property_data=False. Found live 2026-08-26 (100% of both
    this and the ARV pass started failing with AuthenticationError that
    morning, up from occasional failures the night before) that HomeHarvest's
    own GitHub issue #149 documents this exact failure mode: Realtor.com
    (AWS WAF/PerimeterX) specifically blocks the *deeper* per-property calls
    that extra_property_data=True triggers -- the base search still works.
    This function only ever reads `status`, which survives without it;
    estimated_value (needed by fetch_arv_homeharvest, not this function)
    does not, so that function still needs extra_property_data=True and
    still eats the block. Confirmed via the library's own default
    (extra_property_data: bool = True) that we were opted into the blocked
    path by default without ever setting it explicitly.
    """
    import pandas as pd
    from homeharvest import scrape_property

    def clean(val):
        if val is None or pd.isna(val):
            return None
        s = str(val).strip()
        return None if s in ("", "nan", "<NA>", "None") else val

    cutoff = datetime.utcnow() - timedelta(days=ON_MARKET_REFRESH_DAYS)

    def needs_refresh(r):
        if not r.get("address") or not r.get("arv_estimate"):
            return False  # never-checked leads are handled by the ARV pass
        checked_at = r.get("on_market_checked_at")
        if not checked_at:
            return True
        try:
            return datetime.strptime(checked_at, "%Y-%m-%dT%H:%M:%SZ") < cutoff
        except Exception:
            return True

    candidates = [r for r in records if needs_refresh(r)]
    candidates = candidates[:ON_MARKET_REFRESH_LIMIT]

    if not candidates:
        log.info("On-market refresh: no eligible leads — skipping")
        return records

    log.info(f"On-market refresh: {len(candidates)} leads to re-check (cap={ON_MARKET_REFRESH_LIMIT})")
    changed = 0
    errors  = 0

    for rec in candidates:
        full_addr = f"{rec['address']}, {rec.get('city', '')}, TX {rec.get('zip', '')}".strip(", ")
        try:
            df = scrape_property(location=full_addr, extra_property_data=False)
            now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            was_on_market = bool(rec.get("on_market"))

            if df is None or len(df) == 0:
                rec["on_market_checked_at"] = now_iso
                continue

            status = clean(df.iloc[0].get("status")) or ""
            rec["on_market"]            = status in ON_MARKET_STATUSES
            rec["on_market_status"]     = status
            rec["on_market_checked_at"] = now_iso

            if rec["on_market"] != was_on_market:
                changed += 1
                log.info(f"  On-market [{rec.get('doc_number')}] {full_addr}: "
                         f"{was_on_market} -> {rec['on_market']} (status={status})")
        except Exception as e:
            log.warning(f"  On-market [{rec.get('doc_number')}] {full_addr}: error: {e}")
            errors += 1
        finally:
            time.sleep(1)

    log.info(f"On-market refresh: {changed} status changes, {errors} errors out of {len(candidates)} candidates")
    return records


def _month_year_age_days(date_filed):
    """date_filed is stored as 'MM/YYYY' (day-level precision isn't
    available from the source) — approximate age from the 1st of that
    month. Good enough for a coarse staleness threshold, not exact."""
    try:
        parts = date_filed.strip().split("/")
        if len(parts) != 2:
            return None
        dt = datetime(int(parts[1]), int(parts[0]), 1)
        return (TODAY_NAIVE - dt).days
    except Exception:
        return None


def link_appt_to_nof(records):
    """
    Cross-reference APPT (Appointment of Substitute Trustee / pre-fore)
    leads against NOF/TAX leads for the same owner+address.

    If a matching NOF/TAX shows up, the pre-fore signal was real —
    mark the APPT record converted (useful lineage, distinct from a lead
    that just never went anywhere).

    If an APPT has no matching NOF/TAX after ~60 days (NOF typically
    follows an APPT by 2-4 weeks per CLAUDE.md), it likely resolved —
    cured, refinanced, or withdrawn. Flag it stale so it can be
    deprioritized/parked instead of treated as an indefinite live lead.
    """
    def key_for(r):
        owner = (r.get("owner") or "").upper().strip()
        addr  = (r.get("address") or "").upper().strip()
        return (owner, addr) if owner and addr else None

    nof_by_key = {}
    for r in records:
        if r.get("type") in ("NOF", "TAX"):
            k = key_for(r)
            if k:
                nof_by_key.setdefault(k, []).append(r)

    converted = 0
    stale = 0
    for r in records:
        if r.get("type") != "APPT":
            continue
        r["appt_converted"] = False
        r["appt_stale"] = False

        k = key_for(r)
        if k and k in nof_by_key:
            r["appt_converted"] = True
            r["converted_doc_number"] = nof_by_key[k][0].get("doc_number", "")
            converted += 1
            continue

        age = _month_year_age_days(r.get("date_filed", ""))
        if age is not None and age > 60:
            r["appt_stale"] = True
            stale += 1

    log.info(f"APPT cross-ref: {converted} converted to NOF/TAX | {stale} stale (60d+, no NOF yet)")
    return records


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
    # v28.34: tenure bonus capped so total never exceeds 10
    # prevents old leads from being resurfaced with inflated scores
    s += rec.get("tenure_score_bonus", 0)
    # APPT leads that already converted to a real NOF/TAX are redundant —
    # work the NOF instead, which has more info (sale date). Ones that
    # went 60d+ with no NOF likely resolved on their own. Either way,
    # deprioritize the APPT record itself rather than keep surfacing it.
    if rec.get("type") == "APPT" and (rec.get("appt_converted") or rec.get("appt_stale")):
        s = max(s - 4, 0)
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
    log.info("Bexar County Lead Scraper v28.39 (Hybrid)")
    log.info(f"Primary:   PublicSearch.us ({KEEP_DAYS}d window, {CHUNK_DAYS}d chunks, {PAGE_TIMEOUT}s timeout)")
    log.info(f"Secondary: ArcGIS weekly backfill = {IS_SUNDAY}")
    log.info(f"Tertiary:  Code Enforcement 311 ({len(CE_CATEGORIES)} categories, {KEEP_DAYS}d window)")
    log.info(f"Doc fetch: OCR loan/lender detail (backfill-eligible, cap={DOC_FETCH_LIMIT})")
    log.info(f"Tenure:    scoring active — 15+yr=+25pts, 10-14yr=+15pts, 5-9yr=+5pts")
    log.info(f"BCAD:      deed history + ARV via trueautomation.com (backfill-eligible, cap={DEED_FETCH_LIMIT})")
    log.info(f"ARV:       HomeHarvest/Realtor.com (backfill-eligible, cap={ARV_FETCH_LIMIT})")
    log.info(f"Filter:    {KEEP_DAYS}-day cutoff ({CUTOFF_DATE.strftime('%Y-%m-%d')}) | live auctions always kept")
    log.info("=" * 60)

    known_docs, prev_records = load_known_docs()

    # ── Step 1: PublicSearch chunked scrape ───────────────────────────────────
    new_records = scrape_publicsearch(known_docs)

    # ── Step 1b: Doc detail fetch ─────────────────────────────────────────────
    doc_driver = None
    try:
        doc_driver = get_driver()
        # v28.39: login_publicsearch() existed but was never actually called
        # anywhere in this file -- confirmed live 2026-08-20: every single
        # goto_doc_by_docnumber() lookup failed identically (~20s timeout
        # waiting for "table tr td" on the quickSearch results page, 8/8
        # candidates, 0 exceptions). The chunked /results browse in
        # scrape_publicsearch() works fine anonymously, but quickSearch
        # (single-doc-number lookup, used for every doc-detail/OCR hop)
        # appears to require an authenticated session -- exactly what
        # CLERK_EMAIL/CLERK_PASSWORD were already wired up for.
        if not login_publicsearch(doc_driver):
            log.warning("Doc fetch: PublicSearch login failed or was skipped — quickSearch lookups will likely fail")
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

    # ── Step 1c: Lis Pendens — disabled 2026-08-06 ───────────────────────────
    # Site is returning records from 2018/2023 regardless of the 21-day
    # instrumentDateRange filter (confirmed: 4047 "new" LP records appeared
    # in one run, all dated 2018/2023, right after known_docs lost its LP
    # entries from a purge). Foreclosure-only focus for now per user
    # request; needs a real fix (client-side date safety filter at minimum)
    # before re-enabling.
    lp_records = []

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

    prev_by_doc = {r["doc_number"]: r for r in prev_records if r.get("doc_number")}

    VBP_CE_FIELDS = [
        "stacked", "ce_violations", "ce_viol_types", "ce_count",
        "ce_cat_label", "ce_status", "opened_date", "ce_case_id",
    ]

    TENURE_FIELDS = ["sale_date_arcgis", "tenure_years", "tenure_score_bonus",
                     "prop_id", "deed_date", "last_sale_amt",
                     "appr_history", "appr_trend"]  # v28.35

    seen = {}
    for r in new_records + arcgis_records + lp_records + ce_records + prev_records:
        doc = r.get("doc_number", "")
        if not doc:
            continue
        if doc not in seen:
            seen[doc] = r

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

    # ── Step 6b: BCAD deed history + ARV (backfill-eligible: prop_id, no deed_date)
    bcad_driver = None
    try:
        # v28.32: removed is_new check — backfills ALL leads with prop_id and no deed_date
        needs_deed = [r for r in records if r.get("prop_id") and not r.get("deed_date")]
        if needs_deed:
            bcad_driver = get_driver()
            records = fetch_deed_and_arv(records, bcad_driver)
        else:
            log.info("BCAD deed+ARV: no leads with prop_id missing deed_date — skipping")
    except Exception as e:
        log.warning(f"BCAD deed+ARV driver error: {e}")
    finally:
        if bcad_driver:
            try:
                bcad_driver.quit()
            except Exception:
                pass

    # ── Step 6c: ARV via HomeHarvest (free, Realtor.com, no Selenium needed) ──
    try:
        records = fetch_arv_homeharvest(records)
    except Exception as e:
        log.warning(f"ARV (HomeHarvest) error: {e}")

    # ── Step 6d: on-market status refresh for leads already checked once ──
    try:
        records = refresh_on_market_status(records)
    except Exception as e:
        log.warning(f"On-market refresh error: {e}")

    # ── Step 7: Duplicate detection ───────────────────────────────────────────
    records = detect_duplicates(records)
    records = link_appt_to_nof(records)

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
        if (r.get("tenure_years") or 0) >= 10:   r["flags"].append("LONG HELD")
        if r.get("appt_converted"):              r["flags"].append("CONVERTED TO NOF")
        if r.get("appt_stale"):                  r["flags"].append("LIKELY RESOLVED")
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
    with_trend  = sum(1 for r in records if r.get("appr_trend"))
    with_propid = sum(1 for r in records if r.get("prop_id"))

    log.info(f"Final: {len(records)} total | {named} named | {absentee} absentee")
    log.info(f"       {new_ct} new | {has_date} with sale date | "
             f"{soon} auction <=30d | {urgent} URGENT <=14d")
    log.info(f"       VBP stacked: {stacked} confirmed VBP+CE leads")
    log.info(f"       Mortgage intel: {enriched} leads with loan_amount populated")
    log.info(f"       Tenure: {with_tenure} with tenure | {long_held} LONG HELD (10+ yr) | {with_deed} deed dates")
    log.info(f"       BCAD: {with_propid} with prop_id | {with_trend} with appr trend | {with_deed} deed dates")

    # ── Step 10: Save ─────────────────────────────────────────────────────────
    # _source_url is only needed during this run's doc-page click-through —
    # strip it so it doesn't bloat records.json or leak into the dashboard.
    for r in records:
        r.pop("_source_url", None)

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    build_dashboard(records)
    log.info("Done.")
