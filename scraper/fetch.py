"""
Bexar County Motivated Seller Lead Scraper v27.2
HYBRID SCRAPER:
  Primary:   bexar.tx.publicsearch.us  (Selenium, runs 3x daily)
             - Scrapes last 90 days to match dashboard keep window
             - Includes sale date, doc number, address
             - Grantor extraction skipped (detail pages have no party data)
  Secondary: ArcGIS GIS layer (urllib, runs weekly on Sunday)
             - Backfill only — fills gaps missed by primary
             - Absentee owner detection via parcel mailing address

  Owner enrichment: 5-strategy ArcGIS parcel lookup for any record missing owner

  Filter logic (v27.1+):
    KEEP if filed within 90 days
    KEEP if sale_date is in the future (auction still live)
    DROP if older than 90 days AND no future sale date AND no address

  Fixes v27.2:
    - Scrape window bumped from 14 → 90 days (matches dashboard keep window)
    - known_docs rebuilt from filtered records after filter runs, so dropped
      records don't stay "known" forever and get re-evaluated each run
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
PUBLICSEARCH_BASE = "https://bexar.tx.publicsearch.us"
FORECLOSURE_BASE  = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
PARCELS_URL       = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"
PAGES_RECORDS     = "https://e4property.github.io/bexar-leads/records.json"

LAYERS = [
    {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
    {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
]

RUN_TIMESTAMP  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TODAY          = datetime.now(timezone.utc)
TODAY_NAIVE    = datetime.now()
IS_SUNDAY      = TODAY.weekday() == 6
CUTOFF_DATE    = TODAY_NAIVE - timedelta(days=90)  # 90-day rolling window
SCRAPE_DAYS    = 90                                 # Match dashboard keep window


# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/27.2", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                log.debug(f"fetch failed: {e}")
                return {}


def arcgis_query(layer_url, where, fields="*", limit=200):
    """Query ArcGIS with pagination to handle exceededTransferLimit."""
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
                log.debug(f"ArcGIS error: {data.get('error') if data else 'empty'}")
                break
            batch = data.get("features", [])
            all_features.extend(batch)
            exceeded = data.get("exceededTransferLimit", False)
            if not exceeded or len(batch) < limit:
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
    try:
        req = urllib.request.Request(
            PAGES_RECORDS + "?v=" + str(int(time.time())),
            headers={"User-Agent": "BexarScraper/27.2", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            prev = json.loads(r.read().decode("utf-8", errors="replace"))
            docs = {str(rec.get("doc_number", "")) for rec in prev if rec.get("doc_number")}
            log.info(f"Loaded {len(docs)} known doc numbers from previous run")
            return docs, prev
    except Exception as e:
        log.info(f"No previous records found (first run?): {e}")
        return set(), []


# ── RECORD FILTER ─────────────────────────────────────────────────────────────
def should_keep(rec):
    """
    Keep a record if ANY of the following are true:
      1. Has a future sale date (auction still live) — always keep
      2. Filed within the last 90 days
    Drop if:
      - Older than 90 days AND no future sale date
      - Completely empty (no address, no owner, no sale date)
    """
    # Always drop completely empty records
    if not rec.get("address") and not rec.get("owner") and not rec.get("sale_date"):
        return False

    # Check if sale date is in the future — always keep live auctions
    sale_date_str = rec.get("sale_date", "")
    if sale_date_str:
        try:
            sale_dt = datetime.strptime(sale_date_str.strip(), "%m/%d/%Y")
            if sale_dt >= TODAY_NAIVE:
                return True
        except Exception:
            pass

    # Check filed date against 90-day cutoff
    # date_filed format: "M/YYYY" e.g. "4/2026"
    date_filed = rec.get("date_filed", "")
    if date_filed:
        try:
            parts = date_filed.strip().split("/")
            if len(parts) == 2:
                month, year = int(parts[0]), int(parts[1])
                filed_dt = datetime(year, month, 1)
                if filed_dt >= CUTOFF_DATE:
                    return True
                else:
                    return False
        except Exception:
            pass

    # Can't parse date — keep (benefit of the doubt)
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
    opts.add_argument("--window-size=1280,900")
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


# ── PUBLICSEARCH SCRAPER ──────────────────────────────────────────────────────
def scrape_publicsearch(known_docs, days_back=90):
    """
    Scrape bexar.tx.publicsearch.us for foreclosure filings.
    Scrape window matches dashboard keep window (90 days).
    Owner enrichment done later via ArcGIS parcel lookup.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    start_date = (TODAY - timedelta(days=days_back)).strftime("%Y%m%d")
    end_date   = (TODAY + timedelta(days=1)).strftime("%Y%m%d")

    search_url = (
        f"{PUBLICSEARCH_BASE}/results"
        f"?department=FC"
        f"&instrumentDateRange={start_date}%2C{end_date}"
        f"&keywordSearch=false"
        f"&limit=50"
        f"&offset=0"
        f"&sortBy=recordedDate"
        f"&sortDir=desc"
    )

    log.info(f"PublicSearch: scraping last {days_back} days ({start_date} to {end_date})")

    driver  = None
    records = []

    try:
        driver = get_driver()
        page   = 0
        offset = 0

        while True:
            url = search_url.replace("offset=0", f"offset={offset}")
            log.info(f"  Loading page {page+1} (offset={offset}): {url}")
            driver.get(url)

            # Dynamic timeout — later pages need more time for React to hydrate
            page_timeout = min(75 + (page * 20), 150)
            wait = WebDriverWait(driver, page_timeout)

            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "td.col-3")))
                time.sleep(3)
            except Exception as wait_err:
                log.info(f"  Page timed out waiting for results: {wait_err}")
                try:
                    log.info(f"  Page title: {driver.title}")
                    log.info(f"  Page source preview: {driver.page_source[:300]}")
                except Exception:
                    pass
                try:
                    time.sleep(15)
                    els = driver.find_elements(By.CSS_SELECTOR, "td.col-3")
                    if not els:
                        els2 = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                        log.info(f"  Alternate selector found {len(els2)} rows")
                        if not els2:
                            log.info("  Confirmed no results — stopping pagination")
                            break
                except Exception:
                    break

            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows:
                col3_cells = driver.find_elements(By.CSS_SELECTOR, "td.col-3")
                rows = []
                for cell in col3_cells:
                    try:
                        rows.append(cell.find_element(By.XPATH, ".."))
                    except Exception:
                        pass
            if not rows:
                log.info("  No rows found on this page — stopping")
                break

            col3s = driver.find_elements(By.CSS_SELECTOR, "td.col-3")
            first_sample = col3s[0].text[:30] if col3s else ""
            log.info(f"  Found {len(rows)} rows, first col-3 sample: {first_sample}")
            log.info(f"  Found {len(rows)} rows on page {page+1}")

            page_new = 0

            for row in rows:
                try:
                    def get_col(row, col_class):
                        try:
                            el = row.find_element(By.CSS_SELECTOR, f"td.{col_class}")
                            return el.text.strip()
                        except Exception:
                            return ""

                    doc_type_text = get_col(row, "col-3")
                    recorded_date = get_col(row, "col-4")
                    sale_date     = get_col(row, "col-5")
                    doc_number    = get_col(row, "col-6")
                    address       = get_col(row, "col-8")

                    doc_number = doc_number.strip()
                    address    = address.replace("\n", " ").replace(",", " ").strip()
                    sale_date  = sale_date.strip() if sale_date.strip() not in ("N/A", "") else ""

                    if not doc_type_text and not doc_number:
                        continue
                    if not doc_number:
                        continue
                    if doc_number in known_docs:
                        continue

                    rec_type       = "TAX" if "TAX" in doc_type_text.upper() else "NOF"
                    city, zip_code = parse_city_zip(address)
                    month, year    = parse_month_year(recorded_date)

                    rec = {
                        "type":        rec_type,
                        "address":     clean_address(address),
                        "owner":       "",
                        "mail_addr":   "",
                        "absentee":    False,
                        "duplicate":   False,
                        "is_new":      True,
                        "doc_number":  doc_number,
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
                    }
                    records.append(rec)
                    known_docs.add(doc_number)
                    page_new += 1

                except Exception as e:
                    log.debug(f"  Row parse error: {e}")
                    continue

            log.info(f"  Page {page+1}: {page_new} new records")

            # Stop if no new records on this page AND all rows were known
            # (don't stop just because page_new=0 — could be all dupes mid-window)
            if len(rows) < 50:
                break
            if page_new == 0 and page > 0:
                # Two consecutive pages with no new = we've caught up
                log.info("  No new records on this page — stopping pagination")
                break

            offset += 50
            page   += 1
            time.sleep(1.5)

    except Exception as e:
        log.error(f"PublicSearch scrape error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    log.info(f"PublicSearch: {len(records)} new records scraped")
    return records


# ── ADDRESS PARSING ───────────────────────────────────────────────────────────
def clean_address(raw):
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split(",")]
    return parts[0].strip().upper() if parts else raw.strip().upper()


def parse_city_zip(raw):
    parts    = [p.strip() for p in raw.split(",")]
    city     = ""
    zip_code = ""
    if len(parts) >= 4:
        city = parts[1].strip().upper()
        zip_match = re.search(r'\b(\d{5})\b', parts[3])
        zip_code = zip_match.group(1) if zip_match else parts[3].strip()
    elif len(parts) == 3:
        city = parts[1].strip().upper()
        zip_match = re.search(r'\b(\d{5})\b', parts[2])
        zip_code = zip_match.group(1) if zip_match else ""
    else:
        zip_match = re.search(r'\b(\d{5})\b', raw)
        zip_code = zip_match.group(1) if zip_match else ""
    return city, zip_code


def parse_month_year(date_str):
    try:
        parts = date_str.strip().split("/")
        if len(parts) >= 3:
            return parts[0], parts[2]
        elif len(parts) == 2:
            return parts[0], parts[1]
    except Exception:
        pass
    return "", ""


# ── ARCGIS BACKFILL (weekly) ──────────────────────────────────────────────────
def fetch_arcgis_backfill(known_docs):
    log.info("ArcGIS weekly backfill starting...")
    raw = []

    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{FORECLOSURE_BASE}/{idx}"
        log.info(f"  Layer {idx} ({layer['label']})...")
        features = []
        offset   = 0

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
                log.info(f"    offset={offset}: {len(batch)} (total: {len(features)})")
                if len(batch) < 1000:
                    break
                offset += len(batch)
            except Exception as e:
                log.error(f"Layer {idx} query error: {e}")
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


# ── OWNER ENRICHMENT ──────────────────────────────────────────────────────────
def parse_address_parts(address):
    if not address:
        return None
    parts = address.strip().upper().split()
    if not parts or not parts[0].isdigit():
        return None

    num  = parts[0]
    rest = parts[1:] if len(parts) > 1 else []

    SUFFIXES = {
        "ST", "AVE", "DR", "RD", "LN", "CT", "CIR", "BLVD", "WAY", "PL",
        "TRL", "PKWY", "HWY", "LOOP", "PASS", "CV", "PT", "HLS", "TRAIL",
        "GROVE", "RIDGE", "CREEK", "LAKE", "PARK", "GLEN", "RUN", "XING",
        "STREET", "AVENUE", "DRIVE", "ROAD", "LANE", "COURT", "CIRCLE",
        "BOULEVARD", "PARKWAY", "HIGHWAY",
    }

    words  = rest[:]
    suffix = ""
    if words and words[-1] in SUFFIXES:
        suffix = words[-1]
        words  = words[:-1]

    return {
        "num":    num,
        "street": " ".join(rest),
        "words":  words,
        "suffix": suffix,
        "full":   address.strip().upper(),
    }


def match_features(feats, num, required_word=None):
    """Confirmed field names from live API: Situs, Owner, AddrLn1, AddrCity, Zip"""
    for feat in feats:
        a = feat.get("attributes", {})

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

        return {
            "owner":     owner.upper(),
            "mail_addr": mail_addr,
            "absentee":  absentee,
        }
    return None


def lookup_owner(address, zipcode=""):
    parsed = parse_address_parts(address)
    if not parsed:
        return {}

    num        = parsed["num"]
    words      = parsed["words"]
    first_word = words[0] if words else ""
    FIELDS     = "Situs,Owner,AddrLn1,AddrCity,Zip"

    # Strategy 1: street number + first two words
    if len(words) >= 2:
        w1, w2 = words[0], words[1]
        feats  = arcgis_query(PARCELS_URL,
                              f"Situs LIKE '{num} {w1} {w2}%'",
                              fields=FIELDS, limit=50)
        result = match_features(feats, num, first_word)
        if result:
            result["method"] = "s1_two_word"
            return result

    # Strategy 2: street number + first word
    if first_word and len(first_word) >= 3:
        feats  = arcgis_query(PARCELS_URL,
                              f"Situs LIKE '{num} {first_word}%'",
                              fields=FIELDS, limit=100)
        result = match_features(feats, num, first_word)
        if result:
            result["method"] = "s2_num_first_word"
            return result

    # Strategy 3: street number only
    feats  = arcgis_query(PARCELS_URL,
                          f"Situs LIKE '{num} %'",
                          fields=FIELDS, limit=200)
    result = match_features(feats, num, first_word if first_word else None)
    if result:
        result["method"] = "s3_num_only"
        return result

    # Strategy 4: zip code scan + number match
    if zipcode and len(zipcode) >= 5:
        zip5  = zipcode[:5]
        feats = arcgis_query(PARCELS_URL,
                             f"Zip = '{zip5}'",
                             fields=FIELDS, limit=1000)
        result = match_features(feats, num, None)
        if result:
            result["method"] = "s4_zip_scan"
            return result

    # Strategy 5: alternate words in street name
    for word in words[1:]:
        if len(word) < 4:
            continue
        feats  = arcgis_query(PARCELS_URL,
                              f"Situs LIKE '{num} %{word}%'",
                              fields=FIELDS, limit=100)
        result = match_features(feats, num, word)
        if result:
            result["method"] = "s5_alt_word"
            return result

    return {}


def enrich_owners(records):
    missing = [r for r in records if not r.get("owner")]
    log.info(f"Owner enrichment: {len(missing)} records need lookup")
    found = 0

    for i, rec in enumerate(missing):
        addr = rec.get("address", "")
        zip_ = rec.get("zip", "")
        if not addr:
            continue

        result = lookup_owner(addr, zip_)
        if result and result.get("owner"):
            rec["owner"]     = result["owner"]
            rec["mail_addr"] = result.get("mail_addr", "")
            rec["absentee"]  = result.get("absentee", False)
            found += 1
            if found <= 10 or found % 25 == 0:
                log.info(f"  [{i+1}/{len(missing)}] {addr} -> {result['owner']} [{result.get('method','')}]")
        else:
            log.debug(f"  [{i+1}/{len(missing)}] No match: {addr} (zip={zip_})")

        time.sleep(0.2)

    log.info(f"Owner enrichment: {found}/{len(missing)} filled")
    return records


# ── DUPLICATE DETECTION ───────────────────────────────────────────────────────
def detect_duplicates(records):
    from collections import Counter
    owner_counts = Counter(
        r["owner"].upper().strip()
        for r in records
        if r.get("owner") and r["owner"].upper().strip() not in ("", "NULL")
    )
    dupes = 0
    for r in records:
        key = (r.get("owner") or "").upper().strip()
        if key and owner_counts[key] > 1:
            r["duplicate"] = True
            dupes += 1
    log.info(f"Duplicate owners flagged: {dupes}")
    return records


# ── SCORING ───────────────────────────────────────────────────────────────────
def score_record(rec):
    s = 0
    if rec.get("address"):       s += 3
    if rec.get("owner"):         s += 3
    if rec.get("type") == "TAX": s += 2
    if rec.get("absentee"):      s += 2
    if rec.get("sale_date"):     s = min(s + 1, 10)
    return min(s, 10)


def days_until_sale(sale_date_str):
    try:
        sale  = datetime.strptime(sale_date_str.strip(), "%m/%d/%Y")
        delta = (sale - datetime.now()).days
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
        f.write(
            '<!DOCTYPE html><html><head><meta charset="UTF-8"/>'
            '<meta http-equiv="refresh" content="0;url=leads.html"/>'
            '<title>Redirecting...</title></head>'
            '<body><script>window.location.href="leads.html";</script></body></html>'
        )

    log.info(f"Dashboard: {len(clean)} records, {os.path.getsize('dashboard/records.json'):,} bytes")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("data",      exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    log.info("=" * 60)
    log.info("Bexar County Lead Scraper v27.2 (Hybrid)")
    log.info(f"Primary:   PublicSearch.us (last {SCRAPE_DAYS} days)")
    log.info(f"Secondary: ArcGIS weekly backfill = {IS_SUNDAY}")
    log.info(f"Filter:    90-day cutoff ({CUTOFF_DATE.strftime('%Y-%m-%d')}) | live auctions always kept")
    log.info("=" * 60)

    known_docs, prev_records = load_known_docs()

    # ── Step 1: PublicSearch primary scrape ──────────────────────────────────
    new_records = scrape_publicsearch(known_docs, days_back=SCRAPE_DAYS)

    # ── Step 2: ArcGIS weekly backfill (Sundays only) ────────────────────────
    arcgis_records = []
    if IS_SUNDAY:
        arcgis_records = fetch_arcgis_backfill(known_docs)
        log.info(f"ArcGIS backfill added {len(arcgis_records)} records")

    # ── Step 3: Merge new + backfill + previous ───────────────────────────────
    for r in prev_records:
        r["is_new"] = False
    all_records = new_records + arcgis_records + prev_records

    # Deduplicate by doc_number (new takes priority over existing)
    seen = {}
    for r in all_records:
        doc = r.get("doc_number", "")
        if doc and doc not in seen:
            seen[doc] = r
    records = list(seen.values())
    log.info(f"After dedup: {len(records)} total records")

    # ── Step 4: Apply 90-day filter ───────────────────────────────────────────
    before_filter = len(records)
    records = [r for r in records if should_keep(r)]
    dropped = before_filter - len(records)
    log.info(f"After 90-day filter: {len(records)} records kept, {dropped} dropped")

    # ── Step 5: Rebuild known_docs from filtered set ──────────────────────────
    # CRITICAL: rebuild from what we're actually keeping so that dropped records
    # don't stay "known" forever — they'll be re-evaluated on the next scrape run
    known_docs = {str(r.get("doc_number", "")) for r in records if r.get("doc_number")}
    log.info(f"known_docs rebuilt: {len(known_docs)} active doc numbers")

    # ── Step 6: Enrich missing owners via ArcGIS parcel lookup ───────────────
    records = enrich_owners(records)

    # ── Step 7: Detect duplicates ─────────────────────────────────────────────
    records = detect_duplicates(records)

    # ── Step 8: Flag + score ──────────────────────────────────────────────────
    for r in records:
        r["flags"] = []
        if r["type"] == "TAX":                          r["flags"].append("TAX FORE")
        if r.get("absentee"):                           r["flags"].append("ABSENTEE")
        if r.get("duplicate"):                          r["flags"].append("DUPLICATE")
        if r.get("is_new"):                             r["flags"].append("NEW")
        if not r.get("owner"):                          r["flags"].append("NO OWNER")
        if r.get("sale_date"):                          r["flags"].append("HAS SALE DATE")
        d = days_until_sale(r.get("sale_date", ""))
        if d is not None and d <= 30:                   r["flags"].append("AUCTION SOON")
        if d is not None and d <= 14:                   r["flags"].append("URGENT")
        r["score"]           = score_record(r)
        r["days_until_sale"] = d

    def sort_key(r):
        d       = r.get("days_until_sale")
        urgency = 0 if (d is not None and d <= 14) else (1 if (d is not None and d <= 30) else 2)
        return (urgency, -r["score"], d if d is not None else 9999)

    records.sort(key=sort_key)

    # ── Step 9: Summary ───────────────────────────────────────────────────────
    named    = sum(1 for r in records if r.get("owner"))
    absentee = sum(1 for r in records if r.get("absentee"))
    new_ct   = sum(1 for r in records if r.get("is_new"))
    urgent   = sum(1 for r in records if "URGENT"       in r.get("flags", []))
    soon     = sum(1 for r in records if "AUCTION SOON" in r.get("flags", []))
    has_date = sum(1 for r in records if r.get("sale_date"))

    log.info(f"Final: {len(records)} total | {named} named | {absentee} absentee")
    log.info(f"       {new_ct} new | {has_date} with sale date | {soon} auction <=30d | {urgent} URGENT <=14d")

    # ── Step 10: Save ─────────────────────────────────────────────────────────
    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    build_dashboard(records)
    log.info("Done.")

