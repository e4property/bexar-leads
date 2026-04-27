"""
Bexar County Motivated Seller Lead Scraper v26.0
HYBRID SCRAPER:
  Primary:   bexar.tx.publicsearch.us  (Selenium, runs 3x daily)
             - Real-time courthouse filings
             - Includes sale date, doc number, address
             - Grantor (owner) extracted from document detail page
  Secondary: ArcGIS GIS layer (urllib, runs weekly on Sunday)
             - Backfill only — fills gaps missed by primary
             - Absentee owner detection via parcel mailing address

  Owner enrichment: 5-strategy parcel lookup for any record missing owner
"""

import json
import logging
import os
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

RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
TODAY         = datetime.now(timezone.utc)
IS_SUNDAY     = TODAY.weekday() == 6  # Run ArcGIS backfill on Sundays


# ── HELPERS ───────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/26.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                log.debug(f"fetch failed: {e}")
                return {}


def arcgis_query(layer_url, where, fields="*", limit=50):
    try:
        params = urllib.parse.urlencode({
            "where": where, "outFields": fields,
            "returnGeometry": "false",
            "resultRecordCount": limit, "f": "json",
        })
        data = fetch_json(f"{layer_url}/query?{params}")
        if "error" in data:
            log.debug(f"ArcGIS error: {data['error']}")
            return []
        return data.get("features", [])
    except Exception as e:
        log.debug(f"arcgis_query error: {e}")
        return []


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
            headers={"User-Agent": "BexarScraper/26.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            prev = json.loads(r.read().decode("utf-8", errors="replace"))
            docs = {str(rec.get("doc_number", "")) for rec in prev if rec.get("doc_number")}
            log.info(f"Loaded {len(docs)} known doc numbers from previous run")
            return docs, prev
    except Exception as e:
        log.info(f"No previous records found (first run?): {e}")
        return set(), []


# ── SELENIUM SETUP ────────────────────────────────────────────────────────────
def get_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])

    try:
        from selenium.webdriver.chrome.service import Service as ChromeService
        from webdriver_manager.chrome import ChromeDriverManager
        service = ChromeService(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        return webdriver.Chrome(options=opts)


# ── PUBLICSEARCH SCRAPER ──────────────────────────────────────────────────────
def scrape_publicsearch(known_docs, days_back=7):
    """
    Scrape bexar.tx.publicsearch.us for foreclosure filings
    in the last `days_back` days. Returns list of records.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    start_date = (TODAY - timedelta(days=days_back)).strftime("%Y%m%d")
    end_date   = (TODAY + timedelta(days=1)).strftime("%Y%m%d")

    # Build search URL — department FC = Foreclosures, sorted by recorded date desc
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

    driver = None
    records = []

    try:
        driver = get_driver()
        wait   = WebDriverWait(driver, 30)

        page   = 0
        offset = 0

        while True:
            url = search_url.replace("offset=0", f"offset={offset}")
            log.info(f"  Loading page {page+1} (offset={offset}): {url}")
            driver.get(url)

            # Wait for results table to load
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr, .result-row, [data-testid='result-row']")))
                time.sleep(2)  # Extra wait for full render
            except Exception:
                log.info("  No results found or page timed out")
                break

            # Try multiple selectors for result rows
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not rows:
                rows = driver.find_elements(By.CSS_SELECTOR, "[class*='result'][class*='row'], [class*='ResultRow']")
            if not rows:
                log.info("  No rows found on this page")
                break

            log.info(f"  Found {len(rows)} rows on page {page+1}")
            page_new = 0

            for row in rows:
                try:
                    # Use class-based selectors — confirmed column mapping:
                    # col-0=checkbox, col-1=dropdown, col-2=dropdown,
                    # col-3=doc_type, col-4=recorded_date, col-5=sale_date,
                    # col-6=doc_number, col-7=remarks, col-8=address
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

                    # Clean up
                    doc_number = doc_number.strip()
                    address    = address.replace("\n", " ").replace(",", " ").strip()
                    sale_date  = sale_date.strip() if sale_date.strip() not in ("N/A", "") else ""

                    # Log first few for debugging
                    if not doc_type_text and not doc_number:
                        continue

                    if not doc_number:
                        continue

                    # Skip already known docs
                    if doc_number in known_docs:
                        continue

                    # Determine type
                    rec_type = "TAX" if "TAX" in doc_type_text.upper() else "NOF"

                    # Parse city and zip from address
                    city, zip_code = parse_city_zip(address)

                    # Parse recorded date for month/year
                    month, year = parse_month_year(recorded_date)

                    # Try to get link to detail page for grantor (owner)
                    link_el = None
                    try:
                        link_el = row.find_element(By.TAG_NAME, "a")
                    except Exception:
                        pass

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
                        "_detail_link": link_el.get_attribute("href") if link_el else "",
                    }
                    records.append(rec)
                    known_docs.add(doc_number)
                    page_new += 1

                except Exception as e:
                    log.debug(f"  Row parse error: {e}")
                    continue

            log.info(f"  Page {page+1}: {page_new} new records")

            if page_new == 0 or len(rows) < 50:
                break

            offset += 50
            page   += 1
            time.sleep(1.5)

        # Enrich with grantor (owner) from detail pages
        # Only fetch detail for records missing owner — limit to 50 per run to be polite
        records = enrich_from_detail_pages(driver, records, wait, limit=50)

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


def enrich_from_detail_pages(driver, records, wait, limit=50):
    """Visit detail pages to get grantor (owner) name."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    enriched = 0
    for rec in records:
        if enriched >= limit:
            break
        if rec.get("owner") or not rec.get("_detail_link"):
            continue
        try:
            driver.get(rec["_detail_link"])
            time.sleep(1.5)

            # Look for grantor in summary section
            # The page shows: Grantor | Grantee sections
            page_text = driver.page_source

            # Try to find grantor from page source
            grantor = extract_grantor(page_text, driver)
            if grantor:
                rec["owner"] = grantor
                enriched += 1
                log.info(f"  Detail: {rec['address']} -> {grantor}")

        except Exception as e:
            log.debug(f"  Detail page error for {rec.get('doc_number')}: {e}")
        finally:
            time.sleep(0.5)

    log.info(f"  Enriched {enriched} records from detail pages")
    return records


def extract_grantor(page_source, driver):
    """Extract grantor name from detail page."""
    from selenium.webdriver.common.by import By

    try:
        # Try finding grantor label then sibling value
        elements = driver.find_elements(By.XPATH,
            "//*[contains(text(),'Grantor') or contains(text(),'GRANTOR')]/../following-sibling::*[1]")
        for el in elements:
            text = el.text.strip()
            if text and len(text) > 2:
                return text.upper()
    except Exception:
        pass

    try:
        # Try table approach
        rows = driver.find_elements(By.CSS_SELECTOR, "table tr, .field-row, [class*='party']")
        for row in rows:
            text = row.text.upper()
            if "GRANTOR" in text:
                parts = text.replace("GRANTOR", "").strip()
                if parts and len(parts) > 2:
                    return parts
    except Exception:
        pass

    # Fallback: parse from raw HTML
    try:
        import re
        match = re.search(r'[Gg]rantor["\s:>]+([A-Z][A-Z\s,&]+?)[\s<"]{2,}', page_source)
        if match:
            return match.group(1).strip().upper()
    except Exception:
        pass

    return ""


# ── ADDRESS PARSING ───────────────────────────────────────────────────────────
def clean_address(raw):
    """Extract just the street address — format: STREET, CITY, STATE, ZIP"""
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split(",")]
    return parts[0].strip().upper() if parts else raw.strip().upper()


def parse_city_zip(raw):
    """Extract city and zip — format: STREET, CITY, STATE, ZIP"""
    import re
    parts = [p.strip() for p in raw.split(",")]
    # parts[0]=street, parts[1]=city, parts[2]=state, parts[3]=zip (sometimes combined)
    city = ""
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
        # Fallback: scan for zip
        zip_match = re.search(r'\b(\d{5})\b', raw)
        zip_code = zip_match.group(1) if zip_match else ""

    return city, zip_code


def parse_month_year(date_str):
    """Parse '4/23/2026' into month='4', year='2026'"""
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
    """
    Full ArcGIS scrape — runs weekly on Sundays.
    Returns records not already in known_docs.
    """
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
                log.info(f"    offset={offset}: {len(batch)} (total: {len(features)})")
                if len(batch) < 1000:
                    break
                offset += len(batch)
            except Exception as e:
                log.error(f"Layer {idx} query error: {e}")
                break

        for feat in features:
            a      = feat["attributes"]
            month  = pick(a, "MONTH", "MO", default="")
            year   = pick(a, "YEAR",  "YR", default="")
            doc    = pick(a, "DOC_NUMBER", "DOCNUM", "DOC_NUM")

            if doc in known_docs:
                continue  # Already have this one

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
def parse_address(address):
    if not address:
        return None
    parts = address.strip().upper().split()
    if not parts or not parts[0].isdigit():
        return None
    num  = parts[0]
    rest = parts[1:] if len(parts) > 1 else []
    SUFFIXES = {"ST","AVE","DR","RD","LN","CT","CIR","BLVD","WAY","PL",
                "TRL","PKWY","HWY","LOOP","PASS","CV","PT","HLS","TRAIL",
                "GROVE","RIDGE","CREEK","LAKE","PARK","GLEN","RUN","XING"}
    words  = rest[:]
    suffix = ""
    if words and words[-1] in SUFFIXES:
        suffix = words[-1]
        words  = words[:-1]
    return {"num": num, "street": " ".join(rest), "words": words,
            "suffix": suffix, "full": address.strip().upper()}


def match_features(feats, num, first_word=None):
    for feat in feats:
        a       = feat["attributes"]
        owner   = str(a.get("Owner")    or "").strip()
        situs   = str(a.get("Situs")    or "").strip()
        addr1   = str(a.get("AddrLn1")  or "").strip()
        city    = str(a.get("AddrCity") or "").strip()
        zipcode = str(a.get("Zip")      or "").strip()

        if not owner or owner.upper() in ("NULL", "NONE", ""):
            continue

        situs_norm = normalize(situs)
        if not situs_norm.startswith(num + " "):
            continue
        if first_word and first_word not in situs_norm:
            continue

        mail_addr = f"{addr1} {city} {zipcode}".strip() if addr1 and addr1.upper() not in ("NULL","NONE","") else ""
        absentee  = bool(mail_addr) and not normalize(mail_addr).startswith(num + " ")
        return {"owner": owner, "mail_addr": mail_addr, "absentee": absentee}
    return None


def lookup_owner(address, zipcode=""):
    parsed = parse_address(address)
    if not parsed:
        return {}

    num        = parsed["num"]
    words      = parsed["words"]
    first_word = words[0] if words else ""

    if len(words) >= 2:
        feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{words[0]} {words[1]}%'",
                              fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
        result = match_features(feats, num, first_word)
        if result:
            result["method"] = "s1_two_word"; return result

    if first_word and len(first_word) >= 3:
        feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{first_word}%'",
                              fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
        result = match_features(feats, num, first_word)
        if result:
            result["method"] = "s2_one_word"; return result

    for word in words[1:]:
        if len(word) < 4:
            continue
        feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{word}%'",
                              fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
        result = match_features(feats, num, word)
        if result:
            result["method"] = "s3_alt_word"; return result

    if zipcode and len(zipcode) >= 5:
        zip5  = zipcode[:5]
        feats = arcgis_query(PARCELS_URL, f"Zip = '{zip5}'",
                             fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=1000)
        result = match_features(feats, num, None)
        if result:
            result["method"] = "s4_zip_match"; return result

    if len(num) >= 5:
        feats = arcgis_query(PARCELS_URL, f"Situs LIKE '%{num}%'",
                             fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=50)
        result = match_features(feats, num, None)
        if result:
            result["method"] = "s5_num_only"; return result

    return {}


def enrich_owners(records):
    """Run parcel lookup for any record still missing an owner."""
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
        time.sleep(0.15)

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
    # Bonus for having sale date — means we know the auction date
    if rec.get("sale_date"):     s = min(s + 1, 10)
    return min(s, 10)


def days_until_sale(sale_date_str):
    """Return days until auction, or None if no sale date."""
    try:
        sale = datetime.strptime(sale_date_str.strip(), "%m/%d/%Y")
        delta = (sale - datetime.now()).days
        return max(delta, 0)
    except Exception:
        return None


# ── DASHBOARD ─────────────────────────────────────────────────────────────────
def build_dashboard(records):
    os.makedirs("dashboard", exist_ok=True)
    # Remove internal scraper fields before saving
    clean = []
    for r in records:
        rc = {k: v for k, v in r.items() if not k.startswith("_")}
        clean.append(rc)

    json_str = json.dumps(clean, separators=(",", ":"), ensure_ascii=True)
    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        f.write(json_str)
    with open("dashboard/index.html", "w", encoding="utf-8") as f:
        f.write('<!DOCTYPE html><html><head><meta charset="UTF-8"/>'
                '<meta http-equiv="refresh" content="0;url=leads.html"/>'
                '<title>Redirecting...</title></head>'
                '<body><script>window.location.href="leads.html";</script></body></html>')
    log.info(f"Dashboard: {len(clean)} records, {os.path.getsize('dashboard/records.json'):,} bytes")


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    log.info("=" * 60)
    log.info("Bexar County Lead Scraper v26.0 (Hybrid)")
    log.info(f"Primary:   PublicSearch.us (last 7 days)")
    log.info(f"Secondary: ArcGIS weekly backfill = {IS_SUNDAY}")
    log.info("=" * 60)

    known_docs, prev_records = load_known_docs()

    # ── Step 1: PublicSearch primary scrape ──────────────────────────────────
    new_records = scrape_publicsearch(known_docs, days_back=14)

    # ── Step 2: ArcGIS weekly backfill (Sundays only) ────────────────────────
    arcgis_records = []
    if IS_SUNDAY:
        arcgis_records = fetch_arcgis_backfill(known_docs)
        log.info(f"ArcGIS backfill added {len(arcgis_records)} records")

    # ── Step 3: Merge new + backfill + previous ───────────────────────────────
    # Mark existing records as not new
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

    # ── Step 4: Enrich missing owners ─────────────────────────────────────────
    records = enrich_owners(records)

    # ── Step 5: Detect duplicates ─────────────────────────────────────────────
    records = detect_duplicates(records)

    # ── Step 6: Flag + score ──────────────────────────────────────────────────
    for r in records:
        r["flags"] = []
        if r["type"] == "TAX":              r["flags"].append("TAX FORE")
        if r.get("absentee"):               r["flags"].append("ABSENTEE")
        if r.get("duplicate"):              r["flags"].append("DUPLICATE")
        if r.get("is_new"):                 r["flags"].append("NEW")
        if not r.get("owner"):              r["flags"].append("NO OWNER")
        if r.get("sale_date"):              r["flags"].append("HAS SALE DATE")
        d = days_until_sale(r.get("sale_date",""))
        if d is not None and d <= 30:       r["flags"].append("AUCTION SOON")
        if d is not None and d <= 14:       r["flags"].append("URGENT")
        r["score"] = score_record(r)
        r["days_until_sale"] = d

    # Sort: urgent first, then score, then days until sale
    def sort_key(r):
        d = r.get("days_until_sale")
        urgency = 0 if (d is not None and d <= 14) else (1 if (d is not None and d <= 30) else 2)
        return (urgency, -r["score"], d if d is not None else 9999)

    records.sort(key=sort_key)

    # ── Step 7: Summary ───────────────────────────────────────────────────────
    named    = sum(1 for r in records if r.get("owner"))
    absentee = sum(1 for r in records if r.get("absentee"))
    new_ct   = sum(1 for r in records if r.get("is_new"))
    urgent   = sum(1 for r in records if "URGENT" in r.get("flags",[]))
    soon     = sum(1 for r in records if "AUCTION SOON" in r.get("flags",[]))
    has_date = sum(1 for r in records if r.get("sale_date"))

    log.info(f"Final: {len(records)} total | {named} named | {absentee} absentee")
    log.info(f"       {new_ct} new | {has_date} with sale date | {soon} auction ≤30d | {urgent} URGENT ≤14d")

    # ── Step 8: Save ──────────────────────────────────────────────────────────
    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    build_dashboard(records)
    log.info("Done.")

