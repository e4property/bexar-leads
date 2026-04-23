"""
Bexar County Motivated Seller Lead Scraper v15
Primary: bexar.tx.publicsearch.us (Selenium headless Chrome — daily updates with owner names)
Fallback: maps.bexar.org ArcGIS (monthly updates)
"""

import json
import logging
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CLERK_URL    = "https://bexar.tx.publicsearch.us"
ARCGIS_BASE  = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
LOOKBACK_DAYS = 90  # how far back to pull notices

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
GHL_API_BASE    = "https://services.leadconnectorhq.com"


# ── Selenium scraper ──────────────────────────────────────────────────────────
def scrape_clerk_with_selenium():
    """
    Use headless Chrome to scrape bexar.tx.publicsearch.us foreclosure records.
    Returns list of lead records with grantor (owner) names.
    """
    log.info("Starting Selenium scraper for bexar.tx.publicsearch.us...")

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException, NoSuchElementException
    except ImportError as e:
        log.error(f"Selenium not installed: {e}")
        return []

    # Chrome options for headless operation
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = None
    records = []

    try:
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        wait = WebDriverWait(driver, 20)

        # Calculate date range
        now    = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=LOOKBACK_DAYS)
        date_from = cutoff.strftime("%-m/%-d/%Y")  # e.g. 1/22/2026
        date_to   = now.strftime("%-m/%-d/%Y")

        log.info(f"  Date range: {date_from} → {date_to}")

        # Build search URL for Foreclosures department
        search_url = (
            f"{CLERK_URL}/results"
            f"?department=FC"
            f"&instrumentDateRange={cutoff.strftime('%Y%m%d')}%2C{now.strftime('%Y%m%d')}"
            f"&keywordSearch=false"
            f"&limit=250"
            f"&offset=0"
        )

        log.info(f"  Loading: {search_url}")
        driver.get(search_url)
        time.sleep(3)

        # Accept disclaimer if present
        try:
            disclaimer = driver.find_element(By.XPATH, "//button[contains(text(),'Accept') or contains(text(),'agree') or contains(text(),'Continue')]")
            disclaimer.click()
            log.info("  Accepted disclaimer")
            time.sleep(2)
        except NoSuchElementException:
            pass

        # Wait for results table
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr, .result-row, [data-testid='result-row']")))
            log.info("  Results table loaded")
        except TimeoutException:
            log.warning("  Results table not found — trying alternate approach")
            # Try clicking on Foreclosures in the department dropdown
            try:
                driver.get(CLERK_URL)
                time.sleep(2)
                # Look for department dropdown
                dept_dropdown = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "select[name='department'], .department-select, [data-cy='department']")))
                driver.execute_script("arguments[0].value = 'FC'", dept_dropdown)
                driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", dept_dropdown)
                time.sleep(1)
                # Set date range and search
                search_btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], .search-btn, [data-cy='search-button']")
                search_btn.click()
                time.sleep(3)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
            except Exception as e2:
                log.error(f"  Alternative approach failed: {e2}")

        # Log current URL and page source snippet for debugging
        log.info(f"  Current URL: {driver.current_url}")
        page_text = driver.find_element(By.TAG_NAME, "body").text[:500]
        log.info(f"  Page preview: {page_text[:300]}")

        # Extract records from table rows
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        log.info(f"  Found {len(rows)} table rows")

        for i, row in enumerate(rows):
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue

                cell_texts = [c.text.strip() for c in cells]
                log.info(f"  Row {i+1}: {cell_texts}")

                # Map columns based on what we see:
                # GRANTOR | GRANTEE | DOC TYPE | RECORDED DATE | SALE DATE | DOC NUMBER | PROPERTY ADDRESS
                grantor   = cell_texts[0] if len(cell_texts) > 0 else ""
                grantee   = cell_texts[1] if len(cell_texts) > 1 else ""
                doc_type  = cell_texts[2] if len(cell_texts) > 2 else ""
                rec_date  = cell_texts[3] if len(cell_texts) > 3 else ""
                sale_date = cell_texts[4] if len(cell_texts) > 4 else ""
                doc_num   = cell_texts[5] if len(cell_texts) > 5 else ""
                address   = cell_texts[6] if len(cell_texts) > 6 else ""

                # Filter to foreclosure-related doc types
                if doc_type and "FORECLO" not in doc_type.upper() and "TRUSTEE" not in doc_type.upper():
                    # Try REMARKS column layout:
                    # GRANTOR | DOC TYPE | RECORDED DATE | SALE DATE | DOC NUMBER | REMARKS | PROPERTY ADDRESS
                    if len(cell_texts) >= 5:
                        doc_type  = cell_texts[1] if len(cell_texts) > 1 else ""
                        rec_date  = cell_texts[2] if len(cell_texts) > 2 else ""
                        sale_date = cell_texts[3] if len(cell_texts) > 3 else ""
                        doc_num   = cell_texts[4] if len(cell_texts) > 4 else ""
                        address   = cell_texts[6] if len(cell_texts) > 6 else cell_texts[-1]

                if not grantor and not address:
                    continue

                records.append({
                    "type":        "NOF",
                    "source":      "clerk",
                    "address":     address,
                    "owner":       grantor,
                    "mail_addr":   "",
                    "absentee":    False,
                    "doc_number":  doc_num,
                    "year":        rec_date[-4:] if rec_date else "",
                    "month":       rec_date[:2].strip("/") if rec_date else "",
                    "city":        "San Antonio",
                    "zip":         "",
                    "school_dist": "",
                    "date_filed":  rec_date,
                    "sale_date":   sale_date,
                    "grantee":     grantee,
                    "flags":       [],
                    "enriched":    bool(grantor),
                })

            except Exception as e:
                log.warning(f"  Row {i+1} parse error: {e}")
                continue

        named = sum(1 for r in records if r.get("owner"))
        log.info(f"  Clerk scrape complete: {len(records)} records, {named} with owner name")

    except Exception as e:
        log.error(f"Selenium scraper failed: {e}", exc_info=True)
    finally:
        if driver:
            driver.quit()

    return records


# ── ArcGIS fallback ───────────────────────────────────────────────────────────
def fetch_arcgis_records():
    log.info("Fetching ArcGIS records (fallback/supplement)...")

    def fetch_json(url):
        req = urllib.request.Request(
            url, headers={"User-Agent": "BexarScraper/15.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))

    def arcgis_query(layer_url, offset=0):
        try:
            params = urllib.parse.urlencode({
                "where": "1=1", "outFields": "*", "returnGeometry": "false",
                "resultOffset": offset, "resultRecordCount": 1000, "f": "json",
            })
            data = fetch_json(f"{layer_url}/query?{params}")
            if "error" in data: return []
            return data.get("features", [])
        except Exception as e:
            log.warning(f"ArcGIS query error: {e}")
            return []

    def pick(attrs, *candidates, default=""):
        for c in candidates:
            v = attrs.get(c)
            if v is not None and str(v).strip() not in ("", "None", "null", "<Null>"):
                return str(v).strip()
        return default

    LAYERS = [
        {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
        {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
    ]

    raw = []
    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{ARCGIS_BASE}/{idx}"
        try:
            features, offset = [], 0
            while True:
                batch = arcgis_query(layer_url, offset=offset)
                features.extend(batch)
                log.info(f"  Layer {idx}: offset={offset}, got {len(batch)} (total: {len(features)})")
                if len(batch) < 1000: break
                offset += len(batch)
            for feat in features:
                a     = feat["attributes"]
                month = pick(a, "MONTH", "MO", default="")
                year  = pick(a, "YEAR",  "YR", default="")
                raw.append({
                    "type":        layer["type"],
                    "source":      "arcgis",
                    "address":     pick(a, "ADDRESS", "SITUS_ADD", "ADDR"),
                    "owner":       pick(a, "OWNER", "GRANTOR", "OWNER_NAME", default=""),
                    "mail_addr":   "",
                    "absentee":    False,
                    "doc_number":  pick(a, "DOC_NUMBER", "DOCNUM", "DOC_NUM"),
                    "year":        year,
                    "month":       month,
                    "city":        pick(a, "CITY", "MAIL_CITY", default=""),
                    "zip":         pick(a, "ZIP", "ZIPCODE", "ZIP_CODE", default=""),
                    "school_dist": pick(a, "SCHOOL_DIST", default=""),
                    "date_filed":  f"{month}/{year}".strip("/"),
                    "sale_date":   "",
                    "grantee":     "",
                    "flags":       [],
                    "enriched":    False,
                })
        except Exception as e:
            log.error(f"  Layer {idx} failed: {e}")

    log.info(f"ArcGIS: {len(raw)} records")
    return raw


# ── Merge records ─────────────────────────────────────────────────────────────
def merge_records(clerk_records, arcgis_records):
    merged = {}

    # Clerk first (freshest, has owner names)
    for r in clerk_records:
        key = r.get("doc_number") or r.get("address", "").upper()
        if key:
            merged[key] = r

    # ArcGIS — add only what's not already in clerk data
    clerk_addrs = {r.get("address", "").upper() for r in clerk_records if r.get("address")}
    new_from_arcgis = 0
    for r in arcgis_records:
        doc  = r.get("doc_number", "")
        addr = r.get("address", "").upper()
        if doc in merged or addr in clerk_addrs:
            continue
        key = doc or addr
        if key:
            merged[key] = r
            new_from_arcgis += 1

    log.info(f"Merged: {len(clerk_records)} clerk + {new_from_arcgis} new ArcGIS = {len(merged)} total")
    return list(merged.values())


# ── Scoring ───────────────────────────────────────────────────────────────────
def score_record(rec):
    s = 0
    if rec.get("address"):       s += 3
    if rec.get("owner"):         s += 3
    if rec.get("type") == "TAX": s += 2
    if rec.get("absentee"):      s += 2
    return min(s, 10)


# ── GHL Push ─────────────────────────────────────────────────────────────────
def ghl_request(method, endpoint, payload=None):
    try:
        import requests
    except ImportError:
        return None
    url = f"{GHL_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type": "application/json", "Accept": "application/json",
        "Version": "2021-07-28",
        "User-Agent": "Mozilla/5.0 Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://app.justjarvis.com", "Referer": "https://app.justjarvis.com/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20) if method == "GET" \
               else requests.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code in (200, 201): return resp.json()
        log.warning(f"GHL {resp.status_code}: {resp.text[:200]}")
        return {"_error": resp.status_code}
    except Exception as e:
        return {"_error": str(e)}


def ghl_contact_exists(doc):
    r = ghl_request("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&query={urllib.parse.quote(str(doc))}&limit=5")
    if not r or "_error" in r: return False
    for c in r.get("contacts", []):
        if f"doc-{doc}" in (c.get("tags") or []): return True
    return False


def ghl_create_contact(rec):
    owner = rec.get("owner", "").strip()
    parts = owner.split()
    first = parts[0].title() if parts else owner
    last  = " ".join(parts[1:]).title() if len(parts) > 1 else ""
    tags  = ["bexar-lead", rec["type"], f"doc-{rec.get('doc_number', '')}"]
    if rec.get("score", 0) >= 7: tags.append("hot-lead")
    lead_type = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
    return ghl_request("POST", "/contacts/", {
        "locationId": GHL_LOCATION_ID,
        "firstName": first, "lastName": last, "name": owner.title(),
        "address1": rec.get("address", ""),
        "city": rec.get("city", "San Antonio"),
        "state": "TX", "country": "US", "postalCode": rec.get("zip", ""),
        "tags": tags, "source": "Bexar County Clerk Scraper",
        "customFields": [
            {"key": "lead_type",        "field_value": lead_type},
            {"key": "doc_number",       "field_value": rec.get("doc_number", "")},
            {"key": "date_filed",       "field_value": rec.get("date_filed", "")},
            {"key": "sale_date",        "field_value": rec.get("sale_date", "")},
            {"key": "score",            "field_value": str(rec.get("score", 0))},
            {"key": "property_address", "field_value": rec.get("address", "")},
            {"key": "school_district",  "field_value": rec.get("school_dist", "")},
            {"key": "lender",           "field_value": rec.get("grantee", "")},
        ],
    })


def push_to_ghl(records):
    if not GHL_API_KEY:
        log.warning("GHL_API_KEY not set — skipping")
        return
    named = [r for r in records if r.get("owner")]
    log.info(f"GHL push: {len(named)} named leads (of {len(records)} total)")
    test = ghl_request("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&limit=1")
    if not test or "_error" in test:
        log.error("GHL auth failed")
        return
    log.info(f"GHL auth OK — {test.get('total', '?')} existing contacts")
    created = skipped = errors = 0
    for i, rec in enumerate(sorted(named, key=lambda r: -r.get("score", 0))):
        doc = rec.get("doc_number", "")
        if doc and ghl_contact_exists(doc):
            skipped += 1
            continue
        result = ghl_create_contact(rec)
        if result and result.get("contact"):
            created += 1
            log.info(f"  ✓ [{i+1}] {rec.get('owner')} — {rec.get('address')}")
        else:
            errors += 1
        time.sleep(0.15)
    log.info(f"GHL done — Created:{created} | Skipped:{skipped} | Errors:{errors}")


# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Bexar County Motivated Seller Leads</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&display=swap" rel="stylesheet"/>
<style>
:root{--bg:#0d0f14;--surface:#13161e;--surface2:#1a1e2a;--border:#252836;--accent:#00e5ff;--accent3:#a78bfa;--text:#e8eaf0;--muted:#6b7280;--success:#22d3a5;--warning:#fbbf24;--danger:#f87171;--hot:#ff6b35;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'DM Mono',monospace;font-size:13px;min-height:100vh;}
header{display:flex;align-items:center;justify-content:space-between;padding:18px 32px;border-bottom:1px solid var(--border);background:var(--surface);position:sticky;top:0;z-index:100;}
.logo{font-family:'Syne',sans-serif;font-size:20px;font-weight:800;}.logo span{color:var(--accent);}
#last-updated{color:var(--muted);font-size:11px;}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--border);border-bottom:1px solid var(--border);}
.stat-card{background:var(--surface);padding:20px 24px;display:flex;flex-direction:column;gap:6px;}
.stat-num{font-family:'Syne',sans-serif;font-size:36px;font-weight:800;line-height:1;color:var(--accent);}
.stat-card:nth-child(2) .stat-num{color:var(--danger);}
.stat-card:nth-child(3) .stat-num{color:var(--warning);}
.stat-card:nth-child(4) .stat-num{color:var(--success);}
.stat-label{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:1px;}
.controls{display:flex;gap:10px;padding:16px 32px;background:var(--surface);border-bottom:1px solid var(--border);align-items:center;flex-wrap:wrap;}
input[type=text]{flex:1;min-width:200px;background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:8px 14px;font-family:'DM Mono',monospace;font-size:13px;outline:none;transition:border-color .2s;}
input[type=text]:focus{border-color:var(--accent);}
select{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:8px 12px;font-family:'DM Mono',monospace;font-size:13px;cursor:pointer;outline:none;}
.count-badge{color:var(--muted);font-size:11px;white-space:nowrap;padding:0 8px;}
.table-wrap{overflow-x:auto;padding:0 32px 32px;}
table{width:100%;border-collapse:collapse;margin-top:16px;}
thead th{text-align:left;padding:10px 12px;font-size:10px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);border-bottom:1px solid var(--border);white-space:nowrap;}
tbody tr{border-bottom:1px solid var(--border);transition:background .12s;}
tbody tr:hover{background:var(--surface2);}
tbody td{padding:10px 12px;vertical-align:middle;}
.score{display:inline-flex;width:36px;height:36px;border-radius:50%;align-items:center;justify-content:center;font-weight:500;font-size:12px;font-family:'Syne',sans-serif;}
.score-high{background:rgba(34,211,165,.15);color:var(--success);border:1px solid rgba(34,211,165,.3);}
.score-mid{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.3);}
.score-low{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.3);}
.type-badge{display:inline-block;padding:2px 8px;font-size:10px;font-weight:500;border-radius:2px;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;}
.type-nof{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.25);}
.type-tax{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.25);}
.flags{display:flex;gap:4px;flex-wrap:wrap;}
.flag{display:inline-block;padding:2px 6px;font-size:10px;background:rgba(167,139,250,.12);color:var(--accent3);border:1px solid rgba(167,139,250,.25);border-radius:2px;white-space:nowrap;}
.addr{color:var(--text);font-size:12px;max-width:200px;}
.owner{color:var(--success);font-size:12px;font-weight:500;}
.owner-none{color:var(--muted);font-size:12px;}
.city,.doc{color:var(--muted);font-size:12px;}
.state-msg{text-align:center;padding:60px 20px;color:var(--muted);}
.pagination{display:flex;justify-content:center;align-items:center;gap:8px;padding:20px 32px;color:var(--muted);font-size:12px;}
.pagination button{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:6px 14px;cursor:pointer;font-family:'DM Mono',monospace;font-size:12px;}
.pagination button:hover:not(:disabled){border-color:var(--accent);color:var(--accent);}
.pagination button:disabled{opacity:.3;cursor:default;}
@media(max-width:900px){.stats{grid-template-columns:repeat(2,1fr);}.controls,.table-wrap{padding-left:16px;padding-right:16px;}}
</style>
</head>
<body>
<header>
  <div class="logo">🏠 Bexar County <span>Leads</span></div>
  <div id="last-updated">UPDATED_PLACEHOLDER</div>
</header>
<div class="stats">
  <div class="stat-card"><div class="stat-num" id="s-total">—</div><div class="stat-label">Total Leads</div></div>
  <div class="stat-card"><div class="stat-num" id="s-nof">—</div><div class="stat-label">NOF Foreclosures</div></div>
  <div class="stat-card"><div class="stat-num" id="s-tax">—</div><div class="stat-label">Tax Foreclosures</div></div>
  <div class="stat-card"><div class="stat-num" id="s-named">—</div><div class="stat-label">With Owner Name</div></div>
</div>
<div class="controls">
  <input type="text" id="search" placeholder="Search address, owner, doc #…" oninput="applyFilters()"/>
  <select id="type-filter" onchange="applyFilters()">
    <option value="">All Types</option>
    <option value="NOF">NOF Foreclosure</option>
    <option value="TAX">Tax Foreclosure</option>
  </select>
  <select id="owner-filter" onchange="applyFilters()">
    <option value="">All Leads</option>
    <option value="named">With Owner Name</option>
    <option value="unnamed">No Name Yet</option>
  </select>
  <select id="sort-select" onchange="applyFilters()">
    <option value="score-desc">Sort: Score ↓</option>
    <option value="date-desc">Sort: Date ↓</option>
    <option value="score-asc">Sort: Score ↑</option>
  </select>
  <span class="count-badge" id="count-badge"></span>
</div>
<div class="table-wrap">
  <table>
    <thead><tr>
      <th>Score</th><th>Type</th><th>Property Address</th>
      <th>Owner (Grantor)</th><th>Date Filed</th><th>Sale Date</th><th>Doc #</th><th>City/ZIP</th>
    </tr></thead>
    <tbody id="tbody"></tbody>
  </table>
  <div id="state-msg" class="state-msg" style="display:none">No records match.</div>
</div>
<div class="pagination">
  <button id="btn-prev" onclick="changePage(-1)">← Prev</button>
  <span id="page-info"></span>
  <button id="btn-next" onclick="changePage(1)">Next →</button>
</div>
<script>
var ALL_RECORDS=DATA_PLACEHOLDER;
var filtered=[],page=1,PAGE=50;
function init(){
  document.getElementById('s-total').textContent=ALL_RECORDS.length;
  document.getElementById('s-nof').textContent=ALL_RECORDS.filter(function(r){return r.type==='NOF';}).length;
  document.getElementById('s-tax').textContent=ALL_RECORDS.filter(function(r){return r.type==='TAX';}).length;
  document.getElementById('s-named').textContent=ALL_RECORDS.filter(function(r){return r.owner;}).length;
  applyFilters();
}
function applyFilters(){
  var q=document.getElementById('search').value.toLowerCase();
  var t=document.getElementById('type-filter').value;
  var ow=document.getElementById('owner-filter').value;
  var s=document.getElementById('sort-select').value;
  filtered=ALL_RECORDS.filter(function(r){
    var mq=!q||(r.address||'').toLowerCase().indexOf(q)>=0||(r.owner||'').toLowerCase().indexOf(q)>=0||(r.doc_number||'').toLowerCase().indexOf(q)>=0;
    var mt=!t||r.type===t;
    var mow=!ow||(ow==='named'?!!r.owner:!r.owner);
    return mq&&mt&&mow;
  });
  filtered.sort(function(a,b){
    if(s==='score-desc') return b.score-a.score;
    if(s==='score-asc')  return a.score-b.score;
    if(s==='date-desc')  return (b.date_filed||'')>(a.date_filed||'')?1:-1;
    return 0;
  });
  page=1;
  document.getElementById('count-badge').textContent=filtered.length+' of '+ALL_RECORDS.length+' leads';
  render();
}
function render(){
  var tbody=document.getElementById('tbody');
  var msg=document.getElementById('state-msg');
  var slice=filtered.slice((page-1)*PAGE,page*PAGE);
  if(!filtered.length){tbody.innerHTML='';msg.style.display='block';return;}
  msg.style.display='none';
  var rows='';
  for(var i=0;i<slice.length;i++){
    var r=slice[i];
    var sc=r.score||0;
    var scC=sc>=7?'score-high':sc>=4?'score-mid':'score-low';
    var tC=r.type==='TAX'?'type-tax':'type-nof';
    var tL=r.type==='TAX'?'TAX FORE':'NOF';
    var cz=[r.city,r.zip].filter(Boolean).join(' ')||'—';
    var oh=r.owner?'<div class="owner">'+r.owner+'</div>':'<div class="owner-none">—</div>';
    var fh='';
    for(var j=0;j<(r.flags||[]).length;j++) fh+='<span class="flag">'+r.flags[j]+'</span>';
    if(!fh) fh='<span style="color:var(--muted)">—</span>';
    rows+='<tr>'
      +'<td><div class="score '+scC+'">'+sc+'</div></td>'
      +'<td><span class="type-badge '+tC+'">'+tL+'</span></td>'
      +'<td><div class="addr">'+(r.address||'—')+'</div></td>'
      +'<td>'+oh+'</td>'
      +'<td><div class="doc">'+(r.date_filed||'—')+'</div></td>'
      +'<td><div class="doc">'+(r.sale_date||'—')+'</div></td>'
      +'<td><div class="doc">'+(r.doc_number||'—')+'</div></td>'
      +'<td><div class="city">'+cz+'</div></td>'
      +'</tr>';
  }
  tbody.innerHTML=rows;
  var total=Math.ceil(filtered.length/PAGE);
  document.getElementById('page-info').textContent=total>1?'Page '+page+' of '+total:'';
  document.getElementById('btn-prev').disabled=page<=1;
  document.getElementById('btn-next').disabled=page>=total;
}
function changePage(d){page+=d;render();window.scrollTo({top:0,behavior:'smooth'});}
init();
</script>
</body>
</html>"""


def build_dashboard(records):
    updated  = datetime.now(timezone.utc).strftime("Updated: %b %d, %Y %H:%M UTC")
    json_str = json.dumps(records, separators=(",", ":"), ensure_ascii=False)
    html     = DASHBOARD_TEMPLATE.replace("UPDATED_PLACEHOLDER", updated, 1)
    html     = html.replace("DATA_PLACEHOLDER", json_str, 1)
    if "DATA_PLACEHOLDER" in html: raise RuntimeError("Data injection failed!")
    os.makedirs("dashboard", exist_ok=True)
    path = "dashboard/index.html"
    with open(path, "w", encoding="utf-8") as f: f.write(html)
    size = os.path.getsize(path)
    log.info(f"Built {path} — {len(records)} records, {size:,} bytes")
    if size < 50000 and len(records) > 0:
        raise RuntimeError(f"Output too small: {size} bytes")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    log.info("="*60)
    log.info("Bexar County Lead Scraper v15")
    log.info(f"Primary:  {CLERK_URL} (Selenium)")
    log.info(f"Fallback: {ARCGIS_BASE} (ArcGIS)")
    log.info("="*60)

    # Primary: Selenium scraper (daily, has owner names)
    clerk_records = scrape_clerk_with_selenium()

    # Fallback: ArcGIS (always run — gets tax foreclosures)
    arcgis_records = fetch_arcgis_records()

    # Merge both
    records = merge_records(clerk_records, arcgis_records)

    # Score and flag
    for r in records:
        if r["type"] == "TAX":             r["flags"].append("TAX FORE")
        if not r["owner"]:                 r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]: r["flags"].append("NO CITY")
        r["score"] = score_record(r)

    records.sort(key=lambda x: x["score"], reverse=True)

    named = sum(1 for r in records if r["owner"])
    log.info(f"Final: {len(records)} leads | {named} with owner name")

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved data/records.json ({len(records)} records)")

    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved dashboard/records.json ({len(records)} records)")

    build_dashboard(records)
    push_to_ghl(records)
