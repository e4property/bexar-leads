"""
Bexar County Motivated Seller Lead Scraper v16
- Scrapes bexar.tx.publicsearch.us with Selenium (daily, owner names)
- Filters to last 90 days only (no old 2006/2013 records)
- Falls back to ArcGIS for tax foreclosures
- Pushes named leads to GHL
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

CLERK_URL     = "https://bexar.tx.publicsearch.us"
ARCGIS_BASE   = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
LOOKBACK_DAYS = 90

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
GHL_API_BASE    = "https://services.leadconnectorhq.com"


def scrape_clerk():
    """Scrape bexar.tx.publicsearch.us using Selenium headless Chrome."""
    log.info("Starting Clerk scraper...")

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException
    except ImportError as e:
        log.error(f"Selenium not available: {e}")
        return []

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.binary_location = "/usr/bin/chromium-browser"

    driver = None
    records = []

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LOOKBACK_DAYS)

    try:
        service = Service("/usr/bin/chromedriver")
        driver  = webdriver.Chrome(service=service, options=options)
        wait    = WebDriverWait(driver, 20)

        # Build URL — FC = Foreclosures department, date range = last 90 days
        date_from = cutoff.strftime("%Y%m%d")
        date_to   = now.strftime("%Y%m%d")
        url = (
            f"{CLERK_URL}/results"
            f"?department=FC"
            f"&instrumentDateRange={date_from}%2C{date_to}"
            f"&keywordSearch=false"
            f"&limit=500"
            f"&offset=0"
        )

        log.info(f"  Loading: {url}")
        driver.get(url)
        time.sleep(4)

        # Accept disclaimer if shown
        try:
            for btn_text in ["Accept", "agree", "Continue", "I Agree"]:
                btns = driver.find_elements(By.XPATH, f"//button[contains(text(),'{btn_text}')]")
                if btns:
                    btns[0].click()
                    log.info(f"  Clicked: {btn_text}")
                    time.sleep(2)
                    break
        except Exception:
            pass

        log.info(f"  URL after load: {driver.current_url}")
        log.info(f"  Title: {driver.title}")

        # Wait for table
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
        except TimeoutException:
            log.warning("  No table found — logging page text")
            log.warning(f"  Page: {driver.find_element(By.TAG_NAME,'body').text[:500]}")

        # Get all rows
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        log.info(f"  Found {len(rows)} rows")

        if rows:
            # Log first row to understand column layout
            first_cells = [c.text.strip() for c in rows[0].find_elements(By.TAG_NAME, "td")]
            log.info(f"  First row cells: {first_cells}")

            # Log header to understand column order
            headers = [h.text.strip() for h in driver.find_elements(By.CSS_SELECTOR, "table thead th")]
            log.info(f"  Headers: {headers}")

        for row in rows:
            try:
                cells = [c.text.strip() for c in row.find_elements(By.TAG_NAME, "td")]
                if len(cells) < 3:
                    continue

                # Based on headers seen in your screenshots:
                # GRANTOR | GRANTEE | DOC TYPE | RECORDED DATE | SALE DATE | DOC NUMBER | PROPERTY ADDRESS
                # OR (from earlier screenshot without grantor col):
                # DOC TYPE | RECORDED DATE | SALE DATE | DOC NUMBER | REMARKS | PROPERTY ADDRESS

                # Detect layout by checking if first cell looks like a name or doc type
                first = cells[0]
                is_name_first = (
                    len(first) > 3 and
                    not any(x in first.upper() for x in ["NOTICE", "DEED", "RELEASE", "LIEN", "TRUST"]) and
                    not first.replace(" ", "").isdigit()
                )

                if is_name_first and len(cells) >= 5:
                    # Layout: GRANTOR | GRANTEE | DOC TYPE | RECORDED DATE | SALE DATE | DOC# | ADDRESS
                    grantor   = cells[0]
                    grantee   = cells[1] if len(cells) > 1 else ""
                    doc_type  = cells[2] if len(cells) > 2 else ""
                    rec_date  = cells[3] if len(cells) > 3 else ""
                    sale_date = cells[4] if len(cells) > 4 else ""
                    doc_num   = cells[5] if len(cells) > 5 else ""
                    address   = cells[6] if len(cells) > 6 else ""
                else:
                    # Layout: DOC TYPE | RECORDED DATE | SALE DATE | DOC# | REMARKS | ADDRESS
                    grantor   = ""
                    grantee   = ""
                    doc_type  = cells[0]
                    rec_date  = cells[1] if len(cells) > 1 else ""
                    sale_date = cells[2] if len(cells) > 2 else ""
                    doc_num   = cells[3] if len(cells) > 3 else ""
                    address   = cells[5] if len(cells) > 5 else cells[-1]

                # Skip non-foreclosure doc types
                if doc_type and "FORECLO" not in doc_type.upper() and "TRUSTEE" not in doc_type.upper():
                    continue

                # Skip records outside our date window
                if rec_date:
                    try:
                        # Parse various date formats: M/D/YYYY or YYYY-MM-DD
                        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                            try:
                                rec_dt = datetime.strptime(rec_date, fmt)
                                if rec_dt < cutoff.replace(tzinfo=None):
                                    continue  # skip old record
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass

                if not address and not doc_num:
                    continue

                records.append({
                    "type":        "NOF",
                    "source":      "clerk",
                    "address":     address,
                    "owner":       grantor,
                    "mail_addr":   "",
                    "absentee":    False,
                    "doc_number":  doc_num,
                    "year":        rec_date[-4:] if len(rec_date) >= 4 else "",
                    "month":       "",
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
                log.debug(f"  Row parse error: {e}")

        named = sum(1 for r in records if r.get("owner"))
        log.info(f"Clerk: {len(records)} records, {named} with owner name")

    except Exception as e:
        log.error(f"Clerk scraper error: {e}", exc_info=True)
    finally:
        if driver:
            driver.quit()

    return records


def fetch_arcgis():
    """Fetch from ArcGIS — always run to get tax foreclosures."""
    log.info("Fetching ArcGIS records...")

    def fetch_json(url):
        req = urllib.request.Request(
            url, headers={"User-Agent": "BexarScraper/16.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))

    def query(layer_url, offset=0):
        try:
            params = urllib.parse.urlencode({
                "where": "1=1", "outFields": "*", "returnGeometry": "false",
                "resultOffset": offset, "resultRecordCount": 1000, "f": "json",
            })
            data = fetch_json(f"{layer_url}/query?{params}")
            if "error" in data: return []
            return data.get("features", [])
        except Exception as e:
            log.warning(f"ArcGIS error: {e}")
            return []

    def pick(a, *keys, default=""):
        for k in keys:
            v = a.get(k)
            if v is not None and str(v).strip() not in ("", "None", "null", "<Null>"):
                return str(v).strip()
        return default

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=90)

    raw = []
    for idx, typ, label in [(0, "NOF", "Mortgage"), (1, "TAX", "Tax")]:
        layer_url = f"{ARCGIS_BASE}/{idx}"
        features, offset = [], 0
        while True:
            batch = query(layer_url, offset)
            features.extend(batch)
            log.info(f"  Layer {idx}: offset={offset}, {len(batch)} records")
            if len(batch) < 1000: break
            offset += len(batch)
        for feat in features:
            a     = feat["attributes"]
            month = pick(a, "MONTH", "MO", default="")
            year  = pick(a, "YEAR",  "YR", default="")
            raw.append({
                "type":        typ,
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

    log.info(f"ArcGIS: {len(raw)} records")
    return raw


def merge(clerk, arcgis):
    merged = {}
    for r in clerk:
        key = r.get("doc_number") or r.get("address", "").upper()
        if key: merged[key] = r
    clerk_addrs = {r.get("address", "").upper() for r in clerk if r.get("address")}
    added = 0
    for r in arcgis:
        doc  = r.get("doc_number", "")
        addr = r.get("address", "").upper()
        if doc in merged or addr in clerk_addrs: continue
        key = doc or addr
        if key:
            merged[key] = r
            added += 1
    log.info(f"Merged: {len(clerk)} clerk + {added} arcgis = {len(merged)} total")
    return list(merged.values())


def score(rec):
    s = 0
    if rec.get("address"):       s += 3
    if rec.get("owner"):         s += 3
    if rec.get("type") == "TAX": s += 2
    return min(s, 10)


def ghl_req(method, endpoint, payload=None):
    try:
        import requests
    except ImportError:
        return None
    url = f"{GHL_API_BASE}{endpoint}"
    h = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type": "application/json", "Accept": "application/json",
        "Version": "2021-07-28",
        "User-Agent": "Mozilla/5.0 Chrome/120.0.0.0",
        "Origin": "https://app.justjarvis.com", "Referer": "https://app.justjarvis.com/",
    }
    try:
        resp = requests.get(url, headers=h, timeout=20) if method == "GET" \
               else requests.post(url, headers=h, json=payload, timeout=20)
        if resp.status_code in (200, 201): return resp.json()
        log.warning(f"GHL {resp.status_code}: {resp.text[:150]}")
        return {"_error": resp.status_code}
    except Exception as e:
        return {"_error": str(e)}


def push_ghl(records):
    if not GHL_API_KEY:
        log.warning("GHL_API_KEY not set")
        return
    named = [r for r in records if r.get("owner")]
    log.info(f"GHL: {len(named)} named leads to push")
    test = ghl_req("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&limit=1")
    if not test or "_error" in test:
        log.error("GHL auth failed")
        return
    log.info(f"GHL auth OK — {test.get('total','?')} existing contacts")
    created = skipped = errors = 0
    for i, rec in enumerate(sorted(named, key=lambda r: -r.get("score", 0))):
        doc = rec.get("doc_number", "")
        # Check duplicate
        if doc:
            r = ghl_req("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&query={urllib.parse.quote(doc)}&limit=3")
            if r and not r.get("_error"):
                if any(f"doc-{doc}" in (c.get("tags") or []) for c in r.get("contacts", [])):
                    skipped += 1
                    continue
        owner = rec.get("owner", "").strip()
        parts = owner.split()
        first = parts[0].title() if parts else owner
        last  = " ".join(parts[1:]).title() if len(parts) > 1 else ""
        tags  = ["bexar-lead", rec["type"], f"doc-{doc}"]
        if rec.get("score", 0) >= 7: tags.append("hot-lead")
        lt = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
        result = ghl_req("POST", "/contacts/", {
            "locationId": GHL_LOCATION_ID,
            "firstName": first, "lastName": last, "name": owner.title(),
            "address1": rec.get("address", ""),
            "city": rec.get("city", "San Antonio"),
            "state": "TX", "country": "US", "postalCode": rec.get("zip", ""),
            "tags": tags, "source": "Bexar County Clerk",
            "customFields": [
                {"key": "lead_type",        "field_value": lt},
                {"key": "doc_number",       "field_value": doc},
                {"key": "date_filed",       "field_value": rec.get("date_filed", "")},
                {"key": "sale_date",        "field_value": rec.get("sale_date", "")},
                {"key": "score",            "field_value": str(rec.get("score", 0))},
                {"key": "property_address", "field_value": rec.get("address", "")},
                {"key": "lender",           "field_value": rec.get("grantee", "")},
            ],
        })
        if result and result.get("contact"):
            created += 1
            log.info(f"  ✓ [{i+1}] {owner} — {rec.get('address')}")
        else:
            errors += 1
        time.sleep(0.15)
    log.info(f"GHL done — Created:{created} | Skipped:{skipped} | Errors:{errors}")


DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Bexar County Motivated Seller Leads</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&display=swap" rel="stylesheet"/>
<style>
:root{--bg:#0d0f14;--surface:#13161e;--surface2:#1a1e2a;--border:#252836;--accent:#00e5ff;--accent3:#a78bfa;--text:#e8eaf0;--muted:#6b7280;--success:#22d3a5;--warning:#fbbf24;--danger:#f87171;}
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
  </select>
  <span class="count-badge" id="count-badge"></span>
</div>
<div class="table-wrap">
  <table>
    <thead><tr>
      <th>Score</th><th>Type</th><th>Property Address</th>
      <th>Owner (Grantor)</th><th>Lender (Grantee)</th><th>Date Filed</th><th>Sale Date</th><th>Doc #</th>
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
    var oh=r.owner?'<div class="owner">'+r.owner+'</div>':'<div class="owner-none">—</div>';
    var gh=r.grantee?'<div class="doc">'+r.grantee+'</div>':'<div class="owner-none">—</div>';
    rows+='<tr>'
      +'<td><div class="score '+scC+'">'+sc+'</div></td>'
      +'<td><span class="type-badge '+tC+'">'+tL+'</span></td>'
      +'<td><div class="addr">'+(r.address||'—')+'</div></td>'
      +'<td>'+oh+'</td>'
      +'<td>'+gh+'</td>'
      +'<td><div class="doc">'+(r.date_filed||'—')+'</div></td>'
      +'<td><div class="doc">'+(r.sale_date||'—')+'</div></td>'
      +'<td><div class="doc">'+(r.doc_number||'—')+'</div></td>'
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
        raise RuntimeError(f"Too small: {size} bytes")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    log.info("="*60)
    log.info("Bexar County Lead Scraper v16")
    log.info(f"Primary:  {CLERK_URL}")
    log.info(f"Fallback: {ARCGIS_BASE}")
    log.info("="*60)

    clerk_records  = scrape_clerk()
    arcgis_records = fetch_arcgis()
    records        = merge(clerk_records, arcgis_records)

    for r in records:
        if r["type"] == "TAX":             r["flags"].append("TAX FORE")
        if not r["owner"]:                 r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]: r["flags"].append("NO CITY")
        r["score"] = score(r)

    records.sort(key=lambda x: x["score"], reverse=True)
    named = sum(1 for r in records if r["owner"])
    log.info(f"Final: {len(records)} leads | {named} with owner name")

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved {len(records)} records")

    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    build_dashboard(records)
    push_ghl(records)

