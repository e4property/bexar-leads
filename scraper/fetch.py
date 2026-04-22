"""
Bexar County Motivated Seller Lead Scraper v14
Primary source: bexar.tx.publicsearch.us (Bexar County Clerk - daily updates)
Fallback source: maps.bexar.org ArcGIS (monthly updates)
Gets Grantor (owner name) directly from each record detail page.
"""

import json
import logging
import os
import re
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

# ── Sources ───────────────────────────────────────────────────────────────────
CLERK_BASE       = "https://bexar.tx.publicsearch.us"
CLERK_RESULTS    = f"{CLERK_BASE}/api/results"
CLERK_DETAIL     = f"{CLERK_BASE}/api/documents"
ARCGIS_BASE      = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"

# How many days back to search
LOOKBACK_DAYS = 30

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
GHL_API_BASE    = "https://services.leadconnectorhq.com"


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def fetch_json(url, headers=None, retries=3):
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if headers:
        default_headers.update(headers)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=default_headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                raw = r.read().decode("utf-8", errors="replace")
                return json.loads(raw)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                log.debug(f"fetch_json failed {url[:80]}: {e}")
                return {}


def fetch_html(url, headers=None):
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": CLERK_BASE,
    }
    if headers:
        default_headers.update(headers)
    try:
        req = urllib.request.Request(url, headers=default_headers)
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.debug(f"fetch_html failed {url[:80]}: {e}")
        return ""


def arcgis_query(layer_url, where="1=1", fields="*", offset=0, limit=1000):
    try:
        params = urllib.parse.urlencode({
            "where": where, "outFields": fields, "returnGeometry": "false",
            "resultOffset": offset, "resultRecordCount": limit, "f": "json",
        })
        data = fetch_json(f"{layer_url}/query?{params}")
        if "error" in data: return []
        return data.get("features", [])
    except Exception:
        return []


def pick(attrs, *candidates, default=""):
    for c in candidates:
        v = attrs.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>"):
            return str(v).strip()
    return default


# ── Clerk scraper ─────────────────────────────────────────────────────────────
def fetch_clerk_records():
    """
    Fetch foreclosure notices from bexar.tx.publicsearch.us.
    This is the official Bexar County Clerk system — updated daily.
    Returns list of lead records with owner names when available.
    """
    log.info("Fetching from Bexar County Clerk (publicsearch.us)...")

    now     = datetime.now(timezone.utc)
    cutoff  = now - timedelta(days=LOOKBACK_DAYS)
    date_from = cutoff.strftime("%Y%m%d")
    date_to   = (now + timedelta(days=180)).strftime("%Y%m%d")  # include future sale dates

    records = []

    # Try the API endpoint that the website uses internally
    # The URL structure from the browser: /results?department=FC&instrumentDateRange=...
    # Their internal API likely uses /api/results or similar

    api_endpoints = [
        # Try REST API format
        f"{CLERK_BASE}/api/results?department=FC&instrumentDateRange={date_from}%2C{date_to}&keywordSearch=false&limit=500&offset=0",
        # Try alternate format
        f"{CLERK_BASE}/api/search?department=FC&dateFrom={date_from}&dateTo={date_to}&limit=500",
        # Try the results page directly as JSON
        f"{CLERK_BASE}/results?department=FC&instrumentDateRange={date_from}%2C{date_to}&keywordSearch=false&limit=500&offset=0&format=json",
    ]

    clerk_headers = {
        "Referer":  f"{CLERK_BASE}/",
        "Origin":   CLERK_BASE,
        "Accept":   "application/json, text/plain, */*",
    }

    data = {}
    for endpoint in api_endpoints:
        log.info(f"  Trying: {endpoint[:80]}...")
        data = fetch_json(endpoint, headers=clerk_headers)
        if data and not data.get("error"):
            log.info(f"  Got response: {str(data)[:200]}")
            break
        time.sleep(0.5)

    # Parse response — format varies
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (
            data.get("results") or data.get("data") or
            data.get("documents") or data.get("records") or
            data.get("hits") or []
        )

    log.info(f"  Clerk API returned {len(items)} records")

    for item in items:
        # Extract fields — try multiple possible field names
        doc_num   = str(item.get("documentNumber") or item.get("docNumber") or
                       item.get("instrumentNumber") or item.get("id") or "").strip()
        rec_date  = str(item.get("recordedDate") or item.get("instrumentDate") or
                       item.get("recordDate") or "").strip()
        sale_date = str(item.get("saleDate") or item.get("executionDate") or "").strip()
        address   = str(item.get("propertyAddress") or item.get("address") or
                       item.get("legalDescription") or "").strip()
        grantor   = str(item.get("grantor") or item.get("grantorName") or
                       item.get("grantor1") or item.get("owner") or "").strip()
        grantee   = str(item.get("grantee") or item.get("granteeName") or "").strip()
        remarks   = str(item.get("remarks") or item.get("comments") or "").strip()

        if not doc_num and not address:
            continue

        records.append({
            "type":        "NOF",
            "source":      "clerk",
            "address":     address,
            "owner":       grantor,
            "mail_addr":   "",
            "absentee":    False,
            "doc_number":  doc_num,
            "year":        rec_date[:4] if rec_date else "",
            "month":       rec_date[4:6] if len(rec_date) >= 6 else "",
            "city":        "San Antonio",
            "zip":         "",
            "school_dist": "",
            "date_filed":  rec_date,
            "sale_date":   sale_date,
            "remarks":     remarks,
            "grantee":     grantee,
            "flags":       [],
            "enriched":    bool(grantor),
        })

    if records:
        log.info(f"  ✓ Clerk returned {len(records)} foreclosure notices")
        named = sum(1 for r in records if r.get("owner"))
        log.info(f"  With owner name: {named}")
    else:
        log.warning("  Clerk API returned no records — will use ArcGIS fallback")

    return records


# ── ArcGIS fallback ───────────────────────────────────────────────────────────
def fetch_arcgis_records():
    """Fallback: fetch from ArcGIS MapServer (monthly updates)."""
    log.info("Fetching from ArcGIS MapServer (fallback)...")

    LAYERS = [
        {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
        {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
    ]

    raw = []
    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{ARCGIS_BASE}/{idx}"
        log.info(f"  Layer {idx} ({layer['label']})...")
        try:
            features, offset = [], 0
            while True:
                batch = arcgis_query(layer_url, offset=offset)
                features.extend(batch)
                if len(batch) < 1000: break
                offset += len(batch)
            log.info(f"  Layer {idx}: {len(features)} records")
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
                    "remarks":     "",
                    "grantee":     "",
                    "flags":       [],
                    "enriched":    False,
                })
        except Exception as e:
            log.error(f"  Layer {idx} failed: {e}")

    return raw


# ── Merge + deduplicate ───────────────────────────────────────────────────────
def merge_records(clerk_records, arcgis_records):
    """
    Merge clerk and ArcGIS records, preferring clerk (fresher, has owner names).
    Deduplicate by doc number and address.
    """
    merged      = {}
    clerk_count = 0
    arcgis_new  = 0

    # Add clerk records first (higher quality)
    for r in clerk_records:
        key = r.get("doc_number") or r.get("address", "").upper()
        if key:
            merged[key] = r
            clerk_count += 1

    # Add ArcGIS records that aren't already in clerk data
    addr_keys = {r.get("address", "").upper() for r in clerk_records if r.get("address")}
    for r in arcgis_records:
        doc  = r.get("doc_number", "")
        addr = r.get("address", "").upper()
        if doc and doc in merged:
            continue
        if addr and addr in addr_keys:
            continue
        key = doc or addr
        if key:
            merged[key] = r
            arcgis_new += 1

    log.info(f"Merge: {clerk_count} from Clerk + {arcgis_new} new from ArcGIS = {len(merged)} total")
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
    url     = f"{GHL_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type": "application/json", "Accept": "application/json",
        "Version": "2021-07-28",
        "User-Agent": "Mozilla/5.0 Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://app.justjarvis.com",
        "Referer": "https://app.justjarvis.com/",
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
        "tags": tags, "source": "Bexar County Clerk / Scraper",
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
        log.error("GHL auth failed — update GHL_API_KEY secret in GitHub")
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
.src-badge{display:inline-block;padding:1px 5px;font-size:9px;border-radius:2px;background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.2);margin-left:4px;vertical-align:middle;}
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
    <option value="sale-asc">Sort: Sale Date ↑</option>
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
    if(s==='sale-asc')   return (a.sale_date||'zzz')<(b.sale_date||'zzz')?-1:1;
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
    var src=r.source==='clerk'?'<span class="src-badge">CLERK</span>':'';
    var cz=[r.city,r.zip].filter(Boolean).join(' ')||'—';
    var oh=r.owner?'<div class="owner">'+r.owner+'</div>':'<div class="owner-none">—</div>';
    rows+='<tr>'
      +'<td><div class="score '+scC+'">'+sc+'</div></td>'
      +'<td><span class="type-badge '+tC+'">'+tL+'</span>'+src+'</td>'
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
    log.info("Bexar County Lead Scraper v14")
    log.info(f"Primary: {CLERK_BASE}")
    log.info(f"Fallback: {ARCGIS_BASE}")
    log.info(f"Lookback: {LOOKBACK_DAYS} days")
    log.info("="*60)

    # Try Clerk first (daily, has owner names)
    clerk_records  = fetch_clerk_records()

    # Always get ArcGIS too (has tax foreclosures)
    arcgis_records = fetch_arcgis_records()

    # Merge both sources
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

