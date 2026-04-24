"""
Bexar County Motivated Seller Lead Scraper v19
- ArcGIS foreclosure data (working perfectly)
- Owner names from ArcGIS Parcels layer via targeted address queries
  Same server, no bot detection, works from GitHub Actions
"""

import json
import logging
import os
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

FORECLOSURE_BASE = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
PARCELS_URL      = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"

LAYERS = [
    {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
    {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
]

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
GHL_API_BASE    = "https://services.leadconnectorhq.com"


# ── HTTP ──────────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/19.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                log.debug(f"fetch_json failed: {e}")
                return {}


def arcgis_query(layer_url, where, fields="*", offset=0, limit=1000):
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


# ── Owner lookup via ArcGIS Parcels ──────────────────────────────────────────
def lookup_owner_by_address(address):
    """
    Query the Bexar County Parcels ArcGIS layer to find the owner name
    for a given property address. Uses the street number for exact matching.

    Parcel fields available: Situs (street name), Owner, AddrLn1, AddrCity, Zip
    Situs contains the street name only (e.g. 'LAGUNA MADRE ST')
    We match using the street number from AddrLn1 or by Situs LIKE query.
    """
    if not address:
        return {}

    # Extract street number and street name from address
    # e.g. "13114 LAGUNA MADRE ST" -> num="13114", street="LAGUNA MADRE"
    parts = address.strip().upper().split()
    if not parts:
        return {}

    street_num = parts[0] if parts[0].isdigit() else ""
    # Get first meaningful word of street name (skip number)
    street_words = [p for p in parts[1:] if len(p) > 2 and p not in ("ST","AVE","DR","RD","LN","CT","BLVD","WAY","PL","CIR","TRL","PKWY","HWY")]
    street_key = street_words[0] if street_words else (parts[1] if len(parts) > 1 else "")

    if not street_key:
        return {}

    try:
        # Query parcels where Situs contains the street name keyword
        # and AddrLn1 starts with the street number
        if street_num:
            where = f"Situs LIKE '%{street_key}%' AND AddrLn1 LIKE '{street_num}%'"
        else:
            where = f"Situs LIKE '%{street_key}%'"

        features = arcgis_query(
            PARCELS_URL, where,
            fields="Situs,Owner,AddrLn1,AddrCity,Zip",
            limit=10
        )

        if not features:
            # Try broader search with just street key
            where2 = f"Situs LIKE '%{street_key}%'"
            features = arcgis_query(
                PARCELS_URL, where2,
                fields="Situs,Owner,AddrLn1,AddrCity,Zip",
                limit=20
            )

        if not features:
            return {}

        # Find best match
        best_owner = ""
        best_mail  = ""
        best_score = 0

        for feat in features:
            a = feat["attributes"]
            situs  = str(a.get("Situs")  or "").strip().upper()
            owner  = str(a.get("Owner")  or "").strip()
            addr1  = str(a.get("AddrLn1") or "").strip()
            city   = str(a.get("AddrCity") or "").strip()
            zipcode= str(a.get("Zip")    or "").strip()

            if not owner or owner.upper() in ("", "NONE", "NULL"):
                continue

            # Score this match
            score = 0
            if street_num and addr1.startswith(street_num):
                score += 3
            if street_key in situs:
                score += 2
            # Check all address parts match
            match_parts = sum(1 for p in parts[1:3] if p in situs)
            score += match_parts

            if score > best_score:
                best_score = score
                best_owner = owner
                best_mail  = f"{addr1} {city} {zipcode}".strip() if addr1 else ""

        if best_score >= 2 and best_owner:
            return {"owner": best_owner, "mail_addr": best_mail}

    except Exception as e:
        log.debug(f"Parcel lookup error for '{address}': {e}")

    return {}


# ── Fetch foreclosures ────────────────────────────────────────────────────────
def fetch_foreclosures():
    log.info("Fetching foreclosure records from ArcGIS...")
    raw = []
    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{FORECLOSURE_BASE}/{idx}"
        log.info(f"  Layer {idx} ({layer['label']})...")
        features, offset = [], 0
        while True:
            batch = arcgis_query(layer_url, "1=1", offset=offset)
            features.extend(batch)
            log.info(f"    offset={offset}: {len(batch)} (total: {len(features)})")
            if len(batch) < 1000: break
            offset += len(batch)
        for feat in features:
            a     = feat["attributes"]
            month = pick(a, "MONTH", "MO", default="")
            year  = pick(a, "YEAR",  "YR", default="")
            raw.append({
                "type":        layer["type"],
                "address":     pick(a, "ADDRESS", "SITUS_ADD", "ADDR"),
                "owner":       "",
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
            })
    log.info(f"Foreclosures: {len(raw)} total records")
    return raw


# ── Enrich with owner names ───────────────────────────────────────────────────
def enrich_owner_names(records):
    """
    Look up owner names from ArcGIS Parcels layer.
    Uses targeted queries — fast and no bot detection.
    Budget: ~407 queries × 0.2s = ~80 seconds.
    """
    log.info(f"Looking up owner names for {len(records)} records...")

    # First, probe the parcel layer to understand data format
    log.info("  Probing parcel layer...")
    sample = arcgis_query(PARCELS_URL, "1=1", fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=3)
    if sample:
        for i, feat in enumerate(sample[:3]):
            log.info(f"  Parcel sample {i+1}: {dict(feat['attributes'])}")
    else:
        log.warning("  Parcel layer returned no sample records!")

    found = 0
    for i, rec in enumerate(records):
        addr = rec.get("address", "")
        if not addr:
            continue

        result = lookup_owner_by_address(addr)
        if result and result.get("owner"):
            rec["owner"]     = result["owner"]
            rec["mail_addr"] = result.get("mail_addr", "")
            # Absentee: mailing address differs from property address
            if rec["mail_addr"] and addr.upper() not in rec["mail_addr"].upper():
                prop_words = set(addr.upper().split())
                mail_words = set(rec["mail_addr"].upper().split())
                if len(prop_words & mail_words) < 2:
                    rec["absentee"] = True
            found += 1
            if found <= 5 or found % 25 == 0:
                log.info(f"  [{i+1}/{len(records)}] ✓ {addr} → {result['owner']}")

        # Progress update every 50
        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(records)} | Found: {found}")

        time.sleep(0.2)  # rate limiting

    pct      = 100 * found // max(len(records), 1)
    absentee = sum(1 for r in records if r.get("absentee"))
    log.info(f"Owner lookup complete: {found}/{len(records)} ({pct}% hit rate)")
    log.info(f"Absentee owners: {absentee}")
    return records


# ── Scoring ───────────────────────────────────────────────────────────────────
def score_record(rec):
    s = 0
    if rec.get("address"):       s += 3
    if rec.get("owner"):         s += 3
    if rec.get("type") == "TAX": s += 2
    if rec.get("absentee"):      s += 2
    return min(s, 10)


# ── GHL ───────────────────────────────────────────────────────────────────────
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
        "Origin": "https://app.justjarvis.com",
        "Referer": "https://app.justjarvis.com/",
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
        log.warning("GHL_API_KEY not set"); return
    named = [r for r in records if r.get("owner")]
    log.info(f"GHL: {len(named)} named leads to push")
    test = ghl_req("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&limit=1")
    if not test or "_error" in test:
        log.error("GHL auth failed"); return
    log.info(f"GHL auth OK — {test.get('total','?')} existing contacts")
    created = skipped = errors = 0
    for i, rec in enumerate(sorted(named, key=lambda r: -r.get("score", 0))):
        doc = rec.get("doc_number", "")
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
        if rec.get("absentee"):      tags += ["absentee-owner", "high-priority"]
        if rec.get("score", 0) >= 7: tags.append("hot-lead")
        lt = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
        result = ghl_req("POST", "/contacts/", {
            "locationId": GHL_LOCATION_ID,
            "firstName": first, "lastName": last, "name": owner.title(),
            "address1": rec.get("address", ""),
            "city": rec.get("city", "San Antonio"),
            "state": "TX", "country": "US", "postalCode": rec.get("zip", ""),
            "tags": tags, "source": "Bexar County Scraper",
            "customFields": [
                {"key": "lead_type",        "field_value": lt},
                {"key": "doc_number",       "field_value": doc},
                {"key": "date_filed",       "field_value": rec.get("date_filed", "")},
                {"key": "score",            "field_value": str(rec.get("score", 0))},
                {"key": "property_address", "field_value": rec.get("address", "")},
                {"key": "school_district",  "field_value": rec.get("school_dist", "")},
                {"key": "absentee_owner",   "field_value": "Yes" if rec.get("absentee") else "No"},
                {"key": "mailing_address",  "field_value": rec.get("mail_addr", "")},
            ],
        })
        if result and result.get("contact"):
            created += 1
            ab = " 🏠 ABSENTEE" if rec.get("absentee") else ""
            log.info(f"  ✓ [{i+1}]{ab} {owner} — {rec.get('address')}")
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
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:var(--border);border-bottom:1px solid var(--border);}
.stat-card{background:var(--surface);padding:20px 24px;display:flex;flex-direction:column;gap:6px;}
.stat-num{font-family:'Syne',sans-serif;font-size:32px;font-weight:800;line-height:1;color:var(--accent);}
.stat-card:nth-child(2) .stat-num{color:var(--danger);}
.stat-card:nth-child(3) .stat-num{color:var(--warning);}
.stat-card:nth-child(4) .stat-num{color:var(--success);}
.stat-card:nth-child(5) .stat-num{color:var(--hot);}
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
tbody tr.absentee-row{border-left:3px solid var(--hot);}
tbody td{padding:10px 12px;vertical-align:middle;}
.score{display:inline-flex;width:36px;height:36px;border-radius:50%;align-items:center;justify-content:center;font-weight:500;font-size:12px;font-family:'Syne',sans-serif;}
.score-high{background:rgba(34,211,165,.15);color:var(--success);border:1px solid rgba(34,211,165,.3);}
.score-mid{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.3);}
.score-low{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.3);}
.type-badge{display:inline-block;padding:2px 8px;font-size:10px;font-weight:500;border-radius:2px;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;}
.type-nof{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.25);}
.type-tax{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.25);}
.addr{color:var(--text);font-size:12px;max-width:180px;}
.owner{color:var(--success);font-size:12px;font-weight:500;}
.owner-none{color:var(--muted);font-size:12px;}
.doc{color:var(--muted);font-size:12px;}
.flag{display:inline-block;padding:2px 6px;font-size:10px;background:rgba(167,139,250,.12);color:var(--accent3);border:1px solid rgba(167,139,250,.25);border-radius:2px;margin-right:3px;}
.flag-hot{background:rgba(255,107,53,.15);color:var(--hot);border-color:rgba(255,107,53,.3);font-weight:600;}
.state-msg{text-align:center;padding:60px 20px;color:var(--muted);}
.pagination{display:flex;justify-content:center;align-items:center;gap:8px;padding:20px 32px;color:var(--muted);font-size:12px;}
.pagination button{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:6px 14px;cursor:pointer;font-family:'DM Mono',monospace;font-size:12px;}
.pagination button:hover:not(:disabled){border-color:var(--accent);color:var(--accent);}
.pagination button:disabled{opacity:.3;cursor:default;}
@media(max-width:1100px){.stats{grid-template-columns:repeat(3,1fr);}.controls,.table-wrap{padding-left:16px;padding-right:16px;}}
</style>
</head>
<body>
<header>
  <div class="logo">🏠 Bexar County <span>Leads</span></div>
  <div id="last-updated">UPDATED_PLACEHOLDER</div>
</header>
<div class="stats">
  <div class="stat-card"><div class="stat-num" id="s-total">—</div><div class="stat-label">Total Leads</div></div>
  <div class="stat-card"><div class="stat-num" id="s-nof">—</div><div class="stat-label">Foreclosures (NOF)</div></div>
  <div class="stat-card"><div class="stat-num" id="s-tax">—</div><div class="stat-label">Tax Foreclosures</div></div>
  <div class="stat-card"><div class="stat-num" id="s-named">—</div><div class="stat-label">With Owner Name</div></div>
  <div class="stat-card"><div class="stat-num" id="s-absentee">—</div><div class="stat-label">Absentee Owners 🔥</div></div>
</div>
<div class="controls">
  <input type="text" id="search" placeholder="Search address, owner, doc #…" oninput="applyFilters()"/>
  <select id="type-filter" onchange="applyFilters()">
    <option value="">All Types</option>
    <option value="NOF">Foreclosure (NOF)</option>
    <option value="TAX">Tax Foreclosure</option>
  </select>
  <select id="owner-filter" onchange="applyFilters()">
    <option value="">All Leads</option>
    <option value="named">With Owner Name</option>
    <option value="absentee">Absentee Owners 🔥</option>
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
      <th>Owner Name</th><th>Date Filed</th><th>Doc #</th><th>City/ZIP</th><th>Flags</th>
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
  document.getElementById('s-absentee').textContent=ALL_RECORDS.filter(function(r){return r.absentee;}).length;
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
    var mow=!ow||(ow==='named'?!!r.owner:ow==='absentee'?!!r.absentee:!r.owner);
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
    var rc=r.absentee?' class="absentee-row"':'';
    var fh='';
    for(var j=0;j<(r.flags||[]).length;j++){
      var fc=r.flags[j]==='ABSENTEE'?'flag flag-hot':'flag';
      fh+='<span class="'+fc+'">'+r.flags[j]+'</span>';
    }
    if(!fh) fh='<span style="color:var(--muted)">—</span>';
    rows+='<tr'+rc+'>'
      +'<td><div class="score '+scC+'">'+sc+'</div></td>'
      +'<td><span class="type-badge '+tC+'">'+tL+'</span></td>'
      +'<td><div class="addr">'+(r.address||'—')+'</div></td>'
      +'<td>'+oh+'</td>'
      +'<td><div class="doc">'+(r.date_filed||'—')+'</div></td>'
      +'<td><div class="doc">'+(r.doc_number||'—')+'</div></td>'
      +'<td><div class="doc">'+cz+'</div></td>'
      +'<td>'+fh+'</td>'
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
    log.info("Bexar County Lead Scraper v19")
    log.info(f"Foreclosures: {FORECLOSURE_BASE}")
    log.info(f"Owner lookup: {PARCELS_URL}")
    log.info("="*60)

    records = fetch_foreclosures()
    records = enrich_owner_names(records)

    for r in records:
        r["flags"] = []
        if r["type"] == "TAX":             r["flags"].append("TAX FORE")
        if r.get("absentee"):              r["flags"].append("ABSENTEE")
        if not r["owner"]:                 r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]: r["flags"].append("NO CITY")
        r["score"] = score_record(r)

    records.sort(key=lambda x: x["score"], reverse=True)
    named    = sum(1 for r in records if r["owner"])
    absentee = sum(1 for r in records if r["absentee"])
    log.info(f"Final: {len(records)} leads | {named} named | {absentee} absentee")

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved {len(records)} records")

    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    build_dashboard(records)
    push_ghl(records)

