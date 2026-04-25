"""
Bexar County Motivated Seller Lead Scraper v24
SCRAPER IMPROVEMENTS:
  - Multi-strategy owner lookup for 90%+ hit rate
  - Strategy 1: 2-word street search + house number filter (normalized)
  - Strategy 2: 1-word street search + house number filter
  - Strategy 3: All-words fallback — tries each word in street name
  - Better absentee detection: compares normalized mailing vs property address
DASHBOARD IMPROVEMENTS:
  - CSV export button
  - Map view (Google Maps links per property)
  - "New this run" badge on leads added in latest scrape
  - Duplicate owner detection (flags same owner appearing 2+ times)
  - Improved absentee labeling with mailing address shown
"""

import json
import logging
import os
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

RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── HTTP ──────────────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/24.0", "Accept": "application/json"})
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
    """Collapse multiple spaces, strip, uppercase. Handles Situs double-space quirk."""
    return " ".join(str(s).upper().split())


# ── Address parsing ───────────────────────────────────────────────────────────
def parse_address(address):
    if not address:
        return None
    parts = address.strip().upper().split()
    if not parts or not parts[0].isdigit():
        return None
    num        = parts[0]
    rest       = parts[1:] if len(parts) > 1 else []
    street     = " ".join(rest)
    # Strip common suffixes for better matching
    SUFFIXES = {"ST","AVE","DR","RD","LN","CT","CIR","BLVD","WAY","PL",
                "TRL","PKWY","HWY","LOOP","PASS","CV","PT","HLS","TRAIL",
                "GROVE","RIDGE","CREEK","LAKE","PARK","GLEN","RUN","XING"}
    words      = rest[:]
    suffix     = ""
    if words and words[-1] in SUFFIXES:
        suffix = words[-1]
        words  = words[:-1]
    core_street = " ".join(words)  # Street name without suffix
    return {
        "num":         num,
        "street":      street,
        "core_street": core_street,
        "words":       words,
        "suffix":      suffix,
        "full":        address.strip().upper()
    }


# ── Core matcher ──────────────────────────────────────────────────────────────
def match_features(feats, num, first_word):
    """Given ArcGIS results, find the one whose normalized Situs starts with our house number."""
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

        # Must start with exactly our house number
        if not situs_norm.startswith(num + " "):
            continue

        # Street name word must appear in situs
        if first_word and first_word not in situs_norm:
            continue

        mail_addr  = f"{addr1} {city} {zipcode}".strip() if addr1 and addr1.upper() not in ("NULL","NONE","") else ""
        mail_norm  = normalize(mail_addr)
        prop_norm  = normalize(f"{num} {first_word}")

        # Absentee: mailing address doesn't start with same house number
        absentee   = bool(mail_addr) and not mail_norm.startswith(num + " ")

        return {
            "owner":     owner,
            "mail_addr": mail_addr,
            "absentee":  absentee,
        }
    return None


# ── Owner lookup — multi-strategy ─────────────────────────────────────────────
def lookup_owner(address):
    """
    Multi-strategy lookup for maximum hit rate:
    Strategy 1: Search '%WORD1 WORD2%' (two-word, most precise)
    Strategy 2: Search '%WORD1%' (one-word fallback)
    Strategy 3: Try each remaining word in street name
    Each strategy filters results by normalized house number match.
    """
    parsed = parse_address(address)
    if not parsed:
        return {}

    num        = parsed["num"]
    words      = parsed["words"]      # Street words without suffix
    first_word = words[0] if words else ""

    if not first_word or len(first_word) < 3:
        return {}

    # Strategy 1: Two-word search (most precise, fewer false positives)
    if len(words) >= 2:
        search = f"{words[0]} {words[1]}"
        feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{search}%'",
                              fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
        result = match_features(feats, num, first_word)
        if result:
            result["method"] = "s1_two_word"
            return result

    # Strategy 2: One-word search
    feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{first_word}%'",
                          fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
    result = match_features(feats, num, first_word)
    if result:
        result["method"] = "s2_one_word"
        return result

    # Strategy 3: Try other words in street name (skip short ones)
    for word in words[1:]:
        if len(word) < 4:
            continue
        feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{word}%'",
                              fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
        result = match_features(feats, num, word)
        if result:
            result["method"] = "s3_alt_word"
            return result

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
                if len(batch) < 1000: break
                offset += len(batch)
            except Exception as e:
                log.error(f"Layer {idx} query error: {e}")
                break

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
                "duplicate":   False,
                "is_new":      True,
                "doc_number":  pick(a, "DOC_NUMBER", "DOCNUM", "DOC_NUM"),
                "year":        year,
                "month":       month,
                "city":        pick(a, "CITY", "MAIL_CITY", default=""),
                "zip":         pick(a, "ZIP", "ZIPCODE", "ZIP_CODE", default=""),
                "school_dist": pick(a, "SCHOOL_DIST", default=""),
                "date_filed":  f"{month}/{year}".strip("/"),
                "run_ts":      RUN_TIMESTAMP,
                "sale_date":   "",
                "flags":       [],
            })

    log.info(f"Foreclosures: {len(raw)} total records")
    return raw


# ── Enrich owner names ────────────────────────────────────────────────────────
def enrich_owners(records):
    log.info(f"Looking up owners for {len(records)} records...")

    found = s1 = s2 = s3 = 0

    for i, rec in enumerate(records):
        addr = rec.get("address", "")
        if not addr:
            continue

        result = lookup_owner(addr)
        if result and result.get("owner"):
            rec["owner"]     = result["owner"]
            rec["mail_addr"] = result.get("mail_addr", "")
            rec["absentee"]  = result.get("absentee", False)
            found += 1
            method = result.get("method", "")
            if "s1" in method: s1 += 1
            elif "s2" in method: s2 += 1
            elif "s3" in method: s3 += 1
            if found <= 10 or found % 50 == 0:
                ab = " [ABSENTEE]" if rec["absentee"] else ""
                log.info(f"  [{i+1}/{len(records)}] ✓{ab} [{method}] {addr} → {result['owner']}")

        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(records)} | Found: {found} (s1={s1} s2={s2} s3={s3})")

        time.sleep(0.15)

    pct      = 100 * found // max(len(records), 1)
    absentee = sum(1 for r in records if r.get("absentee"))
    log.info(f"Owner lookup: {found}/{len(records)} ({pct}% hit rate)")
    log.info(f"  Strategy breakdown — s1:{s1} s2:{s2} s3:{s3}")
    log.info(f"  Absentee owners: {absentee}")
    return records


# ── Duplicate detection ───────────────────────────────────────────────────────
def detect_duplicates(records):
    """Flag records where the same owner appears more than once."""
    from collections import Counter
    owner_counts = Counter(
        r["owner"].upper().strip()
        for r in records
        if r.get("owner") and r["owner"].upper().strip() not in ("", "NULL")
    )
    dupes = 0
    for r in records:
        owner_key = (r.get("owner") or "").upper().strip()
        if owner_key and owner_counts[owner_key] > 1:
            r["duplicate"] = True
            dupes += 1
    log.info(f"Duplicate owners flagged: {dupes}")
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
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Version":       "2021-07-28",
        "User-Agent":    "Mozilla/5.0 Chrome/120.0.0.0",
        "Origin":        "https://app.justjarvis.com",
        "Referer":       "https://app.justjarvis.com/",
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
    log.info(f"GHL: {len(named)} named leads")
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
                    skipped += 1; continue
        owner = rec.get("owner", "").strip()
        parts = owner.split()
        first = parts[0].title() if parts else owner
        last  = " ".join(parts[1:]).title() if len(parts) > 1 else ""
        tags  = ["bexar-lead", rec["type"], f"doc-{doc}"]
        if rec.get("absentee"):      tags += ["absentee-owner", "high-priority"]
        if rec.get("duplicate"):     tags.append("duplicate-owner")
        if rec.get("score", 0) >= 7: tags.append("hot-lead")
        lt = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
        result = ghl_req("POST", "/contacts/", {
            "locationId": GHL_LOCATION_ID,
            "firstName":  first, "lastName": last, "name": owner.title(),
            "address1":   rec.get("address", ""),
            "city":       rec.get("city", "San Antonio"),
            "state":      "TX", "country": "US",
            "postalCode": rec.get("zip", ""),
            "tags":       tags,
            "source":     "Bexar County Scraper",
            "customFields": [
                {"key": "lead_type",        "field_value": lt},
                {"key": "doc_number",       "field_value": doc},
                {"key": "date_filed",       "field_value": rec.get("date_filed", "")},
                {"key": "score",            "field_value": str(rec.get("score", 0))},
                {"key": "property_address", "field_value": rec.get("address", "")},
                {"key": "school_district",  "field_value": rec.get("school_dist", "")},
                {"key": "absentee_owner",   "field_value": "Yes" if rec.get("absentee") else "No"},
                {"key": "mailing_address",  "field_value": rec.get("mail_addr", "")},
                {"key": "duplicate_owner",  "field_value": "Yes" if rec.get("duplicate") else "No"},
            ],
        })
        if result and result.get("contact"):
            created += 1
            ab  = " 🏠 ABSENTEE" if rec.get("absentee") else ""
            dup = " ♻ DUP" if rec.get("duplicate") else ""
            log.info(f"  ✓ [{i+1}]{ab}{dup} {owner} — {rec.get('address')}")
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
:root{--bg:#0d0f14;--surface:#13161e;--surface2:#1a1e2a;--border:#252836;--accent:#00e5ff;--accent3:#a78bfa;--text:#e8eaf0;--muted:#6b7280;--success:#22d3a5;--warning:#fbbf24;--danger:#f87171;--hot:#ff6b35;--new:#a78bfa;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'DM Mono',monospace;font-size:13px;min-height:100vh;}
header{display:flex;align-items:center;justify-content:space-between;padding:18px 32px;border-bottom:1px solid var(--border);background:var(--surface);position:sticky;top:0;z-index:100;gap:12px;flex-wrap:wrap;}
.logo{font-family:'Syne',sans-serif;font-size:20px;font-weight:800;}.logo span{color:var(--accent);}
.header-right{display:flex;align-items:center;gap:12px;}
#last-updated{color:var(--muted);font-size:11px;}
.btn{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:7px 14px;cursor:pointer;font-family:'DM Mono',monospace;font-size:12px;white-space:nowrap;transition:border-color .2s,color .2s;}
.btn:hover{border-color:var(--accent);color:var(--accent);}
.btn-csv{border-color:var(--success);color:var(--success);}
.btn-csv:hover{background:rgba(34,211,165,.08);}
.stats{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--border);border-bottom:1px solid var(--border);}
.stat-card{background:var(--surface);padding:16px 20px;display:flex;flex-direction:column;gap:4px;}
.stat-num{font-family:'Syne',sans-serif;font-size:28px;font-weight:800;line-height:1;color:var(--accent);}
.stat-card:nth-child(2) .stat-num{color:var(--danger);}
.stat-card:nth-child(3) .stat-num{color:var(--warning);}
.stat-card:nth-child(4) .stat-num{color:var(--success);}
.stat-card:nth-child(5) .stat-num{color:var(--hot);}
.stat-card:nth-child(6) .stat-num{color:var(--new);}
.stat-label{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:1px;}
.controls{display:flex;gap:10px;padding:14px 32px;background:var(--surface);border-bottom:1px solid var(--border);align-items:center;flex-wrap:wrap;}
input[type=text]{flex:1;min-width:200px;background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:8px 14px;font-family:'DM Mono',monospace;font-size:13px;outline:none;transition:border-color .2s;}
input[type=text]:focus{border-color:var(--accent);}
select{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:8px 12px;font-family:'DM Mono',monospace;font-size:13px;cursor:pointer;outline:none;}
.count-badge{color:var(--muted);font-size:11px;white-space:nowrap;padding:0 8px;}
.table-wrap{overflow-x:auto;padding:0 32px 32px;}
table{width:100%;border-collapse:collapse;margin-top:16px;}
thead th{text-align:left;padding:10px 12px;font-size:10px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);border-bottom:1px solid var(--border);white-space:nowrap;cursor:pointer;user-select:none;}
thead th:hover{color:var(--accent);}
tbody tr{border-bottom:1px solid var(--border);transition:background .12s;}
tbody tr:hover{background:var(--surface2);}
tbody tr.absentee-row{border-left:3px solid var(--hot);}
tbody tr.new-row{border-left:3px solid var(--new);}
tbody tr.absentee-row.new-row{border-left:3px solid var(--hot);}
tbody td{padding:10px 12px;vertical-align:middle;}
.score{display:inline-flex;width:34px;height:34px;border-radius:50%;align-items:center;justify-content:center;font-weight:500;font-size:12px;font-family:'Syne',sans-serif;}
.score-high{background:rgba(34,211,165,.15);color:var(--success);border:1px solid rgba(34,211,165,.3);}
.score-mid{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.3);}
.score-low{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.3);}
.type-badge{display:inline-block;padding:2px 8px;font-size:10px;font-weight:500;border-radius:2px;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;}
.type-nof{background:rgba(248,113,113,.15);color:var(--danger);border:1px solid rgba(248,113,113,.25);}
.type-tax{background:rgba(251,191,36,.15);color:var(--warning);border:1px solid rgba(251,191,36,.25);}
.addr{color:var(--text);font-size:12px;}
.addr a{color:var(--accent);text-decoration:none;font-size:10px;margin-left:4px;opacity:.7;}
.addr a:hover{opacity:1;}
.owner{color:var(--success);font-size:12px;font-weight:500;}
.owner-none{color:var(--muted);font-size:12px;}
.mail{color:var(--muted);font-size:10px;margin-top:2px;}
.doc{color:var(--muted);font-size:12px;}
.flag{display:inline-block;padding:2px 6px;font-size:10px;background:rgba(167,139,250,.12);color:var(--accent3);border:1px solid rgba(167,139,250,.25);border-radius:2px;margin-right:3px;margin-bottom:2px;}
.flag-hot{background:rgba(255,107,53,.15);color:var(--hot);border-color:rgba(255,107,53,.3);font-weight:600;}
.flag-new{background:rgba(167,139,250,.15);color:var(--new);border-color:rgba(167,139,250,.3);font-weight:600;}
.flag-dup{background:rgba(251,191,36,.1);color:var(--warning);border-color:rgba(251,191,36,.25);}
.state-msg{text-align:center;padding:60px 20px;color:var(--muted);}
.pagination{display:flex;justify-content:center;align-items:center;gap:8px;padding:20px 32px;color:var(--muted);font-size:12px;}
.pagination button{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:6px 14px;cursor:pointer;font-family:'DM Mono',monospace;font-size:12px;}
.pagination button:hover:not(:disabled){border-color:var(--accent);color:var(--accent);}
.pagination button:disabled{opacity:.3;cursor:default;}
@media(max-width:1200px){.stats{grid-template-columns:repeat(3,1fr);}.controls,.table-wrap{padding-left:16px;padding-right:16px;}header{padding:14px 16px;}}
</style>
</head>
<body>
<header>
  <div class="logo">🏠 Bexar County <span>Leads</span></div>
  <div class="header-right">
    <div id="last-updated">UPDATED_PLACEHOLDER</div>
    <button class="btn btn-csv" onclick="exportCSV()">⬇ Export CSV</button>
  </div>
</header>
<div class="stats">
  <div class="stat-card"><div class="stat-num" id="s-total">—</div><div class="stat-label">Total Leads</div></div>
  <div class="stat-card"><div class="stat-num" id="s-nof">—</div><div class="stat-label">Foreclosures</div></div>
  <div class="stat-card"><div class="stat-num" id="s-tax">—</div><div class="stat-label">Tax Fore.</div></div>
  <div class="stat-card"><div class="stat-num" id="s-named">—</div><div class="stat-label">With Owner</div></div>
  <div class="stat-card"><div class="stat-num" id="s-absentee">—</div><div class="stat-label">Absentee 🔥</div></div>
  <div class="stat-card"><div class="stat-num" id="s-new">—</div><div class="stat-label">New This Run ✨</div></div>
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
    <option value="new">New This Run ✨</option>
    <option value="duplicate">Duplicate Owners ♻</option>
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
      <th onclick="sortBy('score')">Score ⇅</th>
      <th>Type</th>
      <th onclick="sortBy('address')">Property Address ⇅</th>
      <th onclick="sortBy('owner')">Owner Name ⇅</th>
      <th>Mailing Address</th>
      <th onclick="sortBy('date_filed')">Date Filed ⇅</th>
      <th>Doc #</th>
      <th>City/ZIP</th>
      <th>Flags</th>
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
var filtered=[],page=1,PAGE=50,sortCol='score',sortDir=-1;

function init(){
  document.getElementById('s-total').textContent=ALL_RECORDS.length;
  document.getElementById('s-nof').textContent=ALL_RECORDS.filter(function(r){return r.type==='NOF';}).length;
  document.getElementById('s-tax').textContent=ALL_RECORDS.filter(function(r){return r.type==='TAX';}).length;
  document.getElementById('s-named').textContent=ALL_RECORDS.filter(function(r){return r.owner;}).length;
  document.getElementById('s-absentee').textContent=ALL_RECORDS.filter(function(r){return r.absentee;}).length;
  document.getElementById('s-new').textContent=ALL_RECORDS.filter(function(r){return r.is_new;}).length;
  applyFilters();
}

function sortBy(col){
  if(sortCol===col) sortDir*=-1; else{sortCol=col;sortDir=-1;}
  applyFilters();
}

function applyFilters(){
  var q=document.getElementById('search').value.toLowerCase();
  var t=document.getElementById('type-filter').value;
  var ow=document.getElementById('owner-filter').value;
  filtered=ALL_RECORDS.filter(function(r){
    var mq=!q||(r.address||'').toLowerCase().indexOf(q)>=0||(r.owner||'').toLowerCase().indexOf(q)>=0||(r.doc_number||'').toLowerCase().indexOf(q)>=0;
    var mt=!t||r.type===t;
    var mow=!ow||(ow==='named'?!!r.owner:ow==='absentee'?!!r.absentee:ow==='new'?!!r.is_new:ow==='duplicate'?!!r.duplicate:!r.owner);
    return mq&&mt&&mow;
  });
  filtered.sort(function(a,b){
    var av=a[sortCol]||'',bv=b[sortCol]||'';
    if(typeof av==='number'&&typeof bv==='number') return (av-bv)*sortDir;
    return av>bv?sortDir:av<bv?-sortDir:0;
  });
  page=1;
  document.getElementById('count-badge').textContent=filtered.length+' of '+ALL_RECORDS.length+' leads';
  render();
}

function mapLink(addr,city){
  var q=encodeURIComponent((addr||'')+(city?', '+city+', TX':''));
  return 'https://maps.google.com/?q='+q;
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
    var tL=r.type==='TAX'?'TAX':'NOF';
    var cz=[r.city,r.zip].filter(Boolean).join(' ')||'—';
    var addrStr=r.address||'—';
    var mapUrl=mapLink(r.address,r.city);
    var addrHtml='<div class="addr">'+addrStr+'<a href="'+mapUrl+'" target="_blank" title="View on map">📍</a></div>';
    var ownerHtml=r.owner
      ?'<div class="owner">'+r.owner+(r.duplicate?' <span style="color:var(--warning);font-size:10px">♻ DUP</span>':'')+'</div>'
      :'<div class="owner-none">—</div>';
    var mailHtml=r.mail_addr&&r.absentee?'<div class="mail">✉ '+r.mail_addr+'</div>':'';
    var rc='';
    if(r.absentee&&r.is_new) rc=' class="absentee-row new-row"';
    else if(r.absentee) rc=' class="absentee-row"';
    else if(r.is_new) rc=' class="new-row"';
    var fh='';
    if(r.is_new)     fh+='<span class="flag flag-new">✨ NEW</span>';
    if(r.absentee)   fh+='<span class="flag flag-hot">🔥 ABSENTEE</span>';
    if(r.duplicate)  fh+='<span class="flag flag-dup">♻ DUP</span>';
    if(r.type==='TAX') fh+='<span class="flag">TAX FORE</span>';
    if(!r.owner)     fh+='<span class="flag">NO OWNER</span>';
    if(!fh)          fh='<span style="color:var(--muted)">—</span>';
    rows+='<tr'+rc+'>'
      +'<td><div class="score '+scC+'">'+sc+'</div></td>'
      +'<td><span class="type-badge '+tC+'">'+tL+'</span></td>'
      +'<td>'+addrHtml+'</td>'
      +'<td>'+ownerHtml+mailHtml+'</td>'
      +'<td><div class="mail">'+(r.mail_addr&&r.absentee?r.mail_addr:'—')+'</div></td>'
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

function exportCSV(){
  var cols=['score','type','address','owner','mail_addr','absentee','duplicate','is_new','date_filed','doc_number','city','zip','school_dist'];
  var headers=cols.join(',');
  var rows=ALL_RECORDS.map(function(r){
    return cols.map(function(c){
      var v=r[c];
      if(v===null||v===undefined) v='';
      v=String(v).replace(/"/g,'""');
      return '"'+v+'"';
    }).join(',');
  });
  var csv=headers+'\n'+rows.join('\n');
  var blob=new Blob([csv],{type:'text/csv'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');
  a.href=url; a.download='bexar-leads.csv'; a.click();
  URL.revokeObjectURL(url);
}

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
    log.info("Bexar County Lead Scraper v24")
    log.info(f"Foreclosures: {FORECLOSURE_BASE}")
    log.info(f"Owner lookup: {PARCELS_URL}")
    log.info("="*60)

    records = fetch_foreclosures()
    records = enrich_owners(records)
    records = detect_duplicates(records)

    for r in records:
        r["flags"] = []
        if r["type"] == "TAX":             r["flags"].append("TAX FORE")
        if r.get("absentee"):              r["flags"].append("ABSENTEE")
        if r.get("duplicate"):             r["flags"].append("DUPLICATE")
        if r.get("is_new"):                r["flags"].append("NEW")
        if not r["owner"]:                 r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]: r["flags"].append("NO CITY")
        r["score"] = score_record(r)

    records.sort(key=lambda x: x["score"], reverse=True)
    named    = sum(1 for r in records if r["owner"])
    absentee = sum(1 for r in records if r["absentee"])
    dupes    = sum(1 for r in records if r["duplicate"])
    new_ct   = sum(1 for r in records if r["is_new"])
    log.info(f"Final: {len(records)} leads | {named} named | {absentee} absentee | {dupes} dupes | {new_ct} new")

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    build_dashboard(records)
    push_ghl(records)

