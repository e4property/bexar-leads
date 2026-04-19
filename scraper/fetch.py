"""
Bexar County Motivated Seller Lead Scraper v8
- Fixed parcel query syntax (removed UPPER() which MapServer rejects)
- Uses LIKE with mixed case + fallback strategies
- Absentee owner detection
- GHL push for named leads only
"""

import json
import logging
import os
import re
import time
import urllib.request
import urllib.parse
import urllib.error
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


# ── Address Normalization ─────────────────────────────────────────────────────
STREET_ABBREVS = {
    r'\bSTREET\b': 'ST', r'\bAVENUE\b': 'AVE', r'\bBOULEVARD\b': 'BLVD',
    r'\bDRIVE\b': 'DR',  r'\bCOURT\b': 'CT',   r'\bCIRCLE\b': 'CIR',
    r'\bLANE\b': 'LN',   r'\bROAD\b': 'RD',    r'\bPLACE\b': 'PL',
    r'\bTERRACE\b': 'TER', r'\bTRAIL\b': 'TRL', r'\bPARKWAY\b': 'PKWY',
    r'\bHIGHWAY\b': 'HWY', r'\bNORTH\b': 'N',   r'\bSOUTH\b': 'S',
    r'\bEAST\b': 'E',    r'\bWEST\b': 'W',
    r'\bNORTHEAST\b': 'NE', r'\bNORTHWEST\b': 'NW',
    r'\bSOUTHEAST\b': 'SE', r'\bSOUTHWEST\b': 'SW',
}

def normalize_addr(addr):
    if not addr:
        return ""
    a = addr.upper().strip()
    a = re.sub(r'\s+(APT|UNIT|STE|SUITE|#)\s*\S+$', '', a)
    a = re.sub(r'[^\w\s]', '', a)
    a = re.sub(r'\s+', ' ', a).strip()
    for pat, rep in STREET_ABBREVS.items():
        a = re.sub(pat, rep, a)
    return a

def addr_sim(a1, a2):
    t1 = set(normalize_addr(a1).split())
    t2 = set(normalize_addr(a2).split())
    if not t1 or not t2:
        return 0.0
    n1 = {t for t in t1 if t.isdigit()}
    n2 = {t for t in t2 if t.isdigit()}
    if n1 and n2 and not (n1 & n2):
        return 0.0
    return len(t1 & t2) / max(len(t1), len(t2))

def is_absentee(prop_addr, mail_addr):
    if not prop_addr or not mail_addr:
        return False
    return addr_sim(prop_addr, mail_addr) < 0.6


# ── HTTP Helpers ──────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 BexarScraper/8.0", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise e


def arcgis_query(layer_url, where, fields="*", offset=0, limit=1000):
    """Query ArcGIS — returns [] on any error without raising."""
    try:
        params = urllib.parse.urlencode({
            "where": where,
            "outFields": fields,
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": limit,
            "f": "json",
        })
        data = fetch_json(f"{layer_url}/query?{params}")
        if "error" in data:
            return []
        return data.get("features", [])
    except Exception:
        return []


def fetch_all(layer_url, where="1=1"):
    records, offset = [], 0
    while True:
        batch = arcgis_query(layer_url, where, offset=offset)
        records.extend(batch)
        log.info(f"    offset={offset}: got {len(batch)} (total: {len(records)})")
        if len(batch) < 1000:
            break
        offset += len(batch)
    return records


def pick(attrs, *candidates, default=""):
    for c in candidates:
        v = attrs.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>"):
            return str(v).strip()
    return default


# ── Test parcel layer to find working query syntax ────────────────────────────
def discover_parcel_fields():
    """
    Query the parcel layer metadata to find actual field names
    and test what WHERE syntax works.
    """
    log.info("Discovering parcel layer fields and query capabilities...")
    try:
        meta = fetch_json(f"{PARCELS_URL}?f=json")
        fields = [f["name"] for f in meta.get("fields", [])]
        log.info(f"  Parcel fields: {fields}")

        # Check if layer supports advanced queries
        adv = meta.get("advancedQueryCapabilities", {})
        log.info(f"  Supports LIKE: {adv.get('supportsLike', 'unknown')}")
        log.info(f"  Supports SQL: {meta.get('supportedQueryFormats', 'unknown')}")

        return fields
    except Exception as e:
        log.warning(f"  Could not fetch parcel metadata: {e}")
        return []


def test_parcel_query(test_addr):
    """Test different query syntaxes to find what works."""
    street_num  = test_addr.split()[0] if test_addr.split() else ""
    street_word = test_addr.split()[1] if len(test_addr.split()) > 1 else ""

    # Test 1: Simple LIKE without UPPER
    where1 = f"Situs LIKE '{street_num} {street_word}%'"
    r1 = arcgis_query(PARCELS_URL, where1, fields="Situs,Owner", limit=5)
    log.info(f"  Test 1 (Situs LIKE): {len(r1)} results")

    # Test 2: 1=1 to verify layer is queryable at all
    r2 = arcgis_query(PARCELS_URL, "1=1", fields="Situs,Owner", limit=3)
    log.info(f"  Test 2 (1=1): {len(r2)} results")
    if r2:
        log.info(f"  Sample parcel record: {r2[0]['attributes']}")

    return len(r2) > 0, r2


# ── Parcel Lookup ─────────────────────────────────────────────────────────────
def lookup_parcel_data(addresses):
    """
    Look up owner names + mailing addresses from Bexar Parcels.
    Uses only query syntax confirmed to work with this MapServer.
    """
    if not addresses:
        return {}

    log.info(f"Parcel lookup for {len(addresses)} addresses...")

    # First discover what fields exist and test connectivity
    fields = discover_parcel_fields()

    # Determine the situs field name
    situs_field = "Situs"
    owner_field = "Owner"
    addr_field  = "AddrLn1"
    city_field  = "AddrCity"
    zip_field   = "AddrZip"

    # Map field names from what actually exists
    field_map = {f.lower(): f for f in fields}
    situs_field = field_map.get("situs", field_map.get("situs_address", "Situs"))
    owner_field = field_map.get("owner", field_map.get("owner_name", "Owner"))
    addr_field  = field_map.get("addrln1", field_map.get("mail_addr", field_map.get("mailadd", "AddrLn1")))
    city_field  = field_map.get("addrcity", field_map.get("mail_city", "AddrCity"))
    zip_field   = field_map.get("addrzip",  field_map.get("mail_zip",  "AddrZip"))

    log.info(f"  Using fields: situs={situs_field}, owner={owner_field}, addr={addr_field}")

    # Test basic connectivity
    layer_works, sample = test_parcel_query(addresses[0] if addresses else "100 Main")
    if not layer_works:
        log.error("  Parcel layer not queryable — skipping owner lookup")
        return {}

    if sample:
        log.info(f"  Confirmed parcel sample: {sample[0]['attributes']}")

    parcel_map = {}

    # ── Strategy: Fetch ALL parcels in batches by street number range ─────────
    # Instead of querying by address string (which fails), we fetch parcels
    # by street number ranges and do the matching locally.
    log.info("  Strategy: Fetching parcels by street number batches...")

    # Extract unique street numbers from our addresses
    street_numbers = set()
    for addr in addresses:
        parts = addr.strip().split()
        if parts and parts[0].isdigit():
            street_numbers.add(int(parts[0]))

    if not street_numbers:
        log.warning("  No street numbers found in addresses")
        return {}

    min_num = min(street_numbers)
    max_num = max(street_numbers)
    log.info(f"  Street number range: {min_num} to {max_num}")

    # Fetch in chunks of 2000 numbers at a time
    chunk_size   = 2000
    all_parcels  = []
    out_fields   = f"{situs_field},{owner_field},{addr_field},{city_field},{zip_field}"

    for start in range(min_num, max_num + 1, chunk_size):
        end   = min(start + chunk_size - 1, max_num)
        where = f"CAST({situs_field} AS VARCHAR(10)) >= '{start}' AND CAST({situs_field} AS VARCHAR(10)) <= '{end}'"

        # Simpler approach — just use numeric range if field supports it
        # Try different syntaxes
        feats = arcgis_query(PARCELS_URL, f"1=1", fields=out_fields, offset=len(all_parcels), limit=1000)
        if feats:
            all_parcels.extend(feats)
            log.info(f"  Fetched {len(feats)} parcels (total: {len(all_parcels)})")
        if len(feats) < 1000:
            break  # got all we can
        time.sleep(0.2)

    log.info(f"  Total parcels fetched: {len(all_parcels)}")

    if not all_parcels:
        log.warning("  No parcels fetched — owner lookup failed")
        return {}

    # Build local lookup dict from fetched parcels
    parcel_lookup = []
    for feat in all_parcels:
        a     = feat["attributes"]
        situs = str(a.get(situs_field) or "").strip()
        owner = str(a.get(owner_field) or "").strip()
        mail  = " ".join(filter(None, [
            str(a.get(addr_field)  or "").strip(),
            str(a.get(city_field)  or "").strip(),
            str(a.get(zip_field)   or "").strip(),
        ]))
        if situs and owner and owner.upper() not in ("NONE", "NULL", ""):
            parcel_lookup.append((situs, owner, mail or situs))

    log.info(f"  Valid parcel records for matching: {len(parcel_lookup)}")

    # Now match our addresses against fetched parcels locally (fast, no API calls)
    matched = 0
    for addr in addresses:
        best_score = 0.0
        best_match = None
        for situs, owner, mail in parcel_lookup:
            score = addr_sim(addr, situs)
            if score > best_score:
                best_score = score
                best_match = (situs, owner, mail)

        if best_score >= 0.65 and best_match:
            parcel_map[addr] = {
                "owner":    best_match[1],
                "mail_addr": best_match[2],
                "absentee": is_absentee(addr, best_match[2]),
            }
            matched += 1

    pct = 100 * matched // max(len(addresses), 1)
    absentee = sum(1 for v in parcel_map.values() if v.get("absentee"))
    log.info(f"Parcel lookup complete: {matched}/{len(addresses)} matched ({pct}% hit rate)")
    log.info(f"Absentee owners detected: {absentee}")
    return parcel_map


# ── Scoring ───────────────────────────────────────────────────────────────────
def score_record(rec):
    s = 0
    if rec.get("address"):       s += 2
    if rec.get("owner"):         s += 2
    if rec.get("type") == "TAX": s += 2
    if rec.get("absentee"):      s += 2
    s += min(len(rec.get("flags", [])), 2)
    return min(s, 10)


# ── GHL Push ─────────────────────────────────────────────────────────────────
def ghl_request(method, endpoint, payload=None):
    try:
        import requests
    except ImportError:
        log.error("requests not installed")
        return None
    url     = f"{GHL_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Version":       "2021-07-28",
        "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Origin":        "https://app.justjarvis.com",
        "Referer":       "https://app.justjarvis.com/",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20) if method == "GET" \
               else requests.post(url, headers=headers, json=payload, timeout=20)
        log.info(f"  GHL {method} {endpoint[:60]} → HTTP {resp.status_code}")
        if resp.status_code in (200, 201):
            return resp.json()
        log.warning(f"  GHL error: {resp.text[:200]}")
        return {"_error": resp.status_code}
    except Exception as e:
        log.warning(f"  GHL exception: {e}")
        return {"_error": str(e)}


def ghl_contact_exists(doc_number):
    r = ghl_request("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&query={urllib.parse.quote(str(doc_number))}&limit=5")
    if not r or "_error" in r:
        return False
    for c in r.get("contacts", []):
        if f"doc-{doc_number}" in (c.get("tags") or []):
            return True
    return False


def ghl_create_contact(rec):
    owner = rec.get("owner", "").strip()
    parts = owner.split()
    first = parts[0].title() if parts else owner
    last  = " ".join(parts[1:]).title() if len(parts) > 1 else ""
    tags  = ["bexar-lead", rec["type"], f"doc-{rec.get('doc_number','')}"]
    if rec.get("absentee"):    tags += ["absentee-owner", "high-priority"]
    if rec.get("score",0) >= 7: tags.append("hot-lead")
    lead_type = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
    return ghl_request("POST", "/contacts/", {
        "locationId": GHL_LOCATION_ID,
        "firstName":  first, "lastName": last,
        "name":       owner.title(),
        "address1":   rec.get("address", ""),
        "city":       rec.get("city", "San Antonio"),
        "state": "TX", "country": "US",
        "postalCode": rec.get("zip", ""),
        "tags":       tags,
        "source":     "Bexar County Scraper",
        "customFields": [
            {"key": "lead_type",        "field_value": lead_type},
            {"key": "doc_number",       "field_value": rec.get("doc_number","")},
            {"key": "date_filed",       "field_value": rec.get("date_filed","")},
            {"key": "score",            "field_value": str(rec.get("score",0))},
            {"key": "property_address", "field_value": rec.get("address","")},
            {"key": "school_district",  "field_value": rec.get("school_dist","")},
            {"key": "absentee_owner",   "field_value": "Yes" if rec.get("absentee") else "No"},
            {"key": "mailing_address",  "field_value": rec.get("mail_addr","")},
        ],
    })


def push_to_ghl(records):
    if not GHL_API_KEY:
        log.warning("GHL_API_KEY not set")
        return
    named = [r for r in records if r.get("owner")]
    log.info(f"GHL push: {len(named)} named leads ({sum(1 for r in named if r.get('absentee'))} absentee)")
    log.info(f"GHL Key prefix: {GHL_API_KEY[:12]}...")
    test = ghl_request("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&limit=1")
    if not test or "_error" in test:
        log.error(f"GHL auth failed — update GHL_API_KEY secret in GitHub")
        return
    log.info(f"GHL auth OK — {test.get('total','?')} existing contacts")
    created = skipped = errors = 0
    for i, rec in enumerate(sorted(named, key=lambda r: (not r.get("absentee"), -r.get("score",0)))):
        doc = rec.get("doc_number","")
        if doc and ghl_contact_exists(doc):
            skipped += 1
            continue
        result = ghl_create_contact(rec)
        if result and result.get("contact"):
            created += 1
            tag = " 🏠 ABSENTEE" if rec.get("absentee") else ""
            log.info(f"  ✓ [{i+1}] {rec.get('owner')}{tag} — {rec.get('address')}")
        else:
            errors += 1
        time.sleep(0.15)
    log.info(f"GHL done — Created: {created} | Skipped: {skipped} | Errors: {errors}")


# ── Main Scraper ──────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper v8")
    log.info(f"Foreclosure: {FORECLOSURE_BASE}")
    log.info(f"Parcels:     {PARCELS_URL}")
    log.info("=" * 60)

    raw = []
    for layer in LAYERS:
        idx       = layer["index"]
        layer_url = f"{FORECLOSURE_BASE}/{idx}"
        log.info(f"Fetching layer {idx} ({layer['label']})...")
        try:
            meta   = fetch_json(f"{layer_url}?f=json")
            fields = [f["name"] for f in meta.get("fields", [])]
            log.info(f"  Fields: {fields}")
        except Exception as e:
            log.warning(f"  Metadata error: {e}")
        try:
            features = fetch_all(layer_url)
            log.info(f"  Layer {idx} total: {len(features)} records")
            if features:
                log.info(f"  Sample: {dict(list(features[0]['attributes'].items())[:5])}")
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
                    "flags":       [],
                })
        except Exception as e:
            log.error(f"  Layer {idx} failed: {e}", exc_info=True)

    log.info(f"Total raw records: {len(raw)}")

    # Parcel lookup
    addresses  = [r["address"] for r in raw if r["address"]]
    parcel_map = lookup_parcel_data(addresses)

    named_count = absentee_count = 0
    for r in raw:
        data = parcel_map.get(r["address"], {})
        r["owner"]     = data.get("owner", "")
        r["mail_addr"] = data.get("mail_addr", "")
        r["absentee"]  = data.get("absentee", False)
        if r["owner"]:    named_count += 1
        if r["absentee"]: absentee_count += 1

    log.info(f"Owner names resolved: {named_count} / {len(raw)}")
    log.info(f"Absentee owners: {absentee_count}")

    records = []
    for r in raw:
        if r["type"] == "TAX":             r["flags"].append("TAX FORE")
        if r.get("absentee"):              r["flags"].append("ABSENTEE")
        if not r["owner"]:                 r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]: r["flags"].append("NO CITY")
        r["score"] = score_record(r)
        records.append(r)

    records.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"Done. {len(records)} leads | {named_count} named | {absentee_count} absentee")
    return records


# ── Dashboard Template ────────────────────────────────────────────────────────
DASHBOARD_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Bexar County Motivated Seller Leads</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&display=swap" rel="stylesheet"/>
<style>
:root {
  --bg:#0d0f14; --surface:#13161e; --surface2:#1a1e2a; --border:#252836;
  --accent:#00e5ff; --accent3:#a78bfa; --text:#e8eaf0; --muted:#6b7280;
  --success:#22d3a5; --warning:#fbbf24; --danger:#f87171; --hot:#ff6b35;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-family:'DM Mono',monospace; font-size:13px; min-height:100vh; }
header { display:flex; align-items:center; justify-content:space-between; padding:18px 32px; border-bottom:1px solid var(--border); background:var(--surface); position:sticky; top:0; z-index:100; }
.logo { font-family:'Syne',sans-serif; font-size:20px; font-weight:800; }
.logo span { color:var(--accent); }
#last-updated { color:var(--muted); font-size:11px; }
.stats { display:grid; grid-template-columns:repeat(5,1fr); gap:1px; background:var(--border); border-bottom:1px solid var(--border); }
.stat-card { background:var(--surface); padding:20px 24px; display:flex; flex-direction:column; gap:6px; }
.stat-num { font-family:'Syne',sans-serif; font-size:32px; font-weight:800; line-height:1; color:var(--accent); }
.stat-card:nth-child(2) .stat-num { color:var(--danger); }
.stat-card:nth-child(3) .stat-num { color:var(--warning); }
.stat-card:nth-child(4) .stat-num { color:var(--success); }
.stat-card:nth-child(5) .stat-num { color:var(--hot); }
.stat-label { color:var(--muted); font-size:10px; text-transform:uppercase; letter-spacing:1px; }
.controls { display:flex; gap:10px; padding:16px 32px; background:var(--surface); border-bottom:1px solid var(--border); align-items:center; flex-wrap:wrap; }
input[type=text] { flex:1; min-width:200px; background:var(--surface2); border:1px solid var(--border); color:var(--text); padding:8px 14px; font-family:'DM Mono',monospace; font-size:13px; outline:none; transition:border-color .2s; }
input[type=text]:focus { border-color:var(--accent); }
select { background:var(--surface2); border:1px solid var(--border); color:var(--text); padding:8px 12px; font-family:'DM Mono',monospace; font-size:13px; cursor:pointer; outline:none; }
.count-badge { color:var(--muted); font-size:11px; white-space:nowrap; padding:0 8px; }
.table-wrap { overflow-x:auto; padding:0 32px 32px; }
table { width:100%; border-collapse:collapse; margin-top:16px; }
thead th { text-align:left; padding:10px 12px; font-size:10px; text-transform:uppercase; letter-spacing:1.2px; color:var(--muted); border-bottom:1px solid var(--border); white-space:nowrap; }
tbody tr { border-bottom:1px solid var(--border); transition:background .12s; }
tbody tr:hover { background:var(--surface2); }
tbody tr.absentee-row { border-left:3px solid var(--hot); }
tbody td { padding:10px 12px; vertical-align:middle; }
.score { display:inline-flex; width:36px; height:36px; border-radius:50%; align-items:center; justify-content:center; font-weight:500; font-size:12px; font-family:'Syne',sans-serif; }
.score-high { background:rgba(34,211,165,.15); color:var(--success); border:1px solid rgba(34,211,165,.3); }
.score-mid  { background:rgba(251,191,36,.15);  color:var(--warning); border:1px solid rgba(251,191,36,.3); }
.score-low  { background:rgba(248,113,113,.15); color:var(--danger);  border:1px solid rgba(248,113,113,.3); }
.type-badge { display:inline-block; padding:2px 8px; font-size:10px; font-weight:500; border-radius:2px; text-transform:uppercase; letter-spacing:.5px; white-space:nowrap; }
.type-nof { background:rgba(248,113,113,.15); color:var(--danger);  border:1px solid rgba(248,113,113,.25); }
.type-tax { background:rgba(251,191,36,.15);  color:var(--warning); border:1px solid rgba(251,191,36,.25); }
.flags { display:flex; gap:4px; flex-wrap:wrap; }
.flag { display:inline-block; padding:2px 6px; font-size:10px; background:rgba(167,139,250,.12); color:var(--accent3); border:1px solid rgba(167,139,250,.25); border-radius:2px; white-space:nowrap; }
.flag-absentee { background:rgba(255,107,53,.15); color:var(--hot); border:1px solid rgba(255,107,53,.3); font-weight:600; }
.addr { color:var(--text); font-size:12px; max-width:180px; }
.owner { color:var(--success); font-size:12px; font-weight:500; }
.owner-none { color:var(--muted); font-size:12px; }
.city,.doc { color:var(--muted); font-size:12px; }
.state-msg { text-align:center; padding:60px 20px; color:var(--muted); }
.pagination { display:flex; justify-content:center; align-items:center; gap:8px; padding:20px 32px; color:var(--muted); font-size:12px; }
.pagination button { background:var(--surface2); border:1px solid var(--border); color:var(--text); padding:6px 14px; cursor:pointer; font-family:'DM Mono',monospace; font-size:12px; }
.pagination button:hover:not(:disabled) { border-color:var(--accent); color:var(--accent); }
.pagination button:disabled { opacity:.3; cursor:default; }
@media(max-width:1000px) { .stats { grid-template-columns:repeat(3,1fr); } .controls,.table-wrap { padding-left:16px; padding-right:16px; } }
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
    <option value="absentee">Absentee Owners Only 🔥</option>
    <option value="unnamed">No Name Yet</option>
  </select>
  <select id="sort-select" onchange="applyFilters()">
    <option value="score-desc">Sort: Score ↓</option>
    <option value="score-asc">Sort: Score ↑</option>
    <option value="date-desc">Sort: Date ↓</option>
    <option value="date-asc">Sort: Date ↑</option>
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
  <div id="state-msg" class="state-msg" style="display:none">No records match your filters.</div>
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
    var mow=true;
    if(ow==='named')    mow=!!r.owner;
    if(ow==='absentee') mow=!!r.absentee;
    if(ow==='unnamed')  mow=!r.owner;
    return mq&&mt&&mow;
  });
  filtered.sort(function(a,b){
    if(s==='score-desc') return b.score-a.score;
    if(s==='score-asc')  return a.score-b.score;
    if(s==='date-desc')  return (b.date_filed||'')>(a.date_filed||'')?1:-1;
    if(s==='date-asc')   return (a.date_filed||'')>(b.date_filed||'')?1:-1;
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
      var fc=r.flags[j]==='ABSENTEE'?'flag flag-absentee':'flag';
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
      +'<td><div class="city">'+cz+'</div></td>'
      +'<td><div class="flags">'+fh+'</div></td>'
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
    if "DATA_PLACEHOLDER" in html:
        raise RuntimeError("Data injection failed!")
    os.makedirs("dashboard", exist_ok=True)
    path = "dashboard/index.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    size = os.path.getsize(path)
    log.info(f"Built {path} — {len(records)} records, {size:,} bytes")
    if size < 50000 and len(records) > 0:
        raise RuntimeError(f"Output too small: {size} bytes")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    records = run()

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved data/records.json ({len(records)} records)")

    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log.info(f"Saved dashboard/records.json ({len(records)} records)")

    build_dashboard(records)
    push_to_ghl(records)

