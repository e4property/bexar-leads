"""
Bexar County Motivated Seller Lead Scraper v7
- Pulls foreclosure data from maps.bexar.org
- Fuzzy owner name lookup from Bexar Parcels
- Absentee owner detection (mailing address != property address)
- Only pushes named leads to GoHighLevel / Jarvis
- Tags absentee owners as high-priority
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

# ── Sources ───────────────────────────────────────────────────────────────────
FORECLOSURE_BASE = "https://maps.bexar.org/arcgis/rest/services/CC/ForeclosuresProd/MapServer"
PARCELS_URL      = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"

LAYERS = [
    {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
    {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
]

# ── GHL Config ────────────────────────────────────────────────────────────────
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
    r'\bEAST\b': 'E',    r'\bWEST\b': 'W',      r'\bNORTHEAST\b': 'NE',
    r'\bNORTHWEST\b': 'NW', r'\bSOUTHEAST\b': 'SE', r'\bSOUTHWEST\b': 'SW',
}

def normalize_address(addr):
    if not addr:
        return ""
    a = addr.upper().strip()
    a = re.sub(r'\s+(APT|UNIT|STE|SUITE|#)\s*\S+$', '', a)
    a = re.sub(r'[^\w\s]', '', a)
    a = re.sub(r'\s+', ' ', a).strip()
    for pattern, replacement in STREET_ABBREVS.items():
        a = re.sub(pattern, replacement, a)
    return a


def address_similarity(a1, a2):
    t1 = set(normalize_address(a1).split())
    t2 = set(normalize_address(a2).split())
    if not t1 or not t2:
        return 0.0
    nums1 = {t for t in t1 if t.isdigit()}
    nums2 = {t for t in t2 if t.isdigit()}
    if nums1 and nums2 and not (nums1 & nums2):
        return 0.0
    return len(t1 & t2) / max(len(t1), len(t2))


def street_key(addr):
    norm   = normalize_address(addr)
    tokens = norm.split()
    if tokens and tokens[0].isdigit():
        tokens = tokens[1:]
    return " ".join(tokens[:3])


def is_absentee(prop_addr, mail_addr):
    """
    Returns True if the mailing address is different from the property address.
    This means the owner doesn't live at the property = absentee/investor/landlord.
    More motivated to sell.
    """
    if not prop_addr or not mail_addr:
        return False
    n_prop = normalize_address(prop_addr)
    n_mail = normalize_address(mail_addr)
    if not n_prop or not n_mail:
        return False
    # If similarity is low they live elsewhere
    sim = address_similarity(n_prop, n_mail)
    return sim < 0.6


# ── HTTP Helpers ──────────────────────────────────────────────────────────────
def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 BexarScraper/7.0", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise e


def arcgis_query(layer_url, where, fields="*", offset=0, limit=1000):
    params = urllib.parse.urlencode({
        "where": where, "outFields": fields,
        "returnGeometry": "false",
        "resultOffset": offset,
        "resultRecordCount": limit,
        "f": "json",
    })
    data = fetch_json(f"{layer_url}/query?{params}")
    if "error" in data:
        log.warning(f"ArcGIS error: {data['error']}")
        return []
    return data.get("features", [])


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


# ── Parcel Lookup (owner name + mailing address) ──────────────────────────────
def lookup_parcel_data(addresses):
    """
    Returns dict: { "ORIGINAL ADDRESS": {"owner": str, "mail_addr": str, "absentee": bool} }
    Uses 3-strategy fuzzy matching for highest hit rate.
    """
    if not addresses:
        return {}

    log.info(f"Parcel lookup for {len(addresses)} addresses (owner + mailing address)...")
    parcel_map = {}

    def store_match(orig_addr, situs, owner, mail_addr):
        parcel_map[orig_addr] = {
            "owner":     owner,
            "mail_addr": mail_addr,
            "absentee":  is_absentee(situs, mail_addr),
        }

    # ── Strategy 1: Exact normalized batch ───────────────────────────────────
    log.info("  Strategy 1: Exact match...")
    batch_size = 50
    for i in range(0, len(addresses), batch_size):
        batch   = addresses[i:i + batch_size]
        escaped = [normalize_address(a).replace("'", "''") for a in batch]
        quoted  = ", ".join(f"'{a}'" for a in escaped)
        where   = f"UPPER(Situs) IN ({quoted})"
        try:
            feats = arcgis_query(
                PARCELS_URL, where,
                fields="Situs,Owner,AddrLn1,AddrLn2,AddrCity,AddrState,AddrZip",
                limit=500
            )
            for f in feats:
                a      = f["attributes"]
                situs  = (a.get("Situs") or "").strip()
                owner  = (a.get("Owner") or "").strip()
                # Build mailing address from components
                mail   = " ".join(filter(None, [
                    (a.get("AddrLn1") or "").strip(),
                    (a.get("AddrLn2") or "").strip(),
                    (a.get("AddrCity") or "").strip(),
                    (a.get("AddrState") or "").strip(),
                    str(a.get("AddrZip") or "").strip(),
                ]))
                if not mail:
                    mail = situs  # fallback
                norm_situs = normalize_address(situs)
                for orig in batch:
                    if orig not in parcel_map and normalize_address(orig) == norm_situs:
                        store_match(orig, situs, owner, mail)
            time.sleep(0.15)
        except Exception as e:
            log.warning(f"  Strategy 1 batch {i//batch_size+1} error: {e}")

    s1_count = len(parcel_map)
    log.info(f"  Strategy 1: {s1_count} matches")

    # ── Strategy 2: Street-name fuzzy ────────────────────────────────────────
    remaining = [a for a in addresses if a not in parcel_map]
    log.info(f"  Strategy 2: Fuzzy street search for {len(remaining)} remaining...")

    street_groups = {}
    for addr in remaining:
        key = street_key(addr)
        if key:
            street_groups.setdefault(key, []).append(addr)

    for sk, group_addrs in street_groups.items():
        search_word = next((t for t in sk.split() if len(t) > 2 and not t.isdigit()), None)
        if not search_word:
            continue
        where = f"UPPER(Situs) LIKE '%{search_word}%'"
        try:
            feats = arcgis_query(
                PARCELS_URL, where,
                fields="Situs,Owner,AddrLn1,AddrLn2,AddrCity,AddrState,AddrZip",
                limit=200
            )
            parcel_lookup = []
            for f in feats:
                a     = f["attributes"]
                situs = (a.get("Situs") or "").strip()
                owner = (a.get("Owner") or "").strip()
                mail  = " ".join(filter(None, [
                    (a.get("AddrLn1") or "").strip(),
                    (a.get("AddrCity") or "").strip(),
                    (a.get("AddrState") or "").strip(),
                    str(a.get("AddrZip") or "").strip(),
                ]))
                if situs:
                    parcel_lookup.append((situs, owner, mail or situs))

            for addr in group_addrs:
                if addr in parcel_map:
                    continue
                best_score = 0.0
                best_match = None
                for situs, owner, mail in parcel_lookup:
                    score = address_similarity(addr, situs)
                    if score > best_score:
                        best_score = score
                        best_match = (situs, owner, mail)
                if best_score >= 0.7 and best_match:
                    store_match(addr, best_match[0], best_match[1], best_match[2])
                    log.info(f"    Fuzzy ({best_score:.2f}): '{addr}' → '{best_match[1]}'")
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"  Strategy 2 error for '{sk}': {e}")

    s2_count = len(parcel_map) - s1_count
    log.info(f"  Strategy 2: {s2_count} additional matches")

    # ── Strategy 3: Number + street word ─────────────────────────────────────
    remaining2 = [a for a in addresses if a not in parcel_map]
    log.info(f"  Strategy 3: Number+word search for {len(remaining2)} remaining...")
    s3_count = 0

    for addr in remaining2:
        norm   = normalize_address(addr)
        tokens = norm.split()
        num    = tokens[0] if tokens and tokens[0].isdigit() else ""
        word   = next((t for t in tokens[1:] if len(t) > 2), "")
        if not num or not word:
            continue
        where = f"UPPER(Situs) LIKE '{num} %{word}%'"
        try:
            feats = arcgis_query(
                PARCELS_URL, where,
                fields="Situs,Owner,AddrLn1,AddrCity,AddrState,AddrZip",
                limit=20
            )
            best_score = 0.0
            best_match = None
            for f in feats:
                a     = f["attributes"]
                situs = (a.get("Situs") or "").strip()
                owner = (a.get("Owner") or "").strip()
                mail  = " ".join(filter(None, [
                    (a.get("AddrLn1") or "").strip(),
                    (a.get("AddrCity") or "").strip(),
                    str(a.get("AddrZip") or "").strip(),
                ]))
                score = address_similarity(addr, situs)
                if score > best_score:
                    best_score = score
                    best_match = (situs, owner, mail or situs)
            if best_score >= 0.6 and best_match:
                store_match(addr, best_match[0], best_match[1], best_match[2])
                s3_count += 1
            time.sleep(0.1)
        except Exception:
            pass

    log.info(f"  Strategy 3: {s3_count} additional matches")

    total = len(parcel_map)
    pct   = 100 * total // max(len(addresses), 1)
    log.info(f"Parcel lookup complete: {total}/{len(addresses)} matched ({pct}% hit rate)")
    absentee_count = sum(1 for v in parcel_map.values() if v.get("absentee"))
    log.info(f"Absentee owners detected: {absentee_count}")
    return parcel_map


# ── Scoring ───────────────────────────────────────────────────────────────────
def score_record(rec):
    s = 0
    if rec.get("address"):          s += 2
    if rec.get("owner"):            s += 2
    if rec.get("type") == "TAX":    s += 2
    if rec.get("absentee"):         s += 2  # absentee = more motivated
    if "ABSENTEE" in rec.get("flags", []): s += 1
    return min(s, 10)


# ── GHL Push ─────────────────────────────────────────────────────────────────
def ghl_request(method, endpoint, payload=None):
    try:
        import requests
    except ImportError:
        log.error("requests library not installed")
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
        if method == "GET":
            resp = requests.get(url, headers=headers, timeout=20)
        else:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
        log.info(f"  GHL {method} {endpoint[:60]} → HTTP {resp.status_code}")
        if resp.status_code in (200, 201):
            return resp.json()
        log.warning(f"  GHL error: {resp.text[:300]}")
        return {"_error": resp.status_code, "_body": resp.text[:300]}
    except Exception as e:
        log.warning(f"  GHL exception: {e}")
        return {"_error": str(e)}


def ghl_contact_exists(doc_number):
    result   = ghl_request("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&query={urllib.parse.quote(str(doc_number))}&limit=5")
    if not result or "_error" in result:
        return False
    for c in result.get("contacts", []):
        if f"doc-{doc_number}" in (c.get("tags") or []):
            return True
    return False


def ghl_create_contact(rec):
    owner     = rec.get("owner", "").strip()
    lead_type = "Tax Foreclosure" if rec["type"] == "TAX" else "Mortgage Foreclosure"
    parts     = owner.split()
    first     = parts[0].title() if parts else owner
    last      = " ".join(parts[1:]).title() if len(parts) > 1 else ""

    tags = ["bexar-lead", rec["type"], f"doc-{rec.get('doc_number', '')}"]
    if rec.get("absentee"):
        tags.append("absentee-owner")
        tags.append("high-priority")
    if rec.get("score", 0) >= 7:
        tags.append("hot-lead")

    return ghl_request("POST", "/contacts/", {
        "locationId": GHL_LOCATION_ID,
        "firstName":  first,
        "lastName":   last,
        "name":       owner.title(),
        "address1":   rec.get("address", ""),
        "city":       rec.get("city", "San Antonio"),
        "state":      "TX",
        "country":    "US",
        "postalCode": rec.get("zip", ""),
        "tags":       tags,
        "source":     "Bexar County Scraper",
        "customFields": [
            {"key": "lead_type",        "field_value": lead_type},
            {"key": "doc_number",       "field_value": rec.get("doc_number", "")},
            {"key": "date_filed",       "field_value": rec.get("date_filed", "")},
            {"key": "score",            "field_value": str(rec.get("score", 0))},
            {"key": "property_address", "field_value": rec.get("address", "")},
            {"key": "school_district",  "field_value": rec.get("school_dist", "")},
            {"key": "absentee_owner",   "field_value": "Yes" if rec.get("absentee") else "No"},
            {"key": "mailing_address",  "field_value": rec.get("mail_addr", "")},
        ],
    })


def push_to_ghl(records):
    if not GHL_API_KEY:
        log.warning("GHL_API_KEY not set — skipping GHL push")
        return

    named = [r for r in records if r.get("owner")]
    log.info(f"GHL push: {len(named)} named leads ({sum(1 for r in named if r.get('absentee'))} absentee)")
    log.info(f"GHL Key prefix: {GHL_API_KEY[:12]}...")

    test = ghl_request("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&limit=1")
    if not test or "_error" in test:
        log.error(f"GHL auth failed: {test}")
        log.error("→ Update GHL_API_KEY secret in GitHub with fresh token from Jarvis")
        return
    log.info(f"GHL auth OK — {test.get('total','?')} existing contacts")

    created = skipped = errors = 0
    # Push absentee owners first (highest priority)
    sorted_leads = sorted(named, key=lambda r: (not r.get("absentee"), -r.get("score", 0)))

    for i, rec in enumerate(sorted_leads):
        doc = rec.get("doc_number", "")
        if doc and ghl_contact_exists(doc):
            skipped += 1
            continue
        result = ghl_create_contact(rec)
        if result and result.get("contact"):
            created += 1
            absentee_tag = " 🏠 ABSENTEE" if rec.get("absentee") else ""
            log.info(f"  ✓ [{i+1}/{len(sorted_leads)}] {rec.get('owner')}{absentee_tag} — {rec.get('address')}")
        else:
            errors += 1
            log.warning(f"  ✗ [{i+1}/{len(sorted_leads)}] {rec.get('owner')} — {result}")
        time.sleep(0.15)

    log.info(f"GHL done — Created: {created} | Skipped: {skipped} | Errors: {errors}")


# ── Main Scraper ──────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper v7")
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

    # Parcel lookup — owner name + mailing address + absentee detection
    addresses   = [r["address"] for r in raw if r["address"]]
    parcel_map  = lookup_parcel_data(addresses)

    named_count    = 0
    absentee_count = 0
    for r in raw:
        data = parcel_map.get(r["address"], {})
        r["owner"]     = data.get("owner", "")
        r["mail_addr"] = data.get("mail_addr", "")
        r["absentee"]  = data.get("absentee", False)
        if r["owner"]:
            named_count += 1
        if r["absentee"]:
            absentee_count += 1

    log.info(f"Owner names resolved: {named_count} / {len(raw)}")
    log.info(f"Absentee owners: {absentee_count} / {named_count} named leads")

    # Flags + score
    records = []
    for r in raw:
        if r["type"] == "TAX":              r["flags"].append("TAX FORE")
        if r.get("absentee"):               r["flags"].append("ABSENTEE")
        if not r["owner"]:                  r["flags"].append("NO OWNER")
        if not r["city"] and r["address"]:  r["flags"].append("NO CITY")
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
.city, .doc { color:var(--muted); font-size:12px; }
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
    <option value="absentee">Absentee Owners Only</option>
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
    var rowClass=r.absentee?' class="absentee-row"':'';
    var fh='';
    for(var j=0;j<(r.flags||[]).length;j++){
      var fc=r.flags[j]==='ABSENTEE'?'flag flag-absentee':'flag';
      fh+='<span class="'+fc+'">'+r.flags[j]+'</span>';
    }
    if(!fh) fh='<span style="color:var(--muted)">—</span>';
    rows+='<tr'+rowClass+'>'
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


# ── Entry Point ───────────────────────────────────────────────────────────────
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

