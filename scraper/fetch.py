"""
Bexar County Motivated Seller Lead Scraper v25.3
- is_new correctly marks only leads new since last run
- GHL only pushes new leads
- build_dashboard writes index.html and records.json only (NOT leads.html)
- leads.html is managed manually in the repo and never overwritten
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
PAGES_RECORDS    = "https://e4property.github.io/bexar-leads/records.json"

LAYERS = [
    {"index": 0, "type": "NOF", "label": "Mortgage Foreclosure"},
    {"index": 1, "type": "TAX", "label": "Tax Foreclosure"},
]

GHL_API_KEY     = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "UAOJlgeerLu3GChP9jDJ")
GHL_API_BASE    = "https://services.leadconnectorhq.com"

RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "BexarScraper/25.3", "Accept": "application/json"})
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
            headers={"User-Agent": "BexarScraper/25.3", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            prev = json.loads(r.read().decode("utf-8", errors="replace"))
            docs = {str(rec.get("doc_number", "")) for rec in prev if rec.get("doc_number")}
            log.info(f"Loaded {len(docs)} known doc numbers from previous run")
            return docs
    except Exception as e:
        log.info(f"No previous records found (first run?): {e}")
        return set()


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
    return {
        "num":    num,
        "street": " ".join(rest),
        "words":  words,
        "suffix": suffix,
        "full":   address.strip().upper()
    }


def match_features(feats, num, first_word):
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


def lookup_owner(address):
    parsed = parse_address(address)
    if not parsed:
        return {}
    num        = parsed["num"]
    words      = parsed["words"]
    first_word = words[0] if words else ""
    if not first_word or len(first_word) < 3:
        return {}
    if len(words) >= 2:
        feats  = arcgis_query(PARCELS_URL, f"Situs LIKE '%{words[0]} {words[1]}%'",
                              fields="Situs,Owner,AddrLn1,AddrCity,Zip", limit=200)
        result = match_features(feats, num, first_word)
        if result:
            result["method"] = "s1_two_word"; return result
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
    return {}


def fetch_foreclosures(known_docs):
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
            a      = feat["attributes"]
            month  = pick(a, "MONTH", "MO", default="")
            year   = pick(a, "YEAR",  "YR", default="")
            doc    = pick(a, "DOC_NUMBER", "DOCNUM", "DOC_NUM")
            is_new = doc not in known_docs
            raw.append({
                "type":        layer["type"],
                "address":     pick(a, "ADDRESS", "SITUS_ADD", "ADDR"),
                "owner":       "",
                "mail_addr":   "",
                "absentee":    False,
                "duplicate":   False,
                "is_new":      is_new,
                "doc_number":  doc,
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
    new_count = sum(1 for r in raw if r["is_new"])
    log.info(f"Foreclosures: {len(raw)} total | {new_count} NEW this run")
    return raw


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
                log.info(f"  [{i+1}/{len(records)}]{ab} [{method}] {addr} -> {result['owner']}")
        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(records)} | Found: {found} (s1={s1} s2={s2} s3={s3})")
        time.sleep(0.15)
    pct      = 100 * found // max(len(records), 1)
    absentee = sum(1 for r in records if r.get("absentee"))
    log.info(f"Owner lookup: {found}/{len(records)} ({pct}% hit rate)")
    log.info(f"  Strategy breakdown - s1:{s1} s2:{s2} s3:{s3}")
    log.info(f"  Absentee owners: {absentee}")
    return records


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


def score_record(rec):
    s = 0
    if rec.get("address"):       s += 3
    if rec.get("owner"):         s += 3
    if rec.get("type") == "TAX": s += 2
    if rec.get("absentee"):      s += 2
    return min(s, 10)


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
    new_leads = [r for r in records if r.get("owner") and r.get("is_new")]
    log.info(f"GHL: {len(new_leads)} NEW leads to push")
    if not new_leads:
        log.info("No new leads this run - skipping GHL push")
        return
    test = ghl_req("GET", f"/contacts/?locationId={GHL_LOCATION_ID}&limit=1")
    if not test or "_error" in test:
        log.error("GHL auth failed"); return
    log.info(f"GHL auth OK - {test.get('total','?')} existing contacts")
    created = skipped = errors = 0
    for i, rec in enumerate(sorted(new_leads, key=lambda r: -r.get("score", 0))):
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
            "tags":       tags, "source": "Bexar County Scraper",
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
            ab  = " ABSENTEE" if rec.get("absentee") else ""
            dup = " DUP"      if rec.get("duplicate") else ""
            log.info(f"  [{i+1}]{ab}{dup} {owner} - {rec.get('address')}")
        else:
            errors += 1
        time.sleep(0.15)
    log.info(f"GHL done - Created:{created} | Skipped:{skipped} | Errors:{errors}")


def build_dashboard(records):
    """
    Writes records.json and index.html only.
    leads.html is managed manually in the repo and never overwritten here.
    """
    os.makedirs("dashboard", exist_ok=True)

    # Write records.json - this is what the dashboard fetches at runtime
    json_str = json.dumps(records, separators=(",", ":"), ensure_ascii=True)
    with open("dashboard/records.json", "w", encoding="utf-8") as f:
        f.write(json_str)

    # Write a minimal index.html that just redirects to leads.html
    with open("dashboard/index.html", "w", encoding="utf-8") as f:
        f.write('<!DOCTYPE html><html><head><meta charset="UTF-8"/>'
                '<meta http-equiv="refresh" content="0;url=leads.html"/>'
                '<title>Redirecting...</title></head>'
                '<body><script>window.location.href="leads.html";</script></body></html>')

    log.info(f"Built dashboard/records.json - {len(records)} records, {os.path.getsize('dashboard/records.json'):,} bytes")
    log.info("Built dashboard/index.html - redirects to leads.html")


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    log.info("=" * 60)
    log.info("Bexar County Lead Scraper v25.3")
    log.info(f"Foreclosures: {FORECLOSURE_BASE}")
    log.info(f"Owner lookup: {PARCELS_URL}")
    log.info("=" * 60)

    known_docs = load_known_docs()
    records    = fetch_foreclosures(known_docs)
    records    = enrich_owners(records)
    records    = detect_duplicates(records)

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
    log.info(f"Final: {len(records)} leads | {named} named | {absentee} absentee | {dupes} dupes | {new_ct} NEW")

    with open("data/records.json", "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    build_dashboard(records)
    push_ghl(records)

