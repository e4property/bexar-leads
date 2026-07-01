"""
appt_arcgis_backfill.py
Backfills ArcGIS data (address, owner, appraised_value, prop_id, absentee)
for existing APPT records in records.json that are missing these fields.

Run once via GitHub Actions workflow_dispatch or locally.
Does NOT touch any NOF/TAX/LP/VBP records — APPT only.

Usage:
  python scraper/appt_arcgis_backfill.py
"""

import json
import logging
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

RECORDS_PATH = Path("dashboard/records.json")
PARCELS_URL  = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"

# ── Entity filter ─────────────────────────────────────────────────────────────
ENTITY_KEYWORDS = [
    "LLC", "LLP", "L.L.C", "L.L.P", "INC", "CORP", "CORPORATION",
    "LOAN SERVICING", "LOAN SERV", "MORTGAGE", "BANK", "N.A.", " NA ",
    "TRUST", "TRUSTEE", "SERVICES", "SERVICING", "FINANCIAL",
    "AUCTION.COM", "BARRETT DAFFIN", "FRAPPIER", "TURNER", "ENGEL",
    "ASSOCIATION", "FEDERAL", "SAVINGS", "HOLDINGS", "CAPITAL",
    "FUNDING", "PARTNERS", "GROUP", "COMPANY", " CO ", "ATTORNEY",
    "LAW FIRM", "LAW OFFICE", "SUBSTITUTE TRUSTEE", "DEPARTMENT",
    "AGENCY", "SYSTEMS", "FREDDIE MAC", "FANNIE MAE", "HUD",
    "COMMISSIONER", "JPMORGAN", "CHASE", "WELLS FARGO", "NATIONSTAR",
    "MR COOPER", "LAKEVIEW", "PENNYMAC", "NEWREZ", "CALIBER",
]

def is_entity_name(name):
    if not name:
        return True
    upper = name.upper()
    return any(kw in upper for kw in ENTITY_KEYWORDS)

# ── ArcGIS field aliases ───────────────────────────────────────────────────────
OWNER_FIELDS  = ["Owner", "OWNER", "OwnerName"]
SITUS_FIELDS  = ["Situs", "SITUS", "SitusAddress"]
ADDR_FIELDS   = ["AddrLn1", "ADDRLN1", "MailAddr1"]
CITY_FIELDS   = ["AddrCity", "ADDRCITY", "MailCity"]
ZIP_FIELDS    = ["Zip", "ZIP", "ZipCode", "PostalCode"]
APPR_FIELDS   = ["TotVal", "TOTVAL", "AppraisedValue", "Tot_Val"]
TAX_FIELDS    = ["TaxAmt", "TAXAMT", "AnnualTax"]
PROPID_FIELDS = ["PropID", "PROPID", "PropertyID"]
LAND_FIELDS   = ["LandVal", "LANDVAL"]
IMPR_FIELDS   = ["ImprVal", "IMPRVAL"]

def _fetch_json(url, timeout=20):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "BexarApptBackfill/1.0",
                     "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except Exception as e:
        log.debug(f"fetch_json error: {e}")
        return {}

def _arcgis_query(where, limit=200):
    all_features = []
    offset = 0
    while True:
        params = urllib.parse.urlencode({
            "where":             where,
            "outFields":         "*",
            "returnGeometry":    "false",
            "resultOffset":      offset,
            "resultRecordCount": limit,
            "f":                 "json",
        })
        data = _fetch_json(f"{PARCELS_URL}/query?{params}")
        if not data or "error" in data:
            break
        batch = data.get("features", [])
        all_features.extend(batch)
        if not data.get("exceededTransferLimit", False) or len(batch) < limit:
            break
        offset += len(batch)
    return all_features

def _get(attrs, *keys, default=""):
    for k in keys:
        v = attrs.get(k)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>", "NULL"):
            return str(v).strip()
    return default

def _norm(s):
    return " ".join(str(s).upper().split())

def _parse_num(address):
    m = re.match(r"^(\d+)\s+", address.strip())
    return m.group(1) if m else ""

def _parse_words(address):
    clean = re.sub(r"^\d+\s*", "", address.strip().upper())
    return [w for w in re.split(r"\s+", clean) if w and len(w) >= 2]

def _match(features, num, required_word=None):
    for feat in features:
        a = feat.get("attributes", {})
        situs = _get(a, *SITUS_FIELDS)
        owner = _get(a, *OWNER_FIELDS)
        if not situs or not owner:
            continue
        sn = _norm(situs)
        if not sn.startswith(num + " "):
            continue
        if required_word and required_word not in sn:
            continue
        addr1   = _get(a, *ADDR_FIELDS)
        city    = _get(a, *CITY_FIELDS)
        zipcode = _get(a, *ZIP_FIELDS)
        mail    = f"{addr1} {city} {zipcode}".strip() if addr1 else ""
        absentee = bool(mail) and not _norm(mail).startswith(num + " ")
        return {
            "owner":             owner.upper(),
            "address":           situs.upper(),
            "mail_addr":         mail,
            "absentee":          absentee,
            "appraised_value":   _get(a, *APPR_FIELDS),
            "annual_taxes":      _get(a, *TAX_FIELDS),
            "land_value":        _get(a, *LAND_FIELDS),
            "improvement_value": _get(a, *IMPR_FIELDS),
            "prop_id":           _get(a, *PROPID_FIELDS),
            "zip":               zipcode,
        }
    return None

def lookup_by_address(address, zipcode=""):
    if not address:
        return None
    num   = _parse_num(address)
    words = _parse_words(address)
    first = words[0] if words else ""
    if not num:
        return None

    if len(words) >= 2:
        r = _match(_arcgis_query(f"Situs LIKE '{num} {words[0]} {words[1]}%'", 50), num, first)
        if r: r["method"] = "s1_two_word"; return r

    if first and len(first) >= 3:
        r = _match(_arcgis_query(f"Situs LIKE '{num} {first}%'", 100), num, first)
        if r: r["method"] = "s2_first_word"; return r

    r = _match(_arcgis_query(f"Situs LIKE '{num} %'", 200), num, None)
    if r: r["method"] = "s3_num_only"; return r

    if zipcode and len(zipcode) >= 5:
        r = _match(_arcgis_query(f"Zip = '{zipcode[:5]}'", 1000), num, None)
        if r: r["method"] = "s4_zip_scan"; return r

    for word in words[1:]:
        if len(word) < 4:
            continue
        r = _match(_arcgis_query(f"Situs LIKE '{num} %{word}%'", 100), num, word)
        if r: r["method"] = "s5_alt_word"; return r

    return None

def lookup_by_owner(owner_name):
    if not owner_name or is_entity_name(owner_name):
        return None
    parts = owner_name.strip().upper().split()
    if not parts:
        return None
    last = parts[0]
    if len(last) < 3:
        return None
    features = _arcgis_query(f"Owner LIKE '{last}%'", 100)
    name_norm = _norm(owner_name)
    for feat in features:
        a = feat.get("attributes", {})
        arc_owner = _norm(_get(a, *OWNER_FIELDS))
        if not arc_owner:
            continue
        arc_parts = arc_owner.split()
        matches = sum(1 for p in parts if p in arc_parts)
        if matches >= min(2, len(parts)):
            situs   = _get(a, *SITUS_FIELDS)
            addr1   = _get(a, *ADDR_FIELDS)
            city    = _get(a, *CITY_FIELDS)
            zipcode = _get(a, *ZIP_FIELDS)
            mail    = f"{addr1} {city} {zipcode}".strip() if addr1 else ""
            num     = _parse_num(situs.upper() if situs else "")
            absentee = bool(mail) and bool(num) and not _norm(mail).startswith(num + " ")
            return {
                "owner":             arc_owner,
                "address":           situs.upper() if situs else "",
                "mail_addr":         mail,
                "absentee":          absentee,
                "appraised_value":   _get(a, *APPR_FIELDS),
                "annual_taxes":      _get(a, *TAX_FIELDS),
                "land_value":        _get(a, *LAND_FIELDS),
                "improvement_value": _get(a, *IMPR_FIELDS),
                "prop_id":           _get(a, *PROPID_FIELDS),
                "zip":               zipcode,
                "method":            "owner_name",
            }
    return None

def needs_enrichment(rec):
    """True if this APPT record is missing any useful field."""
    return (
        not rec.get("address") or
        not rec.get("owner") or
        not rec.get("appraised_value") or
        not rec.get("prop_id")
    )

def main():
    if not RECORDS_PATH.exists():
        log.error(f"records.json not found at {RECORDS_PATH}")
        return

    records = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))
    log.info(f"Loaded {len(records)} records")

    # Only APPT records that need enrichment
    candidates = [r for r in records if r.get("type") == "APPT" and needs_enrichment(r)]
    log.info(f"Found {len(candidates)} APPT records needing ArcGIS enrichment")

    if not candidates:
        log.info("Nothing to enrich — all APPT records already complete")
        return

    filled_addr  = 0
    filled_owner = 0
    filled_appr  = 0
    filled_prop  = 0
    total_hit    = 0
    total_miss   = 0

    for i, rec in enumerate(candidates):
        address = (rec.get("address") or "").strip()
        owner   = (rec.get("owner") or "").strip()
        zipcode = (rec.get("zip") or "").strip()
        doc     = rec.get("doc_number", "?")

        log.info(f"[{i+1}/{len(candidates)}] {doc} | addr='{address}' owner='{owner}'")

        result = None

        # Try address first
        if address and len(address) > 5:
            result = lookup_by_address(address, zipcode)
            if result:
                log.info(f"  addr hit [{result.get('method','')}] → {result.get('owner','—')} appr={result.get('appraised_value','—')}")

        # Fallback: owner name
        if not result and owner and not is_entity_name(owner):
            result = lookup_by_owner(owner)
            if result:
                log.info(f"  owner hit [{result.get('method','')}] → addr={result.get('address','—')} appr={result.get('appraised_value','—')}")

        if not result:
            log.info(f"  MISS")
            total_miss += 1
            time.sleep(0.2)
            continue

        total_hit += 1

        # Apply — only fill blank fields, never overwrite existing data
        if not rec.get("address") and result.get("address"):
            rec["address"] = result["address"]
            filled_addr += 1

        if not rec.get("owner") and result.get("owner"):
            rec["owner"] = result["owner"].title()
            filled_owner += 1

        if not rec.get("zip") and result.get("zip"):
            rec["zip"] = result["zip"]

        if not rec.get("mail_addr"):
            rec["mail_addr"] = result.get("mail_addr", "")

        if not rec.get("absentee"):
            rec["absentee"] = result.get("absentee", False)

        if not rec.get("appraised_value") and result.get("appraised_value"):
            rec["appraised_value"] = result["appraised_value"]
            filled_appr += 1

        if not rec.get("annual_taxes"):
            rec["annual_taxes"] = result.get("annual_taxes", "")

        if not rec.get("prop_id") and result.get("prop_id"):
            rec["prop_id"] = result["prop_id"]
            filled_prop += 1

        if not rec.get("land_value"):
            rec["land_value"] = result.get("land_value", "")

        if not rec.get("improvement_value"):
            rec["improvement_value"] = result.get("improvement_value", "")

        # Score bump if absentee found
        if rec.get("absentee") and rec.get("score", 7) < 8:
            rec["score"] = 8

        time.sleep(0.2)

    # Save updated records.json
    RECORDS_PATH.write_text(
        json.dumps(records, ensure_ascii=False),
        encoding="utf-8"
    )

    log.info(
        f"\nBackfill complete:"
        f"\n  {total_hit} hits | {total_miss} misses"
        f"\n  {filled_addr} addresses filled"
        f"\n  {filled_owner} owners filled"
        f"\n  {filled_appr} appraised values filled"
        f"\n  {filled_prop} prop IDs filled"
        f"\n  records.json saved ({len(records)} total records)"
    )

if __name__ == "__main__":
    main()
