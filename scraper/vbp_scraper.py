"""
Bexar County VBP + CE Cross-Reference Scraper v1.4
Downloads SA Vacant Building Program PDF monthly,
filters to residential properties, then checks each address
against the SA 311 CE ArcGIS endpoint for open violations.
Adds appraised value lookup from Bexar CAD parcel layer.

v1.4 changes:
  - Adds parcel lookup for appraised_value and land_value on VBP records
  - Normal monthly logic restored (no one-shot enrichment)
  - stacked:true ONLY on VBP records with confirmed CE violations
  - NOF/TAX records never touched
  - Cleans incorrectly stamped NOF records from v1.1
  - Full CE detail written on stamp pass using confirmed addresses

Run: python scraper/vbp_scraper.py
Schedule: Monthly (2nd of month) via GitHub Actions
"""

import os, re, json, time, logging, urllib.request, urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

# ── CONFIG ─────────────────────────────────────────────────────────────────────
VBP_PDF_URL  = "https://docsonline.sanantonio.gov/DSDUploads/VBPInventory.pdf"
RECORDS_PATH = Path("dashboard/records.json")
STATE_PATH   = Path("data/vbp_state.json")
PARCELS_URL  = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0"

MAX_SQ_FT = 6000

SKIP_OWNERS = [
    "city of san antonio", "county of bexar", "housing authority",
    "school district", "isd", "university", "board of regents",
    "church", "ministry", "ministries", "baptist", "methodist",
    "catholic", "presbyterian", "assembly of god", "temple",
    "via metropolitan", "cps energy", "saws", "sa river authority",
    "housing finance", "san antonio housing trust",
    "esperanza peace", "centro sa", "ymca",
]

CE_KEYWORDS = [
    "dangerous premise", "property structure", "vacant", "minimum housing",
    "substandard", "unsecured", "structure maintenance",
]

RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# ── PARCEL LOOKUP ──────────────────────────────────────────────────────────────
def arcgis_query(layer_url, where, fields="*", limit=50):
    try:
        params = urllib.parse.urlencode({
            "where":             where,
            "outFields":         fields,
            "returnGeometry":    "false",
            "resultRecordCount": limit,
            "f":                 "json",
        })
        req = urllib.request.Request(
            f"{layer_url}/query?{params}",
            headers={"User-Agent": "BexarVBP/1.4", "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8", errors="replace"))
        if "error" in data:
            return []
        return data.get("features", [])
    except Exception as e:
        log.debug(f"ArcGIS query error: {e}")
        return []


def get_field(attrs, candidates):
    for c in candidates:
        v = attrs.get(c)
        if v is not None and str(v).strip() not in ("", "None", "null", "<Null>", "NULL", "0"):
            return str(v).strip()
    return ""


def lookup_appraised_value(address):
    """
    Look up appraised value from Bexar CAD parcel layer for a VBP address.
    Returns dict with appraised_value and land_value or empty dict.
    """
    parts = address.strip().upper().split()
    if not parts or not parts[0].isdigit():
        return {}

    num        = parts[0]
    words      = parts[1:]
    first_word = words[0] if words else ""

    APPR_FIELDS = ["TotVal", "TOT_VAL", "TotalVal", "TOTAL_VAL", "AppraisedVal",
                   "APPRAISED_VAL", "AppraisedValue", "APPRAISED_VALUE", "MarketValue"]
    LAND_FIELDS = ["LandVal", "LAND_VAL", "LandValue", "LAND_VALUE"]
    SITUS_FIELDS = ["Situs", "SITUS", "SitusAddress", "SITUS_ADDRESS", "Address", "ADDRESS"]

    def check_features(feats):
        for feat in feats:
            a = feat.get("attributes", {})
            situs = get_field(a, SITUS_FIELDS)
            situs_norm = " ".join(situs.upper().split())
            if not situs_norm.startswith(num + " "):
                continue
            appr = get_field(a, APPR_FIELDS)
            land = get_field(a, LAND_FIELDS)
            if appr:
                return {"appraised_value": appr, "land_value": land}
        return {}

    # Strategy 1: number + first two words
    if len(words) >= 2:
        feats = arcgis_query(PARCELS_URL,
            f"Situs LIKE '{num} {words[0]} {words[1]}%'", limit=10)
        result = check_features(feats)
        if result:
            return result

    # Strategy 2: number + first word
    if first_word and len(first_word) >= 3:
        feats = arcgis_query(PARCELS_URL,
            f"Situs LIKE '{num} {first_word}%'", limit=20)
        result = check_features(feats)
        if result:
            return result

    # Strategy 3: number only
    feats = arcgis_query(PARCELS_URL,
        f"Situs LIKE '{num} %'", limit=50)
    result = check_features(feats)
    if result:
        return result

    return {}


# ── PDF PARSER ─────────────────────────────────────────────────────────────────
def download_vbp_pdf():
    try:
        import pdfplumber
    except ImportError:
        log.info("Installing pdfplumber...")
        os.system("pip install pdfplumber --break-system-packages -q")
        import pdfplumber

    log.info(f"Downloading VBP PDF: {VBP_PDF_URL}")
    pdf_path = Path("/tmp/vbp_inventory.pdf")
    try:
        req = urllib.request.Request(VBP_PDF_URL, headers={
            "User-Agent": "Mozilla/5.0 (compatible; BexarLeads/1.0)"
        })
        with urllib.request.urlopen(req, timeout=60) as r:
            pdf_path.write_bytes(r.read())
        log.info(f"Downloaded: {pdf_path.stat().st_size:,} bytes")
    except Exception as e:
        log.error(f"PDF download failed: {e}")
        return []

    properties = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table:
                    continue
                for row in table:
                    if not row or len(row) < 4:
                        continue
                    if not row[0] or any(h in str(row[0]).lower() for h in
                                         ["address", "vacant building", "please"]):
                        continue
                    try:
                        addr    = str(row[0] or "").strip()
                        zipcode = str(row[1] or "").strip()
                        sq_ft   = str(row[2] or "").strip().replace(",", "")
                        is_sf   = str(row[3] or "").strip().lower()
                        owner   = str(row[-1] or "").strip() if len(row) >= 6 else ""
                        if len(row) >= 7:
                            owner = str(row[-1] or "").strip()
                        if not addr or not re.match(r"^\d+\s", addr):
                            continue
                        properties.append({
                            "address": addr.upper(),
                            "zip":     zipcode,
                            "sq_ft":   int(sq_ft) if sq_ft.isdigit() else 0,
                            "is_sf":   is_sf == "yes",
                            "owner":   owner.upper(),
                        })
                    except Exception:
                        continue
    except Exception as e:
        log.error(f"PDF parse error: {e}")

    log.info(f"VBP PDF: {len(properties)} properties parsed")
    return properties


def vbp_doc_key(addr, zipcode):
    """
    Normalized VBP dedup key. Collapses whitespace/punctuation drift between
    monthly PDF pulls so the same property always maps to the same key —
    prevents duplicate leads and lets reruns match back to the existing
    record instead of creating a new one.
    """
    a = re.sub(r"[^\w\s-]", "", (addr or "").upper().strip())
    a = re.sub(r"\s+", " ", a)
    return f"VBP-{a.replace(' ', '-')}-{(zipcode or '').strip()}"


def filter_properties(properties):
    filtered = []
    for p in properties:
        owner_lower = p["owner"].lower()
        if any(skip in owner_lower for skip in SKIP_OWNERS):
            continue
        if not p["is_sf"] and p["sq_ft"] > MAX_SQ_FT:
            continue
        if p["sq_ft"] == 0 and not p["is_sf"]:
            continue
        filtered.append(p)
    log.info(f"VBP filtered: {len(filtered)}/{len(properties)} residential properties")
    return filtered


# ── CE LOOKUP ──────────────────────────────────────────────────────────────────
CE_API_URL = (
    "https://services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services"
    "/311_All_Service_Calls/FeatureServer/0/query"
)

def check_ce_for_address(street_address):
    parts = street_address.strip().split()
    if len(parts) < 2:
        return []
    search = " ".join(parts[:2])
    where  = f"ObjectDescription LIKE '%{search}%' AND CaseStatus = 'Open'"
    params = urllib.parse.urlencode({
        "where":             where,
        "outFields":         "CASEID,TypeName,CaseStatus,OpenedDateTime,ObjectDescription",
        "returnGeometry":    "false",
        "resultRecordCount": 10,
        "f":                 "json",
    })
    try:
        req = urllib.request.Request(
            f"{CE_API_URL}?{params}",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8", errors="replace"))
        if "error" in data:
            return []
        violations = []
        for feat in data.get("features", []):
            a = feat.get("attributes", {})
            typename = (a.get("TypeName") or "").lower()
            if any(kw in typename for kw in CE_KEYWORDS):
                opened_ms = a.get("OpenedDateTime")
                opened_str = ""
                if opened_ms:
                    try:
                        opened_str = datetime.utcfromtimestamp(
                            int(opened_ms)/1000).strftime("%m/%d/%Y")
                    except Exception:
                        pass
                violations.append({
                    "case_id":  str(a.get("CASEID") or ""),
                    "typename": a.get("TypeName") or "",
                    "status":   a.get("CaseStatus") or "",
                    "opened":   opened_str,
                    "address":  a.get("ObjectDescription") or "",
                })
        return violations
    except Exception as e:
        log.debug(f"CE lookup error for {street_address}: {e}")
        return []


# ── STATE ──────────────────────────────────────────────────────────────────────
def load_state():
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {"last_vbp_count": 0, "last_run": "", "checked": {}}

def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))

def write_records(records):
    text = json.dumps(records)
    json.loads(text)
    tmp = RECORDS_PATH.with_suffix('.tmp')
    tmp.write_text(text)
    tmp.replace(RECORDS_PATH)


# ── CLEAN NOF RECORDS ─────────────────────────────────────────────────────────
def clean_nof_stacked_flags(existing):
    cleaned = 0
    for r in existing:
        if r.get("type") in ("NOF", "TAX"):
            changed = False
            if r.get("stacked"):
                r["stacked"] = False
                changed = True
            flags = r.get("flags") or []
            if "STACKED" in flags:
                flags.remove("STACKED")
                r["flags"] = flags
                changed = True
            if changed:
                cleaned += 1
    return cleaned


# ── STAMP + ENRICH EXISTING VBP RECORDS ───────────────────────────────────────
def stamp_and_enrich_vbp_records(existing, confirmed_ce_addresses):
    """
    For VBP records with confirmed CE violations:
    - Set stacked:true
    - Fetch full CE violation details from API
    - Fetch appraised value from BCAD parcel layer
    Only touches VBP type records.
    """
    stamped  = 0
    enriched = 0

    for r in existing:
        if r.get("type") != "VBP":
            continue

        addr_clean = r.get("address", "").upper().strip()
        if addr_clean not in confirmed_ce_addresses:
            continue

        changed = False

        # Stamp stacked
        if not r.get("stacked"):
            r["stacked"] = True
            changed = True

        flags = r.get("flags") or []
        if "STACKED" not in flags:
            flags.append("STACKED")
            r["flags"] = flags
            changed = True

        # Fetch CE details if missing
        if not r.get("ce_cat_label") and not r.get("ce_violations"):
            violations = check_ce_for_address(addr_clean)
            if violations:
                viol_types = list({v["typename"] for v in violations})
                first = violations[0]
                is_dangerous = any("dangerous" in t.lower() for t in viol_types)
                if is_dangerous and "DANGEROUS PREMISES" not in flags:
                    flags.append("DANGEROUS PREMISES")
                    r["flags"] = flags
                r["ce_violations"] = violations
                r["ce_viol_types"] = viol_types
                r["ce_count"]      = len(violations)
                r["ce_cat_label"]  = first["typename"]
                r["ce_status"]     = first["status"]
                r["opened_date"]   = first["opened"]
                r["ce_case_id"]    = first["case_id"]
                enriched += 1
                time.sleep(0.4)

        # Fetch appraised value if missing
        if not r.get("appraised_value"):
            parcel = lookup_appraised_value(addr_clean)
            if parcel.get("appraised_value"):
                r["appraised_value"] = parcel["appraised_value"]
                r["land_value"]      = parcel.get("land_value", "")
                changed = True
            time.sleep(0.2)

        if changed:
            stamped += 1

    return stamped, enriched


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("VBP + CE Cross-Reference Scraper v1.4")
    log.info("=" * 60)

    state = load_state()
    log.info(f"Last run: {state.get('last_run') or 'never'} | "
             f"Previously checked: {len(state.get('checked', {}))} addresses")

    # Download + parse PDF
    all_props = download_vbp_pdf()
    if not all_props:
        log.error("No properties from PDF — aborting")
        return

    vbp_count = len(all_props)
    props     = filter_properties(all_props)

    # Load existing records
    try:
        existing = json.loads(RECORDS_PATH.read_text())
        log.info(f"Loaded {len(existing)} existing records from {RECORDS_PATH}")
    except Exception as e:
        log.error(f"ABORT: Could not load {RECORDS_PATH}: {e}")
        return

    existing_docs = {r["doc_number"] for r in existing}
    log.info(f"Existing doc numbers: {len(existing_docs)}")

    # Keyed by normalized address+zip (not raw doc_number) so a rerun matches
    # an existing VBP lead even if the PDF's address formatting drifted.
    existing_by_key = {
        vbp_doc_key(r.get("address", ""), r.get("zip", "")): r
        for r in existing if r.get("type") == "VBP"
    }

    # Step 1: Clean incorrectly stamped NOF/TAX records
    cleaned = clean_nof_stacked_flags(existing)
    log.info(f"Cleaned stacked flag from {cleaned} NOF/TAX records")

    # Step 2: Build confirmed CE address set from vbp_state.json
    checked = state.get("checked", {})
    confirmed_ce_addresses = {
        addr.upper().strip()
        for addr, info in checked.items()
        if info.get("violations") is True
    }
    log.info(f"Confirmed CE addresses from state: {len(confirmed_ce_addresses)}")

    # Step 3: Stamp + enrich existing VBP records
    stamped, enriched = stamp_and_enrich_vbp_records(existing, confirmed_ce_addresses)
    log.info(f"Stamped stacked:true on {stamped} existing VBP records")
    log.info(f"CE details fetched for {enriched} VBP records")

    # Step 4: Decide whether to run full CE check
    vbp_missing_stacked = sum(
        1 for r in existing
        if r.get("type") == "VBP" and not r.get("stacked")
    )
    already_stacked_vbp = sum(
        1 for r in existing
        if r.get("type") == "VBP" and r.get("stacked")
    )

    skip_ce_check = False
    if vbp_count == state.get("last_vbp_count") and state.get("last_run") and already_stacked_vbp > 0:
        last_run_dt = datetime.fromisoformat(state["last_run"])
        days_since  = (datetime.now() - last_run_dt).days
        if days_since < 25 and vbp_missing_stacked == 0:
            log.info(f"VBP unchanged ({vbp_count} props), {already_stacked_vbp} stacked, "
                     f"last run {days_since}d ago — skipping CE check")
            skip_ce_check = True
        elif vbp_missing_stacked > 0:
            log.info(f"{vbp_missing_stacked} VBP records missing stacked — running CE check")
        else:
            log.info(f"VBP unchanged but {days_since}d since last run — re-checking CE")
    else:
        log.info("Running full CE check")

    if skip_ce_check:
        if cleaned > 0 or stamped > 0 or enriched > 0:
            write_records(existing)
            log.info(f"records.json updated — {cleaned} NOF cleaned, "
                     f"{stamped} VBP stamped, {enriched} CE enriched")
        else:
            log.info("No changes — records.json unchanged")
        save_state(state)
        log.info("Done.")
        return

    # Step 5: Full CE check for new VBP addresses
    new_leads  = []
    ce_checked = 0
    ce_found   = 0
    skipped    = 0
    updated    = 0

    log.info(f"Checking {len(props)} VBP addresses against CE portal...")

    for i, prop in enumerate(props):
        addr    = prop["address"]
        zipcode = prop["zip"]
        doc_key = vbp_doc_key(addr, zipcode)

        if doc_key in existing_by_key:
            # Already a lead for this property — refresh its CE data in
            # place instead of creating a duplicate. Same 25d cooldown as
            # the new-address check below, so we're not hammering the CE
            # API for every known VBP address on every run.
            if addr in checked and checked[addr].get("checked_at"):
                checked_dt = datetime.fromisoformat(checked[addr]["checked_at"])
                if (datetime.now() - checked_dt).days < 25:
                    skipped += 1
                    continue

            violations = check_ce_for_address(addr)
            ce_checked += 1
            checked[addr] = {
                "checked_at": RUN_TIMESTAMP,
                "violations": len(violations) > 0,
            }

            if violations:
                rec          = existing_by_key[doc_key]
                viol_types   = list({v["typename"] for v in violations})
                first        = violations[0]
                is_dangerous = any("dangerous" in t.lower() for t in viol_types)

                rec["ce_violations"] = violations
                rec["ce_viol_types"] = viol_types
                rec["ce_count"]      = len(violations)
                rec["ce_cat_label"]  = first["typename"]
                rec["ce_status"]     = first["status"]
                rec["opened_date"]   = first["opened"]
                rec["ce_case_id"]    = first["case_id"]

                flags = set(rec.get("flags") or [])
                flags.update(["VACANT STRUCT", "CODE ENFORCE", "STACKED"])
                if is_dangerous:
                    flags.add("DANGEROUS PREMISES")
                rec["flags"]   = list(flags)
                rec["stacked"] = True
                rec["score"]   = max(rec.get("score", 0), 8 + (2 if is_dangerous else 0))
                updated += 1

            time.sleep(0.4)
            skipped += 1
            continue

        if addr in checked and checked[addr].get("checked_at"):
            checked_dt = datetime.fromisoformat(checked[addr]["checked_at"])
            if (datetime.now() - checked_dt).days < 25 and not checked[addr].get("violations"):
                skipped += 1
                continue

        violations = check_ce_for_address(addr)
        ce_checked += 1

        checked[addr] = {
            "checked_at": RUN_TIMESTAMP,
            "violations": len(violations) > 0,
        }

        if violations:
            ce_found   += 1
            viol_types  = list({v["typename"] for v in violations})
            first       = violations[0]
            is_dangerous = any("dangerous" in t.lower() for t in viol_types)
            flags = ["VACANT STRUCT", "CODE ENFORCE", "STACKED"]
            if is_dangerous:
                flags.append("DANGEROUS PREMISES")

            score = 8
            if is_dangerous:
                score += 2

            # Lookup appraised value for new lead
            parcel = lookup_appraised_value(addr)

            lead = {
                "doc_number":      doc_key,
                "type":            "VBP",
                "source":          "vbp_ce",
                "address":         addr,
                "city":            "SAN ANTONIO",
                "zip":             zipcode,
                "date_filed":      RUN_TIMESTAMP[:7],
                "sale_date":       "",
                "owner":           prop["owner"],
                "mail_addr":       "",
                "absentee":        False,
                "duplicate":       False,
                "is_new":          True,
                "stacked":         True,
                "run_ts":          RUN_TIMESTAMP,
                "flags":           flags,
                "score":           score,
                "sq_ft":           prop["sq_ft"],
                "is_sf":           prop["is_sf"],
                "ce_violations":   violations,
                "ce_viol_types":   viol_types,
                "ce_count":        len(violations),
                "ce_cat_label":    first["typename"],
                "ce_status":       first["status"],
                "opened_date":     first["opened"],
                "ce_case_id":      first["case_id"],
                "appraised_value": parcel.get("appraised_value", ""),
                "land_value":      parcel.get("land_value", ""),
                "loan_amount":     "",
                "loan_date":       "",
                "lender":          "",
                "trustee":         "",
                "annual_taxes":    "",
                "ps_doc_id":       "",
            }
            new_leads.append(lead)
            log.info(f"  STACKED [{i+1}/{len(props)}] {addr} — "
                     f"{len(violations)} violation(s): {first['typename'][:40]}"
                     f"{' appr=$'+parcel['appraised_value'] if parcel.get('appraised_value') else ''}")
        else:
            if (i + 1) % 50 == 0:
                log.info(f"  Progress: {i+1}/{len(props)} | "
                         f"{ce_found} stacked | {ce_checked} queried | {skipped} skipped")

        time.sleep(0.4)

    log.info(f"CE check complete: {ce_checked} queried | "
             f"{ce_found} new stacked | {updated} existing updated | {skipped} skipped")

    # Merge and write (existing records were updated in place above, not duplicated)
    merged = existing + new_leads
    write_records(merged)
    log.info(f"records.json saved — {len(merged)} total | "
             f"{cleaned} NOF cleaned | {stamped} VBP stamped | "
             f"{enriched} CE enriched | {updated} refreshed | {len(new_leads)} new VBP+CE leads added")

    # Update state
    state["last_vbp_count"] = vbp_count
    state["last_run"]       = RUN_TIMESTAMP
    state["checked"]        = checked
    save_state(state)

    log.info("Done.")


if __name__ == "__main__":
    main()
