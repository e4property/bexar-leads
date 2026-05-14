"""
Bexar County VBP + CE Cross-Reference Scraper v1.1
Downloads SA Vacant Building Program PDF monthly,
filters to residential properties, then checks each address
against the SA 311 CE ArcGIS endpoint for open violations.
Results added to records.json as STACKED leads.

v1.1 fix: stamps stacked:true on existing NOF records that match
VBP addresses, even when CE check is skipped (VBP unchanged).

Run: python scraper/vbp_scraper.py
Schedule: Monthly (1st of month) via GitHub Actions
"""

import os, re, json, time, logging, urllib.request, urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

# ── CONFIG ─────────────────────────────────────────────────────────────────────
VBP_PDF_URL = "https://docsonline.sanantonio.gov/DSDUploads/VBPInventory.pdf"
RECORDS_PATH = Path("dashboard/records.json")
STATE_PATH   = Path("data/vbp_state.json")

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
    where = f"ObjectDescription LIKE '%{search}%' AND CaseStatus = 'Open'"

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

        features = data.get("features", [])
        violations = []
        for feat in features:
            a = feat.get("attributes", {})
            typename = (a.get("TypeName") or "").lower()
            if any(kw in typename for kw in CE_KEYWORDS):
                violations.append({
                    "case_id":  str(a.get("CASEID") or ""),
                    "typename": a.get("TypeName") or "",
                    "status":   a.get("CaseStatus") or "",
                    "opened":   str(a.get("OpenedDateTime") or ""),
                    "address":  a.get("ObjectDescription") or "",
                })
        return violations

    except Exception as e:
        log.debug(f"CE lookup error for {street_address}: {e}")
        return []


# ── STATE MANAGEMENT ───────────────────────────────────────────────────────────
def load_state():
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {"last_vbp_count": 0, "last_run": "", "checked": {}}

def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ── STAMP HELPER ───────────────────────────────────────────────────────────────
def stamp_stacked_on_nof_records(existing, vbp_addresses):
    """
    Stamp stacked:true on any existing NOF record whose address
    matches a VBP address. Also ensures 'STACKED' appears in flags.
    Returns count of records stamped.
    """
    stamped = 0
    for r in existing:
        addr_clean = r.get("address", "").upper().strip()
        if addr_clean in vbp_addresses:
            changed = False
            if not r.get("stacked"):
                r["stacked"] = True
                changed = True
            flags = r.get("flags") or []
            if "STACKED" not in flags:
                flags.append("STACKED")
                r["flags"] = flags
                changed = True
            if changed:
                stamped += 1
    return stamped


def write_records(records):
    """Atomically write records.json."""
    text = json.dumps(records)
    json.loads(text)  # validate before writing
    tmp = RECORDS_PATH.with_suffix('.tmp')
    tmp.write_text(text)
    tmp.replace(RECORDS_PATH)


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("VBP + CE Cross-Reference Scraper v1.1")
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
    props = filter_properties(all_props)
    vbp_addresses = {p["address"] for p in props}

    # Load existing records — MUST preserve all existing leads
    try:
        existing_text = RECORDS_PATH.read_text()
        existing = json.loads(existing_text)
        log.info(f"Loaded {len(existing)} existing records from {RECORDS_PATH}")
    except Exception as e:
        log.error(f"ABORT: Could not load {RECORDS_PATH}: {e}")
        log.error("Will not write to records.json to avoid data loss")
        return

    existing_docs = {r["doc_number"] for r in existing}
    log.info(f"Existing doc numbers: {len(existing_docs)}")

    # Check if VBP list has changed
    already_stacked = sum(1 for r in existing if r.get("type") == "VBP")
    skip_ce_check = False

    if vbp_count == state.get("last_vbp_count") and state.get("last_run") and already_stacked > 0:
        last_run_dt = datetime.fromisoformat(state["last_run"])
        days_since = (datetime.now() - last_run_dt).days
        if days_since < 25:
            log.info(f"VBP unchanged ({vbp_count} props), {already_stacked} stacked leads exist, "
                     f"last run {days_since}d ago — skipping CE check, stamping NOF records only")
            skip_ce_check = True
        else:
            log.info(f"VBP unchanged but {days_since}d since last run — re-checking CE")
    elif already_stacked == 0:
        log.info("No VBP leads in records.json — forcing full CE check")

    # ── ALWAYS stamp stacked:true on matching NOF records ─────────────────────
    stamped = stamp_stacked_on_nof_records(existing, vbp_addresses)
    log.info(f"Stamped stacked:true on {stamped} existing NOF records")

    if skip_ce_check:
        if stamped > 0:
            write_records(existing)
            log.info(f"records.json updated with {stamped} newly stamped NOF records")
        else:
            log.info("No new stamps needed — records.json unchanged")
        save_state(state)
        log.info("Done.")
        return

    # ── CE CHECK ───────────────────────────────────────────────────────────────
    checked = state.get("checked", {})
    new_leads = []
    ce_checked = 0
    ce_found   = 0
    skipped    = 0

    log.info(f"Checking {len(props)} VBP addresses against CE portal (ArcGIS per-address)...")

    for i, prop in enumerate(props):
        addr    = prop["address"]
        zipcode = prop["zip"]
        doc_key = f"VBP-{addr.replace(' ', '-')}-{zipcode}"

        if doc_key in existing_docs:
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
            ce_found += 1
            viol_types = list({v["typename"] for v in violations})
            flags = ["VACANT STRUCT", "CODE ENFORCE", "STACKED"]
            if any("dangerous" in t.lower() for t in viol_types):
                flags.append("DANGEROUS PREMISES")

            score = 8
            if "DANGEROUS PREMISES" in flags:
                score += 2

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
                "loan_amount":     "",
                "loan_date":       "",
                "lender":          "",
                "trustee":         "",
                "appraised_value": "",
                "annual_taxes":    "",
                "ps_doc_id":       "",
            }
            new_leads.append(lead)
            log.info(f"  STACKED [{i+1}/{len(props)}] {addr} — "
                     f"{len(violations)} CE violation(s): {viol_types[0][:40]}")
        else:
            if (i + 1) % 50 == 0:
                log.info(f"  Progress: {i+1}/{len(props)} checked | "
                         f"{ce_found} stacked | {ce_checked} queried")

        time.sleep(0.4)

    log.info(f"VBP CE check complete: {ce_checked} queried | "
             f"{ce_found} stacked leads | {skipped} skipped")

    # Write records — existing already has stamps applied above
    merged = existing + new_leads
    write_records(merged)
    log.info(f"records.json saved — {len(merged)} total records "
             f"({stamped} NOF stamped, {len(new_leads)} new VBP leads added)")

    # Update state
    state["last_vbp_count"] = vbp_count
    state["last_run"]       = RUN_TIMESTAMP
    state["checked"]        = checked
    save_state(state)

    log.info("Done.")


if __name__ == "__main__":
    main()
