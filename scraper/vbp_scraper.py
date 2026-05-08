"""
Bexar County VBP + CE Cross-Reference Scraper v1.0
Downloads SA Vacant Building Program PDF monthly,
filters to residential properties, then checks each address
against the SA 311 CE ArcGIS endpoint for open violations.
Results added to records.json as STACKED leads.

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
STATE_PATH   = Path("data/vbp_state.json")  # tracks last run + checked addresses

# Max sq ft for "small" residential — excludes large commercial/apartments
MAX_SQ_FT = 6000

# Owner keywords that indicate institutional/non-motivated owners — skip these
SKIP_OWNERS = [
    "city of san antonio", "county of bexar", "housing authority",
    "school district", "isd", "university", "board of regents",
    "church", "ministry", "ministries", "baptist", "methodist",
    "catholic", "presbyterian", "assembly of god", "temple",
    "via metropolitan", "cps energy", "saws", "sa river authority",
    "housing finance", "san antonio housing trust",
    "esperanza peace", "centro sa", "ymca",
]

# CE violation keywords that signal distressed/motivated
CE_KEYWORDS = [
    "dangerous premise", "property structure", "vacant", "minimum housing",
    "substandard", "unsecured", "structure maintenance",
]

RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

# ── PDF PARSER ─────────────────────────────────────────────────────────────────
def download_vbp_pdf():
    """Download VBP PDF and extract text."""
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
                    # Skip header rows
                    if not row[0] or any(h in str(row[0]).lower() for h in
                                         ["address", "vacant building", "please"]):
                        continue
                    try:
                        addr    = str(row[0] or "").strip()
                        zipcode = str(row[1] or "").strip()
                        sq_ft   = str(row[2] or "").strip().replace(",", "")
                        is_sf   = str(row[3] or "").strip().lower()
                        # Market listing and council district vary in position
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
    """Filter to motivated-seller residential properties."""
    filtered = []
    for p in properties:
        owner_lower = p["owner"].lower()

        # Skip institutional owners
        if any(skip in owner_lower for skip in SKIP_OWNERS):
            continue

        # Skip non-SF if large (commercial/large apartment)
        if not p["is_sf"] and p["sq_ft"] > MAX_SQ_FT:
            continue

        # Skip if sq_ft is 0 and not SF (unknown commercial)
        if p["sq_ft"] == 0 and not p["is_sf"]:
            continue

        filtered.append(p)

    log.info(f"VBP filtered: {len(filtered)}/{len(properties)} residential properties")
    return filtered


# ── CE CSV LOADER ──────────────────────────────────────────────────────────────
CE_CSV_URL = (
    "https://data.sanantonio.gov/dataset/93b0e7ee-3a55-4aa9-b27b-d1817e91aec3"
    "/resource/8cb8d6c9-93df-4c7a-b897-85793b21c60e"
    "/download/allservice_property-maintenance.csv"
)

def load_ce_csv():
    """
    Download SA 311 Property Maintenance CSV and build address index.
    Returns dict: normalized_street -> list of open violations
    """
    import csv, io
    log.info(f"Downloading CE property maintenance CSV...")
    try:
        req = urllib.request.Request(CE_CSV_URL, headers={
            "User-Agent": "Mozilla/5.0 (compatible; BexarLeads/1.0)"
        })
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode("utf-8", errors="replace")
        log.info(f"CE CSV downloaded: {len(raw):,} bytes")
    except Exception as e:
        log.warning(f"CE CSV download failed: {e}")
        return {}

    try:
        reader = csv.DictReader(io.StringIO(raw))
        rows = list(reader)
        log.info(f"CE CSV: {len(rows):,} property maintenance records")
        if rows:
            log.info(f"CE CSV columns: {list(rows[0].keys())}")
    except Exception as e:
        log.warning(f"CE CSV parse failed: {e}")
        return {}

    # Build index: normalized street number+name -> violations
    index = {}
    skipped = 0
    for row in rows:
        status   = (row.get("CaseStatus") or "").strip()
        typename = (row.get("TypeName") or "").strip()
        address  = (row.get("OBJECTDESC") or row.get("ObjectDescription") or "").strip()
        case_id  = (row.get("CASEID") or "").strip()
        opened   = (row.get("OPENEDDATETIME") or "").strip()

        if not address or not case_id:
            skipped += 1
            continue

        # Only keep motivated CE types
        tl = typename.lower()
        if not any(kw in tl for kw in CE_KEYWORDS):
            skipped += 1
            continue

        # Normalize address to "NUMBER STREETNAME" for matching
        # Format from CSV: " 5679 EASTERLING, SAN ANTONIO, 78251"
        addr_clean = address.strip().lstrip()
        parts = [p.strip() for p in addr_clean.split(",")]
        street = parts[0].upper() if parts else ""
        if not street or not re.match(r"^\d+\s", street):
            skipped += 1
            continue

        # Key = "NUMBER FIRSTWORD" for fuzzy matching
        words = street.split()
        key = " ".join(words[:2]) if len(words) >= 2 else street

        violation = {
            "case_id":  case_id,
            "typename": typename,
            "status":   status,
            "opened":   opened,
            "address":  street,
        }
        index.setdefault(key, []).append(violation)

    log.info(f"CE index built: {len(index):,} unique address keys ({skipped:,} rows skipped)")
    return index


def check_ce_for_address(street_address, ce_index):
    """
    Look up CE violations for a VBP address using the pre-loaded CSV index.
    Returns list of matching open violations.
    """
    if not ce_index or not street_address:
        return []

    words = street_address.upper().split()
    if len(words) < 2:
        return []

    key = " ".join(words[:2])
    violations = ce_index.get(key, [])
    # Filter to motivated types only
    return [v for v in violations if any(kw in v["typename"].lower() for kw in CE_KEYWORDS)]


# ── STATE MANAGEMENT ───────────────────────────────────────────────────────────
def load_state():
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {"last_vbp_count": 0, "last_run": "", "checked": {}}

def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("VBP + CE Cross-Reference Scraper v1.0")
    log.info("=" * 60)

    state = load_state()
    log.info(f"Last run: {state.get('last_run') or 'never'} | "
             f"Previously checked: {len(state.get('checked', {}))} addresses")

    # Download + parse PDF
    all_props = download_vbp_pdf()
    if not all_props:
        log.error("No properties from PDF — aborting")
        return

    # Check if VBP list has changed
    vbp_count = len(all_props)

    # Filter to residential
    props = filter_properties(all_props)

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

    # Skip CE check if VBP unchanged AND stacked leads already exist
    already_stacked = sum(1 for r in existing if r.get("type") == "VBP")
    if vbp_count == state.get("last_vbp_count") and state.get("last_run") and already_stacked > 0:
        last_run_dt = datetime.fromisoformat(state["last_run"])
        days_since = (datetime.now() - last_run_dt).days
        if days_since < 25:
            log.info(f"VBP unchanged ({vbp_count} props), {already_stacked} stacked leads exist, last run {days_since}d ago — skipping")
            log.info("Exiting without modifying records.json")
            save_state(state)
            return
        log.info(f"VBP unchanged but {days_since}d since last run — re-checking")
    elif already_stacked == 0:
        log.info(f"No stacked leads in records.json — forcing CE check")

    checked = state.get("checked", {})
    new_leads = []
    ce_checked = 0
    ce_found   = 0
    skipped    = 0

    # Load CE CSV once into memory
    ce_index = load_ce_csv()
    if not ce_index:
        log.warning("CE index empty — stacked matching will find 0 violations")

    log.info(f"Checking {len(props)} VBP addresses against CE index...")

    for i, prop in enumerate(props):
        addr    = prop["address"]
        zipcode = prop["zip"]
        doc_key = f"VBP-{addr.replace(' ', '-')}-{zipcode}"

        # Skip if already a lead
        if doc_key in existing_docs:
            skipped += 1
            continue

        # Skip if recently checked with no violations
        if addr in checked and checked[addr].get("checked_at"):
            checked_dt = datetime.fromisoformat(checked[addr]["checked_at"])
            if (datetime.now() - checked_dt).days < 25 and not checked[addr].get("violations"):
                skipped += 1
                continue

        violations = check_ce_for_address(addr, ce_index)
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

            score = 8  # High — vacant + CE violation
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
            if (i + 1) % 100 == 0:
                log.info(f"  Progress: {i+1}/{len(props)} checked | "
                         f"{ce_found} stacked | {ce_checked} queried")



    log.info(f"VBP CE check complete: {ce_checked} queried | "
             f"{ce_found} stacked leads | {skipped} skipped")

    # Save new leads to records.json atomically
    if new_leads:
        merged = existing + new_leads
        # Validate JSON before writing
        test = json.dumps(merged)
        json.loads(test)  # will raise if invalid
        # Write to temp first, then move
        tmp = RECORDS_PATH.with_suffix('.tmp')
        tmp.write_text(test)
        tmp.replace(RECORDS_PATH)
        log.info(f"Added {len(new_leads)} stacked VBP+CE leads → {len(merged)} total in records.json")
    else:
        log.info("No new stacked leads found — records.json unchanged")

    # Update state
    state["last_vbp_count"] = vbp_count
    state["last_run"]       = RUN_TIMESTAMP
    state["checked"]        = checked
    save_state(state)

    log.info("Done.")


if __name__ == "__main__":
    main()
