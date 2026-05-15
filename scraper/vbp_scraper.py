"""
Bexar County VBP + CE Cross-Reference Scraper v1.3 ONE-SHOT ENRICHMENT
This version re-checks the 253 confirmed CE addresses and writes full
violation details (case ID, type, status, opened date) onto existing
VBP records in records.json.

After this run confirms correct data in dashboard, replace with v1.2
for normal monthly operation.

Run: python scraper/vbp_scraper.py
"""

import os, re, json, time, logging, urllib.request, urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

VBP_PDF_URL  = "https://docsonline.sanantonio.gov/DSDUploads/VBPInventory.pdf"
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


def download_vbp_pdf():
    try:
        import pdfplumber
    except ImportError:
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
                        opened_str = datetime.utcfromtimestamp(int(opened_ms)/1000).strftime("%m/%d/%Y")
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


def ms_to_date(ms):
    if not ms:
        return ""
    try:
        return datetime.utcfromtimestamp(int(ms)/1000).strftime("%m/%d/%Y")
    except Exception:
        return ""


def main():
    log.info("=" * 60)
    log.info("VBP + CE Cross-Reference Scraper v1.3 — ONE-SHOT ENRICHMENT")
    log.info("=" * 60)

    state = load_state()
    log.info(f"Last run: {state.get('last_run') or 'never (reset for enrichment)'} | "
             f"Checked entries in state: {len(state.get('checked', {}))}")

    # Download + parse PDF
    all_props = download_vbp_pdf()
    if not all_props:
        log.error("No properties from PDF — aborting")
        return

    vbp_count = len(all_props)
    props     = filter_properties(all_props)
    vbp_addr_map = {p["address"]: p for p in props}

    # Load existing records
    try:
        existing = json.loads(RECORDS_PATH.read_text())
        log.info(f"Loaded {len(existing)} existing records from {RECORDS_PATH}")
    except Exception as e:
        log.error(f"ABORT: Could not load {RECORDS_PATH}: {e}")
        return

    existing_docs = {r["doc_number"] for r in existing}
    log.info(f"Existing doc numbers: {len(existing_docs)}")

    # Clean any incorrectly stamped NOF/TAX records
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
    log.info(f"Cleaned stacked flag from {cleaned} NOF/TAX records")

    # Build index of existing VBP records by address for fast lookup
    vbp_by_addr = {}
    for r in existing:
        if r.get("type") == "VBP":
            vbp_by_addr[r.get("address", "").upper().strip()] = r

    log.info(f"Existing VBP records indexed: {len(vbp_by_addr)}")

    # CE check — only addresses NOT in state.checked (the 253 confirmed ones we cleared)
    checked = state.get("checked", {})
    new_leads    = []
    enriched     = 0
    ce_checked   = 0
    ce_found     = 0
    skipped      = 0

    log.info(f"Starting CE enrichment check for {len(props)} VBP addresses...")
    log.info(f"Addresses already in state (skip): {len(checked)}")
    log.info(f"Addresses to re-check: {len(props) - len([p for p in props if p['address'] in checked])}")

    for i, prop in enumerate(props):
        addr    = prop["address"]
        zipcode = prop["zip"]
        doc_key = f"VBP-{addr.replace(' ', '-')}-{zipcode}"

        # Skip if already in checked state (the false ones we kept)
        if addr in checked:
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
            flags_base = ["VACANT STRUCT", "CODE ENFORCE", "STACKED"]
            is_dangerous = any("dangerous" in t.lower() for t in viol_types)
            if is_dangerous:
                flags_base.append("DANGEROUS PREMISES")

            score = 8
            if is_dangerous:
                score += 2

            # First violation for display fields
            first_viol = violations[0]
            ce_cat_label = first_viol["typename"]
            ce_status    = first_viol["status"]
            opened_date  = first_viol["opened"]

            if doc_key in existing_docs:
                # Enrich existing VBP record in place
                if addr in vbp_by_addr:
                    r = vbp_by_addr[addr]
                    r["stacked"]       = True
                    r["ce_violations"] = violations
                    r["ce_viol_types"] = viol_types
                    r["ce_count"]      = len(violations)
                    r["ce_cat_label"]  = ce_cat_label
                    r["ce_status"]     = ce_status
                    r["opened_date"]   = opened_date
                    r["ce_case_id"]    = first_viol["case_id"]
                    # Update flags
                    existing_flags = r.get("flags") or []
                    for f in flags_base:
                        if f not in existing_flags:
                            existing_flags.append(f)
                    r["flags"] = existing_flags
                    r["score"] = max(r.get("score", 0), score)
                    enriched += 1
                    log.info(f"  ENRICHED [{i+1}/{len(props)}] {addr} — "
                             f"{len(violations)} violation(s): {ce_cat_label[:40]}")
            else:
                # New VBP lead not yet in records.json
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
                    "flags":           flags_base,
                    "score":           score,
                    "sq_ft":           prop["sq_ft"],
                    "is_sf":           prop["is_sf"],
                    "ce_violations":   violations,
                    "ce_viol_types":   viol_types,
                    "ce_count":        len(violations),
                    "ce_cat_label":    ce_cat_label,
                    "ce_status":       ce_status,
                    "opened_date":     opened_date,
                    "ce_case_id":      first_viol["case_id"],
                    "loan_amount":     "",
                    "loan_date":       "",
                    "lender":          "",
                    "trustee":         "",
                    "appraised_value": "",
                    "annual_taxes":    "",
                    "ps_doc_id":       "",
                }
                new_leads.append(lead)
                log.info(f"  NEW STACKED [{i+1}/{len(props)}] {addr} — "
                         f"{len(violations)} violation(s): {ce_cat_label[:40]}")
        else:
            if (i + 1) % 50 == 0:
                log.info(f"  Progress: {i+1}/{len(props)} | "
                         f"{ce_found} found | {ce_checked} queried | {skipped} skipped")

        time.sleep(0.4)

    log.info(f"CE enrichment complete: {ce_checked} queried | "
             f"{ce_found} with violations | {skipped} skipped (already clean) | "
             f"{enriched} existing VBP records enriched | {len(new_leads)} new leads")

    # Merge and write
    merged = existing + new_leads
    write_records(merged)
    log.info(f"records.json saved — {len(merged)} total records")
    log.info(f"Added {len(new_leads)} stacked VBP+CE leads")

    # Update state
    state["last_vbp_count"] = vbp_count
    state["last_run"]       = RUN_TIMESTAMP
    state["checked"]        = checked
    save_state(state)

    log.info(f"Summary: cleaned={cleaned} enriched={enriched} new={len(new_leads)}")
    log.info("Done.")


if __name__ == "__main__":
    main()
