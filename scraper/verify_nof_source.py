"""
verify_nof_source.py

Nightly, incremental version of the 2026-09-01 one-time audit
(verify_nof_source_docs.py). For active NOF/TAX leads that have never
been checked, looks up the real foreclosure notice on
bexar.tx.publicsearch.us by doc_number and cross-checks the actual
Property Address against what's on file. Runs every night so this class
of bug (a fabricated doc_number, or an address that never matched the
real filing in the first place) gets caught automatically instead of by
hand during a manual audit -- see 2026-09-01: a bad address match went
out in a live SMS under the wrong homeowner's name before anyone caught
it.

Budget-capped per run (like ARV/loan enrichment) so this stays a normal
part of the nightly cycle rather than a 2-hour job -- clears the existing
backlog over consecutive nights, and every new lead gets checked within
a night or two of being scraped.

Writes results directly onto each record:
  source_verify_status: "ok" | "mismatch" | "unclear"
  source_verified_at:   ISO timestamp

doc_not_found is unambiguous (confirmed 2026-09-01 for a real case) and
the record is removed outright. address_mismatch / ocr_no_address get
flagged (flags += "SOURCE MISMATCH") for manual review, NOT auto-
corrected -- OCR is noisier than the BCAD API this codebase otherwise
leans on, and a wrong auto-write into a live GHL contact is worse than a
flagged review.

Usage:
  python scraper/verify_nof_source.py
"""
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import fetch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RECORDS_PATH = Path("dashboard/records.json")
NIGHTLY_LIMIT = 60


def normalize_addr(s):
    return " ".join(str(s or "").upper().split())


def addr_core_matches(stored, real):
    s = normalize_addr(stored).split()
    r = normalize_addr(real).split()
    if not s or not r:
        return False
    if s[0] != r[0]:
        return False
    return len(s) < 2 or len(r) < 2 or s[1] in r


def extract_property_address(ocr_text):
    m = re.search(r"Property\s+Address:?\s*\n?\s*([^\n]+)", ocr_text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def main():
    records = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))

    def sale_dt(r):
        try:
            return datetime.strptime(r.get("sale_date", "").strip(), "%m/%d/%Y")
        except Exception:
            return None

    today = datetime.now().date()
    candidates = [
        r for r in records
        if r.get("type") in ("NOF", "TAX")
        and r.get("doc_number")
        and not r.get("source_verify_status")
        and sale_dt(r) and sale_dt(r).date() >= today
    ]
    log.info(f"{len(candidates)} unverified active NOF/TAX leads total")

    batch = candidates[:NIGHTLY_LIMIT]
    log.info(f"Checking {len(batch)} this run (capped at {NIGHTLY_LIMIT})")

    if not batch:
        log.info("Nothing to verify — skipping")
        return

    to_remove = set()
    checked = {"match": 0, "mismatch": 0, "ocr_no_address": 0, "doc_not_found": 0}

    driver = fetch.get_driver()
    try:
        fetch.login_publicsearch(driver)

        for i, rec in enumerate(batch):
            doc_num = rec["doc_number"]
            stored_addr = rec.get("address", "")
            now_iso = datetime.now(timezone.utc).isoformat()

            found = fetch.goto_doc_by_docnumber(driver, doc_num)
            if not found:
                checked["doc_not_found"] += 1
                to_remove.add(doc_num)
                log.warning(f"  [{i+1}/{len(batch)}] {doc_num} ({stored_addr}): NOT FOUND — removing")
                time.sleep(1)
                continue

            text = fetch.ocr_current_doc_page(driver)
            real_addr = extract_property_address(text)

            if not real_addr:
                rec["source_verify_status"] = "unclear"
                rec["source_verified_at"] = now_iso
                checked["ocr_no_address"] += 1
                log.warning(f"  [{i+1}/{len(batch)}] {doc_num}: OCR found doc but no address matched")
            elif addr_core_matches(stored_addr, real_addr):
                rec["source_verify_status"] = "ok"
                rec["source_verified_at"] = now_iso
                checked["match"] += 1
                log.info(f"  [{i+1}/{len(batch)}] {doc_num}: OK")
            else:
                rec["source_verify_status"] = "mismatch"
                rec["source_verified_at"] = now_iso
                rec.setdefault("flags", [])
                if "SOURCE MISMATCH" not in rec["flags"]:
                    rec["flags"].append("SOURCE MISMATCH")
                checked["mismatch"] += 1
                log.warning(f"  [{i+1}/{len(batch)}] {doc_num}: MISMATCH — "
                            f"stored={stored_addr!r} real={real_addr!r}")

            time.sleep(1)

    finally:
        driver.quit()

    if to_remove:
        records = [r for r in records if r.get("doc_number") not in to_remove]

    log.info(f"DONE. {checked} | removed={len(to_remove)}")
    RECORDS_PATH.write_text(json.dumps(records, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log.info("records.json saved")


if __name__ == "__main__":
    main()
