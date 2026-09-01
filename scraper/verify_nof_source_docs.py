"""
verify_nof_source_docs.py

One-time audit: for every active NOF/TAX lead with a sale date of Oct 1,
2026 or later, look up the actual foreclosure notice document on
bexar.tx.publicsearch.us by doc_number (via the same goto_doc_by_docnumber
hop the daily scraper already uses successfully for loan enrichment), OCR
it, and cross-check the real "Property Address" against what we have on
file.

Triggered 2026-09-01 after two confirmed cases of bad source data:
  - doc 20261000205 ("11311 Yuba Trail") didn't exist anywhere in the
    county's own search at any date range -- a fabricated row.
  - doc 20260900176 (13526 Hickory Legend) had a wrong owner attached via
    the (now-fixed) loose BCAD matching, which had already gone out in a
    live SMS under the wrong name before being caught.

BCAD cross-checks the owner; this checks the address against the actual
source document, since address is what BCAD matching depends on in the
first place -- garbage in, garbage out otherwise.

Policy: doc-number lookups that come back with zero results are an
unambiguous "this row is fabricated" signal (already confirmed once) and
get flagged for removal automatically. Address MISMATCHES are written to
a report for manual review, not auto-corrected -- OCR text is noisier
than the BCAD API this session already leaned on, and a wrong auto-write
into a live GHL contact is worse than a manual review pass.

Usage:
  python scraper/verify_nof_source_docs.py
"""
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path

import fetch

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RECORDS_PATH = Path("dashboard/records.json")
REPORT_PATH = Path("scraper/nof_source_audit_report.json")
CHECKPOINT_PATH = Path("scraper/nof_source_audit_checkpoint.json")


def normalize_addr(s):
    return " ".join(str(s or "").upper().split())


def addr_core_matches(stored, real):
    """House number + first street word must both appear -- cheap, robust
    check given OCR noise; doesn't need a full match."""
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


def extract_grantor(ocr_text):
    m = re.search(
        r"Grantor\(s\)\s*/?\s*Mortgagor\(s\):?\s*\n?\s*([^\n]+)",
        ocr_text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def main():
    records = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))

    def sale_dt(r):
        try:
            return datetime.strptime(r.get("sale_date", "").strip(), "%m/%d/%Y")
        except Exception:
            return None

    targets = [
        r for r in records
        if r.get("type") in ("NOF", "TAX")
        and r.get("doc_number")
        and sale_dt(r) and sale_dt(r) >= datetime(2026, 10, 1)
    ]
    log.info(f"Auditing {len(targets)} NOF/TAX leads with sale_date >= 2026-10-01")

    done_docs = set()
    results = []
    if CHECKPOINT_PATH.exists():
        prev = json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        results = prev.get("results", [])
        done_docs = {r["doc_number"] for r in results}
        log.info(f"Resuming from checkpoint: {len(done_docs)} already done")

    driver = fetch.get_driver()
    try:
        fetch.login_publicsearch(driver)

        for i, rec in enumerate(targets):
            doc_num = rec["doc_number"]
            if doc_num in done_docs:
                continue

            stored_addr = rec.get("address", "")
            row = {"doc_number": doc_num, "stored_address": stored_addr,
                   "stored_owner": rec.get("owner", "")}

            found = fetch.goto_doc_by_docnumber(driver, doc_num)
            if not found:
                row["status"] = "doc_not_found"
                log.warning(f"  [{i+1}/{len(targets)}] {doc_num} ({stored_addr}): NOT FOUND")
            else:
                text = fetch.ocr_current_doc_page(driver)
                real_addr = extract_property_address(text)
                real_owner = extract_grantor(text)
                row["real_address"] = real_addr
                row["real_owner"] = real_owner
                if not real_addr:
                    row["status"] = "ocr_no_address"
                    log.warning(f"  [{i+1}/{len(targets)}] {doc_num}: OCR found doc but no "
                                f"Property Address matched — first 200 chars: {text[:200]!r}")
                elif addr_core_matches(stored_addr, real_addr):
                    row["status"] = "match"
                    log.info(f"  [{i+1}/{len(targets)}] {doc_num}: OK ({real_addr})")
                else:
                    row["status"] = "address_mismatch"
                    log.warning(f"  [{i+1}/{len(targets)}] {doc_num}: MISMATCH — "
                                f"stored={stored_addr!r} real={real_addr!r}")

            results.append(row)
            time.sleep(1)

            if (i + 1) % 20 == 0:
                CHECKPOINT_PATH.write_text(json.dumps({"results": results}, ensure_ascii=False, indent=2),
                                            encoding="utf-8")
                log.info(f"  checkpoint saved ({len(results)} done)")

    finally:
        driver.quit()

    CHECKPOINT_PATH.write_text(json.dumps({"results": results}, ensure_ascii=False, indent=2), encoding="utf-8")

    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    log.info(f"DONE. {counts}")

    REPORT_PATH.write_text(json.dumps({"counts": counts, "results": results}, ensure_ascii=False, indent=2),
                            encoding="utf-8")
    log.info(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
