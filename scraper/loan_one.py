"""
loan_one.py
One-off, read-only lookup for a single NOF doc: OCRs the Notice of Trustee's
Sale, hops to the Deed of Trust it references, OCRs that, and logs the loan
terms (principal, rate, dates, loan type, lender). Writes nothing to the repo.

Usage (via the "Loan Lookup (one doc)" workflow):
  DOC_NUMBER=20261100114 python scraper/loan_one.py
"""

import logging
import os
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent))

from fetch import (get_driver, goto_doc_by_docnumber, extract_loan_details,
                   login_publicsearch, ocr_current_doc_page, search_and_ocr_referenced_doc)

KEYWORDS = re.compile(
    r"(principal|interest rate|per annum|maturity|first payment|monthly payment|"
    r"payable|loan amount|note date|dated|FHA|VA |USDA|conventional|MERS|"
    r"mortgage electronic|case no|loan no|balloon|adjustable|initial rate)",
    re.IGNORECASE)


def snippets(text, limit=60):
    out = []
    for line in re.split(r"[\n\r]+", text or ""):
        line = line.strip()
        if 8 < len(line) < 400 and KEYWORDS.search(line):
            out.append(line)
        if len(out) >= limit:
            break
    return out


def main():
    doc = os.environ.get("DOC_NUMBER", "").strip()
    if not doc:
        log.error("DOC_NUMBER env var required")
        sys.exit(1)

    driver = get_driver()
    try:
        if not login_publicsearch(driver):
            log.warning("login failed or skipped")
        if not goto_doc_by_docnumber(driver, doc):
            log.error(f"could not open doc {doc}")
            sys.exit(1)

        notice = ocr_current_doc_page(driver) or ""
        log.info(f"NOTICE_OCR_CHARS={len(notice)}")
        for s in snippets(notice, 40):
            log.info(f"NOTICE| {s}")

        details = extract_loan_details(driver)
        log.info(f"EXTRACTED| {details}")

        m = re.search(r"Document\s+Number\s+(\d{6,})\s+on\s+([A-Za-z]+ \d{1,2},\s*\d{4})",
                      notice, re.IGNORECASE)
        if m:
            ref = m.group(1)
            log.info(f"REFERENCED_DOT={ref} dated {m.group(2)}")
            dot = search_and_ocr_referenced_doc(driver, ref) or ""
            log.info(f"DOT_OCR_CHARS={len(dot)}")
            for s in snippets(dot, 60):
                log.info(f"DOT| {s}")
        else:
            log.info("no referenced deed-of-trust doc number found in notice text")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
