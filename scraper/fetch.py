"""
Bexar County Motivated Seller Lead Scraper
==========================================
Scrapes the Bexar County Clerk portal and BCAD parcel data to identify
motivated seller leads (lis pendens, foreclosures, tax deeds, judgments,
liens, probate, etc.) filed within the last LOOKBACK_DAYS days.

Outputs:
  dashboard/records.json
  data/records.json
  data/leads_export.csv  (GHL-ready)
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dbfread import DBF

# ---------------------------------------------------------------------------
# Optional Playwright import (graceful fallback for environments without it)
# ---------------------------------------------------------------------------
try:
    from playwright.async_api import async_playwright, Page, Browser
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not installed – clerk portal scraping disabled.")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants / Configuration
# ---------------------------------------------------------------------------
LOOKBACK_DAYS: int = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE = "https://bexar.tx.publicsearch.us"
CLERK_SEARCH = f"{CLERK_BASE}/results"
BCAD_BASE = "https://esearch.bcad.org"

# Doc-type codes → (category_key, human label)
DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":      ("LP",      "Lis Pendens"),
    "NOFC":    ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED": ("TAXDEED", "Tax Deed"),
    "JUD":     ("JUD",     "Judgment"),
    "CCJ":     ("JUD",     "Certified Judgment"),
    "DRJUD":   ("JUD",     "Domestic Judgment"),
    "LNCORPTX":("LIEN",   "Corp Tax Lien"),
    "LNIRS":   ("LIEN",   "IRS Lien"),
    "LNFED":   ("LIEN",   "Federal Lien"),
    "LN":      ("LIEN",   "Lien"),
    "LNMECH":  ("LIEN",   "Mechanic Lien"),
    "LNHOA":   ("LIEN",   "HOA Lien"),
    "MEDLN":   ("LIEN",   "Medicaid Lien"),
    "PRO":     ("PRO",    "Probate Document"),
    "NOC":     ("NOC",    "Notice of Commencement"),
    "RELLP":   ("RELLP",  "Release Lis Pendens"),
}

# All doc-type codes we want to search
TARGET_DOC_TYPES = list(DOC_TYPE_MAP.keys())

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def retry(func):
    """Decorator: retry a synchronous function up to MAX_RETRIES times."""
    def wrapper(*args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                log.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, func.__name__, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        log.error("All %d attempts failed for %s", MAX_RETRIES, func.__name__)
        return None
    return wrapper


def clean(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def parse_amount(text: str) -> float:
    """Extract a dollar amount from a string like '$1,234.56'."""
    text = clean(text)
    cleaned = re.sub(r"[^\d.]", "", text)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def normalize_name(name: str) -> list[str]:
    """Return multiple normalized variants of an owner name for lookup."""
    name = name.strip().upper()
    variants = [name]
    # "LAST, FIRST" → try "FIRST LAST" and "LAST FIRST"
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            last, first = parts
            variants.append(f"{first} {last}")
            variants.append(f"{last} {first}")
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            # try reversed
            variants.append(" ".join(reversed(tokens)))
            variants.append(f"{tokens[-1]}, {' '.join(tokens[:-1])}")
    return list(dict.fromkeys(variants))  # dedupe, preserve order


def date_range_str(days: int) -> tuple[str, str]:
    """Return (start_date, end_date) as MM/DD/YYYY strings."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    fmt = "%m/%d/%Y"
    return start.strftime(fmt), end.strftime(fmt)


def is_new_this_week(filed_str: str) -> bool:
    """Return True if the filed date is within the last 7 days."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            filed_dt = datetime.strptime(filed_str, fmt)
            return (datetime.now() - filed_dt).days <= 7
        except ValueError:
            continue
    return False


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def compute_flags(record: dict) -> list[str]:
    flags: list[str] = []
    cat = record.get("cat", "")
    owner = record.get("owner", "").upper()
    amount = record.get("amount", 0.0) or 0.0
    filed = record.get("filed", "")

    if cat == "LP":
        flags.append("Lis pendens")
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "JUD":
        flags.append("Judgment lien")
    if cat == "LIEN" and record.get("doc_type", "") in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if record.get("doc_type", "") == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|HOLDINGS?)\b", owner):
        flags.append("LLC / corp owner")
    if is_new_this_week(filed):
        flags.append("New this week")
    return flags


def compute_score(record: dict, flags: list[str]) -> int:
    score = 30  # base
    score += len(flags) * 10

    # LP + FC combo bonus
    all_cats_for_owner = record.get("_owner_cats", [])
    if "LP" in all_cats_for_owner and "NOFC" in all_cats_for_owner:
        score += 20

    amount = record.get("amount", 0.0) or 0.0
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10

    if "New this week" in flags:
        score += 5
    if record.get("prop_address"):
        score += 5

    return min(score, 100)


# ---------------------------------------------------------------------------
# BCAD Parcel Data Downloader
# ---------------------------------------------------------------------------

class BCAdParcelLoader:
    """
    Downloads the bulk parcel data from BCAD and builds an owner→address
    lookup dictionary.
    """

    PARCEL_URL = f"{BCAD_BASE}/downloads"
    # Known direct download paths (may change; we also try scraping the page)
    CANDIDATE_PATHS = [
        "/downloads/parcel_data.zip",
        "/downloads/Parcels.zip",
        "/downloads/BCAD_Parcels.zip",
        "/downloads/parcel.zip",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
        )
        # owner_name_upper → {prop_address, prop_city, prop_zip, mail_address, mail_city, mail_state, mail_zip}
        self.lookup: dict[str, dict] = {}

    @retry
    def _get_download_url(self) -> str | None:
        """Scrape BCAD downloads page to find the parcel DBF/ZIP link."""
        try:
            resp = self.session.get(self.PARCEL_URL, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if re.search(r"parcel.*\.(zip|dbf)", href, re.I):
                    if href.startswith("http"):
                        return href
                    return BCAD_BASE + href
        except Exception as exc:
            log.warning("Could not scrape BCAD downloads page: %s", exc)

        # Try candidate paths
        for path in self.CANDIDATE_PATHS:
            url = BCAD_BASE + path
            try:
                r = self.session.head(url, timeout=15, allow_redirects=True)
                if r.status_code == 200:
                    return url
            except Exception:
                continue
        return None

    @retry
    def _download_zip(self, url: str) -> bytes | None:
        """Download ZIP file content."""
        log.info("Downloading parcel data from %s", url)
        resp = self.session.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        return resp.content

    def _parse_dbf(self, data: bytes) -> None:
        """Parse DBF bytes and populate self.lookup."""
        # Write to temp file since dbfread needs a path
        tmp_path = Path("/tmp/parcels.dbf")
        tmp_path.write_bytes(data)
        try:
            table = DBF(str(tmp_path), lowernames=True, ignore_missing_memofile=True)
            for row in table:
                # Normalize column names
                r = {k.lower(): clean(v) for k, v in row.items()}

                # Owner name variants
                owner = r.get("owner") or r.get("own1") or r.get("ownername") or ""

                # Site address
                site_addr = r.get("site_addr") or r.get("siteaddr") or r.get("situs") or ""
                site_city = r.get("site_city") or r.get("sitecity") or ""
                site_zip  = r.get("site_zip")  or r.get("sitezip")  or r.get("zip") or ""

                # Mail address
                mail_addr = r.get("addr_1") or r.get("mailadr1") or r.get("mail_addr") or ""
                mail_city = r.get("city") or r.get("mailcity") or ""
                mail_state= r.get("state") or r.get("mailstate") or "TX"
                mail_zip  = r.get("zip") or r.get("mailzip") or ""

                if not owner:
                    continue

                entry = {
                    "prop_address": site_addr,
                    "prop_city":    site_city,
                    "prop_state":   "TX",
                    "prop_zip":     site_zip,
                    "mail_address": mail_addr,
                    "mail_city":    mail_city,
                    "mail_state":   mail_state,
                    "mail_zip":     mail_zip,
                }

                for variant in normalize_name(owner):
                    self.lookup[variant] = entry
        finally:
            tmp_path.unlink(missing_ok=True)

    def load(self) -> None:
        """Main entry point – download and parse parcel data."""
        url = self._get_download_url()
        if not url:
            log.warning("Could not find BCAD parcel download URL. Address enrichment disabled.")
            return

        raw = self._download_zip(url)
        if not raw:
            log.warning("Could not download parcel data.")
            return

        # Handle ZIP containing DBF
        try:
            with zipfile.ZipFile(BytesIO(raw)) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    log.warning("No .dbf file found inside ZIP.")
                    return
                dbf_data = zf.read(dbf_names[0])
        except zipfile.BadZipFile:
            # Maybe it's a raw DBF
            dbf_data = raw

        self._parse_dbf(dbf_data)
        log.info("Loaded %d parcel owner records.", len(self.lookup))

    def get_address(self, owner: str) -> dict:
        """Look up address info for an owner name, trying all variants."""
        for variant in normalize_name(owner):
            if variant in self.lookup:
                return self.lookup[variant]
        return {}


# ---------------------------------------------------------------------------
# Clerk Portal Scraper (Playwright)
# ---------------------------------------------------------------------------

class ClerkScraper:
    """
    Scrapes the Bexar County Clerk portal for documents of specified types
    filed within the date range.
    """

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date
        self.records: list[dict] = []

    async def _search_doc_type(self, page: Page, doc_type: str) -> list[dict]:
        """Search for a single document type and return raw records."""
        results: list[dict] = []
        try:
            await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=60_000)

            # Fill search form fields (selectors may need adjustment based on actual portal)
            # Try to find date range inputs
            await page.wait_for_selector("input[name*='beginDate'], input[name*='startDate'], #beginDate, #startDate, [placeholder*='Start']", timeout=15_000)

            # Document type
            dt_selectors = [
                f"select[name*='docType'] option[value='{doc_type}']",
                f"#docType option[value='{doc_type}']",
            ]
            for sel in dt_selectors:
                try:
                    await page.locator(f"select[name*='docType'], #docType").select_option(value=doc_type)
                    break
                except Exception:
                    pass

            # Start date
            for sel in ["input[name*='beginDate']", "#beginDate", "input[name*='startDate']", "#startDate"]:
                try:
                    await page.fill(sel, self.start_date)
                    break
                except Exception:
                    pass

            # End date
            for sel in ["input[name*='endDate']", "#endDate"]:
                try:
                    await page.fill(sel, self.end_date)
                    break
                except Exception:
                    pass

            # Submit
            for sel in ["button[type='submit']", "input[type='submit']", "#searchBtn", "#btnSearch"]:
                try:
                    await page.click(sel)
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                    break
                except Exception:
                    pass

            # Paginate through results
            page_num = 0
            while True:
                page_num += 1
                records_on_page = await self._parse_results_page(page, doc_type)
                results.extend(records_on_page)

                if not records_on_page:
                    break

                # Try to click "Next" page
                try:
                    next_btn = page.locator("a:has-text('Next'), button:has-text('Next'), [aria-label='Next page']")
                    if await next_btn.count() == 0:
                        break
                    await next_btn.first.click()
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                except Exception:
                    break

        except Exception as exc:
            log.warning("Error searching doc type %s: %s", doc_type, exc)

        return results

    async def _parse_results_page(self, page: Page, doc_type: str) -> list[dict]:
        """Extract records from the current search results page."""
        records: list[dict] = []
        try:
            content = await page.content()
            soup = BeautifulSoup(content, "lxml")

            # Try common table patterns
            table = soup.find("table", {"id": re.compile(r"result|grid|search", re.I)})
            if not table:
                table = soup.find("table")
            if not table:
                return records

            rows = table.find_all("tr")
            if not rows:
                return records

            # Parse header
            header_row = rows[0]
            headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

            def col(row_cells, *names) -> str:
                for name in names:
                    for i, h in enumerate(headers):
                        if name in h and i < len(row_cells):
                            return clean(row_cells[i].get_text(strip=True))
                return ""

            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue

                # Extract link for doc number
                link_tag = row.find("a", href=True)
                clerk_url = ""
                doc_num = ""
                if link_tag:
                    href = link_tag["href"]
                    if not href.startswith("http"):
                        href = CLERK_BASE + href
                    clerk_url = href
                    doc_num = clean(link_tag.get_text(strip=True))

                if not doc_num:
                    doc_num = col(cells, "doc", "instrument", "number", "record")

                filed = col(cells, "date", "filed", "record date")
                grantor = col(cells, "grantor", "owner", "from")
                grantee = col(cells, "grantee", "to", "beneficiary")
                legal = col(cells, "legal", "description", "property")
                amount_str = col(cells, "amount", "consideration", "value")

                if not doc_num and not filed:
                    continue

                cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_type,
                    "filed":     filed,
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     grantor,
                    "grantee":   grantee,
                    "amount":    parse_amount(amount_str),
                    "legal":     legal,
                    "clerk_url": clerk_url,
                })

        except Exception as exc:
            log.warning("Error parsing results page: %s", exc)

        return records

    async def run(self) -> list[dict]:
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright not available. Falling back to HTTP scraper.")
            return await self._http_fallback()

        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            for doc_type in TARGET_DOC_TYPES:
                log.info("Searching clerk portal for doc type: %s", doc_type)
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        recs = await self._search_doc_type(page, doc_type)
                        log.info("  → %d records found for %s", len(recs), doc_type)
                        self.records.extend(recs)
                        break
                    except Exception as exc:
                        log.warning("  Attempt %d failed for %s: %s", attempt, doc_type, exc)
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(RETRY_DELAY)

            await browser.close()

        return self.records

    async def _http_fallback(self) -> list[dict]:
        """HTTP-based fallback scraper using requests + BeautifulSoup."""
        records: list[dict] = []
        session = requests.Session()
        session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"

        for doc_type in TARGET_DOC_TYPES:
            log.info("[HTTP] Searching for doc type: %s", doc_type)
            try:
                params = {
                    "type":      "PT",
                    "docType":   doc_type,
                    "beginDate": self.start_date,
                    "endDate":   self.end_date,
                    "county":    "Bexar",
                }
                resp = session.get(CLERK_SEARCH, params=params, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")

                table = soup.find("table")
                if not table:
                    continue

                rows = table.find_all("tr")
                headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])] if rows else []

                def col(cells, *names) -> str:
                    for name in names:
                        for i, h in enumerate(headers):
                            if name in h and i < len(cells):
                                return clean(cells[i].get_text(strip=True))
                    return ""

                for row in rows[1:]:
                    cells = row.find_all("td")
                    if not cells:
                        continue

                    link_tag = row.find("a", href=True)
                    clerk_url = ""
                    doc_num = ""
                    if link_tag:
                        href = link_tag["href"]
                        if not href.startswith("http"):
                            href = CLERK_BASE + href
                        clerk_url = href
                        doc_num = clean(link_tag.get_text(strip=True))

                    if not doc_num:
                        doc_num = col(cells, "doc", "instrument", "number")

                    filed = col(cells, "date", "filed")
                    grantor = col(cells, "grantor", "owner", "from")
                    grantee = col(cells, "grantee", "to")
                    legal = col(cells, "legal", "description")
                    amount_str = col(cells, "amount", "consideration")

                    if not doc_num and not filed:
                        continue

                    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
                    records.append({
                        "doc_num":   doc_num,
                        "doc_type":  doc_type,
                        "filed":     filed,
                        "cat":       cat,
                        "cat_label": cat_label,
                        "owner":     grantor,
                        "grantee":   grantee,
                        "amount":    parse_amount(amount_str),
                        "legal":     legal,
                        "clerk_url": clerk_url,
                    })

            except Exception as exc:
                log.warning("[HTTP] Error for %s: %s", doc_type, exc)

        return records


# ---------------------------------------------------------------------------
# Enrichment: merge parcel address data
# ---------------------------------------------------------------------------

def enrich_records(records: list[dict], parcel: BCAdParcelLoader) -> list[dict]:
    """Add property/mailing addresses and compute flags/scores."""
    # Build a set of (owner, cat) tuples so we can detect LP+FC combos
    owner_cats: dict[str, list[str]] = {}
    for r in records:
        owner = r.get("owner", "").upper()
        if owner:
            owner_cats.setdefault(owner, []).append(r.get("cat", ""))

    enriched: list[dict] = []
    for r in records:
        try:
            owner = r.get("owner", "")
            addr_info = parcel.get_address(owner)

            r["prop_address"] = addr_info.get("prop_address", "")
            r["prop_city"]    = addr_info.get("prop_city", "San Antonio")
            r["prop_state"]   = addr_info.get("prop_state", "TX")
            r["prop_zip"]     = addr_info.get("prop_zip", "")
            r["mail_address"] = addr_info.get("mail_address", "")
            r["mail_city"]    = addr_info.get("mail_city", "")
            r["mail_state"]   = addr_info.get("mail_state", "TX")
            r["mail_zip"]     = addr_info.get("mail_zip", "")

            # Cross-record cat list for combo detection
            r["_owner_cats"] = owner_cats.get(owner.upper(), [])

            flags = compute_flags(r)
            score = compute_score(r, flags)

            r["flags"] = flags
            r["score"] = score

            # Remove internal helper key
            del r["_owner_cats"]

            enriched.append(r)
        except Exception as exc:
            log.warning("Error enriching record %s: %s", r.get("doc_num", "?"), exc)
            enriched.append(r)

    return enriched


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def build_output(records: list[dict], start_date: str, end_date: str) -> dict:
    with_address = sum(1 for r in records if r.get("prop_address"))
    return {
        "fetched_at":    datetime.now(timezone.utc).isoformat(),
        "source":        "Bexar County Clerk / BCAD",
        "date_range":    {"start": start_date, "end": end_date},
        "total":         len(records),
        "with_address":  with_address,
        "records":       records,
    }


def save_json(data: dict, *paths: str) -> None:
    for path_str in paths:
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        log.info("Saved JSON → %s (%d records)", path, data["total"])


def save_ghl_csv(records: list[dict], path_str: str) -> None:
    """Export a GHL-ready CSV."""
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed",
        "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in records:
            owner = r.get("owner", "")
            parts = owner.split(",", 1) if "," in owner else owner.split(" ", 1)
            first = parts[1].strip() if len(parts) > 1 else ""
            last  = parts[0].strip()

            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", "TX"),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", "TX"),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", r.get("cat", "")),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount", ""),
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "|".join(r.get("flags", [])),
                "Source":                 "Bexar County Clerk",
                "Public Records URL":     r.get("clerk_url", ""),
            })

    log.info("Saved GHL CSV → %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    start_date, end_date = date_range_str(LOOKBACK_DAYS)
    log.info("Date range: %s → %s", start_date, end_date)

    # 1. Load parcel data
    log.info("Loading BCAD parcel data...")
    parcel = BCAdParcelLoader()
    parcel.load()

    # 2. Scrape clerk portal
    log.info("Scraping Bexar County Clerk portal...")
    scraper = ClerkScraper(start_date, end_date)
    raw_records = await scraper.run()
    log.info("Total raw records scraped: %d", len(raw_records))

    # 3. Enrich
    log.info("Enriching records with parcel data and scoring...")
    records = enrich_records(raw_records, parcel)

    # 4. Sort by score descending
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # 5. Save outputs
    output = build_output(records, start_date, end_date)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")

    log.info("Done. %d leads saved (%d with address).", output["total"], output["with_address"])
    log.info("Top score: %d", records[0]["score"] if records else 0)


if __name__ == "__main__":
    asyncio.run(main())
