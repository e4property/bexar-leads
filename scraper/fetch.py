"""
Bexar County Motivated Seller Lead Scraper
Targets the publicsearch.us JSON API directly (no Playwright needed).
Falls back to Playwright if API returns nothing.
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

import requests
from bs4 import BeautifulSoup

try:
    from dbfread import DBF
    DBFREAD_OK = True
except ImportError:
    DBFREAD_OK = False

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

# Portal constants
COUNTY_ID     = "48029"
CLERK_BASE    = "https://bexar.tx.publicsearch.us"
# The SPA calls this API endpoint
API_SEARCH    = CLERK_BASE + "/api/publicly/search/results"
API_DOC       = CLERK_BASE + "/api/publicly/instrument"

BCAD_BASE     = "https://esearch.bcad.org"

DOC_TYPE_MAP = {
    "LP":       ("LP",      "Lis Pendens"),
    "NOFC":     ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED", "Tax Deed"),
    "JUD":      ("JUD",     "Judgment"),
    "CCJ":      ("JUD",     "Certified Judgment"),
    "DRJUD":    ("JUD",     "Domestic Judgment"),
    "LNCORPTX": ("LIEN",   "Corp Tax Lien"),
    "LNIRS":    ("LIEN",   "IRS Lien"),
    "LNFED":    ("LIEN",   "Federal Lien"),
    "LN":       ("LIEN",   "Lien"),
    "LNMECH":   ("LIEN",   "Mechanic Lien"),
    "LNHOA":    ("LIEN",   "HOA Lien"),
    "MEDLN":    ("LIEN",   "Medicaid Lien"),
    "PRO":      ("PRO",    "Probate Document"),
    "NOC":      ("NOC",    "Notice of Commencement"),
    "RELLP":    ("RELLP",  "Release Lis Pendens"),
}

TARGET_DOC_TYPES = list(DOC_TYPE_MAP.keys())
MAX_RETRIES = 3
RETRY_DELAY = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def retry_fn(func, *args, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return None


def clean(val):
    if val is None:
        return ""
    return str(val).strip()


def parse_amount(text):
    text = clean(text)
    cleaned = re.sub(r"[^\d.]", "", text)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def normalize_name(name):
    name = name.strip().upper()
    variants = [name]
    if "," in name:
        parts = [p.strip() for p in name.split(",", 1)]
        if len(parts) == 2:
            last, first = parts
            variants.append(first + " " + last)
            variants.append(last + " " + first)
    else:
        tokens = name.split()
        if len(tokens) >= 2:
            variants.append(" ".join(list(reversed(tokens))))
            variants.append(tokens[-1] + ", " + " ".join(tokens[:-1]))
    seen = []
    for v in variants:
        if v not in seen:
            seen.append(v)
    return seen


def date_range_str(days):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    fmt   = "%m/%d/%Y"
    return start.strftime(fmt), end.strftime(fmt)


def is_new_this_week(filed_str):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            filed_dt = datetime.strptime(filed_str, fmt)
            return (datetime.now() - filed_dt).days <= 7
        except ValueError:
            continue
    return False


def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer":         CLERK_BASE + "/",
        "Origin":          CLERK_BASE,
    })
    # Prime session cookie by visiting homepage
    try:
        s.get(CLERK_BASE + "/", timeout=20)
    except Exception:
        pass
    return s


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def compute_flags(record):
    flags    = []
    cat      = record.get("cat", "")
    owner    = record.get("owner", "").upper()
    doc_type = record.get("doc_type", "")

    if cat == "LP":
        flags.append("Lis pendens")
    if cat == "NOFC":
        flags.append("Pre-foreclosure")
    if cat == "JUD":
        flags.append("Judgment lien")
    if doc_type in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
        flags.append("Tax lien")
    if doc_type == "LNMECH":
        flags.append("Mechanic lien")
    if cat == "PRO":
        flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|HOLDINGS)\b", owner):
        flags.append("LLC / corp owner")
    if is_new_this_week(record.get("filed", "")):
        flags.append("New this week")
    return flags


def compute_score(record, flags):
    score = 30
    score += len(flags) * 10
    owner_cats = record.get("_owner_cats", [])
    if "LP" in owner_cats and "NOFC" in owner_cats:
        score += 20
    amount = record.get("amount", 0.0) or 0.0
    if amount > 100000:
        score += 15
    elif amount > 50000:
        score += 10
    if "New this week" in flags:
        score += 5
    if record.get("prop_address"):
        score += 5
    return min(score, 100)


# ---------------------------------------------------------------------------
# Clerk Portal Scraper - Direct API
# ---------------------------------------------------------------------------

class ClerkScraper:
    """
    Hits the publicsearch.us internal JSON API directly.
    The SPA at bexar.tx.publicsearch.us calls these endpoints in the browser.
    We replicate those calls with requests.
    """

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date   = end_date
        self.session    = make_session()

    def _api_search(self, doc_type, start=0, rows=100):
        """Call the search API and return raw JSON response."""
        params = {
            "type":              "PT",
            "searchType":        "quickSearch",
            "countyId":          COUNTY_ID,
            "dateRange":         "custom",
            "recordedDateRange": "custom",
            "beginDate":         self.start_date,
            "endDate":           self.end_date,
            "docType":           doc_type,
            "start":             start,
            "rows":              rows,
        }
        resp = self.session.get(API_SEARCH, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _parse_api_response(self, data, doc_type):
        """Parse the JSON API response into our record format."""
        records = []
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))

        # The API returns hits in data['hits']['hits'] or data['results'] etc.
        # Try multiple known response shapes
        hits = []
        if isinstance(data, dict):
            hits = (
                data.get("hits", {}).get("hits", [])
                or data.get("results", [])
                or data.get("documents", [])
                or data.get("data", [])
                or []
            )
        elif isinstance(data, list):
            hits = data

        for hit in hits:
            try:
                src = hit.get("_source", hit)

                doc_num  = clean(src.get("docNum") or src.get("instrumentNumber") or src.get("docNumber") or "")
                filed    = clean(src.get("recordedDate") or src.get("filedDate") or src.get("dateRecorded") or "")
                grantor  = clean(src.get("grantor") or src.get("grantorNames") or src.get("owner") or "")
                grantee  = clean(src.get("grantee") or src.get("granteeNames") or "")
                legal    = clean(src.get("legalDescription") or src.get("legal") or "")
                amount   = parse_amount(str(src.get("considerationAmount") or src.get("amount") or "0"))

                # Build clerk URL
                inst_id  = src.get("id") or src.get("instrumentId") or doc_num
                clerk_url = CLERK_BASE + "/instruments/" + str(inst_id) if inst_id else ""

                if not doc_num and not filed:
                    continue

                # Format filed date nicely if it's ISO format
                if filed and "T" in filed:
                    try:
                        filed = datetime.fromisoformat(filed.split("T")[0]).strftime("%m/%d/%Y")
                    except Exception:
                        pass

                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_type,
                    "filed":     filed,
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     grantor,
                    "grantee":   grantee,
                    "amount":    amount,
                    "legal":     legal,
                    "clerk_url": clerk_url,
                })
            except Exception as exc:
                log.warning("Error parsing hit: %s", exc)

        return records

    def search_doc_type(self, doc_type):
        """Search one doc type, paginating through all results."""
        all_records = []
        start = 0
        rows  = 100

        while True:
            try:
                data = retry_fn(self._api_search, doc_type, start, rows)
                if data is None:
                    break

                page_records = self._parse_api_response(data, doc_type)
                all_records.extend(page_records)

                # Check if there are more pages
                total = 0
                if isinstance(data, dict):
                    total = (
                        data.get("hits", {}).get("total", {}).get("value", 0)
                        or data.get("total", 0)
                        or data.get("totalResults", 0)
                        or 0
                    )
                    # Also handle total as plain int
                    if isinstance(total, int) and total == 0:
                        total = len(all_records)

                if not page_records or (start + rows) >= total:
                    break
                start += rows

            except Exception as exc:
                log.warning("Error searching %s at start=%d: %s", doc_type, start, exc)
                break

        return all_records

    def run(self):
        all_records = []
        for doc_type in TARGET_DOC_TYPES:
            log.info("Searching clerk portal for doc type: %s", doc_type)
            try:
                recs = self.search_doc_type(doc_type)
                log.info("  Found %d records for %s", len(recs), doc_type)
                all_records.extend(recs)
            except Exception as exc:
                log.warning("  Failed for %s: %s", doc_type, exc)

        # If API returned nothing, try Playwright as last resort
        if not all_records and PLAYWRIGHT_AVAILABLE:
            log.info("API returned 0 results. Trying Playwright fallback...")
            all_records = asyncio.run(self._playwright_fallback())

        return all_records

    async def _playwright_fallback(self):
        """
        Playwright fallback: navigates the actual SPA, clicks 'Last 1 Week'
        preset, then searches each doc type using the search bar.
        """
        records = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # Intercept API calls to capture responses
            api_responses = []
            async def handle_response(response):
                if "api/publicly/search" in response.url:
                    try:
                        body = await response.json()
                        api_responses.append(body)
                    except Exception:
                        pass
            page.on("response", handle_response)

            for doc_type in TARGET_DOC_TYPES:
                log.info("[Playwright] Searching for %s", doc_type)
                try:
                    api_responses.clear()
                    await page.goto(CLERK_BASE + "/", wait_until="networkidle", timeout=45000)

                    # Click Advanced Search
                    try:
                        await page.click("a[href*='advanced']", timeout=5000)
                        await page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass

                    # Try to find and fill search box with doc type
                    search_selectors = [
                        "input[placeholder*='Search']",
                        "input[placeholder*='search']",
                        "input[type='text']",
                        "input[type='search']",
                        ".search-input",
                        "#searchTerm",
                    ]
                    for sel in search_selectors:
                        try:
                            await page.fill(sel, doc_type)
                            break
                        except Exception:
                            continue

                    # Click "Last 1 Week" date preset
                    try:
                        await page.click("text=Last 1 Week", timeout=5000)
                    except Exception:
                        pass

                    # Submit search
                    try:
                        await page.keyboard.press("Enter")
                        await page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception:
                        pass

                    # Parse intercepted API responses
                    for resp_data in api_responses:
                        recs = self._parse_api_response(resp_data, doc_type)
                        if recs:
                            log.info("  [Playwright] Found %d records for %s", len(recs), doc_type)
                            records.extend(recs)

                except Exception as exc:
                    log.warning("[Playwright] Error for %s: %s", doc_type, exc)

            await browser.close()
        return records


# ---------------------------------------------------------------------------
# BCAD Parcel Loader - raw binary DBF parser
# ---------------------------------------------------------------------------

class BCAdParcelLoader:
    PARCEL_URL = BCAD_BASE + "/downloads"
    CANDIDATE_URLS = [
        "https://esearch.bcad.org/downloads/parcel_data.zip",
        "https://esearch.bcad.org/downloads/Parcels.zip",
        "https://esearch.bcad.org/downloads/BCAD_Parcels.zip",
        "https://esearch.bcad.org/DownloadFiles/ParcelData.zip",
        "https://esearch.bcad.org/DownloadFiles/Parcels.zip",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
        self.lookup = {}

    def _get_download_url(self):
        # First try scraping the downloads page
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(self.PARCEL_URL, timeout=30)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    low  = href.lower()
                    if any(x in low for x in ["parcel", "bcad"]) and any(x in low for x in [".zip", ".dbf"]):
                        if href.startswith("http"):
                            return href
                        return BCAD_BASE + href
                break
            except Exception as exc:
                log.warning("BCAD downloads page attempt %d: %s", attempt + 1, exc)
                time.sleep(RETRY_DELAY)

        # Try candidate URLs
        for url in self.CANDIDATE_URLS:
            try:
                r = self.session.head(url, timeout=15, allow_redirects=True)
                if r.status_code == 200:
                    log.info("Found parcel data at %s", url)
                    return url
            except Exception:
                continue
        return None

    def _download(self, url):
        log.info("Downloading parcel data from %s", url)
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=180, stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as exc:
                log.warning("Download attempt %d: %s", attempt + 1, exc)
                time.sleep(RETRY_DELAY)
        return None

    def _parse_dbf_raw(self, data):
        """
        Pure-Python DBF binary parser.
        Reads field descriptors directly from the header bytes,
        then walks each record without relying on dbfread field type validation.
        """
        import struct
        try:
            if len(data) < 32:
                log.warning("DBF data too short (%d bytes)", len(data))
                return 0

            # Header: bytes 0-31
            num_records = struct.unpack_from("<I", data, 4)[0]
            header_size = struct.unpack_from("<H", data, 8)[0]
            record_size = struct.unpack_from("<H", data, 10)[0]

            log.info("DBF header: %d records, header=%d, record_size=%d", num_records, header_size, record_size)

            if num_records == 0 or record_size == 0:
                return 0

            # Field descriptors start at byte 32, each 32 bytes, end at 0x0D
            fields = []
            offset = 32
            while offset + 32 <= header_size:
                if data[offset] == 0x0D:
                    break
                if data[offset] == 0x00:
                    offset += 32
                    continue
                raw_name   = data[offset:offset + 11]
                field_name = raw_name.split(b"\x00")[0].decode("latin-1", errors="replace").strip().lower()
                field_len  = data[offset + 16]
                fields.append((field_name, field_len))
                offset += 32

            if not fields:
                log.warning("DBF: no fields parsed from header")
                return 0

            log.info("DBF fields (%d): %s", len(fields), [f[0] for f in fields[:15]])

            # Verify field lengths sum to record_size - 1 (deletion flag)
            total_field_len = sum(f[1] for f in fields)
            if total_field_len != record_size - 1:
                log.warning("DBF field length mismatch: sum=%d, record_size-1=%d. Adjusting.", total_field_len, record_size - 1)

            # Parse records
            rec_offset = header_size
            count      = 0
            skipped    = 0

            for _ in range(num_records):
                if rec_offset + record_size > len(data):
                    break

                # Deletion flag: 0x20 = active, 0x2A = deleted
                flag = data[rec_offset]
                if flag == 0x2A:
                    rec_offset += record_size
                    skipped += 1
                    continue

                r = {}
                field_offset = rec_offset + 1  # skip deletion flag
                for fname, flen in fields:
                    raw_val = data[field_offset:field_offset + flen]
                    try:
                        val = raw_val.decode("latin-1", errors="replace").strip()
                    except Exception:
                        val = ""
                    r[fname] = val
                    field_offset += flen

                owner = r.get("owner") or r.get("own1") or r.get("ownername") or r.get("name") or ""
                if owner and owner.strip():
                    entry = {
                        "prop_address": r.get("site_addr") or r.get("siteaddr") or r.get("situs") or r.get("address") or "",
                        "prop_city":    r.get("site_city") or r.get("sitecity") or r.get("city") or "",
                        "prop_state":   "TX",
                        "prop_zip":     r.get("site_zip")  or r.get("sitezip")  or r.get("zip5") or "",
                        "mail_address": r.get("addr_1") or r.get("mailadr1") or r.get("mail_addr") or r.get("mail1") or "",
                        "mail_city":    r.get("mailcity") or r.get("mail_city") or r.get("mcity") or "",
                        "mail_state":   r.get("mailstate") or r.get("mail_state") or r.get("mstate") or "TX",
                        "mail_zip":     r.get("mailzip") or r.get("mail_zip") or r.get("mzip") or "",
                    }
                    for variant in normalize_name(owner):
                        self.lookup[variant] = entry
                    count += 1

                rec_offset += record_size

            log.info("DBF raw parser: loaded %d owner records (%d deleted/skipped)", count, skipped)
            return count

        except Exception as exc:
            log.warning("DBF raw parser error: %s", exc)
            import traceback
            traceback.print_exc()
            return 0

    def _parse_dbf_with_library(self, path):
        """Try dbfread library first (faster), fall back to raw parser."""
        if not DBFREAD_OK:
            return 0
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                table = DBF(path, lowernames=True, ignore_missing_memofile=True, encoding=encoding)
                _ = table.fields  # trigger header parse
                count = 0
                for row in table:
                    try:
                        r = {k.lower(): clean(v) for k, v in row.items()}
                        owner = r.get("owner") or r.get("own1") or r.get("ownername") or ""
                        if not owner:
                            continue
                        entry = {
                            "prop_address": r.get("site_addr") or r.get("siteaddr") or r.get("situs") or "",
                            "prop_city":    r.get("site_city") or r.get("sitecity") or "",
                            "prop_state":   "TX",
                            "prop_zip":     r.get("site_zip")  or r.get("sitezip")  or "",
                            "mail_address": r.get("addr_1") or r.get("mailadr1") or "",
                            "mail_city":    r.get("city") or r.get("mailcity") or "",
                            "mail_state":   r.get("state") or r.get("mailstate") or "TX",
                            "mail_zip":     r.get("zip") or r.get("mailzip") or "",
                        }
                        for variant in normalize_name(owner):
                            self.lookup[variant] = entry
                        count += 1
                    except Exception:
                        continue
                log.info("dbfread library loaded %d records (encoding=%s)", count, encoding)
                return count
            except ValueError as e:
                if "Unknown field type" in str(e):
                    log.warning("dbfread: %s. Switching to raw parser.", e)
                    return 0
                raise
            except Exception as exc:
                log.warning("dbfread attempt with %s: %s", encoding, exc)
                continue
        return 0

    def load(self):
        url = self._get_download_url()
        if not url:
            log.warning("Could not find BCAD parcel download URL. Address enrichment disabled.")
            return

        raw = self._download(url)
        if not raw:
            log.warning("Could not download parcel data.")
            return

        # Extract DBF from ZIP
        dbf_data = None
        try:
            with zipfile.ZipFile(BytesIO(raw)) as zf:
                names = zf.namelist()
                log.info("ZIP contents: %s", names[:10])
                dbf_names = [n for n in names if n.lower().endswith(".dbf")]
                if dbf_names:
                    dbf_data = zf.read(dbf_names[0])
                    log.info("Extracted DBF: %s (%d bytes)", dbf_names[0], len(dbf_data))
                else:
                    log.warning("No .dbf in ZIP. Files: %s", names)
                    # Maybe it's a CSV or other format
                    csv_names = [n for n in names if n.lower().endswith(".csv")]
                    if csv_names:
                        self._parse_csv(zf.read(csv_names[0]))
                        return
        except zipfile.BadZipFile:
            log.info("Not a ZIP file, treating as raw DBF (%d bytes)", len(raw))
            dbf_data = raw

        if not dbf_data:
            return

        # Try library first, then raw parser
        tmp_path = Path("/tmp/parcels.dbf")
        tmp_path.write_bytes(dbf_data)
        try:
            count = self._parse_dbf_with_library(str(tmp_path))
            if count == 0:
                log.info("Library parse returned 0, trying raw binary parser...")
                count = self._parse_dbf_raw(dbf_data)
        finally:
            tmp_path.unlink(missing_ok=True)

        log.info("Total owner lookup entries: %d", len(self.lookup))

    def _parse_csv(self, data):
        """Fallback: parse CSV parcel data."""
        import csv as csvmod
        import io
        try:
            text = data.decode("latin-1", errors="replace")
            reader = csvmod.DictReader(io.StringIO(text))
            count = 0
            for row in reader:
                r = {k.lower().strip(): clean(v) for k, v in row.items()}
                owner = r.get("owner") or r.get("own1") or r.get("ownername") or ""
                if not owner:
                    continue
                entry = {
                    "prop_address": r.get("site_addr") or r.get("siteaddr") or "",
                    "prop_city":    r.get("site_city") or r.get("sitecity") or "",
                    "prop_state":   "TX",
                    "prop_zip":     r.get("site_zip") or r.get("sitezip") or "",
                    "mail_address": r.get("addr_1") or r.get("mailadr1") or "",
                    "mail_city":    r.get("city") or r.get("mailcity") or "",
                    "mail_state":   r.get("state") or r.get("mailstate") or "TX",
                    "mail_zip":     r.get("zip") or r.get("mailzip") or "",
                }
                for variant in normalize_name(owner):
                    self.lookup[variant] = entry
                count += 1
            log.info("CSV parser loaded %d records.", count)
        except Exception as exc:
            log.warning("CSV parser error: %s", exc)

    def get_address(self, owner):
        for variant in normalize_name(owner):
            if variant in self.lookup:
                return self.lookup[variant]
        return {}


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def enrich_records(records, parcel):
    owner_cats = {}
    for r in records:
        owner = r.get("owner", "").upper()
        if owner:
            owner_cats.setdefault(owner, []).append(r.get("cat", ""))

    enriched = []
    for r in records:
        try:
            owner     = r.get("owner", "")
            addr_info = parcel.get_address(owner)

            r["prop_address"] = addr_info.get("prop_address", "")
            r["prop_city"]    = addr_info.get("prop_city", "San Antonio")
            r["prop_state"]   = addr_info.get("prop_state", "TX")
            r["prop_zip"]     = addr_info.get("prop_zip", "")
            r["mail_address"] = addr_info.get("mail_address", "")
            r["mail_city"]    = addr_info.get("mail_city", "")
            r["mail_state"]   = addr_info.get("mail_state", "TX")
            r["mail_zip"]     = addr_info.get("mail_zip", "")
            r["_owner_cats"]  = owner_cats.get(owner.upper(), [])

            flags  = compute_flags(r)
            score  = compute_score(r, flags)
            r["flags"] = flags
            r["score"] = score
            del r["_owner_cats"]
            enriched.append(r)
        except Exception as exc:
            log.warning("Error enriching record %s: %s", r.get("doc_num", "?"), exc)
            enriched.append(r)

    return enriched


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def build_output(records, start_date, end_date):
    with_address = sum(1 for r in records if r.get("prop_address"))
    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bexar County Clerk / BCAD",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": with_address,
        "records":      records,
    }


def save_json(data, *paths):
    for path_str in paths:
        path = Path(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        log.info("Saved JSON to %s (%d records)", path, data["total"])


def save_ghl_csv(records, path_str):
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
    log.info("Saved GHL CSV to %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    start_date, end_date = date_range_str(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_date, end_date)

    log.info("Loading BCAD parcel data...")
    parcel = BCAdParcelLoader()
    parcel.load()

    log.info("Scraping Bexar County Clerk portal (direct API)...")
    scraper     = ClerkScraper(start_date, end_date)
    raw_records = scraper.run()
    log.info("Total raw records scraped: %d", len(raw_records))

    log.info("Enriching records...")
    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    output = build_output(records, start_date, end_date)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")

    log.info("Done. %d leads saved (%d with address).", output["total"], output["with_address"])
    if records:
        log.info("Top score: %d", records[0]["score"])


if __name__ == "__main__":
    main()

