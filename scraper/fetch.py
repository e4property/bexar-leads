"""
Bexar County Motivated Seller Lead Scraper
Uses Playwright to intercept real XHR/fetch calls from the SPA,
capturing the exact API responses the browser receives.
Falls back to HTML parsing if interception yields nothing.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import struct
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

CLERK_BASE   = "https://bexar.tx.publicsearch.us"
BCAD_BASE    = "https://esearch.bcad.org"

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

def clean(val):
    if val is None:
        return ""
    return str(val).strip()

def parse_amount(text):
    cleaned = re.sub(r"[^\d.]", "", clean(text))
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

def date_range_yyyymmdd(days):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

def date_range_mmddyyyy(days):
    end   = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    return start.strftime("%m/%d/%Y"), end.strftime("%m/%d/%Y")

def is_new_this_week(filed_str):
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            filed_dt = datetime.strptime(filed_str.split("T")[0], fmt)
            return (datetime.now() - filed_dt).days <= 7
        except ValueError:
            continue
    return False


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
# Parse a JSON API response from publicsearch.us into our record format
# ---------------------------------------------------------------------------

def parse_api_hit(hit, doc_type):
    """Parse a single hit from any known response shape."""
    src = hit.get("_source", hit)

    # Try many possible field names the API might use
    doc_num = clean(
        src.get("docNum") or src.get("instrumentNumber") or
        src.get("docNumber") or src.get("recordingNumber") or
        src.get("instrument") or ""
    )
    filed = clean(
        src.get("recordedDate") or src.get("filedDate") or
        src.get("dateRecorded") or src.get("recordingDate") or
        src.get("filed") or ""
    )
    grantor = clean(
        src.get("grantor") or src.get("grantorName") or
        src.get("grantors") or src.get("owner") or
        src.get("party1") or ""
    )
    grantee = clean(
        src.get("grantee") or src.get("granteeName") or
        src.get("grantees") or src.get("party2") or ""
    )
    legal = clean(
        src.get("legalDescription") or src.get("legal") or
        src.get("description") or ""
    )
    amount = parse_amount(str(
        src.get("considerationAmount") or src.get("amount") or
        src.get("consideration") or "0"
    ))

    # Build URL from instrument ID
    inst_id   = src.get("id") or src.get("instrumentId") or src.get("docId") or doc_num
    clerk_url = (CLERK_BASE + "/instruments/" + str(inst_id)) if inst_id else ""

    # Format date
    if filed and "T" in filed:
        try:
            filed = datetime.fromisoformat(filed.split("T")[0]).strftime("%m/%d/%Y")
        except Exception:
            pass

    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    return {
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
    }

def parse_api_response(data, doc_type):
    """Extract records from any JSON shape returned by the portal."""
    records = []
    if not data:
        return records
    hits = []
    if isinstance(data, list):
        hits = data
    elif isinstance(data, dict):
        hits = (
            data.get("hits", {}).get("hits", []) or
            data.get("results", []) or
            data.get("documents", []) or
            data.get("data", []) or
            data.get("items", []) or
            []
        )
    for hit in hits:
        try:
            rec = parse_api_hit(hit, doc_type)
            if rec.get("doc_num") or rec.get("filed"):
                records.append(rec)
        except Exception as exc:
            log.warning("parse_api_hit error: %s", exc)
    return records


# ---------------------------------------------------------------------------
# HTML table parser (fallback when JSON not available)
# ---------------------------------------------------------------------------

def parse_html_results(html, doc_type):
    """Parse an HTML results page from the clerk portal."""
    records = []
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    try:
        soup  = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            # Try extracting from divs/cards
            cards = soup.find_all("div", class_=re.compile(r"result|record|item|card", re.I))
            for card in cards:
                text = card.get_text(" ", strip=True)
                doc_num = ""
                filed   = ""
                owner   = ""
                link    = card.find("a", href=True)
                clerk_url = (CLERK_BASE + link["href"]) if link else ""
                if link:
                    doc_num = clean(link.get_text(strip=True))
                m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
                if m:
                    filed = m.group(1)
                records.append({
                    "doc_num":   doc_num,
                    "doc_type":  doc_type,
                    "filed":     filed,
                    "cat":       cat,
                    "cat_label": cat_label,
                    "owner":     owner,
                    "grantee":   "",
                    "amount":    0.0,
                    "legal":     "",
                    "clerk_url": clerk_url,
                })
            return records

        rows    = table.find_all("tr")
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])] if rows else []

        def col(cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(cells):
                        return clean(cells[i].get_text(strip=True))
            return ""

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            link_tag  = row.find("a", href=True)
            clerk_url = ""
            doc_num   = ""
            if link_tag:
                href = link_tag["href"]
                if not href.startswith("http"):
                    href = CLERK_BASE + href
                clerk_url = href
                doc_num   = clean(link_tag.get_text(strip=True))
            if not doc_num:
                doc_num = col(cells, "doc", "instrument", "number")
            filed      = col(cells, "date", "filed", "recorded")
            grantor    = col(cells, "grantor", "owner", "from", "party1")
            grantee    = col(cells, "grantee", "to", "beneficiary", "party2")
            legal      = col(cells, "legal", "description")
            amount_str = col(cells, "amount", "consideration")
            if not doc_num and not filed:
                continue
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
        log.warning("HTML parse error for %s: %s", doc_type, exc)
    return records


# ---------------------------------------------------------------------------
# Clerk Scraper -- Playwright XHR interception + HTML fallback
# ---------------------------------------------------------------------------

class ClerkScraper:
    def __init__(self, start_ymd, end_ymd, start_mdy, end_mdy):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd
        self.start_mdy = start_mdy
        self.end_mdy   = end_mdy

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright not available.")
            return []
        return await self._scrape_with_playwright()

    async def _scrape_with_playwright(self):
        all_records = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                ignore_https_errors=True,
            )
            page = await context.new_page()

            for doc_type in TARGET_DOC_TYPES:
                log.info("Searching for doc type: %s", doc_type)
                recs = await self._search_one_type(page, doc_type)
                log.info("  Found %d records for %s", len(recs), doc_type)
                all_records.extend(recs)

            await browser.close()

        return all_records

    async def _search_one_type(self, page, doc_type):
        """
        Navigate to the results page using the URL pattern discovered from Google.
        Intercept all JSON responses + capture final HTML.
        """
        collected_json = []

        async def capture_response(response):
            url = response.url
            # Capture any JSON response from the portal domain
            if "publicsearch.us" in url and response.status == 200:
                ct = response.headers.get("content-type", "")
                if "json" in ct:
                    try:
                        body = await response.json()
                        collected_json.append((url, body))
                    except Exception:
                        pass

        page.on("response", capture_response)

        # Use the real URL pattern from the Google-indexed URLs
        # Format: /results?department=RP&limit=50&offset=0&recordedDateRange=YYYYMMDD,YYYYMMDD&searchType=docType&searchValue=LP
        url = (
            CLERK_BASE + "/results"
            + "?department=RP"
            + "&limit=100"
            + "&offset=0"
            + "&recordedDateRange=" + self.start_ymd + "," + self.end_ymd
            + "&searchOcrText=false"
            + "&searchType=docType"
            + "&searchValue=" + doc_type
        )

        records = []
        for attempt in range(1, MAX_RETRIES + 1):
            collected_json.clear()
            try:
                await page.goto(url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(2)  # let any lazy-loaded content settle

                # First: try to extract from intercepted JSON
                for resp_url, body in collected_json:
                    parsed = parse_api_response(body, doc_type)
                    if parsed:
                        log.info("  [XHR] Got %d records from %s", len(parsed), resp_url[:80])
                        records.extend(parsed)

                # Second: if no JSON, parse the rendered HTML
                if not records:
                    html = await page.content()
                    html_recs = parse_html_results(html, doc_type)
                    if html_recs:
                        log.info("  [HTML] Got %d records from page", len(html_recs))
                        records.extend(html_recs)

                # Paginate if we got results
                if records:
                    records = await self._paginate(page, url, doc_type, records, collected_json)

                break

            except Exception as exc:
                log.warning("  Attempt %d/%d for %s: %s", attempt, MAX_RETRIES, doc_type, exc)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

        # Remove the response listener for next iteration
        page.remove_listener("response", capture_response)
        return records

    async def _paginate(self, page, base_url, doc_type, initial_records, collected_json):
        """Try to load additional pages of results."""
        all_records = list(initial_records)
        offset = 100

        while len(all_records) >= offset:  # only paginate if we got a full page
            next_url = re.sub(r"offset=\d+", "offset=" + str(offset), base_url)
            if "offset=" not in base_url:
                next_url = base_url + "&offset=" + str(offset)
            collected_json.clear()
            try:
                await page.goto(next_url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(1)

                page_recs = []
                for _, body in collected_json:
                    page_recs.extend(parse_api_response(body, doc_type))
                if not page_recs:
                    html = await page.content()
                    page_recs = parse_html_results(html, doc_type)

                if not page_recs:
                    break
                all_records.extend(page_recs)
                offset += 100
            except Exception as exc:
                log.warning("Pagination error at offset %d: %s", offset, exc)
                break

        return all_records


# ---------------------------------------------------------------------------
# BCAD Parcel Loader
# ---------------------------------------------------------------------------

class BCAdParcelLoader:
    # Try to find the real download page and extract links from it
    DOWNLOAD_PAGES = [
        "https://esearch.bcad.org/DownloadFiles",
        "https://esearch.bcad.org/downloads",
        "https://esearch.bcad.org/Download",
        "https://esearch.bcad.org/",
    ]
    # Direct candidate ZIP URLs
    DIRECT_URLS = [
        "https://esearch.bcad.org/DownloadFiles/ParcelData.zip",
        "https://esearch.bcad.org/DownloadFiles/Parcels.zip",
        "https://esearch.bcad.org/downloads/parcel_data.zip",
        "https://esearch.bcad.org/downloads/Parcels.zip",
        "https://esearch.bcad.org/downloads/BCAD_Parcels.zip",
        "https://esearch.bcad.org/DownloadFiles/BCAD_Parcels.zip",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
        self.lookup = {}

    def _find_download_url(self):
        # Scrape candidate download pages
        for page_url in self.DOWNLOAD_PAGES:
            try:
                resp = self.session.get(page_url, timeout=20)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    low  = href.lower()
                    if any(x in low for x in [".zip", ".dbf", ".csv"]):
                        if any(x in low for x in ["parcel", "bcad", "data", "download"]):
                            full = href if href.startswith("http") else "https://esearch.bcad.org" + href
                            log.info("Found download link: %s", full)
                            return full
            except Exception as exc:
                log.warning("BCAD page %s: %s", page_url, exc)

        # Try direct URLs
        for url in self.DIRECT_URLS:
            try:
                r = self.session.head(url, timeout=15, allow_redirects=True)
                ct = r.headers.get("content-type", "")
                if r.status_code == 200 and "html" not in ct:
                    log.info("Found direct URL: %s", url)
                    return url
            except Exception:
                continue
        return None

    def _download(self, url):
        for attempt in range(MAX_RETRIES):
            try:
                log.info("Downloading from %s", url)
                resp = self.session.get(url, timeout=180, stream=True)
                resp.raise_for_status()
                ct = resp.headers.get("content-type", "")
                if "html" in ct:
                    log.warning("Got HTML response instead of data file from %s", url)
                    return None
                return resp.content
            except Exception as exc:
                log.warning("Download attempt %d: %s", attempt + 1, exc)
                time.sleep(RETRY_DELAY)
        return None

    def _parse_dbf_raw(self, data):
        """Pure binary DBF parser -- no external library needed."""
        try:
            if len(data) < 32:
                return 0
            num_records = struct.unpack_from("<I", data, 4)[0]
            header_size = struct.unpack_from("<H", data, 8)[0]
            record_size = struct.unpack_from("<H", data, 10)[0]
            log.info("DBF: %d records, header=%d, record_size=%d", num_records, header_size, record_size)

            if num_records == 0 or record_size == 0 or header_size > len(data):
                return 0

            # Parse field descriptors
            fields = []
            pos    = 32
            while pos + 32 <= header_size and pos < len(data):
                if data[pos] == 0x0D:
                    break
                if data[pos] == 0x00:
                    pos += 32
                    continue
                fname  = data[pos:pos+11].split(b"\x00")[0].decode("latin-1", errors="replace").strip().lower()
                flen   = data[pos+16]
                if flen > 0 and fname:
                    fields.append((fname, flen))
                pos += 32

            if not fields:
                log.warning("DBF: no fields found in header")
                return 0

            total_flen = sum(f[1] for f in fields)
            expected   = record_size - 1
            if total_flen != expected:
                log.warning("DBF field length sum %d != record_size-1 %d", total_flen, expected)
                # Trim fields to fit within record_size
                trimmed = []
                acc = 0
                for fname, flen in fields:
                    if acc + flen <= expected:
                        trimmed.append((fname, flen))
                        acc += flen
                fields = trimmed

            log.info("DBF fields (%d): %s", len(fields), [f[0] for f in fields[:20]])

            rec_offset = header_size
            count      = 0
            for _ in range(num_records):
                if rec_offset + record_size > len(data):
                    break
                if data[rec_offset] == 0x2A:  # deleted
                    rec_offset += record_size
                    continue
                r   = {}
                fos = rec_offset + 1
                for fname, flen in fields:
                    raw = data[fos:fos+flen]
                    try:
                        r[fname] = raw.decode("latin-1", errors="replace").strip()
                    except Exception:
                        r[fname] = ""
                    fos += flen

                owner = r.get("owner") or r.get("own1") or r.get("ownername") or r.get("name") or ""
                if owner.strip():
                    entry = {
                        "prop_address": r.get("site_addr") or r.get("siteaddr") or r.get("situs") or "",
                        "prop_city":    r.get("site_city") or r.get("sitecity") or "",
                        "prop_state":   "TX",
                        "prop_zip":     r.get("site_zip")  or r.get("sitezip")  or r.get("zip5") or "",
                        "mail_address": r.get("addr_1")  or r.get("mailadr1") or r.get("mail1") or "",
                        "mail_city":    r.get("mailcity") or r.get("mail_city") or "",
                        "mail_state":   r.get("mailstate") or r.get("mail_state") or "TX",
                        "mail_zip":     r.get("mailzip")  or r.get("mail_zip") or "",
                    }
                    for variant in normalize_name(owner):
                        self.lookup[variant] = entry
                    count += 1
                rec_offset += record_size
            log.info("DBF raw parser: %d owner records loaded", count)
            return count
        except Exception as exc:
            log.warning("DBF raw parse error: %s", exc)
            return 0

    def _parse_zip(self, raw):
        try:
            with zipfile.ZipFile(BytesIO(raw)) as zf:
                names = zf.namelist()
                log.info("ZIP contents: %s", names[:10])
                for ext in [".dbf", ".csv"]:
                    matches = [n for n in names if n.lower().endswith(ext)]
                    if matches:
                        return ext, zf.read(matches[0])
        except zipfile.BadZipFile:
            pass
        return ".dbf", raw  # treat as raw DBF

    def load(self):
        url = self._find_download_url()
        if not url:
            log.warning("BCAD: no download URL found. Address enrichment disabled.")
            return

        raw = self._download(url)
        if not raw:
            log.warning("BCAD: download failed.")
            return

        ext, data = self._parse_zip(raw)

        if ext == ".csv":
            self._parse_csv(data)
        else:
            # Try dbfread first, fall back to raw
            count = 0
            if DBFREAD_OK:
                tmp = Path("/tmp/parcels.dbf")
                tmp.write_bytes(data)
                try:
                    for enc in ("latin-1", "utf-8", "cp1252"):
                        try:
                            table = DBF(str(tmp), lowernames=True, ignore_missing_memofile=True, encoding=enc)
                            _ = table.fields
                            for row in table:
                                try:
                                    r = {k.lower(): clean(v) for k, v in row.items()}
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
                                except Exception:
                                    continue
                            if count > 0:
                                break
                        except ValueError as e:
                            if "Unknown field type" in str(e):
                                break
                            continue
                        except Exception:
                            continue
                finally:
                    tmp.unlink(missing_ok=True)

            if count == 0:
                log.info("dbfread got 0, trying raw binary parser...")
                count = self._parse_dbf_raw(data)

        log.info("BCAD lookup: %d owner entries", len(self.lookup))

    def _parse_csv(self, data):
        import csv as csvmod, io
        try:
            text   = data.decode("latin-1", errors="replace")
            reader = csvmod.DictReader(io.StringIO(text))
            count  = 0
            for row in reader:
                r     = {k.lower().strip(): clean(v) for k, v in row.items()}
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
            log.info("CSV parser: %d records", count)
        except Exception as exc:
            log.warning("CSV parse error: %s", exc)

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
            log.warning("Enrich error %s: %s", r.get("doc_num", "?"), exc)
            enriched.append(r)
    return enriched


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def build_output(records, start_mdy, end_mdy):
    return {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Bexar County Clerk / BCAD",
        "date_range":   {"start": start_mdy, "end": end_mdy},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }

def save_json(data, *paths):
    for p in paths:
        path = Path(p)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        log.info("Saved %s (%d records)", path, data["total"])

def save_ghl_csv(records, path_str):
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in records:
            owner  = r.get("owner", "")
            parts  = owner.split(",", 1) if "," in owner else owner.split(" ", 1)
            first  = parts[1].strip() if len(parts) > 1 else ""
            last   = parts[0].strip()
            w.writerow({
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
    log.info("Saved GHL CSV: %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)

    start_ymd, end_ymd = date_range_yyyymmdd(LOOKBACK_DAYS)
    start_mdy, end_mdy = date_range_mmddyyyy(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_mdy, end_mdy)

    log.info("Loading BCAD parcel data...")
    parcel = BCAdParcelLoader()
    parcel.load()

    log.info("Scraping Bexar County Clerk portal...")
    scraper     = ClerkScraper(start_ymd, end_ymd, start_mdy, end_mdy)
    raw_records = await scraper.run()
    log.info("Total raw records: %d", len(raw_records))

    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    output = build_output(records, start_mdy, end_mdy)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")
    log.info("Done. %d leads (%d with address).", output["total"], output["with_address"])


if __name__ == "__main__":
    asyncio.run(main())

