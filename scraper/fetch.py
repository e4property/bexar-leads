"""
Bexar County Motivated Seller Lead Scraper
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
from dbfread import DBF

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
CLERK_BASE    = "https://bexar.tx.publicsearch.us"
CLERK_SEARCH  = CLERK_BASE + "/results"
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


def retry(func):
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


class BCAdParcelLoader:
    PARCEL_URL = BCAD_BASE + "/downloads"
    CANDIDATE_PATHS = [
        "/downloads/parcel_data.zip",
        "/downloads/Parcels.zip",
        "/downloads/BCAD_Parcels.zip",
        "/downloads/parcel.zip",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
        self.lookup = {}

    @retry
    def _get_download_url(self):
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
    def _download_zip(self, url):
        log.info("Downloading parcel data from %s", url)
        resp = self.session.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        return resp.content

    def _parse_dbf(self, data):
        import struct

        tmp_path = Path("/tmp/parcels.dbf")
        tmp_path.write_bytes(data)
        try:
            # Patch dbfread to ignore unknown field types instead of crashing
            try:
                import dbfread.dbf as _dbf_mod
                _orig_check = _dbf_mod.DBF._check_headers if hasattr(_dbf_mod.DBF, "_check_headers") else None
            except Exception:
                pass

            # Use ignore_missing_memofile + chardet encoding fallback
            for encoding in ("utf-8", "latin-1", "cp1252"):
                try:
                    table = DBF(
                        str(tmp_path),
                        lowernames=True,
                        ignore_missing_memofile=True,
                        encoding=encoding,
                    )
                    # Force field list load to trigger any header errors early
                    _ = table.fields
                    break
                except ValueError as e:
                    if "Unknown field type" in str(e):
                        # Patch: strip bad field bytes from header and retry with raw parser
                        log.warning("DBF has unknown field types (%s), using raw row parser.", e)
                        table = None
                        break
                    raise
                except Exception:
                    continue
            else:
                table = None

            if table is None:
                # Fallback: parse DBF rows manually skipping bad fields
                self._parse_dbf_raw(data)
                return

            count = 0
            for row in table:
                try:
                    r = {}
                    for k, v in row.items():
                        try:
                            r[str(k).lower()] = clean(v)
                        except Exception:
                            pass
                    owner = r.get("owner") or r.get("own1") or r.get("ownername") or ""
                    if not owner:
                        continue
                    entry = {
                        "prop_address": r.get("site_addr") or r.get("siteaddr") or r.get("situs") or "",
                        "prop_city":    r.get("site_city") or r.get("sitecity") or "",
                        "prop_state":   "TX",
                        "prop_zip":     r.get("site_zip")  or r.get("sitezip")  or "",
                        "mail_address": r.get("addr_1") or r.get("mailadr1") or r.get("mail_addr") or "",
                        "mail_city":    r.get("city")   or r.get("mailcity") or "",
                        "mail_state":   r.get("state")  or r.get("mailstate") or "TX",
                        "mail_zip":     r.get("zip")    or r.get("mailzip") or "",
                    }
                    for variant in normalize_name(owner):
                        self.lookup[variant] = entry
                    count += 1
                except Exception:
                    continue
            log.info("Parsed %d rows from DBF.", count)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _parse_dbf_raw(self, data):
        """
        Manual DBF parser that skips unknown field types.
        Handles corrupted BCAD DBF files with non-standard field type bytes.
        DBF spec: 32-byte header, then 32-byte field descriptors until 0x0D terminator.
        """
        import struct
        try:
            if len(data) < 32:
                return

            num_records  = struct.unpack_from("<I", data, 4)[0]
            header_size  = struct.unpack_from("<H", data, 8)[0]
            record_size  = struct.unpack_from("<H", data, 10)[0]

            # Parse field descriptors (each 32 bytes, starts at offset 32)
            fields = []
            offset = 32
            while offset + 32 <= header_size:
                if data[offset] == 0x0D:  # terminator
                    break
                raw_name  = data[offset:offset+11]
                field_name = raw_name.split(b"\x00")[0].decode("latin-1", errors="replace").strip().lower()
                field_type = chr(data[offset+11])
                field_len  = data[offset+16]
                fields.append((field_name, field_type, field_len))
                offset += 32

            if not fields:
                log.warning("DBF raw parser: no fields found.")
                return

            # Parse records
            rec_offset = header_size
            count = 0
            for _ in range(num_records):
                if rec_offset + record_size > len(data):
                    break
                # First byte is deletion flag
                if data[rec_offset] == 0x2A:  # '*' = deleted
                    rec_offset += record_size
                    continue

                r = {}
                field_offset = rec_offset + 1
                for fname, ftype, flen in fields:
                    raw_val = data[field_offset:field_offset+flen]
                    try:
                        val = raw_val.decode("latin-1", errors="replace").strip()
                    except Exception:
                        val = ""
                    r[fname] = val
                    field_offset += flen

                owner = r.get("owner") or r.get("own1") or r.get("ownername") or ""
                if owner:
                    entry = {
                        "prop_address": r.get("site_addr") or r.get("siteaddr") or r.get("situs") or "",
                        "prop_city":    r.get("site_city") or r.get("sitecity") or "",
                        "prop_state":   "TX",
                        "prop_zip":     r.get("site_zip")  or r.get("sitezip")  or "",
                        "mail_address": r.get("addr_1") or r.get("mailadr1") or r.get("mail_addr") or "",
                        "mail_city":    r.get("city")   or r.get("mailcity") or "",
                        "mail_state":   r.get("state")  or r.get("mailstate") or "TX",
                        "mail_zip":     r.get("zip")    or r.get("mailzip") or "",
                    }
                    for variant in normalize_name(owner):
                        self.lookup[variant] = entry
                    count += 1

                rec_offset += record_size

            log.info("Raw DBF parser loaded %d owner records.", count)
        except Exception as exc:
            log.warning("Raw DBF parser failed: %s. Address enrichment disabled.", exc)

    def load(self):
        url = self._get_download_url()
        if not url:
            log.warning("Could not find BCAD parcel download URL. Address enrichment disabled.")
            return
        raw = self._download_zip(url)
        if not raw:
            log.warning("Could not download parcel data.")
            return
        try:
            with zipfile.ZipFile(BytesIO(raw)) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    log.warning("No .dbf file found inside ZIP.")
                    return
                dbf_data = zf.read(dbf_names[0])
        except zipfile.BadZipFile:
            dbf_data = raw
        self._parse_dbf(dbf_data)
        log.info("Loaded %d parcel owner records.", len(self.lookup))

    def get_address(self, owner):
        for variant in normalize_name(owner):
            if variant in self.lookup:
                return self.lookup[variant]
        return {}


class ClerkScraper:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date   = end_date
        self.records    = []

    async def _search_doc_type(self, page, doc_type):
        results = []
        try:
            await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=60000)
            await page.wait_for_selector(
                "input[name*='beginDate'], #beginDate, input[name*='startDate']",
                timeout=15000,
            )
            try:
                await page.locator("select[name*='docType'], #docType").select_option(value=doc_type)
            except Exception:
                pass
            for sel in ["input[name*='beginDate']", "#beginDate", "input[name*='startDate']"]:
                try:
                    await page.fill(sel, self.start_date)
                    break
                except Exception:
                    pass
            for sel in ["input[name*='endDate']", "#endDate"]:
                try:
                    await page.fill(sel, self.end_date)
                    break
                except Exception:
                    pass
            for sel in ["button[type='submit']", "input[type='submit']", "#searchBtn"]:
                try:
                    await page.click(sel)
                    await page.wait_for_load_state("networkidle", timeout=30000)
                    break
                except Exception:
                    pass
            while True:
                recs = await self._parse_results_page(page, doc_type)
                results.extend(recs)
                if not recs:
                    break
                try:
                    nxt = page.locator("a:has-text('Next'), button:has-text('Next')")
                    if await nxt.count() == 0:
                        break
                    await nxt.first.click()
                    await page.wait_for_load_state("networkidle", timeout=20000)
                except Exception:
                    break
        except Exception as exc:
            log.warning("Error searching doc type %s: %s", doc_type, exc)
        return results

    async def _parse_results_page(self, page, doc_type):
        records = []
        try:
            content = await page.content()
            soup    = BeautifulSoup(content, "lxml")
            table   = soup.find("table", {"id": re.compile(r"result|grid|search", re.I)})
            if not table:
                table = soup.find("table")
            if not table:
                return records
            rows    = table.find_all("tr")
            if not rows:
                return records
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

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
                filed      = col(cells, "date", "filed", "record date")
                grantor    = col(cells, "grantor", "owner", "from")
                grantee    = col(cells, "grantee", "to", "beneficiary")
                legal      = col(cells, "legal", "description", "property")
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

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright not available. Using HTTP fallback.")
            return await self._http_fallback()
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            for doc_type in TARGET_DOC_TYPES:
                log.info("Searching clerk portal for doc type: %s", doc_type)
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        recs = await self._search_doc_type(page, doc_type)
                        log.info("  Found %d records for %s", len(recs), doc_type)
                        self.records.extend(recs)
                        break
                    except Exception as exc:
                        log.warning("  Attempt %d failed for %s: %s", attempt, doc_type, exc)
                        if attempt < MAX_RETRIES:
                            await asyncio.sleep(RETRY_DELAY)
            await browser.close()
        return self.records

    async def _http_fallback(self):
        records = []
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
                soup    = BeautifulSoup(resp.text, "lxml")
                table   = soup.find("table")
                if not table:
                    continue
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
                    filed      = col(cells, "date", "filed")
                    grantor    = col(cells, "grantor", "owner", "from")
                    grantee    = col(cells, "grantee", "to")
                    legal      = col(cells, "legal", "description")
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
            owner  = r.get("owner", "")
            parts  = owner.split(",", 1) if "," in owner else owner.split(" ", 1)
            first  = parts[1].strip() if len(parts) > 1 else ""
            last   = parts[0].strip()
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


async def main():
    log.info("=" * 60)
    log.info("Bexar County Motivated Seller Lead Scraper")
    log.info("Lookback: %d days", LOOKBACK_DAYS)
    log.info("=" * 60)
    start_date, end_date = date_range_str(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_date, end_date)
    log.info("Loading BCAD parcel data...")
    parcel = BCAdParcelLoader()
    parcel.load()
    log.info("Scraping Bexar County Clerk portal...")
    scraper     = ClerkScraper(start_date, end_date)
    raw_records = await scraper.run()
    log.info("Total raw records scraped: %d", len(raw_records))
    log.info("Enriching records with parcel data and scoring...")
    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)
    output = build_output(records, start_date, end_date)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")
    log.info("Done. %d leads saved (%d with address).", output["total"], output["with_address"])
    if records:
        log.info("Top score: %d", records[0]["score"])


if __name__ == "__main__":
    asyncio.run(main())

