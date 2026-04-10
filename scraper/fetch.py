"""
Bexar County Motivated Seller Lead Scraper
- Clerk portal: Playwright XHR interception + HTML fallback
- Parcel data:  Bexar County ArcGIS REST API (no login, no DBF)
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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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

CLERK_BASE  = "https://bexar.tx.publicsearch.us"
# ArcGIS parcel layer - public, no auth required
ARCGIS_URL  = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

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
# Parse JSON / HTML from clerk portal
# ---------------------------------------------------------------------------

def parse_api_response(data, doc_type):
    records = []
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
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
            data.get("items", []) or []
        )
    for hit in hits:
        try:
            src     = hit.get("_source", hit)
            doc_num = clean(src.get("docNum") or src.get("instrumentNumber") or
                            src.get("docNumber") or src.get("recordingNumber") or "")
            filed   = clean(src.get("recordedDate") or src.get("filedDate") or
                            src.get("dateRecorded") or src.get("recordingDate") or "")
            grantor = clean(src.get("grantor") or src.get("grantorName") or
                            src.get("grantors") or src.get("owner") or "")
            grantee = clean(src.get("grantee") or src.get("granteeName") or
                            src.get("grantees") or "")
            legal   = clean(src.get("legalDescription") or src.get("legal") or "")
            amount  = parse_amount(str(src.get("considerationAmount") or
                                       src.get("amount") or "0"))
            inst_id   = src.get("id") or src.get("instrumentId") or doc_num
            clerk_url = (CLERK_BASE + "/instruments/" + str(inst_id)) if inst_id else ""
            if filed and "T" in filed:
                try:
                    filed = datetime.fromisoformat(filed.split("T")[0]).strftime("%m/%d/%Y")
                except Exception:
                    pass
            if doc_num or filed:
                records.append({
                    "doc_num": doc_num, "doc_type": doc_type,
                    "filed": filed, "cat": cat, "cat_label": cat_label,
                    "owner": grantor, "grantee": grantee,
                    "amount": amount, "legal": legal, "clerk_url": clerk_url,
                })
        except Exception as exc:
            log.warning("parse hit error: %s", exc)
    return records


def parse_html_results(html, doc_type):
    """Parse rendered HTML from the clerk portal results page."""
    records = []
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    try:
        soup = BeautifulSoup(html, "lxml")

        # Look for any table first
        table = soup.find("table")
        if table:
            rows    = table.find_all("tr")
            headers = [th.get_text(strip=True).lower()
                       for th in rows[0].find_all(["th", "td"])] if rows else []

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
                clerk_url, doc_num = "", ""
                if link_tag:
                    href = link_tag["href"]
                    if not href.startswith("http"):
                        href = CLERK_BASE + href
                    clerk_url = href
                    doc_num   = clean(link_tag.get_text(strip=True))
                if not doc_num:
                    doc_num = col(cells, "doc", "instrument", "number")
                filed   = col(cells, "date", "filed", "recorded")
                grantor = col(cells, "grantor", "owner", "from", "party1")
                grantee = col(cells, "grantee", "to", "party2")
                legal   = col(cells, "legal", "description")
                amount  = parse_amount(col(cells, "amount", "consideration"))
                if doc_num or filed:
                    records.append({
                        "doc_num": doc_num, "doc_type": doc_type,
                        "filed": filed, "cat": cat, "cat_label": cat_label,
                        "owner": grantor, "grantee": grantee,
                        "amount": amount, "legal": legal, "clerk_url": clerk_url,
                    })
            return records

        # No table: try result cards/divs
        cards = soup.find_all(
            "div",
            class_=re.compile(r"result|record|item|card|row|instrument", re.I)
        )
        for card in cards:
            text     = card.get_text(" ", strip=True)
            link_tag = card.find("a", href=True)
            clerk_url, doc_num = "", ""
            if link_tag:
                href = link_tag["href"]
                if not href.startswith("http"):
                    href = CLERK_BASE + href
                clerk_url = href
                doc_num   = clean(link_tag.get_text(strip=True))
            filed = ""
            m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
            if m:
                filed = m.group(1)
            if doc_num or filed:
                records.append({
                    "doc_num": doc_num, "doc_type": doc_type,
                    "filed": filed, "cat": cat, "cat_label": cat_label,
                    "owner": "", "grantee": "",
                    "amount": 0.0, "legal": "", "clerk_url": clerk_url,
                })
    except Exception as exc:
        log.warning("HTML parse error for %s: %s", doc_type, exc)
    return records


# ---------------------------------------------------------------------------
# Clerk Scraper
# ---------------------------------------------------------------------------

class ClerkScraper:
    def __init__(self, start_ymd, end_ymd):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd

    def _build_url(self, doc_type, offset=0, limit=200):
        """
        Real URL pattern discovered from Google-indexed pages.
        Uses YYYYMMDD date format, department=RP, searchType=docType.
        """
        return (
            CLERK_BASE + "/results"
            + "?department=RP"
            + "&limit=" + str(limit)
            + "&offset=" + str(offset)
            + "&recordedDateRange=" + self.start_ymd + "," + self.end_ymd
            + "&searchOcrText=false"
            + "&searchType=docType"
            + "&searchValue=" + doc_type
        )

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            log.warning("Playwright not available.")
            return []
        return await self._scrape()

    async def _scrape(self):
        all_records = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                ignore_https_errors=True,
            )
            page = await context.new_page()

            for doc_type in TARGET_DOC_TYPES:
                log.info("Searching for doc type: %s", doc_type)
                recs = await self._search_type(page, doc_type)
                log.info("  Found %d records for %s", len(recs), doc_type)
                all_records.extend(recs)

            await browser.close()
        return all_records

    async def _search_type(self, page, doc_type):
        collected_json = []

        async def capture(response):
            if "publicsearch.us" in response.url and response.status == 200:
                ct = response.headers.get("content-type", "")
                if "json" in ct:
                    try:
                        body = await response.json()
                        collected_json.append(body)
                    except Exception:
                        pass

        page.on("response", capture)
        all_records = []

        for attempt in range(1, MAX_RETRIES + 1):
            collected_json.clear()
            url = self._build_url(doc_type, offset=0, limit=200)
            try:
                await page.goto(url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(2)

                # 1. Try intercepted JSON first
                for body in collected_json:
                    parsed = parse_api_response(body, doc_type)
                    if parsed:
                        log.info("  [XHR] %d records from JSON", len(parsed))
                        all_records.extend(parsed)

                # 2. Fall back to HTML
                if not all_records:
                    html   = await page.content()
                    parsed = parse_html_results(html, doc_type)
                    if parsed:
                        log.info("  [HTML] %d records from page", len(parsed))
                        all_records.extend(parsed)

                # 3. Paginate
                if all_records:
                    page_size = len(all_records)
                    offset    = page_size
                    while len(all_records) == offset:
                        collected_json.clear()
                        next_url = self._build_url(doc_type, offset=offset, limit=200)
                        try:
                            await page.goto(next_url, wait_until="networkidle", timeout=30000)
                            await asyncio.sleep(1)
                            page_recs = []
                            for body in collected_json:
                                page_recs.extend(parse_api_response(body, doc_type))
                            if not page_recs:
                                html = await page.content()
                                page_recs = parse_html_results(html, doc_type)
                            if not page_recs:
                                break
                            all_records.extend(page_recs)
                            offset += len(page_recs)
                        except Exception as exc:
                            log.warning("Pagination error at offset %d: %s", offset, exc)
                            break
                break

            except Exception as exc:
                log.warning("  Attempt %d/%d for %s: %s", attempt, MAX_RETRIES, doc_type, exc)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

        page.remove_listener("response", capture)
        return all_records


# ---------------------------------------------------------------------------
# Parcel Loader -- Bexar County ArcGIS REST API
# ---------------------------------------------------------------------------

class ParcelLoader:
    """
    Queries the public Bexar County ArcGIS parcel layer.
    No login required. Paginates using resultOffset.
    Fields: Owner, Situs, AddrLn1, AddrLn2, AddrCity, AddrSt, Zip
    """

    QUERY_URL  = ARCGIS_URL
    MAX_BATCH  = 1000   # ArcGIS layer max per request
    MAX_TOTAL  = 600000 # ~550k parcels in Bexar County

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
        # owner_name_upper -> address dict
        self.lookup = {}

    def _fetch_batch(self, offset):
        params = {
            "where":         "1=1",
            "outFields":     "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip",
            "resultOffset":  offset,
            "resultRecordCount": self.MAX_BATCH,
            "returnGeometry": "false",
            "f":             "json",
        }
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(self.QUERY_URL, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    log.warning("ArcGIS error at offset %d: %s", offset, data["error"])
                    return []
                features = data.get("features", [])
                return features
            except Exception as exc:
                log.warning("ArcGIS batch attempt %d at offset %d: %s",
                            attempt + 1, offset, exc)
                time.sleep(RETRY_DELAY)
        return []

    def load(self):
        log.info("Loading parcel data from Bexar County ArcGIS REST API...")
        offset = 0
        total  = 0

        while offset < self.MAX_TOTAL:
            features = self._fetch_batch(offset)
            if not features:
                break

            for feat in features:
                attrs = feat.get("attributes", {})
                owner = clean(attrs.get("Owner") or "")
                if not owner:
                    continue

                # Situs = property address (single string like "123 MAIN ST")
                situs    = clean(attrs.get("Situs") or "")
                # Parse city/zip from situs if present (format: "123 MAIN ST, SAN ANTONIO 78201")
                prop_city, prop_zip = "", ""
                situs_parts = situs.rsplit(",", 1)
                prop_addr = situs_parts[0].strip() if situs_parts else situs
                if len(situs_parts) > 1:
                    city_zip = situs_parts[1].strip()
                    cz = city_zip.rsplit(" ", 1)
                    prop_city = cz[0].strip() if cz else ""
                    prop_zip  = cz[1].strip() if len(cz) > 1 else ""

                # Mailing address
                mail_addr  = clean(attrs.get("AddrLn1") or "")
                mail_addr2 = clean(attrs.get("AddrLn2") or "")
                if mail_addr2:
                    mail_addr = mail_addr + " " + mail_addr2
                mail_city  = clean(attrs.get("AddrCity") or "")
                mail_state = clean(attrs.get("AddrSt") or "TX")
                mail_zip   = clean(attrs.get("Zip") or "")

                entry = {
                    "prop_address": prop_addr,
                    "prop_city":    prop_city or "San Antonio",
                    "prop_state":   "TX",
                    "prop_zip":     prop_zip,
                    "mail_address": mail_addr,
                    "mail_city":    mail_city,
                    "mail_state":   mail_state or "TX",
                    "mail_zip":     mail_zip,
                }

                for variant in normalize_name(owner):
                    self.lookup[variant] = entry
                total += 1

            log.info("  Loaded %d parcels so far (offset=%d)...", total, offset)
            if len(features) < self.MAX_BATCH:
                break  # last page
            offset += self.MAX_BATCH
            time.sleep(0.5)  # be polite

        log.info("Parcel load complete: %d owner entries", len(self.lookup))

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
        "source":       "Bexar County Clerk / Bexar County GIS",
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
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
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

    # Load parcel data first (runs in background-friendly sync calls)
    parcel = ParcelLoader()
    parcel.load()

    # Scrape clerk portal
    log.info("Scraping Bexar County Clerk portal...")
    scraper     = ClerkScraper(start_ymd, end_ymd)
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

