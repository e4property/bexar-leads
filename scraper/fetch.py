"""
Bexar County Motivated Seller Lead Scraper
Dumps page screenshots + HTML for debugging, then parses real data.
"""

from __future__ import annotations

import asyncio
import base64
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
CLERK_BASE    = "https://bexar.tx.publicsearch.us"
ARCGIS_URL    = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"
DEBUG_HTML    = os.getenv("DEBUG_HTML", "1") == "1"  # save first page HTML

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

def escape_sql(s):
    return s.replace("'", "''")

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
    if cat == "LP":        flags.append("Lis pendens")
    if cat == "NOFC":      flags.append("Pre-foreclosure")
    if cat == "JUD":       flags.append("Judgment lien")
    if doc_type in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):
                           flags.append("Tax lien")
    if doc_type == "LNMECH": flags.append("Mechanic lien")
    if cat == "PRO":       flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|HOLDINGS)\b", owner):
                           flags.append("LLC / corp owner")
    if is_new_this_week(record.get("filed", "")):
                           flags.append("New this week")
    return flags

def compute_score(record, flags):
    score = 30
    score += len(flags) * 10
    if "LP" in record.get("_owner_cats", []) and "NOFC" in record.get("_owner_cats", []):
        score += 20
    amount = record.get("amount", 0.0) or 0.0
    if amount > 100000: score += 15
    elif amount > 50000: score += 10
    if "New this week" in flags: score += 5
    if record.get("prop_address"): score += 5
    return min(score, 100)


# ---------------------------------------------------------------------------
# Parse JSON from XHR interception
# ---------------------------------------------------------------------------

def extract_from_json(data, doc_type):
    if not data:
        return []
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    records = []
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
            data.get("records", []) or []
        )
        if not hits and ("docNum" in data or "instrumentNumber" in data or "recordingNumber" in data):
            hits = [data]
    for hit in hits:
        try:
            src = hit.get("_source", hit)
            doc_num = clean(src.get("docNum") or src.get("instrumentNumber") or
                            src.get("docNumber") or src.get("recordingNumber") or
                            src.get("instrument") or "")
            filed   = clean(src.get("recordedDate") or src.get("filedDate") or
                            src.get("dateRecorded") or src.get("recordingDate") or "")
            grantor = clean(src.get("grantor") or src.get("grantorName") or
                            src.get("grantors") or src.get("party1Name") or
                            src.get("sellerName") or src.get("owner") or "")
            grantee = clean(src.get("grantee") or src.get("granteeName") or "")
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
            log.warning("JSON parse error: %s", exc)
    return records


# ---------------------------------------------------------------------------
# Clerk Scraper - with full debug dump on first doc type
# ---------------------------------------------------------------------------

class ClerkScraper:
    def __init__(self, start_ymd, end_ymd):
        self.start_ymd   = start_ymd
        self.end_ymd     = end_ymd
        self.debug_saved = False  # only dump HTML once

    def _url(self, doc_type, offset=0, limit=200):
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
            return []
        return await self._scrape()

    async def _scrape(self):
        all_records = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                ignore_https_errors=True,
                viewport={"width": 1280, "height": 900},
            )
            await context.route(
                re.compile(r"\.(png|jpg|jpeg|gif|woff|woff2|ttf|eot)$"),
                lambda route: route.abort()
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
        collected = []

        async def on_response(response):
            if "publicsearch.us" not in response.url:
                return
            if response.status != 200:
                return
            ct = response.headers.get("content-type", "")
            if "json" not in ct:
                return
            try:
                body = await response.json()
                if body:
                    collected.append((response.url, body))
            except Exception:
                pass

        page.on("response", on_response)
        all_records = []

        for attempt in range(1, MAX_RETRIES + 1):
            collected.clear()
            url = self._url(doc_type, offset=0, limit=200)
            try:
                await page.goto(url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(4)  # wait for React

                # --- Dump debug info on first doc type ---
                if not self.debug_saved and DEBUG_HTML:
                    self.debug_saved = True
                    html = await page.content()

                    # Save full HTML (first 50KB)
                    debug_path = Path("data/debug_page.html")
                    debug_path.parent.mkdir(parents=True, exist_ok=True)
                    debug_path.write_text(html[:50000], encoding="utf-8")
                    log.info("[DEBUG] Saved first 50KB of page HTML to data/debug_page.html")

                    # Log all XHR calls made
                    log.info("[DEBUG] XHR calls captured: %d", len(collected))
                    for resp_url, body in collected:
                        log.info("[DEBUG] XHR: %s", resp_url[:120])
                        if isinstance(body, dict):
                            log.info("[DEBUG]   keys=%s", list(body.keys())[:8])
                            # Log a sample of the data
                            for k, v in body.items():
                                if isinstance(v, list) and v:
                                    log.info("[DEBUG]   %s[0]=%s", k, str(v[0])[:200])
                                    break
                                elif isinstance(v, dict):
                                    log.info("[DEBUG]   %s=%s", k, str(v)[:200])
                        elif isinstance(body, list) and body:
                            log.info("[DEBUG]   list[0]=%s", str(body[0])[:200])

                    # Log a snippet of the visible page text
                    soup = BeautifulSoup(html, "lxml")
                    for tag in soup(["script", "style"]):
                        tag.decompose()
                    text = " ".join(soup.get_text(" ").split())
                    log.info("[DEBUG] Page text (first 500 chars): %s", text[:500])

                # --- Try JSON from XHR ---
                for resp_url, body in collected:
                    recs = extract_from_json(body, doc_type)
                    if recs:
                        log.info("  [XHR] %d records from %s", len(recs), resp_url[:80])
                        all_records.extend(recs)

                if all_records:
                    # Paginate
                    offset = len(all_records)
                    while True:
                        collected.clear()
                        next_url = self._url(doc_type, offset=offset, limit=200)
                        await page.goto(next_url, wait_until="networkidle", timeout=30000)
                        await asyncio.sleep(2)
                        page_recs = []
                        for _, body in collected:
                            page_recs.extend(extract_from_json(body, doc_type))
                        if not page_recs or len(page_recs) < 10:
                            break
                        all_records.extend(page_recs)
                        offset += len(page_recs)

                break

            except Exception as exc:
                log.warning("  Attempt %d/%d for %s: %s", attempt, MAX_RETRIES, doc_type, exc)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

        page.remove_listener("response", on_response)
        return all_records


# ---------------------------------------------------------------------------
# ArcGIS parcel lookup
# ---------------------------------------------------------------------------

class ParcelLookup:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "Mozilla/5.0 (compatible; BexarLeadBot/1.0)"
        self.cache = {}

    def _parse_feature(self, attrs):
        situs = clean(attrs.get("Situs") or "")
        parts = situs.rsplit(",", 1)
        prop_addr = parts[0].strip() if parts else situs
        prop_city, prop_zip = "", ""
        if len(parts) > 1:
            cz = parts[1].strip().rsplit(" ", 1)
            prop_city = cz[0].strip() if cz else ""
            prop_zip  = cz[1].strip() if len(cz) > 1 else ""
        mail_ln1  = clean(attrs.get("AddrLn1") or "")
        mail_ln2  = clean(attrs.get("AddrLn2") or "")
        mail_addr = (mail_ln1 + " " + mail_ln2).strip() if mail_ln2 else mail_ln1
        return {
            "prop_address": prop_addr,
            "prop_city":    prop_city or "San Antonio",
            "prop_state":   "TX",
            "prop_zip":     prop_zip,
            "mail_address": mail_addr,
            "mail_city":    clean(attrs.get("AddrCity") or ""),
            "mail_state":   clean(attrs.get("AddrSt") or "TX"),
            "mail_zip":     clean(attrs.get("Zip") or ""),
        }

    def lookup_batch(self, owner_names):
        if not owner_names:
            return
        quoted = ", ".join("'" + escape_sql(n.upper()) + "'" for n in owner_names)
        params = {
            "where":             "UPPER(Owner) IN (" + quoted + ")",
            "outFields":         "Owner,Situs,AddrLn1,AddrLn2,AddrCity,AddrSt,Zip",
            "returnGeometry":    "false",
            "resultRecordCount": 1000,
            "f":                 "json",
        }
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(ARCGIS_URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                for feat in data.get("features", []):
                    attrs = feat.get("attributes", {})
                    owner = clean(attrs.get("Owner") or "").upper()
                    if owner:
                        self.cache[owner] = self._parse_feature(attrs)
                return
            except Exception as exc:
                log.warning("ArcGIS attempt %d: %s", attempt + 1, exc)
                time.sleep(RETRY_DELAY)

    def enrich_all(self, records):
        owners = list({r.get("owner", "").upper()
                       for r in records if r.get("owner", "").strip()})
        if not owners:
            log.info("No owner names to look up.")
            return
        log.info("Looking up %d owner names in ArcGIS...", len(owners))
        for i in range(0, len(owners), 50):
            self.lookup_batch(owners[i:i + 50])
            time.sleep(0.3)
        found = sum(1 for o in owners if o in self.cache)
        log.info("ArcGIS matched %d / %d owners", found, len(owners))

    def get_address(self, owner):
        key = owner.upper().strip()
        if key in self.cache:
            return self.cache[key]
        for variant in normalize_name(owner):
            if variant in self.cache:
                return self.cache[variant]
        return {}


# ---------------------------------------------------------------------------
# Enrichment + Output
# ---------------------------------------------------------------------------

def enrich_records(records, parcel):
    owner_cats = {}
    for r in records:
        o = r.get("owner", "").upper()
        if o:
            owner_cats.setdefault(o, []).append(r.get("cat", ""))
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
            log.warning("Enrich error: %s", exc)
            enriched.append(r)
    return enriched

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

    log.info("Scraping Bexar County Clerk portal...")
    scraper     = ClerkScraper(start_ymd, end_ymd)
    raw_records = await scraper.run()
    log.info("Total raw records scraped: %d", len(raw_records))

    parcel = ParcelLookup()
    parcel.enrich_all(raw_records)

    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    output = build_output(records, start_mdy, end_mdy)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")
    log.info("Done. %d leads (%d with address).", output["total"], output["with_address"])


if __name__ == "__main__":
    asyncio.run(main())

