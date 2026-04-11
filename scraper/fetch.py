"""
Bexar County Motivated Seller Lead Scraper
The portal SPA loads data via XHR. We intercept those calls.
Key insight from debug: page captures skeleton HTML before results load.
Fix: wait for actual result elements, not just networkidle.
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
CLERK_BASE    = "https://bexar.tx.publicsearch.us"
ARCGIS_URL    = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

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
MAX_RETRIES  = 3
RETRY_DELAY  = 5
# How long to wait for results to appear after page load
RESULT_WAIT  = 20000  # ms


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
    if cat == "LP":           flags.append("Lis pendens")
    if cat == "NOFC":         flags.append("Pre-foreclosure")
    if cat == "JUD":          flags.append("Judgment lien")
    if doc_type in ("LNCORPTX","LNIRS","LNFED","TAXDEED"):
                              flags.append("Tax lien")
    if doc_type == "LNMECH":  flags.append("Mechanic lien")
    if cat == "PRO":          flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|LP|LLP|TRUST|HOLDINGS)\b", owner):
                              flags.append("LLC / corp owner")
    if is_new_this_week(record.get("filed", "")):
                              flags.append("New this week")
    return flags

def compute_score(record, flags):
    score = 30
    score += len(flags) * 10
    if "LP" in record.get("_owner_cats",[]) and "NOFC" in record.get("_owner_cats",[]):
        score += 20
    amount = record.get("amount", 0.0) or 0.0
    if amount > 100000: score += 15
    elif amount > 50000: score += 10
    if "New this week" in flags: score += 5
    if record.get("prop_address"): score += 5
    return min(score, 100)


# ---------------------------------------------------------------------------
# Parse XHR JSON response
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
        if not hits and ("docNum" in data or "instrumentNumber" in data):
            hits = [data]
    for hit in hits:
        try:
            src = hit.get("_source", hit)
            doc_num = clean(src.get("docNum") or src.get("instrumentNumber") or
                            src.get("docNumber") or src.get("recordingNumber") or "")
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
# Parse rendered HTML after React loads results
# ---------------------------------------------------------------------------

def extract_from_html(html, doc_type):
    """Parse the fully-rendered results table from the SPA."""
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    records = []
    try:
        soup = BeautifulSoup(html, "lxml")

        # The results render inside a table with class "results-table" or similar
        # Based on the portal's CSS classes observed, look for the main results list
        # Try data-testid attributes first (most reliable)
        rows = (
            soup.find_all(attrs={"data-testid": re.compile(r"result|row|instrument|record", re.I)}) or
            soup.find_all("tr", attrs={"data-testid": True}) or
            soup.find_all("li", class_=re.compile(r"result|instrument|record", re.I)) or
            []
        )

        if rows:
            for row in rows:
                text = row.get_text(" ", strip=True)
                # Skip placeholder/skeleton rows
                if not text or len(text) < 10:
                    continue
                link    = row.find("a", href=True)
                doc_num, clerk_url = "", ""
                if link:
                    href = link["href"]
                    if not href.startswith("http"):
                        href = CLERK_BASE + href
                    clerk_url = href
                    # Extract instrument number from URL or text
                    m = re.search(r"/instruments?/(\d+)", href)
                    doc_num = m.group(1) if m else clean(link.get_text(strip=True))

                # Date pattern
                date_m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
                filed  = date_m.group(1) if date_m else ""

                # Amount pattern
                amount_m = re.search(r"\$[\d,]+\.?\d*", text)
                amount   = parse_amount(amount_m.group(0)) if amount_m else 0.0

                if doc_num or filed:
                    records.append({
                        "doc_num": doc_num, "doc_type": doc_type,
                        "filed": filed, "cat": cat, "cat_label": cat_label,
                        "owner": "", "grantee": "",
                        "amount": amount, "legal": "", "clerk_url": clerk_url,
                    })

        # Fall back to table
        if not records:
            table = soup.find("table")
            if table:
                rows    = table.find_all("tr")
                headers = [th.get_text(strip=True).lower()
                           for th in rows[0].find_all(["th","td"])] if rows else []

                def col(cells, *names):
                    for name in names:
                        for i, h in enumerate(headers):
                            if name in h and i < len(cells):
                                return clean(cells[i].get_text(strip=True))
                    return ""

                for row in rows[1:]:
                    cells = row.find_all("td")
                    if not cells or len(cells) < 2:
                        continue
                    link      = row.find("a", href=True)
                    clerk_url = ""
                    doc_num   = ""
                    if link:
                        href = link["href"]
                        if not href.startswith("http"):
                            href = CLERK_BASE + href
                        clerk_url = href
                        m = re.search(r"/instruments?/(\d+)", href)
                        doc_num = m.group(1) if m else clean(link.get_text(strip=True))
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

    except Exception as exc:
        log.warning("HTML extract error for %s: %s", doc_type, exc)

    return records


# ---------------------------------------------------------------------------
# Clerk Scraper
# ---------------------------------------------------------------------------

class ClerkScraper:
    def __init__(self, start_ymd, end_ymd):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd

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
                args=["--no-sandbox","--disable-setuid-sandbox",
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
            # Block images/fonts to speed up
            await context.route(
                re.compile(r"\.(png|jpg|jpeg|gif|woff|woff2|ttf|eot|svg)(\?.*)?$"),
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
        collected = []  # (url, json_body)

        async def on_response(response):
            url = response.url
            if "publicsearch.us" not in url:
                return
            if response.status != 200:
                return
            ct = response.headers.get("content-type", "")
            if "json" not in ct:
                return
            try:
                body = await response.json()
                if body:
                    collected.append((url, body))
                    log.info("  [XHR] Captured JSON from: %s", url[:100])
            except Exception:
                pass

        page.on("response", on_response)
        all_records = []

        for attempt in range(1, MAX_RETRIES + 1):
            collected.clear()
            url = self._url(doc_type, offset=0, limit=200)
            try:
                # Go to page
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)

                # Wait for EITHER results OR a "no results" indicator
                # These selectors are based on the Neumo SPA pattern
                try:
                    await page.wait_for_selector(
                        # Result rows, or empty state, or error - whichever comes first
                        "[data-testid='resultRow'], "
                        "[data-testid='searchResultRow'], "
                        ".result-row, "
                        ".results-list__item, "
                        ".search-results__no-results, "
                        ".no-results, "
                        "table tbody tr, "
                        "[class*='result'][class*='row'], "
                        "[class*='resultRow']",
                        timeout=RESULT_WAIT,
                    )
                    log.info("  [WAIT] Result element found for %s", doc_type)
                except Exception:
                    log.info("  [WAIT] No result selector matched for %s, checking XHR...", doc_type)
                    # Give a bit more time for XHR even if no DOM element found
                    await asyncio.sleep(3)

                # --- Primary: extract from intercepted XHR JSON ---
                for resp_url, body in collected:
                    recs = extract_from_json(body, doc_type)
                    if recs:
                        log.info("  [XHR] Parsed %d records", len(recs))
                        all_records.extend(recs)

                # --- Secondary: parse the rendered HTML ---
                if not all_records:
                    html = await page.content()
                    recs = extract_from_html(html, doc_type)
                    if recs:
                        log.info("  [HTML] Parsed %d records", len(recs))
                        all_records.extend(recs)

                # Debug: log XHR summary if nothing found
                if not all_records:
                    log.info("  [DEBUG] %d XHR responses captured:", len(collected))
                    for rurl, rbody in collected:
                        btype = type(rbody).__name__
                        bkeys = list(rbody.keys())[:6] if isinstance(rbody, dict) else "list"
                        log.info("    %s | %s | keys=%s", rurl[-60:], btype, bkeys)

                # Paginate
                if all_records:
                    offset = len(all_records)
                    while True:
                        collected.clear()
                        await page.goto(
                            self._url(doc_type, offset=offset, limit=200),
                            wait_until="domcontentloaded", timeout=30000
                        )
                        try:
                            await page.wait_for_selector(
                                "[data-testid='resultRow'], table tbody tr, .result-row",
                                timeout=RESULT_WAIT
                            )
                        except Exception:
                            await asyncio.sleep(3)

                        page_recs = []
                        for _, body in collected:
                            page_recs.extend(extract_from_json(body, doc_type))
                        if not page_recs:
                            html = await page.content()
                            page_recs = extract_from_html(html, doc_type)
                        if not page_recs or len(page_recs) < 5:
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
# ArcGIS parcel lookup (on-demand, by owner name)
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
        owners = list({r.get("owner","").upper()
                       for r in records if r.get("owner","").strip()})
        if not owners:
            log.info("No owner names to look up.")
            return
        log.info("Looking up %d owners in ArcGIS...", len(owners))
        for i in range(0, len(owners), 50):
            self.lookup_batch(owners[i:i+50])
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
        o = r.get("owner","").upper()
        if o:
            owner_cats.setdefault(o,[]).append(r.get("cat",""))
    enriched = []
    for r in records:
        try:
            owner     = r.get("owner","")
            addr_info = parcel.get_address(owner)
            r["prop_address"] = addr_info.get("prop_address","")
            r["prop_city"]    = addr_info.get("prop_city","San Antonio")
            r["prop_state"]   = addr_info.get("prop_state","TX")
            r["prop_zip"]     = addr_info.get("prop_zip","")
            r["mail_address"] = addr_info.get("mail_address","")
            r["mail_city"]    = addr_info.get("mail_city","")
            r["mail_state"]   = addr_info.get("mail_state","TX")
            r["mail_zip"]     = addr_info.get("mail_zip","")
            r["_owner_cats"]  = owner_cats.get(owner.upper(),[])
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
            owner  = r.get("owner","")
            parts  = owner.split(",",1) if "," in owner else owner.split(" ",1)
            first  = parts[1].strip() if len(parts) > 1 else ""
            last   = parts[0].strip()
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",""),
                "Mailing State":          r.get("mail_state","TX"),
                "Mailing Zip":            r.get("mail_zip",""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",""),
                "Property State":         r.get("prop_state","TX"),
                "Property Zip":           r.get("prop_zip",""),
                "Lead Type":              r.get("cat_label",r.get("cat","")),
                "Document Type":          r.get("doc_type",""),
                "Date Filed":             r.get("filed",""),
                "Document Number":        r.get("doc_num",""),
                "Amount/Debt Owed":       r.get("amount",""),
                "Seller Score":           r.get("score",0),
                "Motivated Seller Flags": "|".join(r.get("flags",[])),
                "Source":                 "Bexar County Clerk",
                "Public Records URL":     r.get("clerk_url",""),
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
    log.info("Total raw records: %d", len(raw_records))

    parcel = ParcelLookup()
    parcel.enrich_all(raw_records)

    records = enrich_records(raw_records, parcel)
    records.sort(key=lambda r: r.get("score",0), reverse=True)

    output = build_output(records, start_mdy, end_mdy)
    save_json(output, "dashboard/records.json", "data/records.json")
    save_ghl_csv(records, "data/leads_export.csv")
    log.info("Done. %d leads (%d with address).", output["total"], output["with_address"])


if __name__ == "__main__":
    asyncio.run(main())

