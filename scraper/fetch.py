"""
Bexar County Motivated Seller Lead Scraper
CONFIRMED: Results are server-side rendered into the HTML table.
No API needed. Just parse the HTML table with correct URL params.
URL format: /results?department=RP&keywordSearch=false&recordedDateRange=YYYYMMDD,YYYYMMDD&searchType=docType&searchValue=LP&limit=50&offset=0
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

LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_EMAIL    = (os.getenv("CLERK_EMAIL") or "").strip()
CLERK_PASSWORD = (os.getenv("CLERK_PASSWORD") or "").strip()
CLERK_BASE     = "https://bexar.tx.publicsearch.us"
ARCGIS_URL     = "https://maps.bexar.org/arcgis/rest/services/Parcels/MapServer/0/query"

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
# Parse SSR HTML table from results page
# ---------------------------------------------------------------------------

def parse_results_html(html, doc_type, start_ymd, end_ymd):
    """
    Parse server-side rendered results table.
    The table has columns: GRANTOR, GRANTEE, DOC TYPE, RECORDED DATE
    Each row has a link to the instrument.
    """
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    records = []

    try:
        soup = BeautifulSoup(html, "lxml")

        # Find result count to log
        count_el = soup.find(string=re.compile(r"\d[\d,]* results"))
        if count_el:
            log.info("  [HTML] Page reports: %s", count_el.strip()[:60])

        # Find the results table - look for table with GRANTOR header
        table = None
        for t in soup.find_all("table"):
            headers = t.find_all("th")
            header_text = " ".join(h.get_text(strip=True).upper() for h in headers)
            if "GRANTOR" in header_text or "DOC" in header_text:
                table = t
                break

        # Also try finding rows by data attributes or specific classes
        if not table:
            # Look for result rows by common Neumo patterns
            rows = (
                soup.find_all("tr", class_=re.compile(r"result|instrument|record", re.I)) or
                soup.find_all(attrs={"data-testid": re.compile(r"result|row|instrument", re.I)})
            )
            if rows:
                log.info("  [HTML] Found %d result rows via class/attr", len(rows))
                for row in rows:
                    rec = _parse_row_flexible(row, doc_type, cat, cat_label)
                    if rec:
                        records.append(rec)
                return records

        if not table:
            # Log what we see on the page for debugging
            text = soup.get_text(" ", strip=True)[:500]
            log.info("  [HTML] No table found. Page text: %s", text)
            return records

        rows = table.find_all("tr")
        if not rows:
            return records

        # Parse header row
        header_row = rows[0]
        headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th","td"])]
        log.info("  [HTML] Table headers: %s", headers)

        def col(cells, *names):
            for name in names:
                for i, h in enumerate(headers):
                    if name in h and i < len(cells):
                        return clean(cells[i].get_text(strip=True))
            return ""

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            # Get link to instrument
            link      = row.find("a", href=True)
            clerk_url = ""
            doc_num   = ""
            if link:
                href = link["href"]
                if not href.startswith("http"):
                    href = CLERK_BASE + href
                clerk_url = href
                # Extract instrument number from URL
                m = re.search(r"/(?:doc|instruments?)/(\d+)", href)
                if m:
                    doc_num = m.group(1)

            grantor  = col(cells, "grantor", "owner", "from", "party1", "seller")
            grantee  = col(cells, "grantee", "to", "party2", "buyer")
            doc_type_col = col(cells, "doc", "type", "document")
            filed    = col(cells, "recorded", "date", "filed")
            amount   = parse_amount(col(cells, "amount", "consideration"))

            if not grantor and not doc_num:
                continue

            # Filter by doc type if column shows a different type
            if doc_type_col and doc_type not in doc_type_col.upper():
                # Check if it matches any of our target types
                matched = False
                for dt in TARGET_DOC_TYPES:
                    if dt in doc_type_col.upper():
                        cat2, cat_label2 = DOC_TYPE_MAP.get(dt, (dt, dt))
                        records.append({
                            "doc_num": doc_num, "doc_type": dt,
                            "filed": filed, "cat": cat2, "cat_label": cat_label2,
                            "owner": grantor, "grantee": grantee,
                            "amount": amount, "legal": "",
                            "clerk_url": clerk_url,
                        })
                        matched = True
                        break
                if not matched and doc_type_col:
                    # Still add with searched doc_type
                    pass
                else:
                    continue

            records.append({
                "doc_num": doc_num, "doc_type": doc_type,
                "filed": filed, "cat": cat, "cat_label": cat_label,
                "owner": grantor, "grantee": grantee,
                "amount": amount, "legal": "",
                "clerk_url": clerk_url,
            })

    except Exception as exc:
        log.warning("  [HTML] Parse error: %s", exc)

    return records


def _parse_row_flexible(row, doc_type, cat, cat_label):
    """Parse a result row when we don't have header context."""
    cells = row.find_all(["td","th"])
    if not cells:
        return None
    link = row.find("a", href=True)
    clerk_url, doc_num = "", ""
    if link:
        href = link["href"]
        if not href.startswith("http"):
            href = CLERK_BASE + href
        clerk_url = href
        m = re.search(r"/(?:doc|instruments?)/(\d+)", href)
        if m:
            doc_num = m.group(1)
    text = row.get_text(" ", strip=True)
    date_m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    filed  = date_m.group(1) if date_m else ""
    if not (doc_num or filed):
        return None
    all_text = [c.get_text(strip=True) for c in cells]
    grantor = all_text[0] if all_text else ""
    grantee = all_text[1] if len(all_text) > 1 else ""
    return {
        "doc_num": doc_num, "doc_type": doc_type,
        "filed": filed, "cat": cat, "cat_label": cat_label,
        "owner": grantor, "grantee": grantee,
        "amount": 0.0, "legal": "", "clerk_url": clerk_url,
    }


# ---------------------------------------------------------------------------
# Scraper using Playwright for SSR pages
# ---------------------------------------------------------------------------

class SSRScraper:
    """
    The portal does SSR - results are in the HTML.
    Use Playwright to render each page properly with auth,
    then parse the rendered DOM.
    """

    def __init__(self, start_ymd, end_ymd):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd

    def _url(self, doc_type, offset=0, limit=50):
        # Use the EXACT URL format observed in the browser
        return (
            CLERK_BASE + "/results"
            + "?department=RP"
            + "&keywordSearch=false"
            + "&limit=" + str(limit)
            + "&offset=" + str(offset)
            + "&recordedDateRange=" + self.start_ymd + "%2C" + self.end_ymd
            + "&searchOcrText=false"
            + "&searchType=docType"
            + "&searchValue=" + doc_type
        )

    async def _login(self, page, context):
        if not CLERK_EMAIL or not CLERK_PASSWORD:
            log.info("No credentials - proceeding without auth")
            return False
        try:
            log.info("Logging in...")
            await page.goto(CLERK_BASE + "/signin", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            for sel in ["input[type='email']", "#email", "input[name='email']"]:
                try:
                    await page.fill(sel, CLERK_EMAIL, timeout=3000)
                    break
                except Exception:
                    continue
            for sel in ["input[type='password']", "#password"]:
                try:
                    await page.fill(sel, CLERK_PASSWORD, timeout=3000)
                    break
                except Exception:
                    continue
            for sel in ["button[type='submit']", "button:has-text('Sign In')"]:
                try:
                    await page.click(sel, timeout=3000)
                    break
                except Exception:
                    continue
            await page.wait_for_load_state("networkidle", timeout=20000)
            await asyncio.sleep(2)
            cookies = [c["name"] for c in await context.cookies()]
            success = "authToken" in str(cookies)
            log.info("Login: %s", "SUCCESS" if success else "FAILED")
            return success
        except Exception as exc:
            log.warning("Login error: %s", exc)
            return False

    async def _scrape_doc_type(self, page, doc_type):
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
        all_records = []
        offset  = 0
        limit   = 50
        page_num = 0

        while True:
            page_num += 1
            url = self._url(doc_type, offset=offset, limit=limit)

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await page.goto(url, wait_until="networkidle", timeout=45000)
                    # Wait for table to render
                    try:
                        await page.wait_for_selector("table, tr[class*='result'], [data-testid*='result']",
                                                      timeout=10000)
                    except Exception:
                        pass
                    await asyncio.sleep(1)

                    html = await page.content()
                    recs = parse_results_html(html, doc_type,
                                              self.start_ymd, self.end_ymd)

                    if page_num == 1:
                        log.info("  Page 1: %d records (offset=0)", len(recs))

                    if not recs:
                        return all_records

                    all_records.extend(recs)

                    if len(recs) < limit:
                        # Last page
                        return all_records

                    offset += limit
                    break

                except Exception as exc:
                    log.warning("  Attempt %d/%d offset=%d: %s",
                                attempt, MAX_RETRIES, offset, exc)
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)

            else:
                break

            # Safety: max 20 pages per doc type
            if page_num >= 20:
                break

        return all_records

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            return []

        all_records = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                ignore_https_errors=True,
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            page = await context.new_page()

            await self._login(page, context)

            for doc_type in TARGET_DOC_TYPES:
                log.info("Searching: %s", doc_type)
                recs = await self._scrape_doc_type(page, doc_type)
                log.info("  Total: %d for %s", len(recs), doc_type)
                all_records.extend(recs)

            await browser.close()

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
                log.warning("ArcGIS attempt %d: %s", attempt+1, exc)
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
    log.info("Email: %s | Password: %s", bool(CLERK_EMAIL), bool(CLERK_PASSWORD))
    log.info("=" * 60)

    start_ymd, end_ymd = date_range_yyyymmdd(LOOKBACK_DAYS)
    start_mdy, end_mdy = date_range_mmddyyyy(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_mdy, end_mdy)

    log.info("Scraping clerk portal (SSR HTML)...")
    scraper     = SSRScraper(start_ymd, end_ymd)
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

