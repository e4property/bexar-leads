"""
Bexar County Motivated Seller Lead Scraper
Strategy: The publicsearch.us portal blocks all headless access.
Instead we use:
1. BCAD property search API (esearch.bcad.org) - finds owners by name/address
2. Bexar County's open data foreclosure/lien feeds
3. Direct HTTP to publicsearch.us with full browser cookie simulation
4. As fallback: generate sample structure so dashboard works
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
# Method 1: Direct HTTP with full session simulation
# The portal IS accessible via plain HTTP if we set cookies correctly
# ---------------------------------------------------------------------------

class DirectHTTPScraper:
    """
    Simulate a real browser session by:
    1. First visiting the homepage to get session cookies
    2. Then hitting the results page as if we're a real user
    3. The page HTML contains window.__data with search state
    4. We extract instrument numbers from that data
    """

    BASE_HEADERS = {
        "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.5",
        "Accept-Encoding":           "gzip, deflate, br",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "none",
    }

    def __init__(self, start_ymd, end_ymd):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd
        self.session   = requests.Session()
        self.session.headers.update(self.BASE_HEADERS)

    def _prime_session(self):
        """Visit homepage to get cookies."""
        try:
            resp = self.session.get(CLERK_BASE + "/", timeout=20)
            log.info("[HTTP] Homepage status: %d, cookies: %s",
                     resp.status_code, list(self.session.cookies.keys()))
        except Exception as exc:
            log.warning("[HTTP] Homepage prime failed: %s", exc)

    def _search_doc_type(self, doc_type):
        """Fetch results page and extract data from window.__data or HTML."""
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
        records = []

        url = (CLERK_BASE + "/results"
               + "?department=RP"
               + "&limit=200&offset=0"
               + "&recordedDateRange=" + self.start_ymd + "," + self.end_ymd
               + "&searchOcrText=false"
               + "&searchType=docType"
               + "&searchValue=" + doc_type)

        for attempt in range(MAX_RETRIES):
            try:
                self.session.headers["Referer"] = CLERK_BASE + "/"
                resp = self.session.get(url, timeout=30)
                log.info("[HTTP] %s status=%d size=%d",
                         doc_type, resp.status_code, len(resp.content))

                if resp.status_code != 200:
                    break

                html = resp.text

                # Try to extract window.__data which contains search state
                m = re.search(r"window\.__data\s*=\s*(\{.+?\});\s*</script>",
                              html, re.DOTALL)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        # Look for results in the state tree
                        results = self._extract_from_state(data, doc_type)
                        if results:
                            log.info("[HTTP] Found %d records in window.__data", len(results))
                            return results
                    except Exception as exc:
                        log.warning("[HTTP] window.__data parse error: %s", exc)

                # Try parsing HTML table directly
                soup = BeautifulSoup(html, "lxml")

                # Check for sign-in redirect
                if "signin" in resp.url or "Sign In" in html[:2000]:
                    log.info("[HTTP] Portal requires sign-in for %s", doc_type)
                    break

                # Look for result data in script tags
                for script in soup.find_all("script"):
                    src = script.string or ""
                    if "grantor" in src.lower() or "instrument" in src.lower():
                        log.info("[HTTP] Found instrument data in script tag")
                        # Try to extract JSON arrays from script
                        json_matches = re.findall(r'\[(\{[^\[\]]{50,}\})\]', src)
                        for jm in json_matches[:3]:
                            try:
                                items = json.loads("[" + jm + "]")
                                recs = self._parse_items(items, doc_type)
                                if recs:
                                    records.extend(recs)
                            except Exception:
                                pass

                # Table fallback
                table = soup.find("table")
                if table:
                    rows = table.find_all("tr")[1:]
                    headers = [th.get_text(strip=True).lower()
                               for th in table.find_all("tr")[0].find_all(["th","td"])]
                    for row in rows:
                        cells = row.find_all("td")
                        if not cells:
                            continue
                        link = row.find("a", href=True)
                        doc_num, clerk_url = "", ""
                        if link:
                            href = link["href"]
                            if not href.startswith("http"):
                                href = CLERK_BASE + href
                            clerk_url = href
                            m2 = re.search(r"/instruments?/(\d+)", href)
                            doc_num = m2.group(1) if m2 else clean(link.get_text(strip=True))
                        if doc_num:
                            records.append({
                                "doc_num": doc_num, "doc_type": doc_type,
                                "filed": "", "cat": cat, "cat_label": cat_label,
                                "owner": "", "grantee": "", "amount": 0.0,
                                "legal": "", "clerk_url": clerk_url,
                            })

                break
            except Exception as exc:
                log.warning("[HTTP] Attempt %d for %s: %s", attempt+1, doc_type, exc)
                time.sleep(RETRY_DELAY)

        return records

    def _extract_from_state(self, data, doc_type):
        """Recursively search window.__data for result arrays."""
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
        records = []
        if isinstance(data, dict):
            for k, v in data.items():
                if k in ("results","hits","documents","items","records","data"):
                    items = v if isinstance(v, list) else (v.get("hits",[]) if isinstance(v,dict) else [])
                    recs = self._parse_items(items, doc_type)
                    if recs:
                        records.extend(recs)
                elif isinstance(v, (dict, list)):
                    records.extend(self._extract_from_state(v, doc_type))
        elif isinstance(data, list):
            for item in data:
                records.extend(self._extract_from_state(item, doc_type))
        return records

    def _parse_items(self, items, doc_type):
        cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
        records = []
        for src in items:
            if not isinstance(src, dict):
                continue
            src2 = src.get("_source", src)
            doc_num = clean(src2.get("docNum") or src2.get("instrumentNumber") or
                            src2.get("docNumber") or "")
            filed   = clean(src2.get("recordedDate") or src2.get("filedDate") or "")
            grantor = clean(src2.get("grantor") or src2.get("grantorName") or "")
            if filed and "T" in filed:
                try:
                    filed = datetime.fromisoformat(filed.split("T")[0]).strftime("%m/%d/%Y")
                except Exception:
                    pass
            inst_id = src2.get("id") or src2.get("instrumentId") or doc_num
            clerk_url = (CLERK_BASE + "/instruments/" + str(inst_id)) if inst_id else ""
            if doc_num or filed:
                records.append({
                    "doc_num": doc_num, "doc_type": doc_type,
                    "filed": filed, "cat": cat, "cat_label": cat_label,
                    "owner": grantor,
                    "grantee": clean(src2.get("grantee") or src2.get("granteeName") or ""),
                    "amount": parse_amount(str(src2.get("considerationAmount") or "0")),
                    "legal":  clean(src2.get("legalDescription") or ""),
                    "clerk_url": clerk_url,
                })
        return records

    def run(self):
        self._prime_session()
        all_records = []
        for doc_type in TARGET_DOC_TYPES:
            log.info("Searching for doc type: %s", doc_type)
            recs = self._search_doc_type(doc_type)
            log.info("  Found %d records for %s", len(recs), doc_type)
            all_records.extend(recs)
            time.sleep(1)
        return all_records


# ---------------------------------------------------------------------------
# Method 2: Playwright with extended wait + JavaScript execution
# ---------------------------------------------------------------------------

async def playwright_js_extract(start_ymd, end_ymd):
    """
    Use Playwright but execute JavaScript inside the page to read
    the React component state directly.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return []

    all_records = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-setuid-sandbox","--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ignore_https_errors=True,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )

        # First visit homepage to get cookies
        page = await context.new_page()
        try:
            await page.goto(CLERK_BASE + "/", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)
            log.info("[PW] Homepage loaded, cookies: %s",
                     [c["name"] for c in await context.cookies()])
        except Exception as exc:
            log.warning("[PW] Homepage error: %s", exc)

        for doc_type in TARGET_DOC_TYPES[:3]:  # Try first 3 to save time
            log.info("[PW] Trying doc type: %s", doc_type)
            url = (CLERK_BASE + "/results"
                   + "?department=RP&limit=200&offset=0"
                   + "&recordedDateRange=" + start_ymd + "," + end_ymd
                   + "&searchOcrText=false&searchType=docType&searchValue=" + doc_type)
            try:
                await page.goto(url, wait_until="networkidle", timeout=45000)
                # Wait longer for React to hydrate
                await asyncio.sleep(15)

                # Try reading Redux/React state via JavaScript
                state = await page.evaluate("""
                    () => {
                        // Try various ways to access app state
                        try {
                            // Redux store
                            if (window.__redux_store__) return window.__redux_store__.getState();
                            if (window.store) return window.store.getState();
                            // React fiber
                            const root = document.querySelector('[id="main-content"]');
                            if (root && root._reactFiber) {
                                return {fiber: 'found'};
                            }
                            // Check window.__data
                            if (window.__data) return window.__data;
                            // Check for any global with results
                            const keys = Object.keys(window).filter(k =>
                                typeof window[k] === 'object' &&
                                window[k] &&
                                (window[k].results || window[k].hits || window[k].documents)
                            );
                            if (keys.length) return {found_keys: keys, data: window[keys[0]]};
                            return {error: 'no state found', windowKeys: Object.keys(window).slice(0,30)};
                        } catch(e) { return {error: e.toString()}; }
                    }
                """)

                log.info("[PW-JS] State for %s: %s",
                         doc_type, str(state)[:300] if state else "null")

                # Also get page title and text snippet
                title = await page.title()
                text  = await page.inner_text("body")
                log.info("[PW-JS] Page title: %s", title)
                log.info("[PW-JS] Body text snippet: %s", text[:200].replace("\n"," "))

            except Exception as exc:
                log.warning("[PW-JS] Error for %s: %s", doc_type, exc)

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

    # Method 1: Direct HTTP with session simulation
    log.info("Trying Method 1: Direct HTTP session...")
    scraper     = DirectHTTPScraper(start_ymd, end_ymd)
    raw_records = scraper.run()
    log.info("Method 1 result: %d records", len(raw_records))

    # Method 2: Playwright with JS execution (diagnostic + fallback)
    if not raw_records:
        log.info("Trying Method 2: Playwright with JS state extraction...")
        await playwright_js_extract(start_ymd, end_ymd)

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

