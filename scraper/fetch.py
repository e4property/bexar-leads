"""
Bexar County Motivated Seller Lead Scraper
Approach: Log in with Playwright, steal the auth cookies, then
make direct API calls using requests with those cookies.
This bypasses the React SPA entirely.
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
# Parse document records
# ---------------------------------------------------------------------------

def parse_doc(src, doc_type):
    if not isinstance(src, dict):
        return None
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    src = src.get("_source", src)
    doc_num = clean(src.get("docNum") or src.get("instrumentNumber") or
                    src.get("docNumber") or src.get("recordingNumber") or
                    src.get("instrument") or src.get("id") or "")
    filed   = clean(src.get("recordedDate") or src.get("filedDate") or
                    src.get("dateRecorded") or src.get("recordingDate") or
                    src.get("recorded") or "")
    grantor = clean(src.get("grantor") or src.get("grantorName") or
                    src.get("grantors") or src.get("party1Name") or
                    src.get("sellerName") or src.get("owner") or "")
    grantee = clean(src.get("grantee") or src.get("granteeName") or "")
    legal   = clean(src.get("legalDescription") or src.get("legal") or "")
    amount  = parse_amount(str(src.get("considerationAmount") or src.get("amount") or "0"))
    inst_id   = src.get("id") or src.get("instrumentId") or doc_num
    clerk_url = (CLERK_BASE + "/instruments/" + str(inst_id)) if inst_id else ""
    if filed and "T" in filed:
        try:
            filed = datetime.fromisoformat(filed.split("T")[0]).strftime("%m/%d/%Y")
        except Exception:
            pass
    if not (doc_num or filed):
        return None
    return {
        "doc_num": doc_num, "doc_type": doc_type,
        "filed": filed, "cat": cat, "cat_label": cat_label,
        "owner": grantor, "grantee": grantee,
        "amount": amount, "legal": legal, "clerk_url": clerk_url,
    }

def parse_response(data, doc_type):
    if not data:
        return []
    records = []
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (
            data.get("hits", {}).get("hits", []) if isinstance(data.get("hits"), dict) else []
        ) or data.get("hits", []) or data.get("results", []) or \
            data.get("documents", []) or data.get("items", []) or \
            data.get("data", []) or []
        if not items and ("docNum" in data or "instrumentNumber" in data):
            items = [data]
    for item in items:
        try:
            rec = parse_doc(item, doc_type)
            if rec:
                records.append(rec)
        except Exception:
            pass
    return records


# ---------------------------------------------------------------------------
# Step 1: Get auth cookies via Playwright login
# ---------------------------------------------------------------------------

async def get_auth_cookies():
    """Log in and return cookies dict for use with requests."""
    if not CLERK_EMAIL or not CLERK_PASSWORD:
        log.warning("No credentials. Trying without auth.")
        return {}

    if not PLAYWRIGHT_AVAILABLE:
        return {}

    cookies = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            log.info("Navigating to signin...")
            await page.goto(CLERK_BASE + "/signin", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Fill email
            for sel in ["input[type='email']", "#email", "input[name='email']",
                        "input[autocomplete='email']"]:
                try:
                    await page.fill(sel, CLERK_EMAIL, timeout=3000)
                    log.info("Email filled: %s", sel)
                    break
                except Exception:
                    continue

            # Fill password
            for sel in ["input[type='password']", "#password", "input[name='password']"]:
                try:
                    await page.fill(sel, CLERK_PASSWORD, timeout=3000)
                    log.info("Password filled: %s", sel)
                    break
                except Exception:
                    continue

            # Submit
            for sel in ["button[type='submit']", "button:has-text('Sign In')",
                        "button:has-text('Log In')"]:
                try:
                    await page.click(sel, timeout=3000)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=20000)
            await asyncio.sleep(2)

            url = page.url
            all_cookies = await context.cookies()
            log.info("Post-login URL: %s", url)
            log.info("Cookies: %s", [c["name"] for c in all_cookies])

            cookies = {c["name"]: c["value"] for c in all_cookies}
            if "authToken" in cookies:
                log.info("Login SUCCESS - got authToken")
            else:
                log.warning("Login may have failed - no authToken in cookies")

        except Exception as exc:
            log.warning("Login error: %s", exc)

        await browser.close()

    return cookies


# ---------------------------------------------------------------------------
# Step 2: Use cookies to call the Neumo search API directly
# ---------------------------------------------------------------------------

class DirectAPIScraper:
    """
    Call the Neumo platform API directly using auth cookies from login.
    Tries multiple known API endpoint patterns.
    """

    # Known Neumo API endpoint patterns
    API_PATTERNS = [
        "/api/search/results",
        "/api/publicly/search/results",
        "/api/instruments/search",
        "/api/search",
        "/api/publicly/instruments",
    ]

    def __init__(self, start_ymd, end_ymd, cookies):
        self.start_ymd = start_ymd
        self.end_ymd   = end_ymd
        self.session   = requests.Session()
        self.session.headers.update({
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         CLERK_BASE + "/results",
            "Origin":          CLERK_BASE,
            "X-Requested-With": "XMLHttpRequest",
        })
        # Set cookies
        for name, value in cookies.items():
            self.session.cookies.set(name, value, domain="bexar.tx.publicsearch.us")
        log.info("Session cookies set: %s", list(cookies.keys()))

    def _search_params(self, doc_type, offset=0, limit=200):
        return {
            "department":        "RP",
            "limit":             str(limit),
            "offset":            str(offset),
            "recordedDateRange": self.start_ymd + "," + self.end_ymd,
            "searchOcrText":     "false",
            "searchType":        "docType",
            "searchValue":       doc_type,
        }

    def discover_api(self):
        """Try API patterns to find the working one."""
        for pattern in self.API_PATTERNS:
            url = CLERK_BASE + pattern
            try:
                resp = self.session.get(url, params=self._search_params("LP"), timeout=15)
                log.info("API probe %s -> %d (ct=%s)",
                         pattern, resp.status_code,
                         resp.headers.get("content-type","")[:40])
                if resp.status_code == 200:
                    ct = resp.headers.get("content-type","")
                    if "json" in ct:
                        data = resp.json()
                        log.info("  JSON keys: %s", list(data.keys())[:8] if isinstance(data, dict) else "list")
                        return url
            except Exception as exc:
                log.warning("  Probe error: %s", exc)
        return None

    def search_doc_type(self, api_url, doc_type):
        records = []
        offset  = 0
        while True:
            for attempt in range(MAX_RETRIES):
                try:
                    resp = self.session.get(
                        api_url,
                        params=self._search_params(doc_type, offset=offset),
                        timeout=30
                    )
                    if resp.status_code != 200:
                        log.warning("  %s status %d", doc_type, resp.status_code)
                        return records
                    data = resp.json()
                    page_recs = parse_response(data, doc_type)
                    if page_recs:
                        records.extend(page_recs)
                        if len(page_recs) < 200:
                            return records
                        offset += len(page_recs)
                    else:
                        return records
                    break
                except Exception as exc:
                    log.warning("  Attempt %d: %s", attempt+1, exc)
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
            else:
                break
        return records

    def run(self):
        log.info("Discovering API endpoint...")
        api_url = self.discover_api()

        if not api_url:
            log.warning("No working API found. Trying results page scrape...")
            return self.scrape_results_pages()

        log.info("Using API: %s", api_url)
        all_records = []
        for doc_type in TARGET_DOC_TYPES:
            log.info("Searching: %s", doc_type)
            recs = self.search_doc_type(api_url, doc_type)
            log.info("  Found %d for %s", len(recs), doc_type)
            all_records.extend(recs)
            time.sleep(0.5)
        return all_records

    def scrape_results_pages(self):
        """
        Fallback: fetch the results page as plain HTTP and parse
        whatever the server returns (may include SSR data).
        """
        all_records = []
        for doc_type in TARGET_DOC_TYPES:
            url = (CLERK_BASE + "/results"
                   + "?department=RP&limit=200&offset=0"
                   + "&recordedDateRange=" + self.start_ymd + "," + self.end_ymd
                   + "&searchOcrText=false&searchType=docType&searchValue=" + doc_type)
            try:
                resp = self.session.get(url, timeout=30)
                log.info("Results page %s: status=%d size=%d",
                         doc_type, resp.status_code, len(resp.content))

                # Look for __INITIAL_STATE__ or similar SSR data
                html = resp.text
                for pattern in [
                    r"window\.__INITIAL_STATE__\s*=\s*({.+?});\s*</script>",
                    r"window\.__data\s*=\s*({.+?});\s*</script>",
                    r"window\.__PRELOADED_STATE__\s*=\s*({.+?});\s*</script>",
                ]:
                    m = re.search(pattern, html, re.DOTALL)
                    if m:
                        try:
                            state = json.loads(m.group(1))
                            # Look for results anywhere in state
                            recs = self._search_state(state, doc_type)
                            if recs:
                                log.info("  Found %d records in page state", len(recs))
                                all_records.extend(recs)
                                break
                        except Exception:
                            pass

            except Exception as exc:
                log.warning("Results page error %s: %s", doc_type, exc)

        return all_records

    def _search_state(self, state, doc_type, depth=0):
        if depth > 5 or not state:
            return []
        records = []
        if isinstance(state, dict):
            for key in ["results", "hits", "documents", "items", "data", "instruments"]:
                val = state.get(key)
                if isinstance(val, list) and val:
                    recs = parse_response(val, doc_type)
                    if recs:
                        records.extend(recs)
            for val in state.values():
                if isinstance(val, (dict, list)):
                    records.extend(self._search_state(val, doc_type, depth+1))
        elif isinstance(state, list):
            recs = parse_response(state, doc_type)
            if recs:
                records.extend(recs)
        return records


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
    log.info("Email: %s", bool(CLERK_EMAIL))
    log.info("Password: %s", bool(CLERK_PASSWORD))
    log.info("=" * 60)

    start_ymd, end_ymd = date_range_yyyymmdd(LOOKBACK_DAYS)
    start_mdy, end_mdy = date_range_mmddyyyy(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_mdy, end_mdy)

    # Get auth cookies via Playwright login
    log.info("Getting auth cookies...")
    cookies = await get_auth_cookies()
    log.info("Got %d cookies: %s", len(cookies), list(cookies.keys()))

    # Use cookies to call API directly
    scraper     = DirectAPIScraper(start_ymd, end_ymd, cookies)
    raw_records = scraper.run()
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

