"""
Bexar County Motivated Seller Lead Scraper
Uses authenticated Playwright session + full JS state dump to find results.
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
CLERK_EMAIL    = os.getenv("CLERK_EMAIL", "").strip()
CLERK_PASSWORD = os.getenv("CLERK_PASSWORD", "").strip()
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
# Parse records from any JSON structure
# ---------------------------------------------------------------------------

def parse_record(src, doc_type):
    """Parse a single hit/record from whatever shape the API returns."""
    if not isinstance(src, dict):
        return None
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))
    # Unwrap _source if present (Elasticsearch pattern)
    src = src.get("_source", src)
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
    amount  = parse_amount(str(src.get("considerationAmount") or src.get("amount") or "0"))
    inst_id = src.get("id") or src.get("instrumentId") or doc_num
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

def extract_records(data, doc_type):
    """Extract records from any JSON shape."""
    if not data or not isinstance(data, (dict, list)):
        return []
    records = []
    hits = []
    if isinstance(data, list):
        hits = data
    elif isinstance(data, dict):
        hits = (
            data.get("hits", {}).get("hits", []) if isinstance(data.get("hits"), dict) else
            data.get("hits", []) if isinstance(data.get("hits"), list) else []
        ) or data.get("results", []) or data.get("documents", []) or \
            data.get("data", []) or data.get("items", []) or data.get("records", []) or []
        # If the dict itself looks like a record
        if not hits and ("docNum" in data or "instrumentNumber" in data):
            hits = [data]
    for hit in hits:
        try:
            rec = parse_record(hit, doc_type)
            if rec:
                records.append(rec)
        except Exception as exc:
            log.warning("Parse error: %s", exc)
    return records


# ---------------------------------------------------------------------------
# Deep search through JS state for result arrays
# ---------------------------------------------------------------------------

def search_state_for_records(state, doc_type, depth=0):
    """Recursively search the JS app state for result arrays."""
    if depth > 8 or not state:
        return []
    records = []
    if isinstance(state, dict):
        # Check known result keys first
        for key in ["searchResults", "results", "instruments", "documents",
                    "hits", "items", "records", "data"]:
            val = state.get(key)
            if val and isinstance(val, (list, dict)):
                recs = extract_records(val, doc_type)
                if recs:
                    log.info("  [STATE] Found %d records at key '%s'", len(recs), key)
                    records.extend(recs)
        # Recurse into nested objects
        for key, val in state.items():
            if isinstance(val, (dict, list)) and key not in ("router", "assets", "img",
                                                               "cart", "checkout", "configuration"):
                records.extend(search_state_for_records(val, doc_type, depth + 1))
    elif isinstance(state, list) and state:
        # Check if list items look like records
        recs = extract_records(state, doc_type)
        if recs:
            records.extend(recs)
    return records


# ---------------------------------------------------------------------------
# Authenticated Playwright scraper
# ---------------------------------------------------------------------------

class AuthenticatedScraper:
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

    async def _login(self, page, context):
        if not CLERK_EMAIL or not CLERK_PASSWORD:
            log.warning("No credentials. Set CLERK_EMAIL and CLERK_PASSWORD as GitHub Secrets.")
            return False
        try:
            log.info("Logging in as %s...", CLERK_EMAIL[:3] + "***")
            await page.goto(CLERK_BASE + "/signin", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            # Find and fill email field
            email_filled = False
            for sel in ["input[type='email']", "input[name='email']",
                        "#email", "input[autocomplete='email']",
                        "input[placeholder*='email' i]"]:
                try:
                    await page.fill(sel, CLERK_EMAIL, timeout=3000)
                    email_filled = True
                    log.info("Email filled via: %s", sel)
                    break
                except Exception:
                    continue

            if not email_filled:
                log.warning("Could not find email field")
                # Log page content for debugging
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                inputs = soup.find_all("input")
                log.info("Inputs on page: %s", [(i.get("type"), i.get("name"), i.get("id")) for i in inputs])
                return False

            # Fill password
            for sel in ["input[type='password']", "input[name='password']",
                        "#password", "input[autocomplete='current-password']"]:
                try:
                    await page.fill(sel, CLERK_PASSWORD, timeout=3000)
                    log.info("Password filled via: %s", sel)
                    break
                except Exception:
                    continue

            # Submit
            for sel in ["button[type='submit']", "button:has-text('Sign In')",
                        "button:has-text('Log In')", "input[type='submit']"]:
                try:
                    await page.click(sel, timeout=3000)
                    log.info("Submit clicked via: %s", sel)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=20000)
            await asyncio.sleep(3)

            url   = page.url
            title = await page.title()
            cookies = [c["name"] for c in await context.cookies()]
            log.info("Post-login URL: %s", url)
            log.info("Post-login title: %s", title)
            log.info("Post-login cookies: %s", cookies)

            success = "signin" not in url.lower() and "authToken" in str(cookies)
            log.info("Login %s", "SUCCESS" if success else "FAILED - no authToken")
            return success

        except Exception as exc:
            log.warning("Login exception: %s", exc)
            return False

    async def _scrape_doc_type(self, page, doc_type):
        """Scrape one doc type using XHR interception + JS state."""
        collected = []

        async def on_response(response):
            url = response.url
            if "publicsearch.us" not in url:
                return
            if response.status != 200:
                return
            ct = response.headers.get("content-type", "")
            if "json" in ct:
                try:
                    body = await response.json()
                    if body:
                        collected.append((url, body))
                        log.info("  [XHR] %s", url[-80:])
                except Exception:
                    pass

        page.on("response", on_response)
        all_records = []

        for attempt in range(1, MAX_RETRIES + 1):
            collected.clear()
            url = self._url(doc_type, offset=0, limit=200)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)

                # Wait for React to load results - up to 20s
                await asyncio.sleep(5)
                try:
                    await page.wait_for_function(
                        "() => document.title && !document.title.includes('Loading')",
                        timeout=15000
                    )
                except Exception:
                    pass
                await asyncio.sleep(3)

                title = await page.title()
                log.info("  Page title: %s", title)

                # Method 1: XHR interception
                for resp_url, body in collected:
                    recs = extract_records(body, doc_type)
                    if recs:
                        log.info("  [XHR] %d records from JSON", len(recs))
                        all_records.extend(recs)

                # Method 2: Full JS state dump
                if not all_records:
                    try:
                        # Get the complete window.__data state
                        full_state = await page.evaluate("""
                            () => {
                                try {
                                    // Try window.__data (Neumo SPA)
                                    if (window.__data) {
                                        return {source: '__data', data: window.__data};
                                    }
                                    // Try Redux store
                                    if (window.__REDUX_STORE__) {
                                        return {source: 'redux', data: window.__REDUX_STORE__.getState()};
                                    }
                                    // Scan all window keys for anything with results
                                    const found = {};
                                    for (const key of Object.keys(window)) {
                                        try {
                                            const val = window[key];
                                            if (val && typeof val === 'object' && !Array.isArray(val)) {
                                                const str = JSON.stringify(val).slice(0, 100);
                                                if (str.includes('docNum') || str.includes('grantor') ||
                                                    str.includes('instrument') || str.includes('recordedDate')) {
                                                    found[key] = val;
                                                }
                                            }
                                        } catch(e) {}
                                    }
                                    if (Object.keys(found).length) return {source: 'scan', data: found};
                                    return {source: 'none', data: null};
                                } catch(e) {
                                    return {source: 'error', error: e.toString()};
                                }
                            }
                        """)

                        if full_state:
                            source = full_state.get("source", "unknown")
                            data   = full_state.get("data")
                            log.info("  [JS] State source: %s", source)

                            if data and source == "__data":
                                # Log top-level keys to understand structure
                                if isinstance(data, dict):
                                    log.info("  [JS] Top keys: %s", list(data.keys())[:15])
                                    # Save full state for first doc type
                                    if doc_type == "LP":
                                        state_path = Path("data/state_dump.json")
                                        state_path.parent.mkdir(parents=True, exist_ok=True)
                                        state_path.write_text(
                                            json.dumps(data, indent=2, default=str)[:100000],
                                            encoding="utf-8"
                                        )
                                        log.info("  [JS] Saved state dump to data/state_dump.json")

                                recs = search_state_for_records(data, doc_type)
                                if recs:
                                    all_records.extend(recs)

                            elif data and source == "scan":
                                log.info("  [JS] Scan found keys with instrument data: %s",
                                         list(data.keys()))

                    except Exception as exc:
                        log.warning("  [JS] State extraction error: %s", exc)

                # Log XHR summary if still nothing
                if not all_records:
                    log.info("  [SUMMARY] 0 records. XHR calls: %d", len(collected))
                    for rurl, body in collected[:3]:
                        log.info("    XHR: %s keys=%s", rurl[-50:],
                                 list(body.keys())[:5] if isinstance(body, dict) else type(body).__name__)

                # Paginate if found records
                if all_records:
                    offset = len(all_records)
                    while True:
                        collected.clear()
                        await page.goto(self._url(doc_type, offset=offset), wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(8)
                        page_recs = []
                        for _, body in collected:
                            page_recs.extend(extract_records(body, doc_type))
                        if not page_recs:
                            try:
                                state = await page.evaluate("() => window.__data || null")
                                if state:
                                    page_recs = search_state_for_records(state, doc_type)
                            except Exception:
                                pass
                        if not page_recs or len(page_recs) < 5:
                            break
                        all_records.extend(page_recs)
                        offset += len(page_recs)

                break

            except Exception as exc:
                log.warning("  Attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)

        page.remove_listener("response", on_response)
        return all_records

    async def run(self):
        if not PLAYWRIGHT_AVAILABLE:
            return []

        all_records = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox",
                      "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                ignore_https_errors=True,
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            page = await context.new_page()

            logged_in = await self._login(page, context)
            log.info("Authenticated: %s", logged_in)

            for doc_type in TARGET_DOC_TYPES:
                log.info("Searching: %s", doc_type)
                recs = await self._scrape_doc_type(page, doc_type)
                log.info("  Found %d records for %s", len(recs), doc_type)
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
    log.info("Email configured: %s", bool(CLERK_EMAIL))
    log.info("Password configured: %s", bool(CLERK_PASSWORD))
    log.info("=" * 60)

    start_ymd, end_ymd = date_range_yyyymmdd(LOOKBACK_DAYS)
    start_mdy, end_mdy = date_range_mmddyyyy(LOOKBACK_DAYS)
    log.info("Date range: %s to %s", start_mdy, end_mdy)

    scraper     = AuthenticatedScraper(start_ymd, end_ymd)
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

