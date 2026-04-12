"""
Bexar County Motivated Seller Lead Scraper
- Reads results from window.__data['documents'] key
- Logs in with CLERK_EMAIL / CLERK_PASSWORD secrets
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
# Try multiple env var name variations in case of secret naming issues
CLERK_EMAIL    = (os.getenv("CLERK_EMAIL") or os.getenv("clerk_email") or "").strip()
CLERK_PASSWORD = (os.getenv("CLERK_PASSWORD") or os.getenv("clerk_password") or "").strip()
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
# Parse a single document record from the Neumo SPA state
# ---------------------------------------------------------------------------

def parse_doc(src, doc_type):
    """Parse one document from the window.__data['documents'] structure."""
    if not isinstance(src, dict):
        return None
    cat, cat_label = DOC_TYPE_MAP.get(doc_type, (doc_type, doc_type))

    # Neumo stores documents with these field names
    doc_num = clean(
        src.get("docNum") or src.get("instrumentNumber") or
        src.get("docNumber") or src.get("recordingNumber") or
        src.get("instrument") or src.get("id") or ""
    )
    filed = clean(
        src.get("recordedDate") or src.get("filedDate") or
        src.get("dateRecorded") or src.get("recordingDate") or
        src.get("recorded") or ""
    )
    grantor = clean(
        src.get("grantor") or src.get("grantorName") or
        src.get("grantors") or src.get("party1Name") or
        src.get("sellerName") or src.get("owner") or
        src.get("name") or ""
    )
    grantee = clean(
        src.get("grantee") or src.get("granteeName") or
        src.get("grantees") or src.get("party2Name") or ""
    )
    legal  = clean(src.get("legalDescription") or src.get("legal") or src.get("description") or "")
    amount = parse_amount(str(src.get("considerationAmount") or src.get("amount") or "0"))
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


def extract_from_documents_state(state, doc_type):
    """
    Extract records from window.__data['documents'].
    The 'documents' key contains the search results in the Neumo SPA.
    """
    records = []
    if not state or not isinstance(state, dict):
        return records

    # The key insight: results are in state['documents']
    docs_state = state.get("documents", {})
    if not docs_state:
        return records

    log.info("  [DOCS] documents state keys: %s",
             list(docs_state.keys())[:10] if isinstance(docs_state, dict) else type(docs_state).__name__)

    # Try various sub-keys
    items = []
    if isinstance(docs_state, dict):
        items = (
            docs_state.get("results", []) or
            docs_state.get("items", []) or
            docs_state.get("hits", []) or
            docs_state.get("documents", []) or
            docs_state.get("data", []) or
            docs_state.get("list", []) or
            []
        )
        # If docs_state itself is the list structure
        if not items:
            for k, v in docs_state.items():
                if isinstance(v, list) and len(v) > 0:
                    items = v
                    log.info("  [DOCS] Found list at key '%s' len=%d", k, len(v))
                    break
    elif isinstance(docs_state, list):
        items = docs_state

    log.info("  [DOCS] Items found: %d", len(items))

    for item in items:
        try:
            rec = parse_doc(item, doc_type)
            if rec:
                records.append(rec)
        except Exception as exc:
            log.warning("  [DOCS] Parse error: %s", exc)

    return records


def extract_from_xhr(body, doc_type):
    """Extract records from XHR JSON response."""
    if not body or not isinstance(body, (dict, list)):
        return []
    records = []
    items = []
    if isinstance(body, list):
        items = body
    elif isinstance(body, dict):
        items = (
            body.get("hits", {}).get("hits", []) if isinstance(body.get("hits"), dict) else []
        ) or body.get("hits", []) or body.get("results", []) or \
            body.get("documents", []) or body.get("items", []) or \
            body.get("data", []) or []
        if not items and ("docNum" in body or "instrumentNumber" in body):
            items = [body]
    for item in items:
        try:
            rec = parse_doc(item, doc_type)
            if rec:
                records.append(rec)
        except Exception:
            pass
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
            log.warning("Credentials not set. Proceeding unauthenticated.")
            return False
        try:
            log.info("Logging in as %s***", CLERK_EMAIL[:4])
            await page.goto(CLERK_BASE + "/signin", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2)

            for sel in ["input[type='email']", "#email", "input[name='email']",
                        "input[autocomplete='email']", "input[placeholder*='email' i]"]:
                try:
                    await page.fill(sel, CLERK_EMAIL, timeout=3000)
                    log.info("Email filled: %s", sel)
                    break
                except Exception:
                    continue

            for sel in ["input[type='password']", "#password", "input[name='password']"]:
                try:
                    await page.fill(sel, CLERK_PASSWORD, timeout=3000)
                    log.info("Password filled: %s", sel)
                    break
                except Exception:
                    continue

            for sel in ["button[type='submit']", "button:has-text('Sign In')",
                        "button:has-text('Log In')", "input[type='submit']"]:
                try:
                    await page.click(sel, timeout=3000)
                    log.info("Submit: %s", sel)
                    break
                except Exception:
                    continue

            await page.wait_for_load_state("networkidle", timeout=20000)
            await asyncio.sleep(3)

            url     = page.url
            cookies = [c["name"] for c in await context.cookies()]
            log.info("Post-login URL: %s", url)
            log.info("Post-login cookies: %s", cookies)
            success = "signin" not in url.lower() or "authToken" in str(cookies)
            log.info("Login %s", "SUCCESS" if success else "FAILED")
            return success
        except Exception as exc:
            log.warning("Login error: %s", exc)
            return False

    async def _get_state_and_records(self, page, doc_type):
        """Get window.__data and extract records from 'documents' key."""
        # Wait for page to finish loading results
        try:
            await page.wait_for_function(
                """() => {
                    if (!window.__data) return false;
                    const docs = window.__data.documents;
                    if (!docs) return false;
                    // Check if documents state has loaded (not just router state)
                    const keys = Object.keys(docs);
                    return keys.length > 0;
                }""",
                timeout=20000
            )
            log.info("  [WAIT] documents state loaded")
        except Exception:
            log.info("  [WAIT] timeout - checking state anyway")

        try:
            state = await page.evaluate("() => window.__data || null")
        except Exception as exc:
            log.warning("  [JS] evaluate error: %s", exc)
            return []

        if not state:
            log.info("  [JS] window.__data is null")
            return []

        if isinstance(state, dict):
            log.info("  [JS] state keys: %s", list(state.keys())[:15])

            # Save state dump for first LP search (for debugging)
            if doc_type == "LP":
                try:
                    dump_path = Path("data/state_dump.json")
                    dump_path.parent.mkdir(parents=True, exist_ok=True)
                    dump_path.write_text(
                        json.dumps(state, indent=2, default=str)[:200000],
                        encoding="utf-8"
                    )
                    log.info("  [JS] Saved state dump")
                except Exception:
                    pass

        return extract_from_documents_state(state, doc_type)

    async def _scrape_doc_type(self, page, doc_type):
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
                except Exception:
                    pass

        page.on("response", on_response)
        all_records = []

        for attempt in range(1, MAX_RETRIES + 1):
            collected.clear()
            url = self._url(doc_type, offset=0, limit=200)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(3)

                # Method 1: XHR interception
                for resp_url, body in collected:
                    recs = extract_from_xhr(body, doc_type)
                    if recs:
                        log.info("  [XHR] %d records", len(recs))
                        all_records.extend(recs)

                # Method 2: Read from window.__data['documents']
                if not all_records:
                    recs = await self._get_state_and_records(page, doc_type)
                    if recs:
                        log.info("  [STATE] %d records from documents", len(recs))
                        all_records.extend(recs)

                # Paginate
                if all_records:
                    offset = len(all_records)
                    while True:
                        collected.clear()
                        await page.goto(
                            self._url(doc_type, offset=offset),
                            wait_until="domcontentloaded", timeout=30000
                        )
                        await asyncio.sleep(5)
                        page_recs = []
                        for _, body in collected:
                            page_recs.extend(extract_from_xhr(body, doc_type))
                        if not page_recs:
                            page_recs = await self._get_state_and_records(page, doc_type)
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
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
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
                log.info("  Found %d for %s", len(recs), doc_type)
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
    log.info("Email configured: %s (len=%d)", bool(CLERK_EMAIL), len(CLERK_EMAIL))
    log.info("Password configured: %s (len=%d)", bool(CLERK_PASSWORD), len(CLERK_PASSWORD))
    log.info("=" * 60)

    # Log ALL env vars that might be credentials (for debugging)
    for k, v in os.environ.items():
        if "clerk" in k.lower() or "email" in k.lower():
            log.info("ENV: %s = [len=%d]", k, len(v))

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

